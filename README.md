# [tracker](https://tracker.chicagobuss.dev) [![CI](https://github.com/chicagobuss/tracker/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/chicagobuss/tracker/actions/workflows/ci.yml)

Shared documents, conflict-safe writes, a change feed, and a task queue for
coding agents — over native HTTP MCP. Postgres holds the index and
coordination state; content-addressed blobs live in local files or S3. One
static Go binary with a built-in web UI and simple ops for your own network.

[Site](https://tracker.chicagobuss.dev) · [OpenAPI spec](openapi.yaml) ·
[Agent cheatsheet](web/usage.md) (also served at `/llms.txt`)

## Quickstart

Needs Docker Compose, Bash, `curl`, and Python 3. No Go toolchain required — it
pulls a prebuilt image.

```bash
git clone https://github.com/chicagobuss/tracker && cd tracker
cp .env.example .env          # set PGPASSWORD before the first start
docker compose up -d --wait   # Postgres + tracker (--wait blocks until healthy)
scripts/seed.sh               # welcome doc + getting-started folio (optional)
scripts/smoke.sh              # proves create -> lock -> write -> read round-trip
```

This ^ puts tracker on `http://127.0.0.1:8770`: the web UI in a browser, and the
agent markdown cheatsheet via `curl http://127.0.0.1:8770` (or `/llms.txt`).
`make up` runs compose and `smoke.sh` together; `make seed` runs the seed.

- **Use `--wait`**: compose returns as soon as the container exists, while
  tracker is still connecting to Postgres and running migrations.
- **Change `PGPASSWORD` upfront**: the Postgres image only reads it when first
  initializing its volume; changing it later requires updating the DB user too.

### Connect an agent

```bash
claude mcp add --transport http --scope user tracker http://127.0.0.1:8770/mcp \
  --header "X-Actor: claude-code-$(hostname -s)"
```

Cursor (`url`), Gemini CLI (`httpUrl`), and other Streamable HTTP MCP clients
configure the same endpoint and `X-Actor` header. Add
`--header "Authorization: Bearer <token>"` if `API_TOKENS` is set. Point a new
agent at `welcome` if you ran `scripts/seed.sh`.

### Common knobs

Defaults are safe for a first run: loopback-only, `./data/blobs`, auth off. All
knobs live in `.env`:

| Want to… | Set in `.env` |
|---|---|
| Move off port 8770 | `PORT=8771` and `BASE_URL=http://127.0.0.1:8771` |
| Expose to other machines | `BIND_ADDR=<LAN/Tailscale IP>`, `BASE_URL`, `API_TOKENS` |
| Store blobs in S3 / R2 / MinIO | `STORAGE_TYPE=s3` + `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET` |
| Require bearer auth | `API_TOKENS=<token>` (generate with `openssl rand -hex 32`) |
| Require `X-Actor` on a trusted net | `REQUIRE_ACTOR=true` (when `API_TOKENS` is empty) |
| Pin a default workspace | `DEFAULT_WORKSPACE=<name>` (defaults to `default`) |
| Send OpenTelemetry traces | `OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector>:4318` |

## How it works

- **Documents, folios & web UI.** Documents are versioned markdown (or any
  bytes) with UUID/slug addressing, tags, metadata, and attribution. Folios are
  tableless, gist-like collections: a `kind='folio'` doc whose files are docs
  tagged `folio:<slug>` with slug `<folio>/<filename>` (import GitHub gists via
  `scripts/import_gists.py`). The built-in UI supports dark/light themes,
  full-text search, a collapsible sidebar, a **Rendered / Raw** toggle,
  clipboard copy, shareable revision links, and workspace switching.
- **Two-layer write safety (leases + CAS).** A `PUT` write requires both (a) a
  live TTL'd `doc_locks` lease (`X-Lease-Token`) and (b) `If-Match: <version>`
  optimistic concurrency. Crashed agents' leases expire automatically. Over MCP,
  `update_doc` runs acquire → version-check write → release server-side.
  `409 lease_held` and `412 version_conflict` are coordination working, not bugs
  to work around.
- **Workspaces (Postgres RLS).** Hard partitions enforced by PostgreSQL
  row-level security (`app.workspace`), not query `WHERE` clauses — docs,
  folios, tasks, actors, and tags in one workspace are invisible from another.
  `X-Workspace` (or an MCP tool's `workspace` arg) selects a workspace;
  `API_TOKENS=<tok>:<ws>` confines a token so headers/args cannot escape it.
  Unknown workspace names return `404` rather than silently creating an empty
  store.
- **Task queue.** Enqueue work with `POST /tasks`; workers claim atomically via
  `FOR UPDATE SKIP LOCKED` (FIFO or by ID). Claims expire after `TASK_CLAIM_TTL`
  (default `1h`) so a dead worker's task comes back, and only the current
  claimant can complete it.
- **Change feed & SSE.** Every write appends a monotonic event. Agents resume
  via `GET /changes?since=<cursor>` (`list_changes` over MCP) or stream live via
  `GET /changes/stream` (SSE) instead of polling. Cursors are opaque and bound
  to `(workspace, kind)`.
- **Content-addressed blobs.** Bytes live under `sha256/<hash>` in local files
  (`STORAGE_TYPE=file`) or S3 (`STORAGE_TYPE=s3`). Reads return short-lived
  signed `BASE_URL/blobs/...` URLs (streamed through tracker when using S3), so
  buckets stay private. Note: `/blobs/<sha256>` is addressed by content hash
  rather than workspace; keep `API_TOKENS` set if revision listings or hash
  links should stay gated.

## Agents: MCP + skill

tracker serves **Streamable HTTP MCP** natively at `POST /mcp` (stateless,
tools-only; `GET /mcp` returns `405`). The tools ship inside the binary, so
clients never drift out of sync with the server.

All **26 MCP tools**:
- **Docs:** `list_docs`, `get_doc`, `get_raw`, `create_doc`, `update_doc`,
  `lock_status`, `retag_doc`, `soft_delete_doc`, `restore_doc`,
  `hard_delete_doc`, `list_tags`
- **Folios:** `list_folios`, `create_folio`, `get_folio`, `get_folio_file`,
  `add_folio_file`
- **Tasks:** `list_tasks`, `get_task`, `enqueue_task`, `claim_task`,
  `complete_task`
- **Workspaces:** `list_workspaces`, `create_workspace`
- **Actors & changes:** `list_actors`, `actor_activity`, `list_changes`

Every scoped tool accepts an optional per-call `workspace` argument (refused on
confined tokens; ignored by `list_workspaces` / `create_workspace`).
[`skills/tracker/SKILL.md`](skills/tracker/SKILL.md) is the matching agent skill
covering when and how to consult tracker.

## Identity, auth & going multi-machine

Keep **attribution** separate from **access control**:

- **`X-Actor`** is *who did it* — required on every mutating call and recorded as
  author, lease owner, and task claimant. It is self-asserted attribution
  between cooperating agents, not authentication.
- **`API_TOKENS`** is *who gets in* — comma-separated bearer tokens (`empty` =
  auth off). Append `:<workspace>` (`tok-a:personal,tok-b:work`) to confine a
  token to one workspace. Unauthenticated: `/healthz`, `/version`,
  `/openapi.yaml`, `/llms.txt`, `/usage.md`, static UI assets, and HMAC-signed
  `/blobs/...` links. `/metrics` requires a token when `API_TOKENS` is set.
- **`REQUIRE_ACTOR=true`** (when `API_TOKENS` is empty) requires a non-blank
  `X-Actor` on every API/MCP request — useful on a trusted private overlay where
  reachability is the security boundary. Health checks, static assets, and
  signed blob URLs remain exempt.

To expose tracker across a LAN, Tailscale, or ZeroTier network, bind that
interface in `.env` and enable `API_TOKENS`:

```bash
# .env
BIND_ADDR=100.x.y.z
BASE_URL=http://100.x.y.z:8770
API_TOKENS=<output of: openssl rand -hex 32>
```

```bash
docker compose up -d --wait
```

`scripts/smoke.sh` and `scripts/seed.sh` read `API_TOKENS` from `.env`
automatically.

## API overview

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` · `/version` · `/openapi.yaml` · `/llms.txt` | health (pings Postgres), version, OpenAPI spec, agent cheatsheet |
| `POST` | `/mcp` | native Streamable HTTP MCP endpoint |
| `POST · GET` | `/docs` | create (`content` seeds v1); list/search (`?q=&mode=&kind=&tag=&deleted=&view=&limit=&offset=`) |
| `GET · PUT · PATCH · DELETE` | `/docs/{id}` | read; write bytes (`X-Lease-Token` + `If-Match`); relabel; hard-delete (`confirm` = exact slug) |
| `POST` | `/docs/{id}/soft-delete` · `/docs/{id}/restore` | recoverable delete (`cascade=true` for non-empty folios) · restore |
| `GET` | `/docs/{id}/raw` · `/docs/{id}/revisions[/{v}/raw]` | content bytes · revision history |
| `POST · GET · DELETE` | `/docs/{id}/lock` | acquire/renew (`409` if held) · status · release |
| `GET` | `/tags` | tag vocabulary with counts |
| `GET · POST` | `/folios` · `/folios/{slug}` · `/folios/{slug}/files[/{filename}[/raw]]` | collections + member files |
| `POST · GET` | `/tasks[/{id}]` · `/tasks/claim` · `/tasks/{id}/claim` · `/tasks/{id}/complete` | task queue: enqueue, list (`?status=`), FIFO or ID claim, claimant-only complete |
| `GET · POST` | `/workspaces` | list or register workspaces |
| `GET` | `/actors` · `/actors/{name}/activity` | actor registry + recent writes |
| `GET` | `/changes` · `/changes/stream` | cursor-paged change feed (`?since=&kind=&limit=`) · SSE stream |
| `GET · POST` | `/admin/compaction` · `/admin/compaction/{run\|pause\|resume\|stop}` | blob compaction state & control (requires unconfined token if auth is on) |
| `GET` | `/metrics` | Prometheus exposition (`tracker_blob_reads_total`, `tracker_compaction_*`) |

The authoritative reference is [`openapi.yaml`](openapi.yaml) (served live at
`/openapi.yaml`).

- **Envelopes & views:** Single resources wrap under type (`{"document":…}`,
  `{"folio":…}`, `{"task":…}`, `{"lock":…}`); lists return
  `count/total/limit/offset`; errors return `{"error":{"code","message",…}}`.
  `view=summary` is the REST default; `view=table` (`{cols,rows}`) is the MCP
  default; `view=full` returns whole objects.
- **Multi-segment folio slugs:** `{id}` accepts UUIDs or slugs including
  `myfolio/file.md`. A file literally named `raw`, `lock`, `soft-delete`, or
  `restore` collides with suffix routes — address it via
  `/folios/{slug}/files/{filename}` or UUID.
- **Soft vs. hard delete:** Prefer `POST /docs/{id}/soft-delete` (excluded from
  default search, revision history preserved, restorable). `DELETE /docs/{id}`
  is permanent and always requires `confirm` equal to the exact slug; non-empty
  folios require `cascade=true` on either delete.

## Operations & internals

<details>
<summary><strong>Deploying, updating &amp; local development</strong></summary>

```bash
make update TAG=v1.7.2   # standard deploy: pin TRACKER_IMAGE, pull, restart, smoke-test
make status              # pinned TRACKER_IMAGE vs live /version
make up                  # start stack from .env + smoke-test
make down                # stop stack (pgdata volume survives)
make logs                # tail tracker container logs
make smoke               # run scripts/smoke.sh against running instance
make deploy              # dev: build image from working tree and restart
make test                # run suite against throwaway pgvector container (needs Go)
make test-docker         # same, running Go in a container too (Docker only)
make build               # compile local ./tracker binary with version stamped
```

**Releases vs. source builds:** `make update TAG=vX.Y.Z` pins `TRACKER_IMAGE` in
`.env`, pulls the CI-built image, restarts, smoke-tests, and prints `/version`.
Rolling back is the same command with an earlier tag. Hosts building from a
working tree (`make deploy` / `docker compose -f docker-compose.yml -f
compose.build.yml up -d --build`) override `TRACKER_IMAGE` with
`tracker:local`; `make update` refuses when `COMPOSE_FILE` includes
`compose.build.yml` rather than silently pinning an ignored tag.

The binary embeds `git describe --tags --always --dirty`, logged at startup,
served at `/version`, and written to each backup's `manifest.json`. Running
directly without containers:
`make build && set -a && . ./.env && set +a && ./tracker`.

**Tests:** Lease/CAS and `SKIP LOCKED` semantics live in Postgres, so DB-backed
tests require `TEST_DATABASE_URL` (`make test` / `make test-docker` manage a
scratch `pgvector` container automatically) and skip cleanly when unset so bare
`go test ./...` stays green.

</details>

<details>
<summary><strong>Multi-interface host networking (Linux)</strong></summary>

To bind several interfaces at once (e.g. loopback + LAN + Tailscale/ZeroTier),
use [`compose.host.yml`](compose.host.yml) and list comma-separated addresses in
`LISTEN_ADDR`:

```bash
# .env
LISTEN_ADDR=127.0.0.1:8770,192.168.1.100:8770,10.10.10.10:8770
```

```bash
docker compose -f docker-compose.yml -f compose.host.yml up -d
```

This is Linux-only (`network_mode: host` is a no-op on Docker Desktop for
macOS/Windows). Verify with `scripts/smoke.sh`, since the default loopback
healthcheck is disabled in host mode.

</details>

<details>
<summary><strong>S3 / R2 / MinIO blobs &amp; switching storage backends</strong></summary>

Set `STORAGE_TYPE=s3` and the four `S3_*` variables before starting:

```bash
# .env (Cloudflare R2 example)
STORAGE_TYPE=s3
S3_ENDPOINT=<account-id>.r2.cloudflarestorage.com
S3_ACCESS_KEY=<r2 access key id>
S3_SECRET_KEY=<r2 secret access key>
S3_BUCKET=tracker-blobs
S3_USE_SSL=true
```

For AWS S3 use `S3_ENDPOINT=s3.<region>.amazonaws.com` (`S3_USE_SSL=true`); for
MinIO / RustFS use `S3_ENDPOINT=host:9000` (`S3_USE_SSL=false` over plain HTTP
on a private net). tracker auto-creates the bucket if credentials permit, and
refuses to start if any of the four `S3_*` variables are missing.

To move an existing store between local files and S3, use `migrate-blobs` — it
verifies every blob's sha256, is non-destructive and idempotent, and prints the
`.env` cutover step:

```bash
tracker migrate-blobs --to file --blob-dir ./data/blobs   # S3 -> local files
tracker migrate-blobs --to s3                             # local files -> S3
# flags: --dry-run (check + count only), --verify (re-read from destination)
```

</details>

<details>
<summary><strong>Backup &amp; restore</strong></summary>

State lives in Postgres (index) and blobs (content). `scripts/backup.sh` dumps
Postgres **first** (writes are blob-first, so every referenced key already
exists), then folds the content store into shared content-addressed pack files
(`O(new blobs)`, stored once rather than `BACKUP_KEEP` times). Each tarball
(`db.dump` + `keys.txt` + `manifest.json`) depends on those packs — **a snapshot
without the packs is not a full restore**.

```bash
scripts/backup.sh                 # -> ./backups/tracker-backup-<ts>.tar.gz (+ packs/)
scripts/backup.sh --upload        # also push packs + archive to BACKUP_S3_*
scripts/backup.sh --if-changed    # skip when content tables haven't changed (for cron)

# Example cron (hourly, upload only on change; BACKUP_KEEP defaults to 48 local tarballs):
# 7 * * * * /path/to/tracker/scripts/backup.sh --if-changed --upload >> /path/to/tracker/backups/backup.log 2>&1

scripts/restore.sh ./backups/tracker-backup-<ts>.tar.gz
scripts/restore.sh --from-s3 tracker-backup-<ts>.tar.gz
docker compose up -d tracker
```

On a **fresh machine**: clone the repo, create `.env`, run
`docker compose up -d postgres`, run `scripts/restore.sh`, then
`docker compose up -d tracker`. `scripts/restore.sh` derives required keys from
the restored database itself and pulls them via `scripts/s3util.py` /
`scripts/blobpack.py`.

</details>

<details>
<summary><strong>Blob compaction</strong></summary>

Because every revision stores its own blob, small repeated edits waste space
(on one real store, 44.7 MB of content across 15.7k files took 116 MB on ZFS).
With compaction enabled, tracker packs cold blobs in the background through the
configured backend:

- Stores each blob as the smallest of: raw bytes, zstd with a store-trained
  dictionary, or zstd delta against the previous revision (chains capped at 16,
  like keyframes). On the store above, packing cut disk use to ~12.8 MB (3.5x)
  in a few hundred packs.
- Packs are content-addressed (`packs/<sha256>`), decoded and verified before
  referencing. Reads check packs first, verify the sha256, and fall back to loose
  copies.
- Loose copies are only deleted after `COMPACT_GRACE`, and only after re-verifying
  that the packed copy decodes cleanly.

```bash
# .env
COMPACTION=manual        # off (default) | manual | auto
COMPACT_AFTER=1h         # pack blobs older than this
COMPACT_GRACE=24h        # delete loose copy after this grace period
COMPACT_BATCH=200        # blobs per step
```

In `manual` mode, grant a packing budget explicitly via `/admin/compaction/*`
(requires an unconfined token if `API_TOKENS` is set):

```bash
curl -s $T/admin/compaction                           # state, backlog, ratio, last step
curl -s -X POST "$T/admin/compaction/run?blobs=500"   # manual: pack up to 500 more blobs
curl -s -X POST $T/admin/compaction/pause             # pause packing + reclaim (persisted)
curl -s -X POST $T/admin/compaction/resume
curl -s -X POST $T/admin/compaction/stop              # drop remaining manual budget
```

`GET /metrics` exposes Prometheus gauges and counters (`tracker_compaction_*`,
`tracker_blob_reads_total{source=loose|pack|fallback}`).

</details>

<details>
<summary><strong>OpenTelemetry tracing</strong></summary>

Tracing is off until `OTEL_EXPORTER_OTLP_ENDPOINT` (or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) is set; then tracker exports spans over
OTLP/HTTP.

- HTTP spans are named by matched route (`GET /docs/{id}`, not the raw path) and
  carry `tracker.workspace` and `tracker.actor`.
- MCP calls add `mcp.tool` and set `resource.name` to `POST /mcp <tool>` so
  Datadog and similar backends group each tool as its own endpoint.
- Postgres queries emit child spans with SQL text (never arguments).
- `/healthz` and `/version` are excluded.
- `OTEL_SERVICE_NAME` defaults to `tracker`; standard `OTEL_*` variables
  (`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_HEADERS`,
  `OTEL_TRACES_SAMPLER`, …) work as usual.

</details>

<details>
<summary><strong>Releasing (maintainers)</strong></summary>

Push an annotated `v*` tag to trigger the GitHub Actions pipeline (`test` →
`image` → `release`):

```bash
git tag -a v1.7.2 -m "v1.7.2 — Short title

What changed and why."
git push origin v1.7.2
```

- `test` runs `gofmt`, `go vet`, `go build`, and `go test -race` against a real
  `pgvector:pg17` service (Forgejo CI mirrors `test` and builds local runner
  images on pushes to `main` and PRs).
- `image` builds and pushes `ghcr.io/chicagobuss/tracker:vX.Y.Z` (and
  `sha-<short>`). `latest` tracks `main`, not tags — pin `vX.Y.Z` for stable
  deploys.
- `release` turns the tag annotation into the GitHub Release (first line = title,
  body = notes) only after `image` succeeds; re-running is safe if the release
  already exists. Use `-a` (lightweight tags fall back to auto-generated PR
  notes).

</details>

## What it isn't

It's a personal tool that got useful and runs daily, and it's honest about that.
There's no clustering, no multi-tenant SaaS, and no hosted version — it expects a
private network (LAN, Tailscale, ZeroTier) and the backup scripts in the repo.

MIT licensed. Issues and PRs welcome, though I can't promise how fast I'll get
to them.
