# tracker

Shared documents, conflict-safe editing, and a task queue for coding agents —
all over MCP. Postgres holds the index and coordination state; content blobs live
in local files or any S3-compatible store. Tracker is a small static Go binary
that agents can reach on a LAN, Tailscale, or ZeroTier network.

## Quickstart

Needs Docker Compose, Bash, curl, and Python 3. No Go toolchain or build is
needed — it pulls a prebuilt image.

```bash
git clone https://github.com/chicagobuss/tracker && cd tracker
cp .env.example .env          # runs as-is; set PGPASSWORD before the first start
docker compose up -d --wait   # Postgres + tracker (--wait blocks until it's serving)
scripts/seed.sh               # a welcome doc + example folio, so it isn't empty
scripts/smoke.sh              # health + a full create -> lock -> write -> read round-trip
```

That's a working tracker on `http://127.0.0.1:8770` — web UI in a browser,
markdown index for agents (`curl http://127.0.0.1:8770`). `make up` does the same
and runs the smoke test for you; `make seed` seeds it.

Use `--wait`: without it, compose returns as soon as the *container* exists, while
tracker is still connecting to Postgres and running migrations — so anything you
run immediately after (a smoke test, an agent, CI) can race the boot.

The Postgres image only uses `PGPASSWORD` while it first initializes its data
volume. If you change it later, rotate the database password too; changing only
`.env` will stop tracker from connecting.

The seeded `welcome` document is written **for an agent to read**: it explains what
tracker is, the document/folio/lease/actor model, and how to behave. Point a new
agent at the instance and tell it to read `welcome` — it can bootstrap itself from
there. Delete the seed docs once you have real content.

### Connect an agent

```bash
claude mcp add --transport http --scope user tracker http://127.0.0.1:8770/mcp \
  --header "X-Actor: claude-code-<host>"
```

Start a new Claude session and ask it to read `welcome`. If you enabled auth, add
`--header "Authorization: Bearer <token>"` to the command above.

Already using port 8770? Compose will fail with `address already in use`. Set two
lines in `.env` and re-run `docker compose up -d`:

```bash
PORT=8771
BASE_URL=http://127.0.0.1:8771
```

Use `http://127.0.0.1:8771/mcp` in the agent-registration command above.

Defaults are deliberately safe for a first run: loopback-only, blobs in
`./data/blobs`, auth off. Before you expose it to other machines, read
[Going multi-machine](#going-multi-machine) — it is one setting plus a token.

Common knobs, all in `.env`:

| Want to… | Set |
|---|---|
| Use a different port (8770 taken) | `PORT=8771` and `BASE_URL=http://127.0.0.1:8771` |
| Let other machines reach it | `BIND_ADDR=<your LAN/Tailscale IP>` |
| Keep blobs in S3/R2/MinIO instead of files | `STORAGE_TYPE=s3` + the four required `S3_*` vars |
| Gate access behind a token | Set `API_TOKENS` to a token generated with `openssl rand -hex 32` |

### Identity vs. access

Two different things, easy to conflate:

- **`X-Actor`** is *who did it*. Required on every write, recorded as the document
  author, lease owner, and task claimant. It is **self-asserted** — tracker takes
  your word for it. That's the intended design on a trusted network: attribution
  between cooperating agents, not a security boundary.
- **`API_TOKENS`** is *whether you may talk to the server at all*. It is **empty by
  default, meaning no auth** — anyone who can reach the port can read and write.

So `X-Actor` is not a login, and running without `API_TOKENS` is fine on loopback
or a private overlay network — but it is the only thing standing between an open
port and an unauthenticated write API. Turn it on before you expose tracker
anywhere you don't fully trust.

## Turning auth on

Generate a token in your shell, then paste its literal output into `.env` (do
not put the `$(...)` command substitution in the file):

```bash
openssl rand -hex 32
# .env
API_TOKENS=<paste-the-output-here>     # comma-separate to issue several
```

```bash
docker compose up -d --wait            # picks up the new .env
```

Every request except `/healthz` now needs the token, or it gets a `401`:

```bash
curl http://127.0.0.1:8770/docs                                  # 401
curl -H "Authorization: Bearer <token>" http://127.0.0.1:8770/docs   # 200
```

Point agents at it by adding one header to the MCP registration:

```bash
claude mcp add --transport http --scope user tracker http://127.0.0.1:8770/mcp \
  --header "X-Actor: claude-code" \
  --header "Authorization: Bearer <token>"
```

`scripts/smoke.sh` and `scripts/seed.sh` read `API_TOKENS` from `.env` themselves,
so they keep working with no extra flags. Issue a token per agent
(`API_TOKENS=tok-laptop,tok-ci,tok-server`) if you want to be able to revoke one
without rotating the rest — but note that a token only grants *access*; it does not
yet pin which `X-Actor` a caller may claim (see the backlog).

## Quickstart variant: blobs in S3 / R2 / MinIO

The default keeps blobs in `./data/blobs`, which needs no extra infrastructure. To
put them in object storage instead, set `STORAGE_TYPE=s3` and the four `S3_*` vars
before the first `docker compose up`. Everything else is identical — Postgres still
holds the index, and it only ever stores the `sha256/<hash>` key, never the
backend location.

**Cloudflare R2** (endpoint is your account's, TLS on, no region):

```bash
# .env
STORAGE_TYPE=s3
S3_ENDPOINT=<account-id>.r2.cloudflarestorage.com
S3_ACCESS_KEY=<r2 access key id>
S3_SECRET_KEY=<r2 secret access key>
S3_BUCKET=tracker-blobs
S3_USE_SSL=true
```

**AWS S3:** `S3_ENDPOINT=s3.<region>.amazonaws.com`, `S3_USE_SSL=true`.
**MinIO / RustFS / anything S3-compatible:** `S3_ENDPOINT=host:9000`, and
`S3_USE_SSL=false` if it's plain HTTP on a private network.

```bash
docker compose up -d --wait
scripts/smoke.sh                 # writes a doc, so it proves the bucket works
```

tracker creates the bucket on startup if it doesn't exist and your credentials
allow it; if they don't, create it first and it will just use it. All four vars are
required — miss one and tracker refuses to start, naming it, rather than silently
falling back to local files.

Already running on local files? Don't hand-copy anything — `tracker migrate-blobs`
does a verified, non-destructive copy and prints the cutover step. See
[Switching storage backend](#switching-storage-backend).

## Agents: MCP + skill

tracker speaks MCP natively — the server exposes a **Streamable HTTP** MCP
endpoint at `/mcp`, so any agent connects with one line of config and zero local
code (like Notion's remote MCP server). Add
`--header "Authorization: Bearer <token>"` if you set `API_TOKENS`.

`X-Actor` is the agent's identity, stamped on every write. Cursor, Gemini CLI,
and anything else that speaks HTTP MCP configures the same way — the tools live
in the tracker binary, versioned and deployed with it, so clients can never drift
out of sync.

Tools: `list_docs` (incl. `deleted=exclude|only|include`), `get_doc`, `get_raw`,
`create_doc`, `update_doc` (lease + version-check + release, for you),
`lock_status`, `retag_doc` (tags/metadata without a content rewrite),
`soft_delete_doc` / `restore_doc` / `hard_delete_doc` (hard delete requires
`confirm` equal to the slug), `list_tags`, `list_folios`, `create_folio`,
`get_folio`, `get_folio_file`, `add_folio_file`, the task-queue tools
(`list_tasks`, `get_task`, `enqueue_task`, `claim_task`, `complete_task`),
`list_actors`, and `actor_activity`.

`skills/tracker/SKILL.md` is the matching Claude Code skill (copy to
`~/.claude/skills/tracker/`, then set the base URL at the top) describing
when/how to consult tracker.

The old per-machine stdio bridge (`mcp/tracker_mcp.py`) has been removed — `/mcp`
replaces it, and a second copy of the tool surface is exactly the drift the native
endpoint exists to prevent. It's in git history if you need it.

## Design

- **Leases, not advisory locks.** A `doc_locks` row with a TTL + heartbeat
  answers "who is writing this right now". A crashed agent's lease auto-expires,
  so it can never block a doc forever.
- **Two-layer write safety.** A write requires (a) a live lease the caller holds
  (`X-Lease-Token`) and (b) `If-Match: <version>` optimistic concurrency, so a
  stale or lease-less write can't clobber.
- **Content-addressed blobs.** Bytes are stored under `sha256/<hash>`
  (immutable, deduped) in either a local directory (`STORAGE_TYPE=file`) or an S3
  bucket (`STORAGE_TYPE=s3`). Agents fetch them via a presigned or signed local URL.
- **Task queue.** `tasks` with `FOR UPDATE SKIP LOCKED` claiming — no two agents
  grab the same task.

## Going multi-machine

The default compose is loopback-only on a private bridge network, which works
the same on Linux, macOS, and Windows. To let agents on other machines reach
tracker, publish the port on a trusted interface and turn auth on:

```bash
# .env
BIND_ADDR=100.x.y.z                  # your LAN / Tailscale / ZeroTier IP
BASE_URL=http://100.x.y.z:8770       # what agents will use to reach it
API_TOKENS=<paste-a-generated-token> # never expose tracker without this
```

To bind **several** interfaces at once (loopback + LAN + Tailscale), use Linux
host networking, where tracker binds each address in `LISTEN_ADDR` itself:

```bash
# .env: LISTEN_ADDR=127.0.0.1:8770,192.168.1.100:8770,10.10.10.10:8770
docker compose -f docker-compose.yml -f compose.host.yml up -d
```

Host networking is Linux-only — `network_mode: host` is a no-op on Docker
Desktop, so on macOS/Windows stay with the default compose.

**`X-Actor` is self-asserted attribution, not authenticated identity.** On a
trusted network that's the point. Set `API_TOKENS` if you need writes to be
gated; bind actor→token if you need attribution to be tamper-proof (see the
backlog).

## Ops

```bash
make up                                # start (creates .env if missing) + smoke test
make down                              # stop; data survives in the pgdata volume
make logs                              # follow tracker logs
make smoke                             # prove a running instance round-trips a write
make deploy                            # rebuild from source w/ version + restart
curl http://127.0.0.1:8770/version     # version the running binary reports
```

`make up` pulls the published image. Contributors who want their working tree
built instead use the build override (that's what `make deploy` does):

```bash
docker compose -f docker-compose.yml -f compose.build.yml up -d --build
```

The version is `git describe --tags --always --dirty` — logged at startup, served
at `/version`, and recorded in each backup's `manifest.json`.

Running the binary directly, no container: `make build && set -a && . ./.env &&
set +a && ./tracker` (needs Go and a reachable Postgres).

## Tests

```bash
make test          # throwaway Postgres + go test (needs a local Go toolchain)
make test-docker   # same, but Go runs in a container too (needs only Docker)
```

Both start a scratch pgvector container, run the suite against it, and tear it
down. The lease/CAS state machine and `SKIP LOCKED` task claiming are tested
against a real Postgres, because that's where those semantics actually live — the
tests skip (rather than fail) if `TEST_DATABASE_URL` isn't set, so a bare
`go test ./...` stays green without a database.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` · `/version` · `/openapi.yaml` · `/llms.txt` | health (checks Postgres), version, spec, agent index |
| POST | `/mcp` | native MCP endpoint (Streamable HTTP, tools-only) |
| POST · GET | `/docs` | create (`content` seeds v1); list/search (`?q=&mode=&kind=&tag=&deleted=&view=&limit=&offset=`) |
| GET · PUT · PATCH · DELETE | `/docs/{id}` | read; write content (lease + `If-Match`); relabel; **hard-delete** (requires `confirm` = slug) |
| POST | `/docs/{id}/soft-delete` · `/docs/{id}/restore` | soft-delete (recoverable; optional `cascade` for folios) · restore |
| GET | `/docs/{id}/raw` · `/docs/{id}/revisions[/{v}/raw]` | content bytes; version history |
| POST · GET · DELETE | `/docs/{id}/lock` | acquire/renew (`409` if held) · status · release |
| GET | `/tags` | tag vocabulary with counts |
| GET · POST | `/folios` · `/folios/{slug}` · `/folios/{slug}/files[/{filename}[/raw]]` | collections + their files |
| POST · GET | `/tasks[/{id}]` · `/tasks/claim` · `/tasks/{id}/complete` | task queue: enqueue, list (`?status=`), claim (TTL'd; expired claims re-claimable), complete (claimant-only) |
| GET | `/actors` · `/actors/{name}/activity` | entity registry + activity |

The **authoritative reference is `openapi.yaml`** (served live at `/openapi.yaml`).

**Conventions.** Every response is wrapped — a single resource under its type
(`{"document":…}`, `{"folio":…}`, …), lists as `{"<type>s":[…],"count","total",…}`,
errors as `{"error":{"code","message",…}}` with machine codes. Lists default to a
trimmed `view=summary`; `view=table` is a compact columnar `{cols,rows}`,
`view=full` whole objects. Search is `websearch_to_tsquery` (`mode=web` default;
`mode=plain` for strict AND).

### Acting entity

Every **mutating** request must send an `X-Actor: <name>` header naming the
entity performing it (missing → `400`). That value is stamped into
`created_by`/`updated_by`, the revision `author`, the lease `owner`, and task
`claimed_by`, and upserted into the `actors` registry. A write must come from the
entity that **holds the lease** (actor ≠ lease owner → `423`).

### Folios

A **folio** is a little collection of related documents (think: a GitHub gist).
It's modelled tableless: the folio is itself a document with `kind='folio'` whose
`metadata` holds `{description, public, github_id, ...}`; its files are documents
tagged `folio:<slug>` with slug `<folio-slug>/<filename>`. So a folio file
inherits everything (versioning, leases, attribution, search). Create one with
`POST /folios` and add files with `POST /folios/{slug}/files`; import your recent
gists with `scripts/import_gists.py`.

`{id}` accepts a UUID or a slug — including multi-segment folio slugs like
`myfolio/file.md`, for reads, writes, relabels, delete, `/raw`, and `/lock` alike
(an exact slug always wins over the `/raw`/`/lock`/`/soft-delete`/`/restore`
suffix). The one quirk: a file literally *named* `raw`, `lock`, `soft-delete`, or
`restore` collides with those suffix routes — address those via
`/folios/{slug}/files/{filename}` or the UUID.

### Soft-delete vs hard-delete

- **Soft-delete** (`POST /docs/{id}/soft-delete`, MCP `soft_delete_doc`) sets
  `deleted_at`/`deleted_by`. The row and revision history stay; default search
  (`deleted=exclude`) hides it; `deleted=only|include` finds it; `get_doc` by
  id/slug still works; `restore_doc` brings it back. Prefer this.
- **Hard-delete** (`DELETE /docs/{id}`, MCP `hard_delete_doc`) removes the row
  (revisions cascade; blobs left for GC). It **requires** `confirm` equal to the
  document's exact slug — MCP marks `confirm` required in the tool schema so
  agents cannot call it without an explicit matching value. Folios with files
  need `cascade=true`.

## Backup & restore

State lives in two places that must be captured together: Postgres (the index)
and the blobs (the content). One self-contained tarball holds both — `db.dump` +
`blobs/` + `manifest.json`. That tarball is the portable unit; "R2 vs S3 vs a
local directory" is just where you keep it.

```bash
scripts/backup.sh                 # -> ./backups/tracker-backup-<ts>.tar.gz
scripts/backup.sh --upload        # also push to R2/S3 (set BACKUP_S3_* in .env)

scripts/restore.sh ./backups/tracker-backup-<ts>.tar.gz   # from a local file
scripts/restore.sh --from-s3 tracker-backup-<ts>.tar.gz   # pull from R2/S3 first
docker compose up -d tracker                              # then start the service
```

The backup dumps Postgres **first**, then copies blobs — and since writes are
blob-first, every `content_key` in the dump is guaranteed to have its blob, so the
tarball is always internally consistent. Restore is verified round-trip: restoring
into a scratch DB+bucket reproduces the exact doc/blob counts and a tracker booted
against it serves the content. `scripts/s3util.py` moves blobs and tarballs to any
S3-compatible store (RustFS, AWS S3, Cloudflare R2).

To restore on a **fresh machine**: clone the repo, create `.env`, `docker compose
up -d postgres`, run `restore.sh`, then `docker compose up -d tracker`.

## Switching storage backend

Blobs are content-addressed and Postgres stores only the `sha256/<hash>` key —
never the backend location — so switching between local files and S3 is just a
blob copy plus a config flip. The `migrate-blobs` subcommand does the copy:

```bash
tracker migrate-blobs --to file --blob-dir ./data/blobs   # S3 -> local files
tracker migrate-blobs --to s3                             # local files -> S3
#   --dry-run   hash-check + count, write nothing
#   --verify    also re-read each blob from the destination
```

It reads every referenced blob from the current backend (`STORAGE_TYPE`), verifies
each against its hash, and writes it to the destination. It is **non-destructive**
(the source is left intact) and **idempotent**. On success it prints the cutover
step — set `STORAGE_TYPE` (and `BLOB_DIR` for file) in `.env` and restart — so the
switch is deliberate and reversible.

## Status & roadmap

Running in "production" (lol) and used heavily for months. Recently landed: CI
(GitHub Actions → GHCR images), the native `/mcp` endpoint (replacing the
per-machine client script), task-queue visibility + claim expiry + claim-by-id, a
real `/healthz`, soft/hard delete, and a test suite covering the lease/CAS and
task-claim state machines.

**Want to hack on it?** The authoritative backlog is tracker's own task queue —
`GET /tasks?status=open` (each payload carries details, priorities, and
`file:line` pointers).

### Hardening
- **Request logging / metrics** — only startup log lines exist today; add
  access-log middleware (method, path, status, duration, actor), maybe counters
  for writes/lease conflicts.
- **Auth hardening** — constant-time token compare; implement the actor↔token
  binding the docs hint at (a token pins which `X-Actor` it may assert).
- **Migration version tracking** — every `migrations/*.sql` re-runs on each boot
  and relies on idempotency; a `schema_migrations` table makes the first
  non-idempotent migration safe.
- **Write pre-check** — validate lease/version *before* the blob upload in
  `WriteContent`, so rejected writes (412/423) stop minting orphan blobs.
- **Orphan-blob / expired-lease GC** — refcount sweep of unreferenced blobs
  (pairs with the pre-check) and cleanup of dead `doc_locks` rows.
- **Handler-level tests** — the store is covered; the HTTP/MCP layer isn't.

### Features
- **Folio pagination** — folio listings hardcode `limit 500` with no paging.
- **Web UI tasks panel** — browse the queue via the `GET /tasks` endpoints
  (status filter, payload/result detail).
- **pgvector semantic search** — the `embedding` column already exists; populate
  on write, add a semantic query path.

### Ops
- **Scheduled off-box backups** — cron `scripts/backup.sh --upload` to R2/S3 plus
  retention; the backup/restore scripts are already round-trip verified.
- **Public sandbox instance** — a rate-limited, internet-facing demo: Caddy in
  front (per-IP limits, body caps), bridge networking with only the proxy exposed,
  a seed-restore reset every ~6h, and app-side quotas (`MAX_DOCS` /
  `MAX_TOTAL_BLOB_BYTES`) so one actor can't fill the disk between resets. The
  native `/mcp` endpoint then gives visitors one-command agent onboarding.

Longer-horizon ideas (versioning growth levers — blob compression, retention
thinning, content-defined chunking) are deliberately deferred until metrics
justify them; see the `tracker` folio's `expansion-ideas.md` in the store.

PRs welcome but I can't promise I'll get to them!
