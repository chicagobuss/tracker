# tracker

A tiny coordination store for a fleet of coding agents. Postgres holds the
index + lease/coordination state; content blobs live in local files or RustFS (S3).
Single static Go binary, low footprint, reachable by agents over the network
(e.g. ZeroTier).

## Agents: MCP server + skill

`mcp/tracker_mcp.py` is an MCP server (a self-contained `uv` script — no install)
that exposes tracker to any coding agent: `search_docs`, `list_tags`,
`list_folios`, `get_folio`, `read_doc`, `who_is_editing`, `create_doc`,
`create_folio`, `update_doc` (lease + version-check + release, for you), `retag`
(tags/metadata without a content rewrite), `list_actors`, and the task tools.
Configure per agent via env:
`TRACKER_URL`, `TRACKER_ACTOR` (the agent's identity, stamped on writes),
`TRACKER_TOKEN` (only if `API_TOKENS` is set).

Register with Claude Code:

```bash
claude mcp add tracker --scope user \
  --env TRACKER_URL=http://127.0.0.1:8080 --env TRACKER_ACTOR=claude-code \
  -- uv run --quiet --script /path/to/tracker/mcp/tracker_mcp.py
```

`skills/tracker/SKILL.md` is the matching Claude Code skill (copy to
`~/.claude/skills/tracker/`) describing when/how to consult tracker.

## Why

Agents need a shared source of truth and a way to see **if a doc is already
being written by another agent**. This is a database problem, not a
knowledge-app problem — so: Postgres + S3/Files

## Design

- **Leases, not advisory locks.** A `doc_locks` row with a TTL + heartbeat
  answers "who is writing this right now". A crashed agent's lease auto-expires,
  so it can never block a doc forever.
- **Two-layer write safety.** A write requires (a) a live lease the caller holds
  (`X-Lease-Token`) and (b) `If-Match: <version>` optimistic concurrency, so a
  stale or lease-less write can't clobber.
- **Content-addressed blobs.** Bytes are stored under `sha256/<hash>`
  (immutable, deduped) in either a local directory (`STORAGE_TYPE=file`) or an S3 bucket
  (`STORAGE_TYPE=s3`). Agents fetch them via a presigned URL or direct local URL.
- **Task queue.** `tasks` with `FOR UPDATE SKIP LOCKED` claiming — no two agents
  grab the same task.

## Run

Both Postgres and the service run via Docker Compose (no sudo needed). The
`tracker` container uses host networking, so it binds the loopback/LAN/Tailscale/ZeroTier
IPs in `LISTEN_ADDR` and reaches Postgres (and optionally S3) on the host.

```bash
cp .env.example .env          # fill in secrets (e.g. STORAGE_TYPE=file) + set API_TOKENS
docker compose up -d          # starts pgvector Postgres + tracker
```

Ops (the Makefile stamps the version from git into the binary):

```bash
make deploy                            # rebuild image w/ version + restart (no sudo)
make version                           # show the version that would be embedded
docker compose logs -f tracker         # logs
curl http://127.0.0.1:8080/version     # version the running binary reports
```

The version is `git describe --tags --always --dirty` — logged at startup, served
at `/version`, and recorded in each backup's `manifest.json`.

For local dev without a container: `make build && set -a && . ./.env && set +a && ./tracker`.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` · `/version` · `/openapi.yaml` · `/llms.txt` | health, version, spec, agent index |
| POST · GET | `/docs` | create (`content` seeds v1); list/search (`?q=&mode=&kind=&tag=&view=&limit=&offset=`) |
| GET · PUT · PATCH | `/docs/{id}` | read `{document,content_url,lock}`; write content (lease + `If-Match`); relabel tags/metadata (no lease, no version bump) |
| GET | `/docs/{id}/raw` · `/docs/{id}/revisions[/{v}/raw]` | content bytes; version history |
| POST · GET · DELETE | `/docs/{id}/lock` | acquire/renew (`409` if held) · status · release |
| GET | `/tags` | tag vocabulary with counts |
| GET · POST | `/folios` · `/folios/{slug}` · `/folios/{slug}/files[/{filename}[/raw]]` | collections + their files |
| POST | `/tasks` · `/tasks/claim` · `/tasks/{id}/complete` | task queue |
| GET | `/actors` · `/actors/{name}/activity` | entity registry + activity |

The **authoritative reference is `openapi.yaml`** (served live at `/openapi.yaml`).

**Conventions.** Every response is wrapped — a single resource under its type
(`{"document":…}`, `{"folio":…}`, …), lists as `{"<type>s":[…],"count","total",…}`,
errors as `{"error":{"code","message",…}}` with machine codes. Lists default to a
trimmed `view=summary`; `view=table` is a compact columnar `{cols,rows}`,
`view=full` whole objects. Search is `websearch_to_tsquery` (`mode=web` default;
`mode=plain` for strict AND).

### Folios

A **folio** is a little collection of related documents (think: a GitHub gist).
It's modelled tableless: the folio is itself a document with `kind='folio'`
whose `metadata` holds `{description, public, github_id, ...}`; its files are
documents tagged `folio:<slug>` with slug `<folio-slug>/<filename>`. So a folio
file inherits everything (versioning, leases, attribution, search). Create one
with `POST /folios` and add files with `POST /folios/{slug}/files`; import your
recent gists with `scripts/import_gists.py`.

`{id}` accepts a UUID or a slug — including multi-segment folio slugs like
`myfolio/file.md`; only `/raw` and `/lock` for those still need the
`/folios/{slug}/files/…` route or the UUID.

### Acting entity

Every **mutating** request must send an `X-Actor: <name>` header naming the
entity performing it (missing → `400`). That value is stamped into
`created_by`/`updated_by`, the revision `author`, the lease `owner`, and task
`claimed_by`, and upserted into the `actors` registry. A write must come from
the entity that **holds the lease** (actor ≠ lease owner → `423`).

On a trusted (non-internet) network `X-Actor` is self-asserted attribution, not
authenticated identity. Set `API_TOKENS` and bind actor→token if you need it to
be tamper-proof.

## Backup & restore

State lives in two places that must be captured together: Postgres (the index)
and the blobs (the content). One self-contained tarball holds both —
`db.dump` + `blobs/` + `manifest.json`. That tarball is the portable unit;
"R2 vs S3 vs a local directory" is just where you keep it.

```bash
scripts/backup.sh                 # -> ./backups/tracker-backup-<ts>.tar.gz
scripts/backup.sh --upload        # also push to R2/S3 (set BACKUP_S3_* in .env)

scripts/restore.sh ./backups/tracker-backup-<ts>.tar.gz   # from a local file
scripts/restore.sh --from-s3 tracker-backup-<ts>.tar.gz   # pull from R2/S3 first
docker compose up -d tracker                               # then start the service
```

The backup dumps Postgres **first**, then copies blobs — and since writes are
blob-first, every `content_key` in the dump is guaranteed to have its blob, so
the tarball is always internally consistent. Restore is verified round-trip:
restoring into a scratch DB+bucket reproduces the exact doc/blob counts and a
tracker booted against it serves the content. `scripts/s3util.py` moves blobs and
tarballs to any S3-compatible store (RustFS, AWS S3, Cloudflare R2).

To restore on a **fresh machine**: clone the repo, create `.env` (point
`S3_*`/`DATABASE_URL` at that host's RustFS+Postgres), `docker compose up -d
postgres`, run `restore.sh`, then `docker compose up -d tracker`.

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

## Status

Running in "production" (lol). I've been using it heavily for several weeks. Known follow-ups:

- pre-check lease/version before blob upload (rejected writes can leave GC-able orphans)
- pgvector semantic search
- scheduled backups
- orphan/expired-lease GC
- CI/CD, more pacakaging, etc.
- Even simpler example MCP/skill usage

PRs welcome but I can't promise I'll get to them!
