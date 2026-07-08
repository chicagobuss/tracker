# tracker

A tiny coordination store for a fleet of coding agents. Postgres holds the
index + lease/coordination state; content blobs live in local files or RustFS (S3).
Single static Go binary, low footprint, reachable by agents over the network
(e.g. ZeroTier).

## Agents: MCP server + skill

`mcp/tracker_mcp.py` is an MCP server (a self-contained `uv` script — no install)
that exposes tracker to any coding agent: `search_docs`, `list_folios`,
`get_folio`, `read_doc`, `who_is_editing`, `create_doc`, `create_folio`,
`update_doc` (acquires the lease, writes with the version check, and releases for
you), `list_actors`, and the task tools. Configure per agent via env:
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
knowledge-app problem — so: Postgres + S3, not Anytype.

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
`tracker` container uses host networking, so it binds the loopback/LAN/ZeroTier
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

The version is `git describe --tags --always --dirty` (a tag if HEAD is tagged,
else the short sha, `-dirty` if uncommitted). It's logged at startup, served at
`/version`, and recorded in every backup's `manifest.json` (`binary_version`), so
a backup always knows which build produced it.

For local dev without a container: `make build && set -a && . ./.env && set +a && ./tracker`.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | health check |
| GET | `/openapi.yaml` | the full OpenAPI 3.1 spec (authoritative reference) |
| POST | `/docs` | create doc (`{slug,title,kind,tags,metadata,content,content_type}`; `content` seeds v1) |
| GET | `/docs` | list/search (`?q=&kind=&tag=&limit=&offset=`) |
| GET | `/docs/{id}` | `{document, content_url, lock}` |
| PUT | `/docs/{id}` | write content; headers `X-Actor`, `X-Lease-Token`, `If-Match: <version>` |
| POST | `/docs/{id}/lock` | acquire/renew lease (`{ttl_seconds,reason,lease_token}`); `409` if held |
| GET | `/docs/{id}/lock` | is it locked, by whom |
| DELETE | `/docs/{id}/lock` | release (header `X-Lease-Token`) |
| GET | `/folios` · POST `/folios` | list / create a folio |
| GET | `/folios/{slug}` | a folio + its files |
| POST | `/folios/{slug}/files` | add a file (server applies the tag + slug) |
| GET | `/folios/{slug}/files/{filename}` | a folio file by name (`/raw` for bytes) |
| POST | `/tasks` · `/tasks/claim` · `/tasks/{id}/complete` | task queue |
| GET | `/actors` · `/actors/{name}/activity` | entity registry + activity |

The **authoritative reference is `openapi.yaml`** (served live at `/openapi.yaml`).

**Conventions.** Every response is wrapped — a single resource under its type
(`{"document":…}`, `{"folio":…}`, `{"task":…}`, `{"lock":…}`), lists as
`{"<type>s":[…],"count","total","limit","offset"}`, errors as
`{"error":{"code","message",…}}` with machine codes (`not_found`, `lease_held`,
`no_lease`, `version_conflict`, …).

### Folios

A **folio** is a little collection of related documents (think: a GitHub gist).
It's modelled tableless: the folio is itself a document with `kind='folio'`
whose `metadata` holds `{description, public, github_id, ...}`; its files are
documents tagged `folio:<slug>` with slug `<folio-slug>/<filename>`. So a folio
file inherits everything (versioning, leases, attribution, search). Create one
with `POST /folios` and add files with `POST /folios/{slug}/files`; import your
recent gists with `scripts/import_gists.py`.

`{id}` accepts a UUID or a single-segment slug; folio files (slug has `/`) are
addressed by UUID or via `/folios/{slug}/files/{filename}`.

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

## Status

v0 — smoke-tested end to end. Known follow-ups: pre-check lease/version before
S3 upload (rejected writes currently leave GC-able orphan blobs); pgvector
semantic search; scheduled backups (cron `backup.sh --upload`); orphan/expired-lease GC.
