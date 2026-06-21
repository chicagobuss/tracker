# tracker

A tiny coordination store for a fleet of coding agents. Postgres holds the
index + lease/coordination state; content blobs live in RustFS (S3). Single
static Go binary, low footprint, reachable by agents over the network
(e.g. ZeroTier).

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
- **Content-addressed blobs.** Bytes are stored in RustFS under `sha256/<hash>`
  (immutable, deduped); agents fetch them via a presigned URL rather than
  proxying through the API.
- **Task queue.** `tasks` with `FOR UPDATE SKIP LOCKED` claiming — no two agents
  grab the same task.

## Run

```bash
cp .env.example .env        # fill in secrets + set API_TOKENS before exposing
docker compose up -d        # pgvector Postgres
go build -o tracker .
set -a && . ./.env && set +a && ./tracker
```

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | health check |
| POST | `/docs` | create doc (`{slug,title,kind,tags,metadata,content,content_type}`; `content` seeds v1) |
| GET | `/docs` | list/search (`?q=&kind=&tag=`) |
| GET | `/docs/{id}` | metadata + presigned `content_url` + live `locked_by` |
| PUT | `/docs/{id}` | write content; headers `X-Lease-Token`, `If-Match: <version>` |
| POST | `/docs/{id}/lock` | acquire/renew lease (`{ttl_seconds,reason,lease_token}`); `409` if held |
| GET | `/docs/{id}/lock` | is it locked, by whom |
| DELETE | `/docs/{id}/lock` | release (header `X-Lease-Token`) |
| POST | `/tasks` | enqueue task (`{title,payload}`) |
| POST | `/tasks/claim` | atomically claim next open task; `204` if empty |
| POST | `/tasks/{id}/complete` | finish (`{status,result}`) |
| GET | `/actors` | registry of entities with `first_seen`/`last_seen`/`action_count` |
| GET | `/actors/{name}/activity` | that entity's recent doc writes (`?limit=`) |
| GET | `/folios` | list folios (collections; `kind=folio` documents) |
| GET | `/folios/{slug}` | a folio document + its member files |

### Folios

A **folio** is a little collection of related documents (think: a GitHub gist).
It's modelled tableless: the folio is itself a document with `kind='folio'`
whose `metadata` holds `{description, public, github_id, ...}`; its files are
documents tagged `folio:<slug>` with slug `<folio-slug>/<filename>`. So a folio
file inherits everything (versioning, leases, attribution, search). Import your
recent gists with `scripts/import_gists.py` (uses the `gh` CLI).

`{id}` accepts either the UUID or the slug.

### Acting entity

Every **mutating** request must send an `X-Actor: <name>` header naming the
entity performing it (missing → `400`). That value is stamped into
`created_by`/`updated_by`, the revision `author`, the lease `owner`, and task
`claimed_by`, and upserted into the `actors` registry. A write must come from
the entity that **holds the lease** (actor ≠ lease owner → `423`).

On a trusted (non-internet) network `X-Actor` is self-asserted attribution, not
authenticated identity. Set `API_TOKENS` and bind actor→token if you need it to
be tamper-proof.

## Status

v0 — smoke-tested end to end. Known follow-ups: pre-check lease/version before
S3 upload (rejected writes currently leave GC-able orphan blobs); pgvector
semantic search; MCP gateway; systemd unit; orphan/expired-lease GC.
