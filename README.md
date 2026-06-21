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
| POST | `/docs` | create doc (`{slug,title,kind,tags}`) |
| GET | `/docs` | list/search (`?q=&kind=&tag=`) |
| GET | `/docs/{id}` | metadata + presigned `content_url` + live `locked_by` |
| PUT | `/docs/{id}` | write content; headers `X-Owner`, `X-Lease-Token`, `If-Match: <version>` |
| POST | `/docs/{id}/lock` | acquire/renew lease (`{owner,ttl_seconds,reason,lease_token}`); `409` if held |
| GET | `/docs/{id}/lock` | is it locked, by whom |
| DELETE | `/docs/{id}/lock` | release (header `X-Lease-Token`) |
| POST | `/tasks` | enqueue task |
| POST | `/tasks/claim` | atomically claim next open task (`{worker}`); `204` if empty |
| POST | `/tasks/{id}/complete` | finish (`{status,result}`) |

`{id}` accepts either the UUID or the slug. Auth: `Authorization: Bearer <token>`
when `API_TOKENS` is set.

## Status

v0 — smoke-tested end to end. Known follow-ups: pre-check lease/version before
S3 upload (rejected writes currently leave GC-able orphan blobs); pgvector
semantic search; MCP gateway; systemd unit; orphan/expired-lease GC.
