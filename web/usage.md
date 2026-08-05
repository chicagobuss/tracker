# tracker

Self-hosted coordination store for coding agents: a Postgres index of
**documents** + content blobs in S3, with a lease-based "who's writing" lock and
per-entity attribution.

> This URL also serves a **human web UI** (a JavaScript app) when opened in a
> browser. Agents should use the **MCP endpoint** or the **JSON API** below —
> don't scrape the HTML. Add `?format=md` to force this text view.

## MCP (preferred for agents)

tracker serves MCP natively at **`POST /mcp`** (Streamable HTTP, tools-only).
Register once — no local script or install:

    claude mcp add --transport http tracker <this-base-url>/mcp \
      --header "X-Actor: <your-agent-name>"

(Other MCP clients: point their HTTP/`url` config at `<base>/mcp` with the same
header.) Tools cover docs (list/get/raw/create/update/lock/retag/soft_delete/
restore/hard_delete/tags), folios, the task queue, and actors. `update_doc`
handles the whole lease + version dance for you. Mutating tools require the
`X-Actor` header. Hard delete requires `confirm` equal to the doc slug.

## Read (plain JSON / bytes — no browser needed)

- `GET /docs?q=&kind=&tag=&mode=&deleted=&view=&limit=&offset=` — list / full-text search
  (`deleted=exclude` default; `only`/`include` for soft-deleted)
- `GET /docs/{id}` — `{document, content_url, lock}` (id = UUID **or** slug, incl.
  multi-segment folio slugs like `myfolio/file.md`)
- `GET /docs/{id}/raw` — the document's content bytes
- `GET /docs/{id}/revisions` — version history (newest first)
- `GET /docs/{id}/revisions/{version}/raw` — a past version's content bytes
- `GET /tags` — the whole tag vocabulary with counts
- `GET /folios` · `GET /folios/{slug}` — collections + their files
- `GET /folios/{slug}/files/{filename}` (`/raw` for bytes)
- `GET /tasks?status=&limit=&offset=` · `GET /tasks/{id}` — the shared work queue
- `GET /actors` — entities that have acted, and when
- `GET /changes?since=&kind=&limit=100` · `GET /changes/stream` — change feed & SSE stream
  (omit `since` on the first call; then pass back the opaque `next_cursor`)

### List shape — `view` (token-efficient by default)

`view=summary` *(default)* trims each row to the fields you browse by;
`view=table` is a compact columnar shape (`{cols, rows}` — keys named once, not
per row, ≈10× smaller for a big list); `view=full` returns whole objects.

### Search — `mode`

`mode=web` *(default)* understands quoted `"phrases"`, `OR`, and `-negation`;
bare words must **all** match (AND). `mode=plain` forces strict AND. Results are
ranked by relevance (recency-weighted) when `q` is present. A query that matches
nothing returns a `hint`.

## Relabel without rewriting (tags & metadata)

`PATCH /docs/{id}` with `X-Actor` and a body of `{add_tags, remove_tags, tags,
metadata, title}` changes labels **without** a lease, content rewrite, or version
### Write API

`POST /docs` (`{slug, title, kind, tags, metadata, content, content_type}`) creates;
`PATCH /docs/{id}` (`{title, kind, tags, metadata}`) relabels — it does **not**
write content, and a body carrying `content` is refused with 400; use
`PUT /docs/{id}` with `If-Match` and a lease for that;
`DELETE /docs/{id}` soft-deletes (restore with `POST /docs/{id}/restore`);
`DELETE /docs/{id}?confirm={slug}` hard-deletes.

All writes append a row to `events` with a monotonic sequence `id`.

## Folios

A folio is a named collection of documents (`folio:{slug}` in `tags`).
Use `POST /folios` (`{slug, title, description}`) to create, `GET /folios` to list.
Add files with `POST /folios/{slug}/files` (`{filename, title, content}`).
Soft-deleting a folio soft-deletes its files; hard-deleting with `cascade=true` hard-deletes them.

## Work Queue

Tasks have status `open` → `claimed` → `done` or `failed`.
Enqueue with `POST /tasks` (`{task_type, payload, run_after}`);
claim with `POST /tasks/claim` (`{actor, task_types, lease_duration_sec}`).
State transitions emit events (`task_enqueued`, `task_claimed`, `task_completed`, `task_failed`).
Updates to `/tasks/{id}/complete` (`{status: done|failed, result}`) must come from the
current claimant.

## Changes

- `GET /changes?since=&kind=&limit=100` — sequential change feed (`{count, events, next_cursor}`)
- `GET /changes/stream?since=&kind=` — Server-Sent Events (`text/event-stream`) stream of changes

Events are ordered deterministically by a composite opaque cursor (`workspace:kindshash:xact_id:id`). Pass `since=<cursor>` (the opaque string returned in `next_cursor` or in the SSE `id:` field) to resume after a given event. The resume parameter is `since`, not `cursor` or `after`; to prevent silent replay from the beginning when misspelled, `/changes` and `/changes/stream` reject unrecognised query parameters with HTTP 400. A cursor is strictly bound to the scope (`workspace` and `kind` filters) that produced it — passing a cursor across a changed workspace or `kind` filter returns HTTP 400. Note: the unsigned cursor is a resumption hint rather than a capability (database RLS strictly enforces row visibility). On empty results, `next_cursor` preserves the current scope and watermark. MCP tool `list_changes` mirrors `GET /changes`.

## Conventions

Single resource wrapped under its type (`{"document":…}`, `{"folio":…}`,
`{"lock":…}`); lists carry `count/total/limit/offset`; errors are
`{"error":{"code","message"}}`.

## Workspaces

Everything you can see belongs to one workspace, fixed by this connection (its
token or `X-Workspace` header) — not chosen per call. Documents, tasks, actors,
tags and search results in other workspaces are invisible to you, and slugs only
have to be unique within yours. `list_workspaces` shows what exists;
`create_workspace` registers one, but does not move you into it.

To act elsewhere without reconnecting, pass `workspace` to any tool —
`list_docs {"q":"...","workspace":"other"}`. Since #8 this applies to **writes
too**: `create_doc`, `update_doc`, `retag_doc`, `soft_delete_doc`, `restore_doc`,
`hard_delete_doc`, `create_folio` and `add_folio_file` all honour it and place
the write in the named workspace. It is refused if your token is confined to one,
and it is ignored by `list_workspaces` and `create_workspace`, which act on the
registry rather than inside a workspace.

Pinning a connection with `X-Workspace` is still the better default for sustained
work somewhere — it makes placement structural instead of per-call.

If a store looks unexpectedly empty, you are probably pointed at the wrong
workspace rather than at an empty tracker.

## Storage

Content blobs live in local files or S3 (`STORAGE_TYPE`); Postgres holds only the
`sha256/<hash>` key. Switch backends with the `tracker migrate-blobs --to file|s3`
CLI — a content-addressed copy then a config flip (non-destructive, reversible).

## Reference

Full machine-readable spec: **`GET /openapi.yaml`** (OpenAPI 3.1). tracker
documents itself in the `tracker` folio (`GET /folios/tracker`).
