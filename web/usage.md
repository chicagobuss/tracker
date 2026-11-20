# tracker

Self-hosted coordination store for coding agents. Postgres holds the document
index, TTL write leases, task queue, workspaces (enforced by row-level
security), and change feed; content blobs live under `sha256/<hash>` in local
files (`STORAGE_TYPE=file`, default) or S3-compatible object storage
(`STORAGE_TYPE=s3`).

Open this URL in a browser for the human web UI; fetch `/llms.txt` or pass
`?format=md` for this document.

## MCP (preferred for agents)

tracker serves MCP natively at **`POST /mcp`** (Streamable HTTP, tools-only,
stateless; `GET /mcp` returns `405`). Register once — no local script needed:

```bash
claude mcp add --transport http --scope user tracker <base-url>/mcp \
  --header "X-Actor: <your-agent-name>"
```

Add `--header "Authorization: Bearer <token>"` when `API_TOKENS` is set, and
`--header "X-Workspace: <workspace>"` to pin a workspace. Other Streamable HTTP
clients (Cursor `url`, Gemini CLI `httpUrl`) configure the same endpoint and
headers.

Tools (26):
- **Docs:** `list_docs`, `get_doc`, `get_raw`, `create_doc`, `update_doc`,
  `lock_status`, `retag_doc`, `soft_delete_doc`, `restore_doc`,
  `hard_delete_doc`, `list_tags`
- **Folios:** `list_folios`, `create_folio`, `get_folio`, `get_folio_file`,
  `add_folio_file`
- **Tasks:** `list_tasks`, `get_task`, `enqueue_task`, `claim_task`,
  `complete_task`
- **Workspaces:** `list_workspaces`, `create_workspace`
- **Actors & changes:** `list_actors`, `actor_activity`, `list_changes`

Behavioral notes:
- `update_doc` runs the lease acquire + `If-Match` write + release server-side.
- `list_docs`, `list_folios`, and `get_folio` default to `view=table`
  (`{cols, rows}`) over MCP for token efficiency.
- Mutating tools require the `X-Actor` header.
- `hard_delete_doc` requires `confirm` equal to the exact slug; deleting a
  non-empty folio (soft or hard) requires `cascade=true`.

## Read & search (REST)

- `GET /docs?q=&kind=&tag=&mode=&deleted=&view=&limit=&offset=` — list/search
  (`deleted=exclude` default; `only` / `include` for soft-deleted)
- `GET /docs/{id}` — `{document, content_url, lock}` (`{id}` is a UUID or slug,
  including multi-segment folio slugs like `myfolio/file.md`)
- `GET /docs/{id}/raw` — current content bytes
- `GET /docs/{id}/revisions` — version history (newest first)
- `GET /docs/{id}/revisions/{version}/raw` — past version content bytes
- `GET /docs/{id}/lock` — live lease state (`{locked, lock}`)
- `GET /tags` — tag vocabulary with usage counts
- `GET /folios` · `GET /folios/{slug}` (`limit`, `offset`, `view`) ·
  `GET /folios/{slug}/files/{filename}[/raw]`
- `GET /tasks?status=&limit=&offset=` · `GET /tasks/{id}`
- `GET /workspaces`
- `GET /actors` · `GET /actors/{name}/activity?limit=`
- `GET /changes?since=&kind=&limit=100` · `GET /changes/stream?since=&kind=`

### Views & search

- `view`: `summary` (REST default), `table` (`{cols, rows}`, MCP default),
  `full` (whole document objects).
- `mode`: `web` (default; quoted `"phrases"`, `OR`, `-negation`, bare words
  AND) or `plain` (strict AND). Multi-word queries that match nothing return a
  `hint`.

## Write, leases & deletes (REST)

Every mutating request requires `X-Actor: <name>` (missing → `400`).

- **Create:** `POST /docs` (`{slug, title, kind, tags, metadata, content,
  content_type}`); `content` seeds version 1. For a file (image, PDF, …) send
  `content_base64` instead; `content_type` then defaults from the slug's
  extension, and the response includes `embed`: markdown that shows it
  (`![name](/docs/<slug>/raw?workspace=<ws>)` for images, a link otherwise).
- **Relabel (no content write, no lease, no version bump):** `PATCH /docs/{id}`
  (`{add_tags, remove_tags, tags, metadata, title, kind}`). Strict JSON — sending
  `content` fails with `400`.
- **Write content (two-layer safety — both required):**
  1. Acquire/renew lease: `POST /docs/{id}/lock` (`{reason, ttl_seconds,
     lease_token}` → `{lock}`; default TTL `60s`; `409 lease_held` if another
     actor holds it).
  2. Write bytes: `PUT /docs/{id}` with `X-Actor`, `X-Lease-Token: <token>`,
     `If-Match: <version>`, and `Content-Type` (defaults to `text/markdown`).
     Fails with `423 no_lease` or `412 version_conflict` without writing.
  3. Release lease: `DELETE /docs/{id}/lock` with `X-Lease-Token: <token>`.
- **Soft-delete & restore (preferred):** `POST /docs/{id}/soft-delete`
  (send `{"cascade": true}` for non-empty folios) · `POST /docs/{id}/restore`.
- **Hard-delete (irreversible):** `DELETE /docs/{id}` — always requires
  `confirm` equal to the document's exact slug (via JSON body
  `{"confirm":"<slug>","cascade":true}`, `X-Confirm-Slug` header, or
  `?confirm=<slug>&cascade=true`).

Quirk for multi-segment folio slugs: a file literally named `raw`, `lock`,
`soft-delete`, or `restore` collides with those suffix routes — address it via
`/folios/{slug}/files/{filename}` or its UUID.

## Folios

A folio is a `kind="folio"` document (`metadata` holds `{description, public}`);
its files are documents tagged `folio:<slug>` with slug `<folio>/<filename>`.
Create with `POST /folios` (`{slug, title, description, public}`) and add files
with `POST /folios/{slug}/files` (`{filename, title, kind, tags, content,
content_base64, content_type, metadata}`). Attachments (non-text files) are
listed with their `content_type`; text is indexed for search, files by title.
Over MCP, `get_doc` returns an attachment's `content_url` and `embed` instead of
its bytes; `get_raw` returns `content_base64`. Deleting a non-empty folio (soft or hard) returns
`409 folio_not_empty` unless `cascade=true`.

## Work queue

Tasks transition `open` → `claimed` → `done` | `failed`.

- Enqueue: `POST /tasks` (`{title, payload}`).
- Claim: `POST /tasks/claim` (atomic FIFO via `FOR UPDATE SKIP LOCKED`;
  claimant comes from `X-Actor`; returns `{"task": null}` when empty) or
  `POST /tasks/{id}/claim` for a specific task. Claims expire after
  `TASK_CLAIM_TTL` (default `1h`) and become re-claimable (`attempts` increments).
- Complete: `POST /tasks/{id}/complete` (`{status: "done"|"failed", result}`) —
  claimant-only (`409 not_claimant`). Over MCP, `complete_task` defaults
  `status` to `"done"`.

## Change feed & SSE

Every write appends a monotonic event (`doc_created`, `doc_updated`,
`doc_retagged`, `doc_soft_deleted`, `doc_restored`, `doc_hard_deleted`,
`task_enqueued`, `task_claimed`, `task_completed`).

- `GET /changes?since=&kind=&limit=100` (and MCP `list_changes`) returns
  `{count, events, next_cursor}`.
- `GET /changes/stream?since=&kind=` streams `text/event-stream` (`data:` is
  event JSON, `id:` is the cursor; also honors `Last-Event-ID`).
- Pass `since=<next_cursor>` (not `cursor` or `after`) to resume. Unknown query
  parameters return `400` to prevent silent replay from typos.
- Cursors are opaque (`<workspace>:<kindshash>:<xact_id>:<id>`) and bound to
  the `(workspace, kind)` scope that produced them; reusing a cursor across a
  different scope returns `400`.

## Workspaces

Workspaces are hard partitions enforced by Postgres row-level security
(`app.workspace`): docs, folios, tasks, tags, actors, and search results are
isolated per workspace.

- Connection scope precedence: workspace-confined bearer token
  (`API_TOKENS=<token>:<workspace>`) > `X-Workspace` header > server
  `DEFAULT_WORKSPACE` (`default`). Responses echo `X-Workspace`. Unknown
  workspaces return `404` (never auto-created).
- Registry endpoints (`GET /workspaces`, `POST /workspaces`, MCP
  `list_workspaces` / `create_workspace`) are not workspace-scoped; creating a
  workspace does not switch your connection into it.
- Per-call override: all scoped MCP tools accept an optional `workspace`
  argument (reads and writes alike), and REST endpoints accept `?workspace=`
  (when `X-Workspace` is absent). Overrides are refused when your token is
  confined to a workspace, and ignored by registry tools.
- If a store looks unexpectedly empty, check which workspace you are querying.

## Conventions & reference

- Single resources wrap under their type (`{"document":…}`, `{"folio":…}`,
  `{"lock":…}`, `{"task":…}`); lists return `count`, `total`, `limit`, `offset`;
  errors return `{"error":{"code","message",…}}`.
- Full machine-readable OpenAPI 3.1 spec: `GET /openapi.yaml`.
