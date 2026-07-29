---
name: tracker
description: >-
  Use the self-hosted tracker coordination store to read and write shared docs
  and folios and to coordinate with other coding agents. Consult it BEFORE
  starting non-trivial work (project context, dev guidance, design folios), to
  record decisions/notes, and whenever you need to know what another agent is
  doing or editing. Trigger on: "check tracker", "what's the dev process / north
  star", shared notes/gists/folios, cross-agent coordination, or recording an
  outcome other agents should see.
---

# tracker

<!-- SETUP: replace with your instance's base URL (and add the ones you reach it
     by from other machines, e.g. a LAN or Tailscale/ZeroTier address). -->
**Base URL:** `http://127.0.0.1:8770`

tracker is a self-hosted coordination store (Postgres index + file/S3 blobs). It
is the source of truth for cross-agent coordination and shared documents — it
**replaces the old GitHub-gist flow**.

Prefer the **`tracker` MCP tools** (served natively by tracker at `/mcp`;
register once with `claude mcp add --transport http tracker <base>/mcp --header
"X-Actor: <you>"`). Every write is stamped with your `X-Actor` identity, so
changes are attributed by entity. The full API reference is `GET
/openapi.yaml`; tracker's own docs live in the `tracker` folio
(`GET /folios/tracker`).

## Core concepts

- **document** — one markdown file: slug, content, version, attribution.
- **folio** — a little collection of related documents (what a gist was). A
  folio is itself a `kind=folio` doc whose metadata holds its description; its
  files are docs tagged `folio:<slug>`.
- **lease** — a TTL "who's-writing" lock on a doc. A write requires holding the
  lease + matching the doc's current version (optimistic concurrency).
- **actor** — the entity performing an action; required on every write.
- **workspace** — a hard partition of the store, enforced by row-level
  security: docs, folios, tasks and actors in one are invisible from another.
  Pre-existing content lives in `default`. Use a separate workspace to give a
  focused build-out blinders, so unrelated work never pollutes its search
  results.

## Workspaces

**You do not choose a workspace per call — your connection is pinned to one**,
by its `X-Workspace` header or by a token bound to a workspace
(`API_TOKENS=<token>:<name>`). A confined token wins; the header cannot
override it.

- `list_workspaces()` — discover where you may work. Not itself scoped.
- `create_workspace(name)` — register one. **Creating it does not switch you
  into it.** Names match `[a-z0-9][a-z0-9_-]{0,62}`.
- **Reads** can target another workspace per call: `list_docs`, `get_doc`,
  `list_folios`, `get_folio`, `get_folio_file` accept `workspace=`. Rejected
  when your token is confined.
- **Writes cannot.** `create_doc`, `add_folio_file`, `create_folio`,
  `update_doc`, `retag_doc` and the delete tools have no workspace argument —
  they always land in your connection's workspace.

So to write into a workspace, **point a connection at it** rather than reaching
for curl. `POST /mcp` runs the same auth middleware as the REST API, so the
header works there too — register a second server:

```
claude mcp add --scope user --transport http tracker-<ws> <base>/mcp \
  --header "X-Actor: <you>" --header "X-Workspace: <ws>"
```

Pass `--scope user`, or `claude mcp add` writes project-local config tied to
your current directory. Tools load at session start, so restart after
registering. Every response echoes the `X-Workspace` actually used — check it
to confirm placement. An unknown workspace is rejected (404) rather than
auto-created, so a typo cannot silently open an empty world.

A doc written into a workspace should be **self-contained**: an agent working
there cannot read another workspace's folios, so inline the numbers and rules
it needs instead of linking to docs that will not resolve.

## When to use it

1. **Before non-trivial work** — search/read relevant context first:
   - `list_docs(q=...)` — full-text find (quoted `"phrases"`, `OR`,
     `-negation`; bare words all must match). Returns a compact `cols`/`rows`
     table — address a doc by its `slug`, then `get_doc(id,
     include_content=true)` or `get_raw(id)` for content.
   - `list_folios()` / `get_folio(slug)` — browse collections.
   - `list_tags()` — discover the tag vocabulary (`folio:*`, `topic:*`, …).
   - Always check the shared **dev-guidance** folio for cross-project conventions.
   - Empty results may just mean you are in the wrong workspace — confirm with
     `list_workspaces()` before concluding something does not exist.
2. **Before editing a shared doc** — `lock_status(id)` to avoid colliding with
   another agent.
3. **Writing/recording**:
   - `update_doc(id, content)` — replaces content safely (it acquires the
     lease, writes with the version check, and releases — all for you). If
     another agent holds the lease it fails clearly without writing; retry later.
   - `create_doc(slug, title, content)` — new doc;
     `add_folio_file(slug, filename, content)` — new doc inside a folio.
   - `create_folio(slug, description)` — new collection.
   - `retag_doc(id, add_tags=, remove_tags=, tags=, metadata=, title=, kind=)` —
     change tags/metadata/title/kind WITHOUT rewriting content (no lease, version
     unchanged). Use namespaced tags: `topic:x`, `status:x`.
   - `soft_delete_doc(id)` — hide from normal search; history kept; restorable.
     Prefer this. Use `list_docs(deleted="only")` to find soft-deleted docs.
   - `restore_doc(id)` — undo a soft-delete.
   - `hard_delete_doc(id, confirm="<exact-slug>")` — irreversible. `confirm`
     is required and must equal the slug (MCP schema enforces the argument;
     the server rejects mismatches). Prefer soft-delete.
4. **Coordination / who's active** — `list_actors()` shows entities and their
   last activity. Use tasks (`enqueue_task`, `list_tasks`, `claim_task`,
   `complete_task`) for a shared work queue (claims are atomic, and expired
   claims from crashed agents are re-claimable).

## Etiquette

- Identify honestly: your writes are stamped with your configured actor.
- Write where the work belongs. If a task has its own workspace, use a
  connection pinned to it — do not scatter its docs into `default`.
- Don't hold a lease longer than you're actively writing; `update_doc` releases
  immediately, so prefer it.
- On a `version changed under you` error, re-read the doc and reapply — someone
  else wrote in between.
- Keep shared docs terse and additive, in the style of the doc you're editing.
