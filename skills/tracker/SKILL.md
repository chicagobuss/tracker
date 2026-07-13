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

tracker is a self-hosted coordination store (Postgres index + RustFS blobs)
reachable at `http://127.0.0.1:8080` locally, or over ZeroTier
`http://10.10.10.10:8080` / LAN `http://192.168.1.100:8080`. It is the source
of truth for cross-agent coordination and shared documents — it **replaces the
old GitHub-gist flow**.

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

## When to use it

1. **Before non-trivial work** — search/read relevant context first:
   - `list_docs(q=...)` — full-text find (quoted `"phrases"`, `OR`,
     `-negation`; bare words all must match). Returns a compact `cols`/`rows`
     table — address a doc by its `slug`, then `get_doc(id,
     include_content=true)` or `get_raw(id)` for content.
   - `list_folios()` / `get_folio(slug)` — browse collections.
   - `list_tags()` — discover the tag vocabulary (`folio:*`, `topic:*`, …).
   - Always check the shared **dev-guidance** folio for cross-project conventions.
2. **Before editing a shared doc** — `lock_status(id)` to avoid colliding with
   another agent.
3. **Writing/recording**:
   - `update_doc(id, content)` — replaces content safely (it acquires the
     lease, writes with the version check, and releases — all for you). If
     another agent holds the lease it fails clearly without writing; retry later.
   - `create_doc(slug, title, content)` — new doc;
     `add_folio_file(slug, filename, content)` — new doc inside a folio.
   - `create_folio(slug, description)` — new collection.
   - `retag_doc(id, add_tags=, remove_tags=, tags=, metadata=, title=)` —
     change tags/metadata/title WITHOUT rewriting content (no lease, version
     unchanged). Use namespaced tags: `topic:x`, `kind:x`, `status:x`.
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
- Don't hold a lease longer than you're actively writing; `update_doc` releases
  immediately, so prefer it.
- On a `version changed under you` error, re-read the doc and reapply — someone
  else wrote in between.
- Keep shared docs terse and additive, in the style of the doc you're editing.
