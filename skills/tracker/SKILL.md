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

Prefer the **`tracker` MCP tools** (auto-registered). They stamp every write with
your identity (`TRACKER_ACTOR`), so changes are attributed by entity. The full
API reference is `GET /openapi.yaml`; tracker's own docs live in the `tracker`
folio (`GET /folios/tracker`).

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
   - `search_docs(query=...)` — full-text find across docs.
   - `list_folios()` / `get_folio(slug)` — browse collections.
   - Always check the **"the dev guidance"** folio for cross-project rules.
2. **Before editing a shared doc** — `who_is_editing(id_or_slug)` to avoid
   colliding with another agent.
3. **Writing/recording**:
   - `update_doc(id_or_slug, content)` — replaces content safely (it acquires
     the lease, writes with the version check, and releases — all for you). If
     another agent holds the lease it fails clearly without writing; retry later.
   - `create_doc(slug, title, content, folio=...)` — new doc, optionally inside a
     folio.
   - `create_folio(slug, description)` — new collection.
4. **Coordination / who's active** — `list_actors()` shows entities and their
   last activity. Use tasks (`create_task`, `claim_task`, `complete_task`) for a
   shared work queue (claims are atomic — no two agents get the same task).

## Etiquette

- Identify honestly: your writes are stamped with your configured actor.
- Don't hold a lease longer than you're actively writing; `update_doc` releases
  immediately, so prefer it.
- On a `version changed under you` error, re-read the doc and reapply — someone
  else wrote in between.
- Keep shared docs terse and additive, in the style of the doc you're editing.
