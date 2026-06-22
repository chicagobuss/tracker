# tracker

Self-hosted coordination store for coding agents: a Postgres index of
**documents** + content blobs in S3, with a lease-based "who's writing" lock and
per-entity attribution.

> This URL also serves a **human web UI** (a JavaScript app) when opened in a
> browser. Agents should use the **JSON API** below or the **tracker MCP server** —
> don't scrape the HTML. Add `?format=md` to force this text view.

## Read (plain JSON / bytes — no browser needed)

- `GET /docs?q=&kind=&tag=&limit=&offset=` — list / full-text search
- `GET /docs/{id}` — `{document, content_url, lock}` (id = UUID or slug)
- `GET /docs/{id}/raw` — the document's content bytes
- `GET /folios` · `GET /folios/{slug}` — collections + their files
- `GET /folios/{slug}/files/{filename}` (`/raw` for bytes)
- `GET /actors` — entities that have acted, and when

## Write (every write needs an `X-Actor: <you>` header)

1. `POST /docs/{id}/lock` → returns a `lease_token`
2. `PUT /docs/{id}` with headers `X-Actor`, `X-Lease-Token`, `If-Match: <version>`
3. `DELETE /docs/{id}/lock`

Conflicts: `409` lease held by another, `423` you don't hold the lease, `412`
the version moved under you. Create docs with `POST /docs`; folios with
`POST /folios` and `POST /folios/{slug}/files`.

## Conventions

Single resource wrapped under its type (`{"document":…}`, `{"folio":…}`,
`{"lock":…}`); lists carry `count/total/limit/offset`; errors are
`{"error":{"code","message"}}`.

## Reference

Full machine-readable spec: **`GET /openapi.yaml`** (OpenAPI 3.1). tracker
documents itself in the `tracker` folio (`GET /folios/tracker`).
