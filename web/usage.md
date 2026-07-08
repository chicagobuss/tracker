# tracker

Self-hosted coordination store for coding agents: a Postgres index of
**documents** + content blobs in S3, with a lease-based "who's writing" lock and
per-entity attribution.

> This URL also serves a **human web UI** (a JavaScript app) when opened in a
> browser. Agents should use the **JSON API** below or the **tracker MCP server** —
> don't scrape the HTML. Add `?format=md` to force this text view.

## Read (plain JSON / bytes — no browser needed)

- `GET /docs?q=&kind=&tag=&mode=&view=&limit=&offset=` — list / full-text search
- `GET /docs/{id}` — `{document, content_url, lock}` (id = UUID **or** slug, incl.
  multi-segment folio slugs like `myfolio/file.md`)
- `GET /docs/{id}/raw` — the document's content bytes
- `GET /docs/{id}/revisions` — version history (newest first)
- `GET /docs/{id}/revisions/{version}/raw` — a past version's content bytes
- `GET /tags` — the whole tag vocabulary with counts
- `GET /folios` · `GET /folios/{slug}` — collections + their files
- `GET /folios/{slug}/files/{filename}` (`/raw` for bytes)
- `GET /actors` — entities that have acted, and when

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
bump — tags/metadata are labels, not content. Tag convention: namespace your
tags — `folio:<slug>`, `topic:<x>`, `kind:<x>`, `status:<x>`, `agent:<x>`.

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

## Storage

Content blobs live in local files or S3 (`STORAGE_TYPE`); Postgres holds only the
`sha256/<hash>` key. Switch backends with the `tracker migrate-blobs --to file|s3`
CLI — a content-addressed copy then a config flip (non-destructive, reversible).

## Reference

Full machine-readable spec: **`GET /openapi.yaml`** (OpenAPI 3.1). tracker
documents itself in the `tracker` folio (`GET /folios/tracker`).
