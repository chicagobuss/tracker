-- Backfill fts for documents created without content: their titles were never
-- indexed (fts was only set on content writes). Idempotent: only touches nulls.
update documents set fts = to_tsvector('english', title) where fts is null;
