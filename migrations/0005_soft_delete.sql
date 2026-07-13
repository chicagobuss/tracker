-- Soft-delete tombstones: row + revisions stay; default search excludes them.
-- Hard delete removes the row (revisions/locks cascade); blobs are left for GC.
alter table documents add column if not exists deleted_at timestamptz;
alter table documents add column if not exists deleted_by text;
create index if not exists documents_live_idx on documents (updated_at desc) where deleted_at is null;
create index if not exists documents_deleted_idx on documents (deleted_at desc) where deleted_at is not null;
