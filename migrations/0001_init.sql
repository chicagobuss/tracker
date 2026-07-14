-- Coordination store: Postgres index; content blobs live in the configured
-- blob store (local files or any S3-compatible bucket).
-- Applied idempotently on server startup.

create extension if not exists vector;       -- pgvector, for later semantic search
create extension if not exists pgcrypto;     -- gen_random_uuid()

-- ---------------------------------------------------------------------------
-- documents: the index. Content lives in the blob store, keyed by content_key (sha256).
-- ---------------------------------------------------------------------------
create table if not exists documents (
  id            uuid primary key default gen_random_uuid(),
  slug          text unique not null,
  title         text not null default '',
  kind          text not null default 'note',    -- note | spec | task | ...
  content_key   text,                              -- blob key (sha256), null until first write
  content_hash  text,                              -- sha256 hex of current content
  content_type  text not null default 'text/markdown',
  size_bytes    bigint not null default 0,
  tags          text[] not null default '{}',
  metadata      jsonb not null default '{}',
  fts           tsvector,                          -- keyword search
  embedding     vector(1536),                      -- optional, semantic search
  version       integer not null default 0,        -- bumped on every content write (optimistic CAS)
  created_by    text,
  updated_by    text,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create index if not exists documents_fts_idx   on documents using gin (fts);
create index if not exists documents_tags_idx  on documents using gin (tags);
create index if not exists documents_kind_idx  on documents (kind);

-- ---------------------------------------------------------------------------
-- document_revisions: immutable history. Blobs are retained because keys are
-- content-hashes, so old versions remain fetchable from the blob store.
-- ---------------------------------------------------------------------------
create table if not exists document_revisions (
  id           bigserial primary key,
  document_id  uuid not null references documents(id) on delete cascade,
  version      integer not null,
  content_key  text not null,
  content_hash text not null,
  size_bytes   bigint not null default 0,
  author       text,
  created_at   timestamptz not null default now(),
  unique (document_id, version)
);

-- ---------------------------------------------------------------------------
-- doc_locks: lease-based "who is writing this doc right now". TTL + heartbeat;
-- a dead agent's lease auto-expires. One live lease per document.
-- ---------------------------------------------------------------------------
create table if not exists doc_locks (
  document_id  uuid primary key references documents(id) on delete cascade,
  owner        text not null,                       -- agent id
  lease_token  uuid not null default gen_random_uuid(),
  reason       text,
  acquired_at  timestamptz not null default now(),
  renewed_at   timestamptz not null default now(),
  expires_at   timestamptz not null
);

-- ---------------------------------------------------------------------------
-- tasks: simple work queue for agent coordination. Claim with SKIP LOCKED.
-- ---------------------------------------------------------------------------
create table if not exists tasks (
  id          uuid primary key default gen_random_uuid(),
  title       text not null default '',
  status      text not null default 'open',         -- open | claimed | done | failed
  claimed_by  text,
  claimed_at  timestamptz,
  payload     jsonb not null default '{}',
  result      jsonb,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);
create index if not exists tasks_status_idx on tasks (status, created_at);
