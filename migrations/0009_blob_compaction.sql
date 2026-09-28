-- Blob compaction (compact.go). Cold content blobs are re-encoded and packed
-- together through the configured BlobStore; these tables record where each
-- packed blob lives. They hold content keys and byte ranges only — no document
-- data and no workspace — so they sit outside row-level security.

create table if not exists blob_packs (
  key         text primary key,            -- packs/<sha256 of the pack's bytes>
  bytes       bigint not null,
  members     integer not null,
  raw_bytes   bigint not null,             -- sum of the members' original sizes
  created_at  timestamptz not null default now()
);

-- Dictionaries trained on this store's own blobs, stored as blobs themselves.
create table if not exists blob_dicts (
  key         text primary key,            -- dicts/<sha256 of the dictionary>
  bytes       integer not null,
  samples     integer not null,
  created_at  timestamptz not null default now()
);

create table if not exists blob_locations (
  content_key      text primary key,       -- sha256/<hex>, as in document_revisions
  pack_key         text not null references blob_packs(key),
  pack_offset      bigint not null,
  pack_length      bigint not null,
  codec            text not null,          -- raw | zstd | delta
  dict_key         text references blob_dicts(key),  -- zstd: the trained dictionary, if any
  base_key         text,                   -- delta: the content key whose bytes are the dictionary
  depth            integer not null default 0,        -- deltas to decode to reach this blob
  raw_size         bigint not null,
  packed_at        timestamptz not null default now(),
  loose_deleted_at timestamptz             -- set once the original loose blob is removed
);
create index if not exists blob_locations_pack on blob_locations (pack_key);
create index if not exists blob_locations_reclaim on blob_locations (packed_at) where loose_deleted_at is null;

-- One row: the operator's controls. Persisted so a restart never silently
-- resumes a paused compactor or forgets a granted budget.
create table if not exists compaction_control (
  id          integer primary key default 1 check (id = 1),
  paused      boolean not null default false,
  budget      bigint not null default 0,   -- blobs still to pack in manual mode
  updated_at  timestamptz not null default now(),
  updated_by  text
);
insert into compaction_control (id) values (1) on conflict do nothing;

-- Request handlers read content through the connection Scoped pinned for them,
-- which runs as tracker_agent; they need to find packed blobs, and
-- migrate-blobs (also scoped) lists packs and dictionaries to copy them.
grant select on blob_locations, blob_packs, blob_dicts to tracker_agent;
