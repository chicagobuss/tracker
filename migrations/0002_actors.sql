-- actors: registry of entities (agents/humans) acting on the store. Auto-upserted
-- on every mutating request. "Last changes by entity" is read from
-- document_revisions; this table gives a cheap single last_seen + activity count
-- across all action types, and lists entities even before their first write.
create table if not exists actors (
  name         text primary key,
  first_seen   timestamptz not null default now(),
  last_seen    timestamptz not null default now(),
  action_count bigint not null default 0
);
create index if not exists actors_last_seen_idx on actors (last_seen desc);
