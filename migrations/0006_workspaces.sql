-- Workspaces: a hard partition of the store, so unrelated bodies of work (and
-- throwaway greenfield experiments) never pollute each other's search results
-- or an agent's context.
--
-- The separation is enforced by Postgres row-level security, not by application
-- WHERE clauses. Each request runs in a transaction with app.workspace set, and
-- the policies below filter every table to it. A query that forgets its
-- predicate therefore returns nothing instead of another workspace's rows, and
-- a write that targets the wrong workspace is rejected rather than misfiled.
--
-- Applied idempotently on startup like every other migration, so each statement
-- is written to survive being re-run.

-- ---------------------------------------------------------------------------
-- workspaces: the registry. Deliberately has no RLS of its own — it holds only
-- names, and listing them is how an agent discovers where it may work.
-- ---------------------------------------------------------------------------
create table if not exists workspaces (
  name        text primary key,
  description text not null default '',
  created_by  text,
  created_at  timestamptz not null default now()
);

insert into workspaces (name, description)
  values ('default', 'Everything that predates workspaces')
  on conflict (name) do nothing;

-- ---------------------------------------------------------------------------
-- Membership columns. Existing rows land in 'default', and a request that sends
-- no workspace resolves to 'default' too, so agents written before this change
-- keep seeing exactly what they saw.
-- ---------------------------------------------------------------------------
alter table documents add column if not exists workspace text not null default 'default';
alter table tasks     add column if not exists workspace text not null default 'default';
alter table actors    add column if not exists workspace text not null default 'default';

alter table documents drop constraint if exists documents_workspace_fkey;
alter table documents add  constraint documents_workspace_fkey
  foreign key (workspace) references workspaces(name) on update cascade;
alter table tasks drop constraint if exists tasks_workspace_fkey;
alter table tasks add  constraint tasks_workspace_fkey
  foreign key (workspace) references workspaces(name) on update cascade;
alter table actors drop constraint if exists actors_workspace_fkey;
alter table actors add  constraint actors_workspace_fkey
  foreign key (workspace) references workspaces(name) on update cascade;

-- Slugs are unique per workspace rather than globally: two experiments must both
-- be able to hold a doc called 'readme'.
alter table documents drop constraint if exists documents_slug_key;
create unique index if not exists documents_ws_slug_idx on documents (workspace, slug);
create index if not exists documents_ws_kind_idx on documents (workspace, kind);
create index if not exists tasks_ws_status_idx on tasks (workspace, status, created_at);

-- actors were keyed by name alone, but one agent acts in several workspaces and
-- should carry separate activity counts in each.
alter table actors drop constraint if exists actors_pkey;
create unique index if not exists actors_ws_name_idx on actors (workspace, name);

-- ---------------------------------------------------------------------------
-- tracker_agent: the role requests actually run as.
--
-- This exists because of a Postgres rule that quietly defeats the whole scheme:
-- a superuser bypasses row-level security unconditionally, and FORCE does not
-- change that. The stock postgres image makes POSTGRES_USER a superuser, so the
-- account tracker connects with is exactly the account policies do not apply to.
--
-- Rather than require operators to re-provision their database, each request
-- does SET ROLE to this unprivileged role for the duration of the connection
-- (and RESET ROLE on release). Migrations and DDL keep running as the real
-- connecting user, which is what they need.
-- ---------------------------------------------------------------------------
do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'tracker_agent') then
    create role tracker_agent nologin nosuperuser nobypassrls;
  end if;
end $$;

-- The connecting user must be a member to SET ROLE. Creating the role grants
-- admin option, so this succeeds for a non-superuser owner too.
grant tracker_agent to current_user;

grant usage on schema public to tracker_agent;
grant select, insert, update, delete
  on documents, document_revisions, doc_locks, tasks, actors to tracker_agent;
grant select, insert on workspaces to tracker_agent;
grant usage, select on all sequences in schema public to tracker_agent;

-- ---------------------------------------------------------------------------
-- Row-level security.
--
-- FORCE matters as much as ENABLE: tracker connects as the table owner, and an
-- owner bypasses its own policies unless the table forces them. Without those
-- lines every policy here would be decorative.
--
-- current_setting(..., true) yields NULL when unset, and `workspace = NULL` is
-- NULL rather than true — so an unscoped connection sees no rows and can write
-- none. The failure mode is empty results, never cross-workspace bleed.
-- ---------------------------------------------------------------------------
alter table documents enable row level security;
alter table documents force  row level security;
alter table tasks     enable row level security;
alter table tasks     force  row level security;
alter table actors    enable row level security;
alter table actors    force  row level security;
alter table document_revisions enable row level security;
alter table document_revisions force  row level security;
alter table doc_locks enable row level security;
alter table doc_locks force  row level security;

-- The '*' branch is the maintenance escape hatch: migrate-blobs must enumerate
-- every blob in the store, not one workspace's. It is unreachable from a request
-- because every caller-supplied name is checked against
-- ^[a-z0-9][a-z0-9_-]{0,62}$ before it ever reaches set_config.
--
-- CREATE POLICY has no IF NOT EXISTS, and migrations re-run on every boot.
drop policy if exists ws_isolation on documents;
create policy ws_isolation on documents
  using      (workspace = current_setting('app.workspace', true)
              or current_setting('app.workspace', true) = '*')
  with check (workspace = current_setting('app.workspace', true));

drop policy if exists ws_isolation on tasks;
create policy ws_isolation on tasks
  using      (workspace = current_setting('app.workspace', true)
              or current_setting('app.workspace', true) = '*')
  with check (workspace = current_setting('app.workspace', true));

drop policy if exists ws_isolation on actors;
create policy ws_isolation on actors
  using      (workspace = current_setting('app.workspace', true)
              or current_setting('app.workspace', true) = '*')
  with check (workspace = current_setting('app.workspace', true));

-- Revisions and locks carry no workspace column of their own: they are reachable
-- only through a document, so they inherit its scope. The subquery is itself
-- subject to the documents policy, which is what makes this safe — if the parent
-- document is invisible, so is every row hanging off it.
drop policy if exists ws_isolation on document_revisions;
create policy ws_isolation on document_revisions
  using      (exists (select 1 from documents d where d.id = document_revisions.document_id))
  with check (exists (select 1 from documents d where d.id = document_revisions.document_id));

drop policy if exists ws_isolation on doc_locks;
create policy ws_isolation on doc_locks
  using      (exists (select 1 from documents d where d.id = doc_locks.document_id))
  with check (exists (select 1 from documents d where d.id = doc_locks.document_id));
