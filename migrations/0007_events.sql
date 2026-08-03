-- Change feed: monotonic event stream for document and task mutations.
--
-- Applied idempotently on startup.

create table if not exists events (
  id          bigserial primary key,
  ts          timestamptz not null default now(),
  workspace   text        not null,
  kind        text        not null,
  doc_id      uuid,
  slug        text,
  actor       text,
  version     int,
  task_id     text
);
create index if not exists events_ws_id_idx on events (workspace, id);

grant select, insert on events to tracker_agent;
grant usage, select on all sequences in schema public to tracker_agent;

alter table events enable row level security;
alter table events force  row level security;

drop policy if exists ws_isolation on events;
create policy ws_isolation on events
  using      (workspace = current_setting('app.workspace', true)
              or current_setting('app.workspace', true) = '*')
  with check (workspace = current_setting('app.workspace', true));
