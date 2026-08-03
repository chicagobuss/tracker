-- Commit-order watermark for change feed events.
--
-- pg_current_xact_id() stamps each event with its transaction's xid8.
-- Reads filter by `xact_id < pg_snapshot_xmin(pg_current_snapshot())` so an event
-- only becomes visible once all older transactions have committed or aborted,
-- preventing sequence allocation order from skipping events.

alter table events add column if not exists xact_id xid8 not null default pg_current_xact_id();
create index if not exists events_ws_xact_id_idx on events (workspace, xact_id, id);
