-- tasks.attempts: how many times a task has been claimed (a claim on an
-- expired-claim task re-claims it and bumps this). Applied idempotently.
alter table tasks add column if not exists attempts integer not null default 0;
