alter table if exists members
  add column if not exists last_active_at timestamptz;

alter table if exists members
  alter column last_active_at set default now();

update members
set last_active_at = coalesce(last_active_at, created_at, now())
where last_active_at is null;

create index if not exists idx_members_last_active_at
  on members(last_active_at desc nulls last);
