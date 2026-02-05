-- Super admin provisioning tables and constraints
create extension if not exists pgcrypto;

create table if not exists churches (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  created_at timestamptz not null default now()
);

create table if not exists admins (
  id uuid primary key default gen_random_uuid(),
  church_id uuid not null references churches(id) on delete cascade,
  name text not null,
  phone text,
  pin_hash text not null,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

-- Ensure funds table has church_id and unique code per church
alter table funds add column if not exists church_id uuid;
alter table funds add column if not exists code text;

-- Backfill church_id nulls is out of scope here; enforce uniqueness for new data
do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'funds_church_code_unique'
  ) then
    alter table funds add constraint funds_church_code_unique unique (church_id, code);
  end if;
end$$;

create index if not exists idx_admins_church on admins(church_id);
create index if not exists idx_funds_church on funds(church_id);
