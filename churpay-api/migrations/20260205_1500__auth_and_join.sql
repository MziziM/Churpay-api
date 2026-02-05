create extension if not exists pgcrypto;

-- Core members table for auth
create table if not exists members (
  id uuid primary key default gen_random_uuid(),
  full_name text not null,
  phone text,
  email text,
  password_hash text not null,
  role text not null default 'member',
  church_id uuid references churches(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint members_role_valid check (role in ('member','admin'))
);

-- Ensure existing tables gain required columns
alter table members add column if not exists church_id uuid references churches(id) on delete set null;
alter table members add column if not exists role text not null default 'member';
alter table members add column if not exists phone text;
alter table members add column if not exists email text;
alter table members add column if not exists full_name text;

-- Unique constraints (optional fields guarded by partial indexes)
create unique index if not exists idx_members_phone_unique on members (phone) where phone is not null;
create unique index if not exists idx_members_email_unique on members (lower(email)) where email is not null;
create index if not exists idx_members_church on members (church_id);

-- Join codes for churches
alter table churches add column if not exists join_code text;
create unique index if not exists idx_churches_join_code on churches (join_code);

-- Seed join_code for the Great Commission Church of Christ if present
update churches
set join_code = coalesce(join_code, 'GCCOC-1234')
where lower(name) = 'great commission church of christ';

-- Backfill any missing join codes with deterministic short tokens
update churches
set join_code = 'CH' || upper(substr(md5(random()::text), 1, 6))
where join_code is null;

-- Ensure funds table exists for fresh setups
create table if not exists funds (
  id uuid primary key default gen_random_uuid(),
  church_id uuid not null references churches(id) on delete cascade,
  code text not null,
  name text not null,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

-- Unique code per church
alter table funds add column if not exists church_id uuid;
alter table funds add column if not exists code text;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'funds_church_code_unique'
  ) then
    alter table funds add constraint funds_church_code_unique unique (church_id, code);
  end if;
end$$;

create index if not exists idx_funds_church on funds(church_id);
