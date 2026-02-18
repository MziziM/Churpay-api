create extension if not exists pgcrypto;

-- Multiple bank accounts per church (for onboarding review + future features).
create table if not exists church_bank_accounts (
  id uuid primary key default gen_random_uuid(),
  church_id uuid not null references churches(id) on delete cascade,
  bank_name text not null,
  account_name text not null,
  account_number text not null,
  branch_code text,
  account_type text,
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_church_bank_accounts_church
  on church_bank_accounts (church_id, created_at desc);

-- Allow at most one primary bank account per church.
create unique index if not exists idx_church_bank_accounts_primary_unique
  on church_bank_accounts (church_id)
  where is_primary = true;

-- Bank accounts provided during onboarding (before a church exists).
create table if not exists church_onboarding_bank_accounts (
  id uuid primary key default gen_random_uuid(),
  request_id uuid not null references church_onboarding_requests(id) on delete cascade,
  bank_name text not null,
  account_name text not null,
  account_number text not null,
  branch_code text,
  account_type text,
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_church_onboarding_bank_accounts_request
  on church_onboarding_bank_accounts (request_id, created_at desc);

-- Allow at most one primary bank account per onboarding request.
create unique index if not exists idx_church_onboarding_bank_accounts_primary_unique
  on church_onboarding_bank_accounts (request_id)
  where is_primary = true;

