create extension if not exists pgcrypto;

create table if not exists auth_login_challenges (
  id uuid primary key default gen_random_uuid(),
  role text not null,
  member_id uuid references members(id) on delete cascade,
  identifier text,
  email text not null,
  code_hash text not null,
  token_hash text not null,
  attempts integer not null default 0,
  max_attempts integer not null default 5,
  expires_at timestamptz not null,
  consumed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists auth_login_challenges
  add constraint auth_login_challenges_role_valid
    check (role in ('admin', 'super')) not valid;

alter table if exists auth_login_challenges
  validate constraint auth_login_challenges_role_valid;

alter table if exists auth_login_challenges
  add constraint auth_login_challenges_attempts_nonnegative
    check (attempts >= 0 and max_attempts > 0 and attempts <= max_attempts + 1) not valid;

alter table if exists auth_login_challenges
  validate constraint auth_login_challenges_attempts_nonnegative;

create index if not exists idx_auth_login_challenges_role_email_created
  on auth_login_challenges (role, lower(email), created_at desc);

create index if not exists idx_auth_login_challenges_member_created
  on auth_login_challenges (member_id, created_at desc);

create index if not exists idx_auth_login_challenges_expires
  on auth_login_challenges (expires_at);
