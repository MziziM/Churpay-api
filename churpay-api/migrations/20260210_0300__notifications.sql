-- In-app notifications + push tokens (Expo push)

create extension if not exists pgcrypto;

create table if not exists notifications (
  id uuid primary key default gen_random_uuid(),
  member_id uuid not null references members(id) on delete cascade,
  type text not null,
  title text not null,
  body text not null,
  data jsonb,
  created_at timestamptz not null default now(),
  read_at timestamptz,
  push_sent_at timestamptz
);

create index if not exists idx_notifications_member_created
  on notifications (member_id, created_at desc);

create index if not exists idx_notifications_member_unread
  on notifications (member_id)
  where read_at is null;

create table if not exists push_tokens (
  id uuid primary key default gen_random_uuid(),
  member_id uuid not null references members(id) on delete cascade,
  token text not null,
  platform text,
  device_id text,
  created_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  revoked_at timestamptz
);

create unique index if not exists idx_push_tokens_token_unique
  on push_tokens (token);

create index if not exists idx_push_tokens_member_last_seen
  on push_tokens (member_id, last_seen_at desc);

