create table if not exists public_contact_messages (
  id uuid primary key default gen_random_uuid(),
  full_name text not null,
  church_name text,
  email text not null,
  phone text,
  message text not null,
  source text not null default 'website',
  created_at timestamptz not null default now()
);

create index if not exists idx_public_contact_messages_created_at
  on public_contact_messages (created_at desc);
