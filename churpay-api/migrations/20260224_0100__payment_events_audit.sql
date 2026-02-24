create table if not exists payment_events (
  id uuid primary key default gen_random_uuid(),
  intent_id uuid references payment_intents(id) on delete set null,
  provider text not null,
  provider_event_id text null,
  type text not null,
  status text not null,
  amount numeric(12,2) null,
  currency text null,
  church_id uuid null,
  source text null,
  payload jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  payload_hash text not null,
  occurred_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create unique index if not exists idx_payment_events_provider_event_unique
  on payment_events (provider, provider_event_id)
  where provider_event_id is not null;

create unique index if not exists idx_payment_events_fallback_unique
  on payment_events (provider, coalesce(intent_id::text, ''), type, occurred_at, payload_hash);

create index if not exists idx_payment_events_intent_created
  on payment_events (intent_id, created_at desc);

create index if not exists idx_payment_events_church_created
  on payment_events (church_id, created_at desc);
