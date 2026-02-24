alter table if exists payment_intents
  add column if not exists provider text;

alter table if exists payment_intents
  add column if not exists provider_intent_ref text;

alter table if exists payment_intents
  add column if not exists status text;

alter table if exists payment_intents
  add column if not exists source text;

alter table if exists payment_intents
  add column if not exists church_id uuid;

alter table if exists payment_intents
  add column if not exists amount numeric(12,2);

alter table if exists payment_intents
  add column if not exists currency text;

alter table if exists payment_intents
  add column if not exists metadata jsonb not null default '{}'::jsonb;

alter table if exists payment_intents
  add column if not exists created_at timestamptz;

alter table if exists payment_intents
  add column if not exists updated_at timestamptz;

update payment_intents
set
  provider = coalesce(nullif(trim(provider), ''), 'payfast'),
  status = upper(coalesce(nullif(trim(status), ''), 'PENDING')),
  source = coalesce(nullif(trim(source), ''), 'DIRECT_APP'),
  currency = coalesce(nullif(trim(currency), ''), 'ZAR'),
  provider_intent_ref = coalesce(nullif(trim(provider_intent_ref), ''), nullif(trim(m_payment_id), '')),
  metadata = coalesce(metadata, '{}'::jsonb),
  created_at = coalesce(created_at, now()),
  updated_at = coalesce(updated_at, coalesce(created_at, now()))
where
  provider is null
  or status is null
  or source is null
  or currency is null
  or provider_intent_ref is null
  or metadata is null
  or created_at is null
  or updated_at is null;

create unique index if not exists idx_payment_intents_provider_intent_ref_unique
  on payment_intents (provider, provider_intent_ref)
  where provider_intent_ref is not null and btrim(provider_intent_ref) <> '';

create index if not exists idx_payment_intents_provider_status_updated
  on payment_intents (provider, status, updated_at desc);

create index if not exists idx_payment_intents_source_created
  on payment_intents (source, created_at desc);
