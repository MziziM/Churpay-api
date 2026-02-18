-- Stabilization: constraints + indexes for payment integrity and query performance.

-- Idempotency guard for PayFast references.
create unique index if not exists idx_payment_intents_m_payment_id_unique
  on payment_intents (m_payment_id)
  where m_payment_id is not null and btrim(m_payment_id) <> '';

-- One transaction row per payment intent.
create unique index if not exists idx_transactions_payment_intent_unique
  on transactions (payment_intent_id)
  where payment_intent_id is not null;

-- Fast lookup by reference and common admin/super filtering fields.
create index if not exists idx_transactions_reference
  on transactions (reference);

create index if not exists idx_transactions_church_created_at
  on transactions (church_id, created_at desc);

create index if not exists idx_transactions_fund_created_at
  on transactions (fund_id, created_at desc);

create index if not exists idx_payment_intents_status_updated
  on payment_intents (status, updated_at desc);

create index if not exists idx_payment_intents_church_created
  on payment_intents (church_id, created_at desc);

-- Defensive money constraints (non-negative values).
alter table if exists payment_intents
  add constraint payment_intents_amount_nonnegative
    check (amount >= 0) not valid;
alter table if exists payment_intents
  validate constraint payment_intents_amount_nonnegative;

alter table if exists payment_intents
  add constraint payment_intents_amount_gross_nonnegative
    check (coalesce(amount_gross, 0) >= 0) not valid;
alter table if exists payment_intents
  validate constraint payment_intents_amount_gross_nonnegative;

alter table if exists payment_intents
  add constraint payment_intents_platform_fee_nonnegative
    check (coalesce(platform_fee_amount, 0) >= 0) not valid;
alter table if exists payment_intents
  validate constraint payment_intents_platform_fee_nonnegative;

alter table if exists transactions
  add constraint transactions_amount_nonnegative
    check (amount >= 0) not valid;
alter table if exists transactions
  validate constraint transactions_amount_nonnegative;

alter table if exists transactions
  add constraint transactions_amount_gross_nonnegative
    check (coalesce(amount_gross, 0) >= 0) not valid;
alter table if exists transactions
  validate constraint transactions_amount_gross_nonnegative;

alter table if exists transactions
  add constraint transactions_platform_fee_nonnegative
    check (coalesce(platform_fee_amount, 0) >= 0) not valid;
alter table if exists transactions
  validate constraint transactions_platform_fee_nonnegative;
