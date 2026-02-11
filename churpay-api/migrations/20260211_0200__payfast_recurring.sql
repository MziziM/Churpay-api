-- PayFast recurring givings (subscription-based donations)

create table if not exists recurring_givings (
  id uuid primary key default gen_random_uuid(),
  member_id uuid not null references members(id) on delete cascade,
  church_id uuid not null references churches(id) on delete cascade,
  fund_id uuid not null references funds(id) on delete restrict,
  status text not null default 'PENDING_SETUP',
  frequency integer not null,
  cycles integer not null default 0,
  cycles_completed integer not null default 0,
  billing_date date not null,
  donation_amount numeric(12,2) not null,
  platform_fee_amount numeric(12,2) not null default 0,
  gross_amount numeric(12,2) not null,
  currency text not null default 'ZAR',
  payfast_token text,
  setup_payment_intent_id uuid references payment_intents(id) on delete set null,
  setup_m_payment_id text,
  notes text,
  last_charged_at timestamptz,
  next_billing_date date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  cancelled_at timestamptz
);

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'recurring_givings_status_check') then
    alter table recurring_givings
      add constraint recurring_givings_status_check
      check (status in ('PENDING_SETUP','ACTIVE','PAUSED','CANCELLED','FAILED','COMPLETED'));
  end if;

  if not exists (select 1 from pg_constraint where conname = 'recurring_givings_frequency_check') then
    alter table recurring_givings
      add constraint recurring_givings_frequency_check
      check (frequency in (1,2,3,4,5,6));
  end if;

  if not exists (select 1 from pg_constraint where conname = 'recurring_givings_cycles_nonnegative') then
    alter table recurring_givings
      add constraint recurring_givings_cycles_nonnegative
      check (cycles >= 0 and cycles_completed >= 0);
  end if;

  if not exists (select 1 from pg_constraint where conname = 'recurring_givings_amounts_nonnegative') then
    alter table recurring_givings
      add constraint recurring_givings_amounts_nonnegative
      check (
        coalesce(donation_amount, 0) >= 0
        and coalesce(platform_fee_amount, 0) >= 0
        and coalesce(gross_amount, 0) >= 0
      );
  end if;
end$$;

create unique index if not exists idx_recurring_givings_payfast_token_unique
  on recurring_givings(payfast_token)
  where payfast_token is not null and payfast_token <> '';

create index if not exists idx_recurring_givings_member_created
  on recurring_givings(member_id, created_at desc);

create index if not exists idx_recurring_givings_church_status
  on recurring_givings(church_id, status, created_at desc);

alter table if exists payment_intents
  add column if not exists recurring_giving_id uuid references recurring_givings(id) on delete set null,
  add column if not exists recurring_cycle_no integer;

alter table if exists transactions
  add column if not exists recurring_giving_id uuid references recurring_givings(id) on delete set null,
  add column if not exists recurring_cycle_no integer;

create index if not exists idx_payment_intents_recurring_giving
  on payment_intents(recurring_giving_id, created_at desc);

create index if not exists idx_transactions_recurring_giving
  on transactions(recurring_giving_id, created_at desc);

do $$
begin
  if exists (select 1 from pg_constraint where conname = 'payment_intents_source_check') then
    alter table payment_intents drop constraint payment_intents_source_check;
  end if;
end$$;

do $$
begin
  alter table payment_intents
    add constraint payment_intents_source_check
    check (source is null or source in ('DIRECT_APP','PUBLIC_GIVE','SHARE_LINK','CASH','RECURRING'));
exception
  when duplicate_object then null;
end$$;
