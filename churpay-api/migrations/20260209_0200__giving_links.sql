-- Shareable giving links ("Give for someone")
-- Allows an external payer to complete a PayFast donation on behalf of a member.

create table if not exists giving_links (
  id uuid primary key default gen_random_uuid(),
  token text not null,
  requester_member_id uuid not null references members(id) on delete cascade,
  church_id uuid not null references churches(id) on delete cascade,
  fund_id uuid not null references funds(id) on delete cascade,
  amount_type text not null default 'FIXED',
  amount_fixed numeric(12,2),
  currency text not null default 'ZAR',
  message text,
  status text not null default 'ACTIVE',
  expires_at timestamptz not null,
  max_uses integer not null default 1,
  use_count integer not null default 0,
  paid_at timestamptz,
  paid_payment_intent_id uuid,
  created_at timestamptz not null default now()
);

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'giving_links_amount_type_check') then
    alter table giving_links add constraint giving_links_amount_type_check check (amount_type in ('FIXED','OPEN'));
  end if;
  if not exists (select 1 from pg_constraint where conname = 'giving_links_status_check') then
    alter table giving_links add constraint giving_links_status_check check (status in ('ACTIVE','PAID','EXPIRED','CANCELLED'));
  end if;
end$$;

create unique index if not exists idx_giving_links_token_unique on giving_links(token);
create index if not exists idx_giving_links_status_expires on giving_links(status, expires_at);
create index if not exists idx_giving_links_requester on giving_links(requester_member_id, created_at desc);

alter table if exists giving_links
  add column if not exists paid_payment_intent_id uuid references payment_intents(id) on delete set null;

alter table if exists payment_intents
  add column if not exists source text,
  add column if not exists giving_link_id uuid references giving_links(id) on delete set null,
  add column if not exists on_behalf_of_member_id uuid references members(id) on delete set null;

do $$
begin
  if not exists (select 1 from pg_constraint where conname = 'payment_intents_source_check') then
    alter table payment_intents add constraint payment_intents_source_check
      check (source is null or source in ('DIRECT_APP','PUBLIC_GIVE','SHARE_LINK','CASH'));
  end if;
end$$;

alter table if exists transactions
  add column if not exists giving_link_id uuid references giving_links(id) on delete set null,
  add column if not exists on_behalf_of_member_id uuid references members(id) on delete set null;

create index if not exists idx_transactions_on_behalf on transactions(on_behalf_of_member_id);
create index if not exists idx_transactions_giving_link on transactions(giving_link_id);

