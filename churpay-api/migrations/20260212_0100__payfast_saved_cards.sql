-- PayFast "saved card" (tokenization + ad hoc charges)
-- Stores the PayFast token (adhoc agreement) on the member so future charges can be server-initiated.

alter table if exists members
  add column if not exists payfast_adhoc_token text,
  add column if not exists payfast_adhoc_token_created_at timestamptz,
  add column if not exists payfast_adhoc_token_revoked_at timestamptz;

create unique index if not exists idx_members_payfast_adhoc_token_unique
  on members(payfast_adhoc_token)
  where payfast_adhoc_token is not null and payfast_adhoc_token <> '';

alter table if exists payment_intents
  add column if not exists save_card_requested boolean not null default false;

-- Extend allowed payment_intents.source values for "saved card" charges.
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
      check (source is null or source in ('DIRECT_APP','PUBLIC_GIVE','SHARE_LINK','CASH','RECURRING','SAVED_CARD'));
exception
  when duplicate_object then null;
end$$;

