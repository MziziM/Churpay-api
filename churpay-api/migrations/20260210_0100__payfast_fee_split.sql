-- Option 3 fee split (PayFast fee is a church cost; member pays Churpay processing fee).
--
-- Member charged: amount_gross = amount + platform_fee_amount
-- PayFast fee (provider): payfast_fee_amount (positive)
-- Church net: church_net_amount = max(0, amount - payfast_fee_amount)

alter table if exists payment_intents
  add column if not exists payfast_fee_amount numeric(12,2),
  add column if not exists church_net_amount numeric(12,2);

alter table if exists transactions
  add column if not exists payfast_fee_amount numeric(12,2),
  add column if not exists church_net_amount numeric(12,2);

-- For non-PayFast providers (cash/manual/simulated), default is zero provider fee and full net amount.
update payment_intents
set
  payfast_fee_amount = coalesce(payfast_fee_amount, 0),
  church_net_amount = coalesce(church_net_amount, amount)
where
  coalesce(provider,'') <> 'payfast'
  and (payfast_fee_amount is null or church_net_amount is null);

update transactions
set
  payfast_fee_amount = coalesce(payfast_fee_amount, 0),
  church_net_amount = coalesce(church_net_amount, amount)
where
  coalesce(provider,'') <> 'payfast'
  and (payfast_fee_amount is null or church_net_amount is null);

-- Defensive money constraints (non-negative values).
alter table if exists payment_intents
  add constraint payment_intents_payfast_fee_nonnegative
    check (coalesce(payfast_fee_amount, 0) >= 0) not valid;
alter table if exists payment_intents
  validate constraint payment_intents_payfast_fee_nonnegative;

alter table if exists payment_intents
  add constraint payment_intents_church_net_nonnegative
    check (coalesce(church_net_amount, 0) >= 0) not valid;
alter table if exists payment_intents
  validate constraint payment_intents_church_net_nonnegative;

alter table if exists transactions
  add constraint transactions_payfast_fee_nonnegative
    check (coalesce(payfast_fee_amount, 0) >= 0) not valid;
alter table if exists transactions
  validate constraint transactions_payfast_fee_nonnegative;

alter table if exists transactions
  add constraint transactions_church_net_nonnegative
    check (coalesce(church_net_amount, 0) >= 0) not valid;
alter table if exists transactions
  validate constraint transactions_church_net_nonnegative;

