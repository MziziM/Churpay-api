alter table if exists payment_intents
  add column if not exists platform_fee_amount numeric(12,2),
  add column if not exists platform_fee_pct numeric(6,4) default 0.0075,
  add column if not exists platform_fee_fixed numeric(12,2) default 2.50,
  add column if not exists amount_gross numeric(12,2),
  add column if not exists superadmin_cut_amount numeric(12,2),
  add column if not exists superadmin_cut_pct numeric(6,4) default 1.0;

alter table if exists transactions
  add column if not exists platform_fee_amount numeric(12,2),
  add column if not exists platform_fee_pct numeric(6,4) default 0.0075,
  add column if not exists platform_fee_fixed numeric(12,2) default 2.50,
  add column if not exists amount_gross numeric(12,2),
  add column if not exists superadmin_cut_amount numeric(12,2),
  add column if not exists superadmin_cut_pct numeric(6,4) default 1.0;

update payment_intents
set
  platform_fee_pct = coalesce(platform_fee_pct, 0.0075),
  platform_fee_fixed = coalesce(platform_fee_fixed, 2.50),
  platform_fee_amount = coalesce(platform_fee_amount, round((2.50 + amount * 0.0075)::numeric, 2)),
  amount_gross = coalesce(amount_gross, round((amount + coalesce(platform_fee_amount, (2.50 + amount * 0.0075)))::numeric, 2)),
  superadmin_cut_pct = coalesce(superadmin_cut_pct, 1.0),
  superadmin_cut_amount = coalesce(superadmin_cut_amount, round((coalesce(platform_fee_amount, (2.50 + amount * 0.0075)) * coalesce(superadmin_cut_pct, 1.0))::numeric, 2))
where
  platform_fee_amount is null
  or amount_gross is null
  or superadmin_cut_amount is null
  or platform_fee_pct is null
  or platform_fee_fixed is null
  or superadmin_cut_pct is null;

update transactions
set
  platform_fee_pct = coalesce(platform_fee_pct, 0.0075),
  platform_fee_fixed = coalesce(platform_fee_fixed, 2.50),
  platform_fee_amount = coalesce(platform_fee_amount, round((2.50 + amount * 0.0075)::numeric, 2)),
  amount_gross = coalesce(amount_gross, round((amount + coalesce(platform_fee_amount, (2.50 + amount * 0.0075)))::numeric, 2)),
  superadmin_cut_pct = coalesce(superadmin_cut_pct, 1.0),
  superadmin_cut_amount = coalesce(superadmin_cut_amount, round((coalesce(platform_fee_amount, (2.50 + amount * 0.0075)) * coalesce(superadmin_cut_pct, 1.0))::numeric, 2))
where
  platform_fee_amount is null
  or amount_gross is null
  or superadmin_cut_amount is null
  or platform_fee_pct is null
  or platform_fee_fixed is null
  or superadmin_cut_pct is null;
