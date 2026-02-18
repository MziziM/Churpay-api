-- Visitor/payer metadata for QR/public giving flow
alter table if exists payment_intents
  add column if not exists payer_name text,
  add column if not exists payer_phone text,
  add column if not exists payer_email text,
  add column if not exists payer_type text;

alter table if exists transactions
  add column if not exists payer_name text,
  add column if not exists payer_phone text,
  add column if not exists payer_email text,
  add column if not exists payer_type text;

