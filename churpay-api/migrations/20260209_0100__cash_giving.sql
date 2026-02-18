-- Cash giving records (no payment processor).
-- Stored on payment_intents + transactions so existing dashboards/history can reuse joins.

alter table if exists payment_intents
  add column if not exists service_date date,
  add column if not exists notes text,
  add column if not exists cash_verified_by_admin boolean not null default false,
  add column if not exists cash_verified_by uuid references members(id) on delete set null,
  add column if not exists cash_verified_at timestamptz,
  add column if not exists cash_verification_note text;

