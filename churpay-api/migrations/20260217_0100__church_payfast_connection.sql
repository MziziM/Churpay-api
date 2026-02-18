-- Church-level PayFast credential connection (non-custodial model).
-- NOTE: merchant key + passphrase are encrypted by the app before persisting.

alter table churches
  add column if not exists payfast_merchant_id text,
  add column if not exists payfast_merchant_key text,
  add column if not exists payfast_passphrase text,
  add column if not exists payfast_connected boolean not null default false,
  add column if not exists payfast_connected_at timestamptz,
  add column if not exists payfast_last_connect_attempt_at timestamptz,
  add column if not exists payfast_last_connect_status text,
  add column if not exists payfast_last_connect_error text,
  add column if not exists payfast_disconnected_at timestamptz;

create index if not exists idx_churches_payfast_connected
  on churches (payfast_connected, payfast_connected_at desc);

create index if not exists idx_churches_payfast_last_attempt
  on churches (payfast_last_connect_attempt_at desc);

alter table churches
  add constraint churches_payfast_last_connect_status_valid
  check (
    payfast_last_connect_status is null
    or payfast_last_connect_status in ('connected', 'failed', 'disconnected')
  ) not valid;

alter table churches
  validate constraint churches_payfast_last_connect_status_valid;

-- Settlement policy disclosure update (preserve super-admin edits).
update legal_documents
set
  body = trim(trailing E'\n' from body) ||
    E'\n\nEach church maintains its own PayFast merchant account and banking configuration. Churpay does not hold or intermediate donor funds.',
  version = version + 1,
  updated_at = now()
where
  doc_key = 'payfast_fees'
  and updated_by = 'seed'
  and position(
    'Each church maintains its own PayFast merchant account and banking configuration. Churpay does not hold or intermediate donor funds.'
    in body
  ) = 0;
