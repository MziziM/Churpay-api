-- Editable legal/content documents + terms acceptance tracking

create table if not exists legal_documents (
  doc_key text primary key,
  title text not null,
  body text not null,
  version integer not null default 1,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_legal_documents_updated_at
  on legal_documents (updated_at desc);

alter table members
  add column if not exists terms_accepted_at timestamptz;
alter table members
  add column if not exists terms_version integer;

create index if not exists idx_members_terms_accepted_at
  on members (terms_accepted_at);

alter table churches
  add column if not exists terms_accepted_at timestamptz;
alter table churches
  add column if not exists terms_version integer;

create index if not exists idx_churches_terms_accepted_at
  on churches (terms_accepted_at);

alter table church_onboarding_requests
  add column if not exists terms_accepted_at timestamptz;
alter table church_onboarding_requests
  add column if not exists terms_version integer;

create index if not exists idx_church_onboarding_terms_accepted_at
  on church_onboarding_requests (terms_accepted_at);

insert into legal_documents (doc_key, title, body, version, updated_by)
values
  (
    'terms',
    'Terms and Conditions',
    'Churpay provides payment facilitation and giving operations tooling for churches.\n\nBy using this service, users agree to lawful use, accurate account information, and applicable payment provider terms.\n\nThese terms may be updated as product, legal, or regulatory requirements change.',
    1,
    'seed'
  ),
  (
    'payfast_fees',
    'PayFast fees (church payout/withdrawal)',
    'PayFast charges transaction fees and payout fees.\n\nImportant: PayFast fees are charged by PayFast and are deducted from the funds before payout/withdrawal. Churches are responsible for PayFast fees.\n\nSee the PayFast fee schedule: https://payfast.io/fees',
    1,
    'seed'
  )
on conflict (doc_key) do nothing;

