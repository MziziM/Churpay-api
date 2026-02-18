create table if not exists church_onboarding_requests (
  id uuid primary key default gen_random_uuid(),
  church_name text not null,
  requested_join_code text,
  admin_full_name text not null,
  admin_phone text not null,
  admin_email text not null,
  cipc_document bytea not null,
  cipc_filename text not null,
  cipc_mime text not null,
  bank_confirmation_document bytea not null,
  bank_confirmation_filename text not null,
  bank_confirmation_mime text not null,
  verification_status text not null default 'pending',
  verification_note text,
  verified_by text,
  verified_at timestamptz,
  approved_church_id uuid references churches(id) on delete set null,
  approved_admin_member_id uuid references members(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists church_onboarding_requests
  add constraint church_onboarding_requests_verification_status_valid
    check (verification_status in ('pending', 'approved', 'rejected')) not valid;
alter table if exists church_onboarding_requests
  validate constraint church_onboarding_requests_verification_status_valid;

create index if not exists idx_church_onboarding_status_created
  on church_onboarding_requests (verification_status, created_at desc);

create index if not exists idx_church_onboarding_admin_email
  on church_onboarding_requests (lower(admin_email));

create index if not exists idx_church_onboarding_approved_church
  on church_onboarding_requests (approved_church_id);
