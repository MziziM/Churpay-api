alter table if exists members
  add column if not exists email_verified boolean;
alter table if exists members
  add column if not exists email_verified_at timestamptz;
alter table if exists members
  add column if not exists email_verification_token_hash text;
alter table if exists members
  add column if not exists email_verification_code_hash text;
alter table if exists members
  add column if not exists email_verification_expires_at timestamptz;
alter table if exists members
  add column if not exists email_verification_sent_at timestamptz;

update members
set email_verified = true
where email_verified is null;

alter table if exists members
  alter column email_verified set default false;
alter table if exists members
  alter column email_verified set not null;

create index if not exists idx_members_email_verified
  on members (email_verified);

create index if not exists idx_members_email_verification_expires
  on members (email_verification_expires_at);

alter table if exists church_onboarding_requests
  add column if not exists admin_email_verified boolean;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verified_at timestamptz;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verification_token_hash text;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verification_code_hash text;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verification_expires_at timestamptz;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verification_sent_at timestamptz;
alter table if exists church_onboarding_requests
  add column if not exists admin_email_verification_attempts integer;

update church_onboarding_requests
set admin_email_verified = false
where admin_email_verified is null;

update church_onboarding_requests
set
  admin_email_verified = true,
  admin_email_verified_at = coalesce(admin_email_verified_at, verified_at, created_at)
where verification_status = 'approved'
  and coalesce(admin_email_verified, false) = false;

update church_onboarding_requests
set admin_email_verification_attempts = 0
where admin_email_verification_attempts is null;

alter table if exists church_onboarding_requests
  alter column admin_email_verified set default false;
alter table if exists church_onboarding_requests
  alter column admin_email_verified set not null;
alter table if exists church_onboarding_requests
  alter column admin_email_verification_attempts set default 0;
alter table if exists church_onboarding_requests
  alter column admin_email_verification_attempts set not null;

alter table if exists church_onboarding_requests
  add constraint church_onboarding_email_verification_attempts_nonnegative
    check (admin_email_verification_attempts >= 0) not valid;
alter table if exists church_onboarding_requests
  validate constraint church_onboarding_email_verification_attempts_nonnegative;

create index if not exists idx_church_onboarding_admin_email_verified
  on church_onboarding_requests (admin_email_verified, created_at desc);

create index if not exists idx_church_onboarding_admin_email_verification_expires
  on church_onboarding_requests (admin_email_verification_expires_at);
