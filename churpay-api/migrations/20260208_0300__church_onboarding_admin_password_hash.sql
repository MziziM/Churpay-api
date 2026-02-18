alter table if exists church_onboarding_requests
  add column if not exists admin_password_hash text;

create index if not exists idx_church_onboarding_admin_password_hash_present
  on church_onboarding_requests ((admin_password_hash is not null));
