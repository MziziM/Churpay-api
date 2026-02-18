-- Track cookie consent during member registration and church onboarding.

alter table members
  add column if not exists cookie_consent_at timestamptz;
alter table members
  add column if not exists cookie_consent_version integer;

create index if not exists idx_members_cookie_consent_at
  on members (cookie_consent_at);

alter table churches
  add column if not exists cookie_consent_at timestamptz;
alter table churches
  add column if not exists cookie_consent_version integer;

create index if not exists idx_churches_cookie_consent_at
  on churches (cookie_consent_at);

alter table church_onboarding_requests
  add column if not exists cookie_consent_at timestamptz;
alter table church_onboarding_requests
  add column if not exists cookie_consent_version integer;

create index if not exists idx_church_onboarding_cookie_consent_at
  on church_onboarding_requests (cookie_consent_at);
