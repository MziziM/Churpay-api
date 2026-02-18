-- Add accountant role + admin portal per-church settings

-- 1) Extend members.role constraint to allow 'accountant'
alter table members
  drop constraint if exists members_role_valid;

alter table members
  add constraint members_role_valid
  check (role in ('member', 'admin', 'accountant'))
  not valid;

alter table members
  validate constraint members_role_valid;

-- 2) Store per-church admin portal settings (role-based tab visibility, etc.)
alter table churches
  add column if not exists admin_portal_settings jsonb not null default '{}'::jsonb;

