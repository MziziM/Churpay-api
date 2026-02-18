create extension if not exists pgcrypto;

create table if not exists job_adverts (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  slug text not null,
  church_id uuid references churches(id) on delete set null,
  employment_type text not null default 'FULL_TIME',
  location text not null default 'South Africa',
  department text,
  summary text,
  description text not null,
  requirements text,
  application_url text,
  application_email text,
  status text not null default 'DRAFT',
  published_at timestamptz,
  expires_at timestamptz,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_job_adverts_status check (status in ('DRAFT', 'PUBLISHED', 'CLOSED')),
  constraint chk_job_adverts_employment_type check (employment_type in ('FULL_TIME', 'PART_TIME', 'CONTRACT', 'INTERNSHIP', 'VOLUNTEER'))
);

create unique index if not exists idx_job_adverts_slug_unique
  on job_adverts (lower(slug));

create index if not exists idx_job_adverts_status_published
  on job_adverts (status, published_at desc, created_at desc);

create index if not exists idx_job_adverts_church
  on job_adverts (church_id);

create index if not exists idx_job_adverts_expires
  on job_adverts (expires_at);
