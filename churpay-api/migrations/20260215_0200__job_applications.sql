create extension if not exists pgcrypto;

-- Public job applications submitted via churpay.com with CV attachments.
create table if not exists job_applications (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references job_adverts(id) on delete cascade,
  full_name text not null,
  email text not null,
  phone text,
  message text,
  cv_document bytea not null,
  cv_filename text not null,
  cv_mime text not null,
  cv_download_token_hash text not null,
  cv_download_expires_at timestamptz not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_job_applications_job_created
  on job_applications (job_id, created_at desc);

create index if not exists idx_job_applications_created
  on job_applications (created_at desc);

