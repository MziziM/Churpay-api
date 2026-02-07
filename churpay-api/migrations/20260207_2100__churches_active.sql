alter table churches
  add column if not exists active boolean not null default true;

create index if not exists idx_churches_active on churches(active);
