alter table members
  add column if not exists date_of_birth date;

create index if not exists idx_members_date_of_birth
  on members(date_of_birth);
