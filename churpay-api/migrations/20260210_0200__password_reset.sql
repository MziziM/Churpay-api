-- Password reset workflow (email code/token).
-- Stores hashed reset token/code with expiry on the member record.

alter table if exists members
  add column if not exists password_reset_token_hash text,
  add column if not exists password_reset_code_hash text,
  add column if not exists password_reset_expires_at timestamptz,
  add column if not exists password_reset_sent_at timestamptz,
  add column if not exists password_reset_used_at timestamptz;

