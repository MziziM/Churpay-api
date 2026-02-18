-- Seed editable privacy policy document for super-admin Legal & content settings.

insert into legal_documents (doc_key, title, body, version, updated_by)
values
  (
    'privacy',
    'Privacy Policy',
    'Churpay respects and protects personal information in line with applicable privacy laws, including POPIA in South Africa.\n\nWe collect only the data required to provide giving services, account security, reporting, and support.\n\nPayment details are processed by approved payment providers and are not stored in plain form by Churpay.\n\nOur website and web app may use essential and analytics cookies to improve reliability, security, and user experience. Cookie choices can be managed from the website consent banner.\n\nWe use appropriate technical and organizational safeguards to protect personal information from unauthorized access, loss, or misuse.\n\nUsers may request updates or corrections to personal information through their church administrator or Churpay support.\n\nThis policy may be updated from time to time to reflect legal, security, or product changes.',
    1,
    'seed'
  )
on conflict (doc_key) do nothing;
