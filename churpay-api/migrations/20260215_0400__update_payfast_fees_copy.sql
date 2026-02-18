-- Update seeded PayFast fee disclosure copy.
-- Only update the original seeded v1 document so super-admin edits are preserved.

update legal_documents
set
  title = 'PayFast fees (charged by PayFast)',
  body = $body$
PayFast fees are charged by PayFast and may change over time. Always refer to the official PayFast fee schedule for the latest fees:
https://payfast.io/fees

Examples from PayFast (excl. VAT, subject to change):
- Card payments: 3.2% + R2.00 per transaction.
- Standard payout/withdrawal: R8.70 per payout request.
- Immediate payout: 0.8% (minimum R14.00).

How this affects your church on Churpay:
- Churpay charges a processing fee on top of the donation amount (paid by the donor).
- The PayFast transaction fee is deducted from the donation amount before payout/withdrawal.
- The church net received is Donation amount minus PayFast fee.
$body$,
  version = 2,
  updated_at = now()
where
  doc_key = 'payfast_fees'
  and version = 1
  and updated_by = 'seed';

-- If the doc didn't exist for some reason, create it.
insert into legal_documents (doc_key, title, body, version, updated_by)
select
  'payfast_fees',
  'PayFast fees (charged by PayFast)',
  $body$
PayFast fees are charged by PayFast and may change over time. Always refer to the official PayFast fee schedule for the latest fees:
https://payfast.io/fees

Examples from PayFast (excl. VAT, subject to change):
- Card payments: 3.2% + R2.00 per transaction.
- Standard payout/withdrawal: R8.70 per payout request.
- Immediate payout: 0.8% (minimum R14.00).

How this affects your church on Churpay:
- Churpay charges a processing fee on top of the donation amount (paid by the donor).
- The PayFast transaction fee is deducted from the donation amount before payout/withdrawal.
- The church net received is Donation amount minus PayFast fee.
$body$,
  1,
  'seed'
where not exists (select 1 from legal_documents where doc_key = 'payfast_fees');

