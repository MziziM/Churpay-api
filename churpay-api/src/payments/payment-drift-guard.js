import { db } from "../db.js";

const REQUIRED_PAYMENT_COLUMNS = [
  "provider",
  "provider_intent_ref",
  "status",
  "source",
  "church_id",
  "amount",
  "currency",
  "metadata",
  "created_at",
  "updated_at",
];

const REQUIRED_PAYMENT_EVENT_COLUMNS = [
  "provider",
  "provider_event_id",
  "type",
  "status",
  "payload",
  "metadata",
  "payload_hash",
  "occurred_at",
  "created_at",
];

const REQUIRED_MIGRATIONS = [
  "20260223_1300__payment_intents_source_event_ticket.sql",
  "20260224_0100__payment_events_audit.sql",
  "20260224_0200__payment_intents_provider_normalization.sql",
];

function normalize(value) {
  return String(value || "").trim();
}

async function hasTable(tableName) {
  const row = await db.one(
    `
    select exists (
      select 1
      from information_schema.tables
      where table_schema = 'public' and table_name = $1
    ) as ok
    `,
    [tableName]
  );
  return Boolean(row?.ok);
}

async function listColumns(tableName) {
  const rows = await db.manyOrNone(
    `
    select column_name
    from information_schema.columns
    where table_schema = 'public' and table_name = $1
    `,
    [tableName]
  );
  return new Set((rows || []).map((row) => normalize(row.column_name)));
}

function migrationDriftError(message) {
  const err = new Error(`MIGRATION_DRIFT: ${message}`);
  err.code = "MIGRATION_DRIFT";
  return err;
}

export async function assertPaymentSchemaReady() {
  const enforce = !["0", "false", "no", "off"].includes(String(process.env.ENFORCE_PAYMENT_SCHEMA_GUARD || "1").toLowerCase());
  if (!enforce) return;

  const hasMigrationsTable = await hasTable("schema_migrations");
  if (!hasMigrationsTable) {
    throw migrationDriftError("schema_migrations table is missing");
  }

  const migrationRows = await db.manyOrNone("select filename from schema_migrations");
  const appliedMigrations = new Set((migrationRows || []).map((row) => normalize(row.filename)));
  for (const filename of REQUIRED_MIGRATIONS) {
    if (!appliedMigrations.has(filename)) {
      throw migrationDriftError(`required migration not applied: ${filename}`);
    }
  }

  if (!(await hasTable("payment_intents"))) {
    throw migrationDriftError("payment_intents table is missing");
  }
  const paymentIntentColumns = await listColumns("payment_intents");
  for (const column of REQUIRED_PAYMENT_COLUMNS) {
    if (!paymentIntentColumns.has(column)) {
      throw migrationDriftError(`payment_intents missing column ${column}`);
    }
  }

  if (!(await hasTable("payment_events"))) {
    throw migrationDriftError("payment_events table is missing");
  }
  const paymentEventColumns = await listColumns("payment_events");
  for (const column of REQUIRED_PAYMENT_EVENT_COLUMNS) {
    if (!paymentEventColumns.has(column)) {
      throw migrationDriftError(`payment_events missing column ${column}`);
    }
  }
}

export default assertPaymentSchemaReady;
