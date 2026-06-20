import type { Pool } from "pg";
import { dsError } from "../util/ds_error";

export const POSTGRES_SCHEMA_VERSION = 1;

type PgSchemaExecutor = Pick<Pool, "query">;

export async function migratePostgresStore(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await ensureSchemaVersionTable(client);
    const currentVersion = await readPostgresSchemaVersion(client);
    if (currentVersion != null && currentVersion !== POSTGRES_SCHEMA_VERSION) {
      throw dsError(`postgres schema version ${currentVersion} is not supported by version ${POSTGRES_SCHEMA_VERSION}`);
    }
    await installWalControlPlaneSchema(client);
    await setPostgresSchemaVersion(client, POSTGRES_SCHEMA_VERSION);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function readPostgresSchemaVersion(executor: PgSchemaExecutor): Promise<number | null> {
  const table = await executor.query<{ exists: string | null }>(`SELECT to_regclass('schema_version')::text AS exists;`);
  if (!table.rows[0]?.exists) return null;
  const rows = await executor.query<{ version: number | string }>(`SELECT version FROM schema_version;`);
  if (rows.rows.length === 0) return null;
  if (rows.rows.length > 1) throw dsError("postgres schema_version must contain exactly one row");
  return Number(rows.rows[0]!.version);
}

async function ensureSchemaVersionTable(executor: PgSchemaExecutor): Promise<void> {
  await executor.query(`
    CREATE TABLE IF NOT EXISTS schema_version (
      version integer NOT NULL
    );
  `);
}

async function setPostgresSchemaVersion(executor: PgSchemaExecutor, version: number): Promise<void> {
  await executor.query(`DELETE FROM schema_version;`);
  await executor.query(`INSERT INTO schema_version(version) VALUES($1);`, [version]);
}

async function installWalControlPlaneSchema(executor: PgSchemaExecutor): Promise<void> {
  await executor.query(`
      CREATE TABLE IF NOT EXISTS streams (
        stream text PRIMARY KEY,
        created_at_ms bigint NOT NULL,
        updated_at_ms bigint NOT NULL,
        content_type text NOT NULL,
        profile text NULL,
        stream_seq text NULL,
        closed integer NOT NULL DEFAULT 0,
        closed_producer_id text NULL,
        closed_producer_epoch integer NULL,
        closed_producer_seq integer NULL,
        ttl_seconds integer NULL,
        epoch integer NOT NULL DEFAULT 0,
        next_offset bigint NOT NULL DEFAULT 0,
        logical_size_bytes bigint NOT NULL DEFAULT 0,
        wal_rows bigint NOT NULL DEFAULT 0,
        wal_bytes bigint NOT NULL DEFAULT 0,
        last_append_ms bigint NOT NULL,
        expires_at_ms bigint NULL,
        stream_flags integer NOT NULL DEFAULT 0
      );
    `);
  await executor.query(`
      CREATE TABLE IF NOT EXISTS wal (
        stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
        "offset" bigint NOT NULL,
        ts_ms bigint NOT NULL,
        payload bytea NOT NULL,
        payload_len integer NOT NULL,
        routing_key bytea NULL,
        content_type text NULL,
        flags integer NOT NULL DEFAULT 0,
        PRIMARY KEY (stream, "offset")
      );
    `);
  await executor.query(`
      CREATE TABLE IF NOT EXISTS schemas (
        stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
        schema_json text NOT NULL,
        updated_at_ms bigint NOT NULL
      );
    `);
  await executor.query(`
      CREATE TABLE IF NOT EXISTS stream_profiles (
        stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
        profile_json text NOT NULL,
        updated_at_ms bigint NOT NULL
      );
    `);
  await executor.query(`
      CREATE TABLE IF NOT EXISTS producer_state (
        stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
        producer_id text NOT NULL,
        epoch integer NOT NULL,
        last_seq integer NOT NULL,
        updated_at_ms bigint NOT NULL,
        PRIMARY KEY (stream, producer_id)
      );
    `);
  await executor.query(`CREATE INDEX IF NOT EXISTS wal_stream_offset_idx ON wal(stream, "offset");`);
  await executor.query(`CREATE INDEX IF NOT EXISTS wal_stream_routing_offset_idx ON wal(stream, routing_key, "offset");`);
  await executor.query(`CREATE INDEX IF NOT EXISTS streams_updated_at_idx ON streams(updated_at_ms);`);
}
