import type { Pool } from "pg";

export const POSTGRES_SCHEMA_VERSION = 1;

export async function migratePostgresStore(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_version (
        version integer NOT NULL
      );
    `);
    await client.query(
      `INSERT INTO schema_version(version)
       SELECT $1
       WHERE NOT EXISTS (SELECT 1 FROM schema_version);`,
      [POSTGRES_SCHEMA_VERSION]
    );
    await client.query(`
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
    await client.query(`
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
    await client.query(`
      CREATE TABLE IF NOT EXISTS schemas (
        stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
        schema_json text NOT NULL,
        updated_at_ms bigint NOT NULL
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS stream_profiles (
        stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
        profile_json text NOT NULL,
        updated_at_ms bigint NOT NULL
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS producer_state (
        stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
        producer_id text NOT NULL,
        epoch integer NOT NULL,
        last_seq integer NOT NULL,
        updated_at_ms bigint NOT NULL,
        PRIMARY KEY (stream, producer_id)
      );
    `);
    await client.query(`CREATE INDEX IF NOT EXISTS wal_stream_offset_idx ON wal(stream, "offset");`);
    await client.query(`CREATE INDEX IF NOT EXISTS wal_stream_routing_offset_idx ON wal(stream, routing_key, "offset");`);
    await client.query(`CREATE INDEX IF NOT EXISTS streams_updated_at_idx ON streams(updated_at_ms);`);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
