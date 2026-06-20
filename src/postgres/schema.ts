import type { Pool } from "pg";
import { dsError } from "../util/ds_error";

export const POSTGRES_SCHEMA_VERSION = 1;

type PgSchemaExecutor = Pick<Pool, "query">;

export type PostgresMigrationOptions = {
  fullMode?: boolean;
};

export async function migratePostgresStore(pool: Pool, opts: PostgresMigrationOptions = {}): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await ensureSchemaVersionTable(client);
    const currentVersion = await readPostgresSchemaVersion(client);
    if (currentVersion != null && currentVersion !== POSTGRES_SCHEMA_VERSION) {
      throw dsError(`postgres schema version ${currentVersion} is not supported by version ${POSTGRES_SCHEMA_VERSION}`);
    }
    await installWalControlPlaneSchema(client);
    if (opts.fullMode) {
      await installFullModeSegmentSchema(client);
      await installFullModeIndexSchema(client);
    }
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

async function installFullModeSegmentSchema(executor: PgSchemaExecutor): Promise<void> {
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS sealed_through bigint NOT NULL DEFAULT -1;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS uploaded_through bigint NOT NULL DEFAULT -1;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS uploaded_segment_count integer NOT NULL DEFAULT 0;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS pending_rows bigint NOT NULL DEFAULT 0;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS pending_bytes bigint NOT NULL DEFAULT 0;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS last_segment_cut_ms bigint NOT NULL DEFAULT 0;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS segment_in_progress integer NOT NULL DEFAULT 0;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS segment_claim_token text NULL;`);
  await executor.query(`ALTER TABLE streams ADD COLUMN IF NOT EXISTS segment_claimed_at_ms bigint NULL;`);
  await executor.query(`
    UPDATE streams
    SET pending_rows = wal_rows,
        pending_bytes = wal_bytes,
        last_segment_cut_ms = last_append_ms
    WHERE sealed_through = -1
      AND uploaded_through = -1
      AND uploaded_segment_count = 0
      AND segment_in_progress = 0
      AND segment_claim_token IS NULL
      AND pending_rows = 0
      AND pending_bytes = 0;
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS segments (
      segment_id text PRIMARY KEY,
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      segment_index integer NOT NULL,
      start_offset bigint NOT NULL,
      end_offset bigint NOT NULL,
      block_count integer NOT NULL,
      last_append_ms bigint NOT NULL,
      payload_bytes bigint NOT NULL DEFAULT 0,
      size_bytes integer NOT NULL,
      local_path text NOT NULL,
      created_at_ms bigint NOT NULL,
      uploaded_at_ms bigint NULL,
      r2_etag text NULL,
      UNIQUE(stream, segment_index)
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS stream_segment_meta (
      stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
      segment_count integer NOT NULL,
      segment_offsets bytea NOT NULL,
      segment_blocks bytea NOT NULL,
      segment_last_ts bytea NOT NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS manifests (
      stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
      generation integer NOT NULL,
      uploaded_generation integer NOT NULL,
      last_uploaded_at_ms bigint NULL,
      last_uploaded_etag text NULL,
      last_uploaded_size_bytes bigint NULL
    );
  `);
  await executor.query(`ALTER TABLE schemas ADD COLUMN IF NOT EXISTS uploaded_size_bytes bigint NOT NULL DEFAULT 0;`);
  await executor.query(`CREATE INDEX IF NOT EXISTS streams_pending_bytes_idx ON streams(pending_bytes);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS streams_last_cut_idx ON streams(last_segment_cut_ms);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS streams_inprog_pending_idx ON streams(segment_in_progress, pending_bytes, last_segment_cut_ms);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS segments_stream_start_idx ON segments(stream, start_offset);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS segments_pending_upload_idx ON segments(uploaded_at_ms);`);
}

async function installFullModeIndexSchema(executor: PgSchemaExecutor): Promise<void> {
  await executor.query(`
    CREATE TABLE IF NOT EXISTS index_state (
      stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
      index_secret bytea NOT NULL,
      indexed_through integer NOT NULL,
      updated_at_ms bigint NOT NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS index_runs (
      run_id text PRIMARY KEY,
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      level integer NOT NULL,
      start_segment integer NOT NULL,
      end_segment integer NOT NULL,
      object_key text NOT NULL,
      size_bytes bigint NOT NULL,
      filter_len integer NOT NULL,
      record_count integer NOT NULL,
      retired_gen integer NULL,
      retired_at_ms bigint NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS secondary_index_state (
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      index_name text NOT NULL,
      index_secret bytea NOT NULL,
      config_hash text NOT NULL,
      indexed_through integer NOT NULL,
      updated_at_ms bigint NOT NULL,
      PRIMARY KEY(stream, index_name)
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS secondary_index_runs (
      run_id text PRIMARY KEY,
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      index_name text NOT NULL,
      level integer NOT NULL,
      start_segment integer NOT NULL,
      end_segment integer NOT NULL,
      object_key text NOT NULL,
      size_bytes bigint NOT NULL,
      filter_len integer NOT NULL,
      record_count integer NOT NULL,
      retired_gen integer NULL,
      retired_at_ms bigint NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS lexicon_index_state (
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      source_kind text NOT NULL,
      source_name text NOT NULL,
      indexed_through integer NOT NULL,
      updated_at_ms bigint NOT NULL,
      PRIMARY KEY(stream, source_kind, source_name)
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS lexicon_index_runs (
      run_id text PRIMARY KEY,
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      source_kind text NOT NULL,
      source_name text NOT NULL,
      level integer NOT NULL,
      start_segment integer NOT NULL,
      end_segment integer NOT NULL,
      object_key text NOT NULL,
      size_bytes bigint NOT NULL,
      record_count integer NOT NULL,
      retired_gen integer NULL,
      retired_at_ms bigint NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS search_companion_plans (
      stream text PRIMARY KEY REFERENCES streams(stream) ON DELETE CASCADE,
      generation integer NOT NULL,
      plan_hash text NOT NULL,
      plan_json text NOT NULL,
      updated_at_ms bigint NOT NULL
    );
  `);
  await executor.query(`
    CREATE TABLE IF NOT EXISTS search_segment_companions (
      stream text NOT NULL REFERENCES streams(stream) ON DELETE CASCADE,
      segment_index integer NOT NULL,
      object_key text NOT NULL,
      plan_generation integer NOT NULL,
      sections_json text NOT NULL,
      section_sizes_json text NOT NULL,
      size_bytes bigint NOT NULL,
      primary_timestamp_min_ms bigint NULL,
      primary_timestamp_max_ms bigint NULL,
      updated_at_ms bigint NOT NULL,
      PRIMARY KEY(stream, segment_index)
    );
  `);
  await executor.query(`CREATE INDEX IF NOT EXISTS index_runs_active_idx ON index_runs(stream, retired_gen, level, start_segment);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS index_runs_retired_idx ON index_runs(stream, retired_gen, retired_at_ms);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS secondary_index_runs_active_idx ON secondary_index_runs(stream, index_name, retired_gen, level, start_segment);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS secondary_index_runs_retired_idx ON secondary_index_runs(stream, index_name, retired_gen, retired_at_ms);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS lexicon_index_runs_active_idx ON lexicon_index_runs(stream, source_kind, source_name, retired_gen, level, start_segment);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS lexicon_index_runs_retired_idx ON lexicon_index_runs(stream, source_kind, source_name, retired_gen, retired_at_ms);`);
  await executor.query(`CREATE INDEX IF NOT EXISTS search_segment_companions_plan_idx ON search_segment_companions(stream, plan_generation, segment_index);`);
}
