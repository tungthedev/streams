import type { Pool } from "pg";
import type { SecondaryIndexStore } from "../store/index_store";
import type { SecondaryIndexRunRow, SecondaryIndexStateRow } from "../store/rows";
import type { PgExecutor } from "./types";
import { pgInt, toBigInt, toBytes, PostgresIndexSharedStore } from "./rows";

export class PostgresSecondaryIndexStore extends PostgresIndexSharedStore implements SecondaryIndexStore {
  async getSecondaryIndexState(stream: string, indexName: string): Promise<SecondaryIndexStateRow | null> {
    return getPostgresSecondaryIndexState(this.pool, stream, indexName);
  }

  async listSecondaryIndexStates(stream: string): Promise<SecondaryIndexStateRow[]> {
    return listPostgresSecondaryIndexStates(this.pool, stream);
  }

  async upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO secondary_index_state(stream, index_name, index_secret, config_hash, indexed_through, updated_at_ms)
       VALUES($1, $2, $3, $4, $5, $6)
       ON CONFLICT(stream, index_name) DO UPDATE SET
         index_secret = excluded.index_secret,
         config_hash = excluded.config_hash,
         indexed_through = excluded.indexed_through,
         updated_at_ms = excluded.updated_at_ms;`,
      [stream, indexName, Buffer.from(indexSecret), configHash, indexedThrough, pgInt(this.nowMs())]
    );
  }

  async updateSecondaryIndexedThrough(stream: string, indexName: string, indexedThrough: number): Promise<void> {
    await this.pool.query(
      `UPDATE secondary_index_state
       SET indexed_through = $1, updated_at_ms = $2
       WHERE stream = $3 AND index_name = $4;`,
      [indexedThrough, pgInt(this.nowMs()), stream, indexName]
    );
  }

  async listSecondaryIndexRuns(stream: string, indexName: string): Promise<SecondaryIndexRunRow[]> {
    return listPostgresSecondaryIndexRuns(this.pool, stream, indexName, false);
  }

  async listRetiredSecondaryIndexRuns(stream: string, indexName: string): Promise<SecondaryIndexRunRow[]> {
    return listPostgresRetiredSecondaryIndexRuns(this.pool, stream, indexName);
  }

  async insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await this.pool.query(
      `INSERT INTO secondary_index_runs(
         run_id, stream, index_name, level, start_segment, end_segment, object_key, size_bytes, filter_len, record_count
       )
       VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`,
      [
        row.run_id,
        row.stream,
        row.index_name,
        row.level,
        row.start_segment,
        row.end_segment,
        row.object_key,
        row.size_bytes,
        row.filter_len,
        row.record_count,
      ]
    );
  }

  async retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`UPDATE secondary_index_runs SET retired_gen = $1, retired_at_ms = $2 WHERE run_id = ANY($3::text[]);`, [
      retiredGen,
      pgInt(retiredAtMs),
      runIds,
    ]);
  }

  async deleteSecondaryIndexRuns(runIds: string[]): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`DELETE FROM secondary_index_runs WHERE run_id = ANY($1::text[]);`, [runIds]);
  }

  async deleteSecondaryIndex(stream: string, indexName: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`DELETE FROM secondary_index_runs WHERE stream = $1 AND index_name = $2;`, [stream, indexName]);
      await client.query(`DELETE FROM secondary_index_state WHERE stream = $1 AND index_name = $2;`, [stream, indexName]);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}

export async function loadPostgresSecondaryIndexManifest(
  executor: PgExecutor,
  stream: string
): Promise<{
  secondaryIndexStates: SecondaryIndexStateRow[];
  secondaryIndexRuns: SecondaryIndexRunRow[];
  retiredSecondaryIndexRuns: SecondaryIndexRunRow[];
}> {
  const secondaryIndexStates = await listPostgresSecondaryIndexStates(executor, stream);
  const secondaryIndexRuns = (
    await Promise.all(secondaryIndexStates.map((state) => listPostgresSecondaryIndexRuns(executor, stream, state.index_name, false)))
  ).flat();
  const retiredSecondaryIndexRuns = (
    await Promise.all(secondaryIndexStates.map((state) => listPostgresRetiredSecondaryIndexRuns(executor, stream, state.index_name)))
  ).flat();
  return { secondaryIndexStates, secondaryIndexRuns, retiredSecondaryIndexRuns };
}

async function getPostgresSecondaryIndexState(
  executor: PgExecutor,
  stream: string,
  indexName: string
): Promise<SecondaryIndexStateRow | null> {
  const res = await executor.query(`SELECT * FROM secondary_index_state WHERE stream = $1 AND index_name = $2 LIMIT 1;`, [
    stream,
    indexName,
  ]);
  return res.rows[0] ? coerceSecondaryIndexStateRow(res.rows[0]) : null;
}

async function listPostgresSecondaryIndexStates(executor: PgExecutor, stream: string): Promise<SecondaryIndexStateRow[]> {
  const res = await executor.query(`SELECT * FROM secondary_index_state WHERE stream = $1 ORDER BY index_name ASC;`, [stream]);
  return res.rows.map(coerceSecondaryIndexStateRow);
}

async function listPostgresSecondaryIndexRuns(
  executor: PgExecutor,
  stream: string,
  indexName: string,
  includeRetired: boolean
): Promise<SecondaryIndexRunRow[]> {
  const retiredClause = includeRetired ? "" : " AND retired_gen IS NULL";
  const res = await executor.query(
    `SELECT * FROM secondary_index_runs
     WHERE stream = $1 AND index_name = $2${retiredClause}
     ORDER BY level ASC, start_segment ASC, end_segment ASC;`,
    [stream, indexName]
  );
  return res.rows.map(coerceSecondaryIndexRunRow);
}

async function listPostgresRetiredSecondaryIndexRuns(
  executor: PgExecutor,
  stream: string,
  indexName: string
): Promise<SecondaryIndexRunRow[]> {
  const res = await executor.query(
    `SELECT * FROM secondary_index_runs
     WHERE stream = $1 AND index_name = $2 AND retired_gen IS NOT NULL
     ORDER BY retired_gen ASC, retired_at_ms ASC, level ASC, start_segment ASC;`,
    [stream, indexName]
  );
  return res.rows.map(coerceSecondaryIndexRunRow);
}

function coerceSecondaryIndexStateRow(row: any): SecondaryIndexStateRow {
  return {
    stream: String(row.stream),
    index_name: String(row.index_name),
    index_secret: toBytes(row.index_secret),
    config_hash: String(row.config_hash),
    indexed_through: Number(row.indexed_through),
    updated_at_ms: toBigInt(row.updated_at_ms),
  };
}

function coerceSecondaryIndexRunRow(row: any): SecondaryIndexRunRow {
  return {
    run_id: String(row.run_id),
    stream: String(row.stream),
    index_name: String(row.index_name),
    level: Number(row.level),
    start_segment: Number(row.start_segment),
    end_segment: Number(row.end_segment),
    object_key: String(row.object_key),
    size_bytes: Number(row.size_bytes),
    filter_len: Number(row.filter_len),
    record_count: Number(row.record_count),
    retired_gen: row.retired_gen == null ? null : Number(row.retired_gen),
    retired_at_ms: row.retired_at_ms == null ? null : toBigInt(row.retired_at_ms),
  };
}
