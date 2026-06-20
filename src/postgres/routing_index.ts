import type { Pool } from "pg";
import type { RoutingIndexStore } from "../store/index_store";
import type { IndexRunRow, IndexStateRow } from "../store/rows";
import type { PgExecutor } from "./types";
import { pgInt, PostgresIndexSharedStore, toBigInt, toBytes } from "./rows";

export class PostgresRoutingIndexStore extends PostgresIndexSharedStore implements RoutingIndexStore {
  async getIndexState(stream: string): Promise<IndexStateRow | null> {
    return getPostgresIndexState(this.pool, stream);
  }

  async upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): Promise<void> {
    await this.pool.query(
      `INSERT INTO index_state(stream, index_secret, indexed_through, updated_at_ms)
       VALUES($1, $2, $3, $4)
       ON CONFLICT(stream) DO UPDATE SET
         index_secret = excluded.index_secret,
         indexed_through = excluded.indexed_through,
         updated_at_ms = excluded.updated_at_ms;`,
      [stream, Buffer.from(indexSecret), indexedThrough, pgInt(this.nowMs())]
    );
  }

  async updateIndexedThrough(stream: string, indexedThrough: number): Promise<void> {
    await this.pool.query(`UPDATE index_state SET indexed_through = $1, updated_at_ms = $2 WHERE stream = $3;`, [
      indexedThrough,
      pgInt(this.nowMs()),
      stream,
    ]);
  }

  async listIndexRuns(stream: string): Promise<IndexRunRow[]> {
    return listPostgresIndexRuns(this.pool, stream, false);
  }

  async listIndexRunsAll(stream: string): Promise<IndexRunRow[]> {
    return listPostgresIndexRuns(this.pool, stream, true);
  }

  async listRetiredIndexRuns(stream: string): Promise<IndexRunRow[]> {
    const res = await this.pool.query(
      `SELECT * FROM index_runs
       WHERE stream = $1 AND retired_gen IS NOT NULL
       ORDER BY retired_gen ASC, retired_at_ms ASC, level ASC, start_segment ASC;`,
      [stream]
    );
    return res.rows.map(coerceIndexRunRow);
  }

  async insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await this.pool.query(
      `INSERT INTO index_runs(run_id, stream, level, start_segment, end_segment, object_key, size_bytes, filter_len, record_count)
       VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);`,
      [row.run_id, row.stream, row.level, row.start_segment, row.end_segment, row.object_key, row.size_bytes, row.filter_len, row.record_count]
    );
  }

  async retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`UPDATE index_runs SET retired_gen = $1, retired_at_ms = $2 WHERE run_id = ANY($3::text[]);`, [
      retiredGen,
      pgInt(retiredAtMs),
      runIds,
    ]);
  }

  async deleteIndexRuns(runIds: string[]): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`DELETE FROM index_runs WHERE run_id = ANY($1::text[]);`, [runIds]);
  }

  async deleteIndex(stream: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`DELETE FROM index_runs WHERE stream = $1;`, [stream]);
      await client.query(`DELETE FROM index_state WHERE stream = $1;`, [stream]);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}

export async function loadPostgresRoutingIndexManifest(
  executor: PgExecutor,
  stream: string
): Promise<{ indexState: IndexStateRow | null; indexRuns: IndexRunRow[]; retiredRuns: IndexRunRow[] }> {
  return {
    indexState: await getPostgresIndexState(executor, stream),
    indexRuns: await listPostgresIndexRuns(executor, stream, false),
    retiredRuns: await listPostgresRetiredIndexRuns(executor, stream),
  };
}

async function getPostgresIndexState(executor: PgExecutor, stream: string): Promise<IndexStateRow | null> {
  const res = await executor.query(`SELECT * FROM index_state WHERE stream = $1 LIMIT 1;`, [stream]);
  return res.rows[0] ? coerceIndexStateRow(res.rows[0]) : null;
}

async function listPostgresIndexRuns(executor: PgExecutor, stream: string, includeRetired: boolean): Promise<IndexRunRow[]> {
  const retiredClause = includeRetired ? "" : " AND retired_gen IS NULL";
  const res = await executor.query(
    `SELECT * FROM index_runs
     WHERE stream = $1${retiredClause}
     ORDER BY level ASC, start_segment ASC, end_segment ASC;`,
    [stream]
  );
  return res.rows.map(coerceIndexRunRow);
}

async function listPostgresRetiredIndexRuns(executor: PgExecutor, stream: string): Promise<IndexRunRow[]> {
  const res = await executor.query(
    `SELECT * FROM index_runs
     WHERE stream = $1 AND retired_gen IS NOT NULL
     ORDER BY retired_gen ASC, retired_at_ms ASC, level ASC, start_segment ASC;`,
    [stream]
  );
  return res.rows.map(coerceIndexRunRow);
}

function coerceIndexStateRow(row: any): IndexStateRow {
  return {
    stream: String(row.stream),
    index_secret: toBytes(row.index_secret),
    indexed_through: Number(row.indexed_through),
    updated_at_ms: toBigInt(row.updated_at_ms),
  };
}

function coerceIndexRunRow(row: any): IndexRunRow {
  return {
    run_id: String(row.run_id),
    stream: String(row.stream),
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
