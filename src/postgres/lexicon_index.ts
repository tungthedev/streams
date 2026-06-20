import type { LexiconIndexStore } from "../store/index_store";
import type { LexiconIndexRunRow, LexiconIndexStateRow } from "../store/rows";
import type { WalReadRow } from "../store/wal_store";
import type { PgExecutor } from "./types";
import { pgInt, toBigInt, PostgresIndexSharedStore } from "./rows";

export class PostgresLexiconIndexStore extends PostgresIndexSharedStore implements LexiconIndexStore {
  constructor(
    pool: ConstructorParameters<typeof PostgresIndexSharedStore>[0],
    nowMs: () => bigint,
    private readonly readWal: (stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array) => AsyncIterable<WalReadRow>
  ) {
    super(pool, nowMs);
  }

  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    return this.readWal(stream, startOffset, endOffset, routingKey);
  }

  async getLexiconIndexState(stream: string, sourceKind: string, sourceName: string): Promise<LexiconIndexStateRow | null> {
    return getPostgresLexiconIndexState(this.pool, stream, sourceKind, sourceName);
  }

  async upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): Promise<void> {
    await this.pool.query(
      `INSERT INTO lexicon_index_state(stream, source_kind, source_name, indexed_through, updated_at_ms)
       VALUES($1, $2, $3, $4, $5)
       ON CONFLICT(stream, source_kind, source_name) DO UPDATE SET
         indexed_through = excluded.indexed_through,
         updated_at_ms = excluded.updated_at_ms;`,
      [stream, sourceKind, sourceName, indexedThrough, pgInt(this.nowMs())]
    );
  }

  async updateLexiconIndexedThrough(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): Promise<void> {
    await this.pool.query(
      `UPDATE lexicon_index_state
       SET indexed_through = $1, updated_at_ms = $2
       WHERE stream = $3 AND source_kind = $4 AND source_name = $5;`,
      [indexedThrough, pgInt(this.nowMs()), stream, sourceKind, sourceName]
    );
  }

  async listLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): Promise<LexiconIndexRunRow[]> {
    return listPostgresLexiconIndexRuns(this.pool, stream, sourceKind, sourceName, false);
  }

  async listLexiconIndexRunsAll(stream: string, sourceKind: string, sourceName: string): Promise<LexiconIndexRunRow[]> {
    return listPostgresLexiconIndexRuns(this.pool, stream, sourceKind, sourceName, true);
  }

  async listRetiredLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): Promise<LexiconIndexRunRow[]> {
    return listPostgresRetiredLexiconIndexRuns(this.pool, stream, sourceKind, sourceName);
  }

  async insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await this.pool.query(
      `INSERT INTO lexicon_index_runs(
         run_id, stream, source_kind, source_name, level, start_segment, end_segment, object_key, size_bytes, record_count
       )
       VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);`,
      [
        row.run_id,
        row.stream,
        row.source_kind,
        row.source_name,
        row.level,
        row.start_segment,
        row.end_segment,
        row.object_key,
        row.size_bytes,
        row.record_count,
      ]
    );
  }

  async retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`UPDATE lexicon_index_runs SET retired_gen = $1, retired_at_ms = $2 WHERE run_id = ANY($3::text[]);`, [
      retiredGen,
      pgInt(retiredAtMs),
      runIds,
    ]);
  }

  async deleteLexiconIndexRuns(runIds: string[]): Promise<void> {
    if (runIds.length === 0) return;
    await this.pool.query(`DELETE FROM lexicon_index_runs WHERE run_id = ANY($1::text[]);`, [runIds]);
  }

  async deleteLexiconIndexSource(stream: string, sourceKind: string, sourceName: string): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`DELETE FROM lexicon_index_runs WHERE stream = $1 AND source_kind = $2 AND source_name = $3;`, [
        stream,
        sourceKind,
        sourceName,
      ]);
      await client.query(`DELETE FROM lexicon_index_state WHERE stream = $1 AND source_kind = $2 AND source_name = $3;`, [
        stream,
        sourceKind,
        sourceName,
      ]);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}

export async function loadPostgresLexiconIndexManifest(
  executor: PgExecutor,
  stream: string
): Promise<{
  lexiconIndexStates: LexiconIndexStateRow[];
  lexiconIndexRuns: LexiconIndexRunRow[];
  retiredLexiconIndexRuns: LexiconIndexRunRow[];
}> {
  const lexiconIndexStates = await listPostgresLexiconIndexStates(executor, stream);
  const lexiconIndexRuns = (
    await Promise.all(
      lexiconIndexStates.map((state) => listPostgresLexiconIndexRuns(executor, stream, state.source_kind, state.source_name, false))
    )
  ).flat();
  const retiredLexiconIndexRuns = (
    await Promise.all(
      lexiconIndexStates.map((state) => listPostgresRetiredLexiconIndexRuns(executor, stream, state.source_kind, state.source_name))
    )
  ).flat();
  return { lexiconIndexStates, lexiconIndexRuns, retiredLexiconIndexRuns };
}

async function getPostgresLexiconIndexState(
  executor: PgExecutor,
  stream: string,
  sourceKind: string,
  sourceName: string
): Promise<LexiconIndexStateRow | null> {
  const res = await executor.query(
    `SELECT * FROM lexicon_index_state
     WHERE stream = $1 AND source_kind = $2 AND source_name = $3
     LIMIT 1;`,
    [stream, sourceKind, sourceName]
  );
  return res.rows[0] ? coerceLexiconIndexStateRow(res.rows[0]) : null;
}

async function listPostgresLexiconIndexStates(executor: PgExecutor, stream: string): Promise<LexiconIndexStateRow[]> {
  const res = await executor.query(
    `SELECT * FROM lexicon_index_state
     WHERE stream = $1
     ORDER BY source_kind ASC, source_name ASC;`,
    [stream]
  );
  return res.rows.map(coerceLexiconIndexStateRow);
}

async function listPostgresLexiconIndexRuns(
  executor: PgExecutor,
  stream: string,
  sourceKind: string,
  sourceName: string,
  includeRetired: boolean
): Promise<LexiconIndexRunRow[]> {
  const retiredClause = includeRetired ? "" : " AND retired_gen IS NULL";
  const res = await executor.query(
    `SELECT * FROM lexicon_index_runs
     WHERE stream = $1 AND source_kind = $2 AND source_name = $3${retiredClause}
     ORDER BY level ASC, start_segment ASC, end_segment ASC;`,
    [stream, sourceKind, sourceName]
  );
  return res.rows.map(coerceLexiconIndexRunRow);
}

async function listPostgresRetiredLexiconIndexRuns(
  executor: PgExecutor,
  stream: string,
  sourceKind: string,
  sourceName: string
): Promise<LexiconIndexRunRow[]> {
  const res = await executor.query(
    `SELECT * FROM lexicon_index_runs
     WHERE stream = $1 AND source_kind = $2 AND source_name = $3 AND retired_gen IS NOT NULL
     ORDER BY retired_gen ASC, retired_at_ms ASC, level ASC, start_segment ASC;`,
    [stream, sourceKind, sourceName]
  );
  return res.rows.map(coerceLexiconIndexRunRow);
}

function coerceLexiconIndexStateRow(row: any): LexiconIndexStateRow {
  return {
    stream: String(row.stream),
    source_kind: String(row.source_kind),
    source_name: String(row.source_name),
    indexed_through: Number(row.indexed_through),
    updated_at_ms: toBigInt(row.updated_at_ms),
  };
}

function coerceLexiconIndexRunRow(row: any): LexiconIndexRunRow {
  return {
    run_id: String(row.run_id),
    stream: String(row.stream),
    source_kind: String(row.source_kind),
    source_name: String(row.source_name),
    level: Number(row.level),
    start_segment: Number(row.start_segment),
    end_segment: Number(row.end_segment),
    object_key: String(row.object_key),
    size_bytes: Number(row.size_bytes),
    record_count: Number(row.record_count),
    retired_gen: row.retired_gen == null ? null : Number(row.retired_gen),
    retired_at_ms: row.retired_at_ms == null ? null : toBigInt(row.retired_at_ms),
  };
}
