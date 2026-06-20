import type {
  ObjectStoreAccountingStore,
  ObjectStoreRequestCountRow,
  ObjectStoreRequestSummary,
  StorageStatsStore,
} from "../store/stats_accounting_store";
import { summarizeObjectStoreRequestCounts } from "../store/stats_accounting_store";
import { STREAM_FLAG_DELETED, STREAM_FLAG_TOUCH } from "../store/rows";
import type { PgExecutor } from "./types";

function toBigInt(value: unknown): bigint {
  return typeof value === "bigint" ? value : BigInt(value as any);
}

function clampNumber(value: bigint): number {
  return value > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(value);
}

export class PostgresStatsAccountingStore implements StorageStatsStore, ObjectStoreAccountingStore {
  constructor(
    private readonly executor: PgExecutor,
    private readonly currentTimeMs: () => bigint
  ) {}

  async countStreams(): Promise<number> {
    const res = await this.executor.query<{ cnt: string | number | bigint }>(
      `SELECT COUNT(*) AS cnt
       FROM streams
       WHERE (stream_flags & $1) = 0;`,
      [STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH]
    );
    return Number(res.rows[0]?.cnt ?? 0);
  }

  async getWalDbSizeBytes(): Promise<number> {
    const res = await this.executor.query<{ total: string | number | bigint | null }>(
      `SELECT COALESCE(pg_total_relation_size('wal'::regclass), 0)::bigint AS total;`
    );
    return clampNumber(toBigInt(res.rows[0]?.total ?? 0));
  }

  async getMetaDbSizeBytes(): Promise<number> {
    const res = await this.executor.query<{ total: string | number | bigint | null }>(
      `WITH rels(name) AS (
         VALUES
           ('streams'),
           ('schemas'),
           ('stream_profiles'),
           ('producer_state'),
           ('segments'),
           ('stream_segment_meta'),
           ('manifests'),
           ('index_state'),
           ('index_runs'),
           ('secondary_index_state'),
           ('secondary_index_runs'),
           ('lexicon_index_state'),
           ('lexicon_index_runs'),
           ('search_companion_plans'),
           ('search_segment_companions'),
           ('stream_touch_state'),
           ('live_templates'),
           ('objectstore_request_counts')
       )
       SELECT COALESCE(SUM(pg_total_relation_size(to_regclass(name))), 0)::bigint AS total
       FROM rels
       WHERE to_regclass(name) IS NOT NULL;`
    );
    return clampNumber(toBigInt(res.rows[0]?.total ?? 0));
  }

  async recordObjectStoreRequestByHash(streamHash: string, artifact: string, op: string, bytes = 0, count = 1): Promise<void> {
    if (!streamHash || !artifact || !op) return;
    await this.executor.query(
      `INSERT INTO objectstore_request_counts(stream_hash, artifact, op, count, bytes, updated_at_ms)
       VALUES($1, $2, $3, $4, $5, $6)
       ON CONFLICT(stream_hash, artifact, op) DO UPDATE SET
         count = objectstore_request_counts.count + excluded.count,
         bytes = objectstore_request_counts.bytes + excluded.bytes,
         updated_at_ms = excluded.updated_at_ms;`,
      [streamHash, artifact, op, Math.max(0, Math.floor(count)), Math.max(0, Math.floor(bytes)), this.currentTimeMs().toString()]
    );
  }

  async getObjectStoreRequestSummaryByHash(streamHash: string): Promise<ObjectStoreRequestSummary> {
    const res = await this.executor.query<{ artifact: string; op: string; count: string | number | bigint }>(
      `SELECT artifact, op, count
       FROM objectstore_request_counts
       WHERE stream_hash = $1
       ORDER BY artifact ASC, op ASC;`,
      [streamHash]
    );
    return summarizeObjectStoreRequestCounts(
      res.rows.map(
        (row): ObjectStoreRequestCountRow => ({
          artifact: String(row.artifact),
          op: String(row.op),
          count: toBigInt(row.count ?? 0),
        })
      )
    );
  }
}
