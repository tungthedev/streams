import type { Pool } from "pg";
import type { CompanionProgressStore, SearchCompanionIndexStore } from "../store/index_store";
import type { SearchCompanionPlanRow, SearchSegmentCompanionRow } from "../store/rows";
import type { PgExecutor } from "./types";
import { getSegmentByIndexWithExecutor, pgInt, toBigInt } from "./rows";

export class PostgresCompanionIndexStore implements SearchCompanionIndexStore, CompanionProgressStore {
  constructor(private readonly pool: Pool, private readonly currentTimeMs: () => bigint) {}

  async getSegmentByIndex(stream: string, segmentIndex: number) {
    return getSegmentByIndexWithExecutor(this.pool, stream, segmentIndex);
  }

  async countUploadedSegments(stream: string): Promise<number> {
    const res = await this.pool.query<{ max_idx: number | string | null }>(
      `SELECT MAX(segment_index) AS max_idx FROM segments WHERE stream = $1 AND r2_etag IS NOT NULL;`,
      [stream]
    );
    const maxIdx = res.rows[0]?.max_idx == null ? -1 : Number(res.rows[0]!.max_idx);
    return maxIdx >= 0 ? maxIdx + 1 : 0;
  }

  async getSearchCompanionPlan(stream: string): Promise<SearchCompanionPlanRow | null> {
    return getPostgresSearchCompanionPlan(this.pool, stream);
  }

  async listSearchCompanionPlanStreams(): Promise<string[]> {
    const res = await this.pool.query<{ stream: string }>(`SELECT stream FROM search_companion_plans ORDER BY stream ASC;`);
    return res.rows.map((row) => String(row.stream));
  }

  async upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO search_companion_plans(stream, generation, plan_hash, plan_json, updated_at_ms)
       VALUES($1, $2, $3, $4, $5)
       ON CONFLICT(stream) DO UPDATE SET
         generation = excluded.generation,
         plan_hash = excluded.plan_hash,
         plan_json = excluded.plan_json,
         updated_at_ms = excluded.updated_at_ms;`,
      [stream, generation, planHash, planJson, pgInt(this.currentTimeMs())]
    );
  }

  async deleteSearchCompanionPlan(stream: string): Promise<void> {
    await this.pool.query(`DELETE FROM search_companion_plans WHERE stream = $1;`, [stream]);
  }

  async getSearchSegmentCompanion(stream: string, segmentIndex: number): Promise<SearchSegmentCompanionRow | null> {
    return getPostgresSearchSegmentCompanion(this.pool, stream, segmentIndex);
  }

  async listSearchSegmentCompanions(stream: string): Promise<SearchSegmentCompanionRow[]> {
    return listPostgresSearchSegmentCompanions(this.pool, stream);
  }

  async upsertSearchSegmentCompanion(
    stream: string,
    segmentIndex: number,
    objectKey: string,
    planGeneration: number,
    sectionsJson: string,
    sectionSizesJson: string,
    sizeBytes: number,
    primaryTimestampMinMs: bigint | null,
    primaryTimestampMaxMs: bigint | null
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO search_segment_companions(
         stream, segment_index, object_key, plan_generation, sections_json, section_sizes_json,
         size_bytes, primary_timestamp_min_ms, primary_timestamp_max_ms, updated_at_ms
       )
       VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT(stream, segment_index) DO UPDATE SET
         object_key = excluded.object_key,
         plan_generation = excluded.plan_generation,
         sections_json = excluded.sections_json,
         section_sizes_json = excluded.section_sizes_json,
         size_bytes = excluded.size_bytes,
         primary_timestamp_min_ms = excluded.primary_timestamp_min_ms,
         primary_timestamp_max_ms = excluded.primary_timestamp_max_ms,
         updated_at_ms = excluded.updated_at_ms;`,
      [
        stream,
        segmentIndex,
        objectKey,
        planGeneration,
        sectionsJson,
        sectionSizesJson,
        sizeBytes,
        primaryTimestampMinMs == null ? null : pgInt(primaryTimestampMinMs),
        primaryTimestampMaxMs == null ? null : pgInt(primaryTimestampMaxMs),
        pgInt(this.currentTimeMs()),
      ]
    );
  }

  async deleteSearchSegmentCompanions(stream: string): Promise<void> {
    await this.pool.query(`DELETE FROM search_segment_companions WHERE stream = $1;`, [stream]);
  }
}

export async function loadPostgresSearchCompanionManifest(
  executor: PgExecutor,
  stream: string
): Promise<{ searchCompanionPlan: SearchCompanionPlanRow | null; searchSegmentCompanions: SearchSegmentCompanionRow[] }> {
  return {
    searchCompanionPlan: await getPostgresSearchCompanionPlan(executor, stream),
    searchSegmentCompanions: await listPostgresSearchSegmentCompanions(executor, stream),
  };
}

export async function getPostgresSearchCompanionPlan(executor: PgExecutor, stream: string): Promise<SearchCompanionPlanRow | null> {
  const res = await executor.query(`SELECT * FROM search_companion_plans WHERE stream = $1 LIMIT 1;`, [stream]);
  return res.rows[0] ? coerceSearchCompanionPlanRow(res.rows[0]) : null;
}

export async function getPostgresSearchSegmentCompanion(
  executor: PgExecutor,
  stream: string,
  segmentIndex: number
): Promise<SearchSegmentCompanionRow | null> {
  const res = await executor.query(`SELECT * FROM search_segment_companions WHERE stream = $1 AND segment_index = $2 LIMIT 1;`, [
    stream,
    segmentIndex,
  ]);
  return res.rows[0] ? coerceSearchSegmentCompanionRow(res.rows[0]) : null;
}

export async function listPostgresSearchSegmentCompanions(executor: PgExecutor, stream: string): Promise<SearchSegmentCompanionRow[]> {
  const res = await executor.query(`SELECT * FROM search_segment_companions WHERE stream = $1 ORDER BY segment_index ASC;`, [stream]);
  return res.rows.map(coerceSearchSegmentCompanionRow);
}

function coerceSearchCompanionPlanRow(row: any): SearchCompanionPlanRow {
  return {
    stream: String(row.stream),
    generation: Number(row.generation),
    plan_hash: String(row.plan_hash),
    plan_json: String(row.plan_json),
    updated_at_ms: toBigInt(row.updated_at_ms),
  };
}

function coerceSearchSegmentCompanionRow(row: any): SearchSegmentCompanionRow {
  return {
    stream: String(row.stream),
    segment_index: Number(row.segment_index),
    object_key: String(row.object_key),
    plan_generation: Number(row.plan_generation),
    sections_json: String(row.sections_json),
    section_sizes_json: String(row.section_sizes_json),
    size_bytes: Number(row.size_bytes),
    primary_timestamp_min_ms: row.primary_timestamp_min_ms == null ? null : toBigInt(row.primary_timestamp_min_ms),
    primary_timestamp_max_ms: row.primary_timestamp_max_ms == null ? null : toBigInt(row.primary_timestamp_max_ms),
    updated_at_ms: toBigInt(row.updated_at_ms),
  };
}
