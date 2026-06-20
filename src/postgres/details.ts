import type { Pool } from "pg";
import type {
  ExactIndexDetailSnapshot,
  FullModeDetailsSnapshot,
  FullModeDetailsSnapshotRequest,
  FullModeDetailsStore,
  FullModeLagSnapshotRequest,
  IndexStorageSummary,
  LexiconIndexStorageSummary,
  SecondaryIndexStorageSummary,
} from "../store/full_mode_details_store";
import type { SchemaRegistryRow } from "../store/schema_profile_store";
import type { ManifestRow } from "../store/segment_manifest_store";
import { readU64LE } from "../util/endian";
import { getPostgresSearchCompanionPlan, listPostgresSearchSegmentCompanions } from "./companions";
import { loadPostgresLexiconIndexManifest } from "./lexicon_index";
import { loadPostgresRoutingIndexManifest } from "./routing_index";
import { loadPostgresSecondaryIndexManifest } from "./secondary_index";
import type { PgExecutor } from "./types";
import { toBigInt, toBytes } from "./rows";

export class PostgresFullModeDetailsStore implements FullModeDetailsStore {
  constructor(private readonly pool: Pool) {}

  async getFullModeDetailsSnapshot(request: FullModeDetailsSnapshotRequest): Promise<FullModeDetailsSnapshot> {
    const stream = request.stream;
    const [segmentCount, uploadedSegmentCount, manifest, schemaRow, routingIndex, secondaryIndex, lexiconIndex, companionPlan, companionRows] =
      await Promise.all([
        countSegments(this.pool, stream),
        countUploadedSegments(this.pool, stream),
        getManifestRow(this.pool, stream),
        getSchemaRegistry(this.pool, stream),
        loadPostgresRoutingIndexManifest(this.pool, stream),
        loadPostgresSecondaryIndexManifest(this.pool, stream),
        loadPostgresLexiconIndexManifest(this.pool, stream),
        getPostgresSearchCompanionPlan(this.pool, stream),
        listPostgresSearchSegmentCompanions(this.pool, stream),
      ]);
    const exactIndexes: ExactIndexDetailSnapshot[] = request.exactIndexNames.map((indexName) => ({
      indexName,
      state: secondaryIndex.secondaryIndexStates.find((state) => state.index_name === indexName) ?? null,
      activeRuns: secondaryIndex.secondaryIndexRuns.filter((run) => run.index_name === indexName),
      retiredRuns: secondaryIndex.retiredSecondaryIndexRuns.filter((run) => run.index_name === indexName),
    }));
    return {
      segmentCount,
      uploadedSegmentCount,
      manifest,
      schemaRow,
      uploadedSegmentBytes: await sumBigInt(this.pool, `SELECT COALESCE(SUM(size_bytes), 0) AS total FROM segments WHERE stream = $1 AND uploaded_at_ms IS NOT NULL;`, [stream]),
      pendingSealedSegmentBytes: await sumBigInt(this.pool, `SELECT COALESCE(SUM(size_bytes), 0) AS total FROM segments WHERE stream = $1 AND uploaded_at_ms IS NULL;`, [stream]),
      routingIndexStorage: await storageSummary(this.pool, "index_runs", stream),
      secondaryIndexStorage: await secondaryStorageSummary(this.pool, stream),
      lexiconIndexStorage: await lexiconStorageSummary(this.pool, stream),
      bundledCompanionStorage: await storageSummary(this.pool, "search_segment_companions", stream),
      routingState: routingIndex.indexState,
      routingRuns: routingIndex.indexRuns,
      retiredRoutingRuns: routingIndex.retiredRuns,
      exactIndexes,
      routingLexiconState:
        lexiconIndex.lexiconIndexStates.find((state) => state.source_kind === "routing_key" && state.source_name === "") ?? null,
      routingLexiconRuns: lexiconIndex.lexiconIndexRuns.filter((run) => run.source_kind === "routing_key" && run.source_name === ""),
      retiredRoutingLexiconRuns: lexiconIndex.retiredLexiconIndexRuns.filter(
        (run) => run.source_kind === "routing_key" && run.source_name === ""
      ),
      companionPlan,
      companionRows,
    };
  }

  async getFullModeLagSnapshot(request: FullModeLagSnapshotRequest): Promise<Map<number, bigint>> {
    const meta = await getSegmentMeta(this.pool, request.stream);
    const out = new Map<number, bigint>();
    if (!meta) return out;
    const indexes = Array.from(new Set(request.segmentIndexes.filter((index) => Number.isInteger(index) && index >= 0))).sort((a, b) => a - b);
    for (const index of indexes) {
      if (index >= meta.segmentCount) continue;
      const offset = index * 8;
      if (offset + 8 > meta.segmentLastTs.byteLength) continue;
      out.set(index, readU64LE(meta.segmentLastTs, offset) / 1_000_000n);
    }
    return out;
  }
}

async function countSegments(executor: PgExecutor, stream: string): Promise<number> {
  const res = await executor.query<{ count: string }>(`SELECT COUNT(*) AS count FROM segments WHERE stream = $1;`, [stream]);
  return Number(res.rows[0]?.count ?? 0);
}

async function countUploadedSegments(executor: PgExecutor, stream: string): Promise<number> {
  const res = await executor.query<{ max_idx: number | string | null }>(
    `SELECT MAX(segment_index) AS max_idx FROM segments WHERE stream = $1 AND r2_etag IS NOT NULL;`,
    [stream]
  );
  const maxIdx = res.rows[0]?.max_idx == null ? -1 : Number(res.rows[0]!.max_idx);
  return maxIdx >= 0 ? maxIdx + 1 : 0;
}

async function getManifestRow(executor: PgExecutor, stream: string): Promise<ManifestRow> {
  const res = await executor.query(
    `SELECT stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes
     FROM manifests WHERE stream = $1 LIMIT 1;`,
    [stream]
  );
  if (!res.rows[0]) {
    return { stream, generation: 0, uploaded_generation: 0, last_uploaded_at_ms: null, last_uploaded_etag: null, last_uploaded_size_bytes: null };
  }
  const row = res.rows[0];
  return {
    stream: String(row.stream),
    generation: Number(row.generation),
    uploaded_generation: Number(row.uploaded_generation),
    last_uploaded_at_ms: row.last_uploaded_at_ms == null ? null : toBigInt(row.last_uploaded_at_ms),
    last_uploaded_etag: row.last_uploaded_etag == null ? null : String(row.last_uploaded_etag),
    last_uploaded_size_bytes: row.last_uploaded_size_bytes == null ? null : toBigInt(row.last_uploaded_size_bytes),
  };
}

async function getSchemaRegistry(executor: PgExecutor, stream: string): Promise<SchemaRegistryRow | null> {
  const res = await executor.query(`SELECT stream, schema_json, updated_at_ms, uploaded_size_bytes FROM schemas WHERE stream = $1 LIMIT 1;`, [stream]);
  const row = res.rows[0];
  if (!row) return null;
  return {
    stream: String(row.stream),
    registry_json: String(row.schema_json),
    updated_at_ms: toBigInt(row.updated_at_ms),
    uploaded_size_bytes: toBigInt(row.uploaded_size_bytes ?? 0),
  };
}

async function sumBigInt(executor: PgExecutor, sql: string, params: unknown[]): Promise<bigint> {
  const res = await executor.query<{ total: string | number | bigint | null }>(sql, params);
  return toBigInt(res.rows[0]?.total ?? 0);
}

async function storageSummary(executor: PgExecutor, tableName: "index_runs" | "search_segment_companions", stream: string): Promise<IndexStorageSummary> {
  const res = await executor.query<{ cnt: string; total: string | number | bigint | null }>(
    `SELECT COUNT(*) AS cnt, COALESCE(SUM(size_bytes), 0) AS total FROM ${tableName} WHERE stream = $1;`,
    [stream]
  );
  return { object_count: Number(res.rows[0]?.cnt ?? 0), bytes: toBigInt(res.rows[0]?.total ?? 0) };
}

async function secondaryStorageSummary(executor: PgExecutor, stream: string): Promise<SecondaryIndexStorageSummary[]> {
  const res = await executor.query(
    `SELECT index_name, COUNT(*) AS cnt, COALESCE(SUM(size_bytes), 0) AS total
     FROM secondary_index_runs
     WHERE stream = $1
     GROUP BY index_name
     ORDER BY index_name ASC;`,
    [stream]
  );
  return res.rows.map((row) => ({
    index_name: String(row.index_name),
    object_count: Number(row.cnt ?? 0),
    bytes: toBigInt(row.total ?? 0),
  }));
}

async function lexiconStorageSummary(executor: PgExecutor, stream: string): Promise<LexiconIndexStorageSummary[]> {
  const res = await executor.query(
    `SELECT source_kind, source_name, COUNT(*) AS cnt, COALESCE(SUM(size_bytes), 0) AS total
     FROM lexicon_index_runs
     WHERE stream = $1
     GROUP BY source_kind, source_name
     ORDER BY source_kind ASC, source_name ASC;`,
    [stream]
  );
  return res.rows.map((row) => ({
    source_kind: String(row.source_kind),
    source_name: String(row.source_name),
    object_count: Number(row.cnt ?? 0),
    bytes: toBigInt(row.total ?? 0),
  }));
}

async function getSegmentMeta(executor: PgExecutor, stream: string): Promise<{ segmentCount: number; segmentLastTs: Uint8Array } | null> {
  const res = await executor.query(
    `SELECT segment_count, segment_last_ts
     FROM stream_segment_meta
     WHERE stream = $1
     LIMIT 1;`,
    [stream]
  );
  const row = res.rows[0];
  if (!row) return null;
  return { segmentCount: Number(row.segment_count), segmentLastTs: toBytes(row.segment_last_ts) };
}
