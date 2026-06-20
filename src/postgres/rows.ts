import type { Pool } from "pg";
import type { ManifestGenerationRow } from "../store/index_store";
import type { SegmentRow, StreamRow } from "../store/rows";
import type { PgExecutor, PgStreamRow } from "./types";

export class PostgresIndexSharedStore {
  constructor(protected readonly pool: Pool, private readonly currentTimeMs: () => bigint) {}

  nowMs(): bigint {
    return this.currentTimeMs();
  }

  async getStream(stream: string): Promise<StreamRow | null> {
    return getStreamWithExecutor(this.pool, stream);
  }

  async getSegmentByIndex(stream: string, segmentIndex: number): Promise<SegmentRow | null> {
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

  async countSegmentsForStream(stream: string): Promise<number> {
    const res = await this.pool.query<{ count: string }>(`SELECT COUNT(*) AS count FROM segments WHERE stream = $1;`, [stream]);
    return Number(res.rows[0]?.count ?? 0);
  }

  async getManifestRow(stream: string): Promise<ManifestGenerationRow> {
    return getManifestGenerationWithExecutor(this.pool, stream);
  }
}

export async function getStreamWithExecutor(executor: PgExecutor, stream: string): Promise<StreamRow | null> {
  const res = await executor.query<PgStreamRow>(`SELECT * FROM streams WHERE stream = $1;`, [stream]);
  return res.rows[0] ? coerceStreamRow(res.rows[0]) : null;
}

export async function getSegmentByIndexWithExecutor(executor: PgExecutor, stream: string, segmentIndex: number): Promise<SegmentRow | null> {
  const res = await executor.query(
    `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms,
            payload_bytes, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag
     FROM segments
     WHERE stream = $1 AND segment_index = $2
     LIMIT 1;`,
    [stream, segmentIndex]
  );
  return res.rows[0] ? coerceSegmentRow(res.rows[0]) : null;
}

async function getManifestGenerationWithExecutor(executor: PgExecutor, stream: string): Promise<ManifestGenerationRow> {
  const res = await executor.query<{ generation: number | string }>(`SELECT generation FROM manifests WHERE stream = $1 LIMIT 1;`, [stream]);
  if (res.rows[0]) return { generation: Number(res.rows[0].generation) };
  await executor.query(
    `INSERT INTO manifests(stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes)
     VALUES($1, 0, 0, NULL, NULL, NULL)
     ON CONFLICT(stream) DO NOTHING;`,
    [stream]
  );
  return { generation: 0 };
}

export function coerceSegmentRow(row: any): SegmentRow {
  return {
    segment_id: String(row.segment_id),
    stream: String(row.stream),
    segment_index: Number(row.segment_index),
    start_offset: toBigInt(row.start_offset),
    end_offset: toBigInt(row.end_offset),
    block_count: Number(row.block_count),
    last_append_ms: toBigInt(row.last_append_ms),
    payload_bytes: toBigInt(row.payload_bytes),
    size_bytes: Number(row.size_bytes),
    local_path: String(row.local_path),
    created_at_ms: toBigInt(row.created_at_ms),
    uploaded_at_ms: row.uploaded_at_ms == null ? null : toBigInt(row.uploaded_at_ms),
    r2_etag: row.r2_etag == null ? null : String(row.r2_etag),
  };
}

function coerceStreamRow(row: PgStreamRow): StreamRow {
  const walRows = toBigInt(row.wal_rows);
  const walBytes = toBigInt(row.wal_bytes);
  const lastAppendMs = toBigInt(row.last_append_ms);
  return {
    stream: String(row.stream),
    created_at_ms: toBigInt(row.created_at_ms),
    updated_at_ms: toBigInt(row.updated_at_ms),
    content_type: String(row.content_type),
    profile: row.profile == null ? null : String(row.profile),
    stream_seq: row.stream_seq == null ? null : String(row.stream_seq),
    closed: Number(row.closed),
    closed_producer_id: row.closed_producer_id == null ? null : String(row.closed_producer_id),
    closed_producer_epoch: row.closed_producer_epoch == null ? null : Number(row.closed_producer_epoch),
    closed_producer_seq: row.closed_producer_seq == null ? null : Number(row.closed_producer_seq),
    ttl_seconds: row.ttl_seconds == null ? null : Number(row.ttl_seconds),
    epoch: Number(row.epoch),
    next_offset: toBigInt(row.next_offset),
    sealed_through: row.sealed_through == null ? -1n : toBigInt(row.sealed_through),
    uploaded_through: row.uploaded_through == null ? -1n : toBigInt(row.uploaded_through),
    uploaded_segment_count: row.uploaded_segment_count == null ? 0 : Number(row.uploaded_segment_count),
    pending_rows: row.pending_rows == null ? walRows : toBigInt(row.pending_rows),
    pending_bytes: row.pending_bytes == null ? walBytes : toBigInt(row.pending_bytes),
    logical_size_bytes: toBigInt(row.logical_size_bytes),
    wal_rows: walRows,
    wal_bytes: walBytes,
    last_append_ms: lastAppendMs,
    last_segment_cut_ms: row.last_segment_cut_ms == null ? lastAppendMs : toBigInt(row.last_segment_cut_ms),
    segment_in_progress: row.segment_in_progress == null ? 0 : Number(row.segment_in_progress),
    expires_at_ms: row.expires_at_ms == null ? null : toBigInt(row.expires_at_ms),
    stream_flags: Number(row.stream_flags),
  };
}

export function pgInt(value: bigint): string {
  return value.toString();
}

export function toBigInt(value: unknown): bigint {
  return typeof value === "bigint" ? value : BigInt(String(value));
}

export function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  return new Uint8Array(value as ArrayBuffer);
}
