import type { StreamReadRow } from "../store/segment_read_store";
import type { PgExecutor } from "./types";
import { pgInt } from "./rows";

export async function restorePostgresStreamRow(executor: PgExecutor, row: StreamReadRow): Promise<void> {
  await executor.query(
    `INSERT INTO streams(
       stream, created_at_ms, updated_at_ms, content_type, profile, stream_seq, closed,
       closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
       epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
       pending_rows, pending_bytes, logical_size_bytes, wal_rows, wal_bytes,
       last_append_ms, last_segment_cut_ms, segment_in_progress, expires_at_ms, stream_flags
     )
     VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
     ON CONFLICT(stream) DO UPDATE SET
       created_at_ms = excluded.created_at_ms,
       updated_at_ms = excluded.updated_at_ms,
       content_type = excluded.content_type,
       profile = excluded.profile,
       stream_seq = excluded.stream_seq,
       closed = excluded.closed,
       closed_producer_id = excluded.closed_producer_id,
       closed_producer_epoch = excluded.closed_producer_epoch,
       closed_producer_seq = excluded.closed_producer_seq,
       ttl_seconds = excluded.ttl_seconds,
       epoch = excluded.epoch,
       next_offset = excluded.next_offset,
       sealed_through = excluded.sealed_through,
       uploaded_through = excluded.uploaded_through,
       uploaded_segment_count = excluded.uploaded_segment_count,
       pending_rows = excluded.pending_rows,
       pending_bytes = excluded.pending_bytes,
       logical_size_bytes = excluded.logical_size_bytes,
       wal_rows = excluded.wal_rows,
       wal_bytes = excluded.wal_bytes,
       last_append_ms = excluded.last_append_ms,
       last_segment_cut_ms = excluded.last_segment_cut_ms,
       segment_in_progress = excluded.segment_in_progress,
       segment_claim_token = NULL,
       segment_claimed_at_ms = NULL,
       expires_at_ms = excluded.expires_at_ms,
       stream_flags = excluded.stream_flags;`,
    [
      row.stream,
      pgInt(row.created_at_ms),
      pgInt(row.updated_at_ms),
      row.content_type,
      row.profile,
      row.stream_seq,
      row.closed,
      row.closed_producer_id,
      row.closed_producer_epoch,
      row.closed_producer_seq,
      row.ttl_seconds,
      row.epoch,
      pgInt(row.next_offset),
      pgInt(row.sealed_through),
      pgInt(row.uploaded_through),
      row.uploaded_segment_count ?? 0,
      pgInt(row.pending_rows),
      pgInt(row.pending_bytes),
      pgInt(row.logical_size_bytes),
      pgInt(row.wal_rows),
      pgInt(row.wal_bytes),
      pgInt(row.last_append_ms),
      pgInt(row.last_segment_cut_ms),
      row.segment_in_progress ?? 0,
      row.expires_at_ms == null ? null : pgInt(row.expires_at_ms),
      row.stream_flags,
    ]
  );
}

export async function upsertPostgresStreamProfile(
  executor: PgExecutor,
  nowMs: bigint,
  stream: string,
  profileJson: string
): Promise<void> {
  await executor.query(
    `INSERT INTO stream_profiles(stream, profile_json, updated_at_ms)
     VALUES($1, $2, $3)
     ON CONFLICT(stream) DO UPDATE SET
       profile_json = excluded.profile_json,
       updated_at_ms = excluded.updated_at_ms;`,
    [stream, profileJson, pgInt(nowMs)]
  );
}

export async function deletePostgresStreamProfile(executor: PgExecutor, stream: string): Promise<void> {
  await executor.query(`DELETE FROM stream_profiles WHERE stream = $1;`, [stream]);
}

export async function upsertPostgresSchemaRegistry(
  executor: PgExecutor,
  nowMs: bigint,
  stream: string,
  registryJson: string
): Promise<void> {
  await executor.query(
    `INSERT INTO schemas(stream, schema_json, updated_at_ms)
     VALUES($1, $2, $3)
     ON CONFLICT(stream) DO UPDATE SET
       schema_json = excluded.schema_json,
       updated_at_ms = excluded.updated_at_ms;`,
    [stream, registryJson, pgInt(nowMs)]
  );
}
