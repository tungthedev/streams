import type { PoolClient } from "pg";

export type PgExecutor = Pick<PoolClient, "query">;

export type PgStreamRow = {
  stream: string;
  created_at_ms: string | number | bigint;
  updated_at_ms: string | number | bigint;
  content_type: string;
  profile: string | null;
  stream_seq: string | null;
  closed: number | boolean;
  closed_producer_id: string | null;
  closed_producer_epoch: number | null;
  closed_producer_seq: number | null;
  ttl_seconds: number | null;
  epoch: number;
  next_offset: string | number | bigint;
  sealed_through?: string | number | bigint;
  uploaded_through?: string | number | bigint;
  uploaded_segment_count?: number | string;
  pending_rows?: string | number | bigint;
  pending_bytes?: string | number | bigint;
  logical_size_bytes: string | number | bigint;
  wal_rows: string | number | bigint;
  wal_bytes: string | number | bigint;
  last_append_ms: string | number | bigint;
  last_segment_cut_ms?: string | number | bigint;
  segment_in_progress?: number | string;
  expires_at_ms: string | number | bigint | null;
  stream_flags: number;
};
