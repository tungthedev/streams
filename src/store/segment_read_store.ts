export type StreamReadRow = {
  stream: string;
  created_at_ms: bigint;
  updated_at_ms: bigint;
  content_type: string;
  profile: string | null;
  stream_seq: string | null;
  closed: number;
  closed_producer_id: string | null;
  closed_producer_epoch: number | null;
  closed_producer_seq: number | null;
  ttl_seconds: number | null;
  epoch: number;
  next_offset: bigint;
  sealed_through: bigint;
  uploaded_through: bigint;
  uploaded_segment_count: number;
  pending_rows: bigint;
  pending_bytes: bigint;
  logical_size_bytes: bigint;
  wal_rows: bigint;
  wal_bytes: bigint;
  last_append_ms: bigint;
  last_segment_cut_ms: bigint;
  segment_in_progress: number;
  expires_at_ms: bigint | null;
  stream_flags: number;
};

export type SegmentReadRow = {
  segment_id: string;
  stream: string;
  segment_index: number;
  start_offset: bigint;
  end_offset: bigint;
  block_count: number;
  last_append_ms: bigint;
  payload_bytes: bigint;
  size_bytes: number;
  local_path: string;
  created_at_ms: bigint;
  uploaded_at_ms: bigint | null;
  r2_etag: string | null;
};

export type SearchCompanionPlanReadRow = {
  stream: string;
  generation: number;
  plan_hash: string;
  plan_json: string;
  updated_at_ms: bigint;
};

export type SearchSegmentCompanionReadRow = {
  stream: string;
  segment_index: number;
  object_key: string;
  plan_generation: number;
  sections_json: string;
  section_sizes_json: string;
  size_bytes: number;
  primary_timestamp_min_ms: bigint | null;
  primary_timestamp_max_ms: bigint | null;
  updated_at_ms: bigint;
};

export interface StreamReadStore {
  nowMsForRead(): Promise<bigint>;
  getStreamForRead(stream: string): Promise<StreamReadRow | null>;
  isDeleted(row: StreamReadRow): boolean;
}

export interface SegmentReadStore {
  listSegmentsForRead(stream: string): Promise<SegmentReadRow[]>;
  getSegmentByIndexForRead(stream: string, segmentIndex: number): Promise<SegmentReadRow | null>;
  findSegmentForOffsetForRead(stream: string, offset: bigint): Promise<SegmentReadRow | null>;
  countSegmentsForRead(stream: string): Promise<number>;
  getSearchCompanionPlanForRead(stream: string): Promise<SearchCompanionPlanReadRow | null>;
  listSearchSegmentCompanionsForRead(stream: string): Promise<SearchSegmentCompanionReadRow[]>;
  getSearchSegmentCompanionForRead(stream: string, segmentIndex: number): Promise<SearchSegmentCompanionReadRow | null>;
}
