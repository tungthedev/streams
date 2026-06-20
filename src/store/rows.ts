export type StreamRow = {
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

export type SegmentRow = {
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

export type SegmentMetaRow = {
  stream: string;
  segment_count: number;
  segment_offsets: Uint8Array;
  segment_blocks: Uint8Array;
  segment_last_ts: Uint8Array;
};

export type IndexStateRow = {
  stream: string;
  index_secret: Uint8Array;
  indexed_through: number;
  updated_at_ms: bigint;
};

export type IndexRunRow = {
  run_id: string;
  stream: string;
  level: number;
  start_segment: number;
  end_segment: number;
  object_key: string;
  size_bytes: number;
  filter_len: number;
  record_count: number;
  retired_gen: number | null;
  retired_at_ms: bigint | null;
};

export type SecondaryIndexStateRow = {
  stream: string;
  index_name: string;
  index_secret: Uint8Array;
  config_hash: string;
  indexed_through: number;
  updated_at_ms: bigint;
};

export type SecondaryIndexRunRow = {
  run_id: string;
  stream: string;
  index_name: string;
  level: number;
  start_segment: number;
  end_segment: number;
  object_key: string;
  size_bytes: number;
  filter_len: number;
  record_count: number;
  retired_gen: number | null;
  retired_at_ms: bigint | null;
};

export type LexiconIndexStateRow = {
  stream: string;
  source_kind: string;
  source_name: string;
  indexed_through: number;
  updated_at_ms: bigint;
};

export type LexiconIndexRunRow = {
  run_id: string;
  stream: string;
  source_kind: string;
  source_name: string;
  level: number;
  start_segment: number;
  end_segment: number;
  object_key: string;
  size_bytes: number;
  record_count: number;
  retired_gen: number | null;
  retired_at_ms: bigint | null;
};

export type SearchCompanionPlanRow = {
  stream: string;
  generation: number;
  plan_hash: string;
  plan_json: string;
  updated_at_ms: bigint;
};

export type SearchSegmentCompanionRow = {
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
