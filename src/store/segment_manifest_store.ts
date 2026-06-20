import type { MaybePromise } from "./capabilities";
import type { StreamReadRow } from "./segment_read_store";
import type { WalReadRow } from "./wal_store";
import type {
  IndexRunRow,
  IndexStateRow,
  LexiconIndexRunRow,
  LexiconIndexStateRow,
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SecondaryIndexRunRow,
  SecondaryIndexStateRow,
  SegmentMetaRow,
  SegmentRow,
} from "./rows";
export type {
  IndexRunRow,
  IndexStateRow,
  LexiconIndexRunRow,
  LexiconIndexStateRow,
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SecondaryIndexRunRow,
  SecondaryIndexStateRow,
  SegmentMetaRow,
  SegmentRow,
} from "./rows";

export type SegmentCandidateRow = {
  stream: string;
  pending_bytes: bigint;
  pending_rows: bigint;
  last_segment_cut_ms: bigint;
  sealed_through: bigint;
  next_offset: bigint;
  epoch: number;
};

export type SegmentClaim = {
  token: string;
};

export type SealedSegmentCommit = {
  segmentId: string;
  stream: string;
  segmentIndex: number;
  startOffset: bigint;
  endOffset: bigint;
  blockCount: number;
  lastAppendMs: bigint;
  payloadBytes: bigint;
  sizeBytes: number;
  localPath: string;
  rowsSealed: bigint;
  claimToken?: string;
};

export type ManifestRow = {
  stream: string;
  generation: number;
  uploaded_generation: number;
  last_uploaded_at_ms: bigint | null;
  last_uploaded_etag: string | null;
  last_uploaded_size_bytes: bigint | null;
};

export type StreamProfileRow = {
  stream: string;
  profile_json: string;
  updated_at_ms: bigint;
};

export type ManifestPublicationSnapshot = {
  publicationToken?: string;
  streamRow: StreamReadRow;
  prevUploadedSegmentCount: number;
  uploadedPrefixCount: number;
  uploadedThrough: bigint;
  publishedLogicalSizeBytes: bigint;
  generation: number;
  segmentMeta: SegmentMetaRow;
  profileJson: Record<string, any> | null;
  indexState: IndexStateRow | null;
  indexRuns: IndexRunRow[];
  retiredRuns: IndexRunRow[];
  secondaryIndexStates: SecondaryIndexStateRow[];
  secondaryIndexRuns: SecondaryIndexRunRow[];
  retiredSecondaryIndexRuns: SecondaryIndexRunRow[];
  lexiconIndexStates: LexiconIndexStateRow[];
  lexiconIndexRuns: LexiconIndexRunRow[];
  retiredLexiconIndexRuns: LexiconIndexRunRow[];
  searchCompanionPlan: SearchCompanionPlanRow | null;
  searchSegmentCompanions: SearchSegmentCompanionRow[];
};

export type ManifestPublicationOptions = {
  wait?: boolean;
};

export interface SegmentStore {
  getSegmentStreamState(stream: string): MaybePromise<StreamReadRow | null>;
  isDeleted(row: StreamReadRow): boolean;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  candidates(minPendingBytes: bigint, minPendingRows: bigint, maxIntervalMs: bigint, limit: number): MaybePromise<SegmentCandidateRow[]>;
  recentSegmentCompressionRatio(stream: string, limit?: number): MaybePromise<number | null>;
  tryClaimSegment(stream: string): MaybePromise<SegmentClaim | null>;
  setSegmentInProgress(stream: string, inProgress: number, claim?: SegmentClaim): MaybePromise<void>;
  nextSegmentIndexForStream(stream: string): MaybePromise<number>;
  commitSealedSegment(row: SealedSegmentCommit): MaybePromise<void>;
}

export interface ManifestStore {
  nowMs(): bigint;
  countPendingSegments(): MaybePromise<number>;
  pendingUploadHeads(limit: number): MaybePromise<SegmentRow[]>;
  markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): MaybePromise<void>;
  loadManifestPublicationSnapshot(stream: string, opts?: ManifestPublicationOptions): MaybePromise<ManifestPublicationSnapshot | null>;
  commitManifest(
    stream: string,
    generation: number,
    etag: string,
    uploadedAtMs: bigint,
    uploadedThrough: bigint,
    sizeBytes: number,
    publicationToken?: string
  ): MaybePromise<void>;
  releaseManifestPublication?(publicationToken: string): MaybePromise<void>;
  getSegmentForManifestCleanup(stream: string, segmentIndex: number): MaybePromise<SegmentRow | null>;
}
