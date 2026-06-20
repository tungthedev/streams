import type {
  IndexRunRow,
  LexiconIndexRunRow,
  SecondaryIndexRunRow,
} from "./rows";
import type { StreamReadRow } from "./segment_read_store";
import type { ProfileTouchControlStore } from "./touch_store";

export interface BootstrapRestoreStore {
  readonly touch: ProfileTouchControlStore;
  close(): void;
  nowMs(): bigint;
  restoreStreamRow(row: StreamReadRow): void;
  upsertStreamProfile(stream: string, profileJson: string): void;
  deleteStreamProfile(stream: string): void;
  upsertSegmentMeta(stream: string, count: number, offsets: Uint8Array, blocks: Uint8Array, lastTs: Uint8Array): void;
  upsertManifestRow(
    stream: string,
    generation: number,
    uploadedGeneration: number,
    uploadedAtMs: bigint | null,
    etag: string | null,
    sizeBytes: number | null
  ): void;
  createSegmentRow(row: {
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
  }): void;
  markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): void;
  upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): void;
  insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): void;
  insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): void;
  insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): void;
  upsertSearchSegmentCompanion(
    stream: string,
    segmentIndex: number,
    objectKey: string,
    planGeneration: number,
    sectionsJson: string,
    sectionSizesJson: string,
    sizeBytes: number,
    primaryTimestampMinMs: bigint | null,
    primaryTimestampMaxMs: bigint | null
  ): void;
  upsertSchemaRegistry(stream: string, registryJson: string): void;
  setSchemaUploadedSizeBytes(stream: string, sizeBytes: number): void;
}
