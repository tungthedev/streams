import type {
  IndexRunRow,
  LexiconIndexRunRow,
  SecondaryIndexRunRow,
} from "./rows";
import type { StreamReadRow } from "./segment_read_store";
import type { ProfileTouchControlStore } from "./touch_store";
import type { MaybePromise } from "./capabilities";

export interface BootstrapRestoreStore {
  readonly touch: ProfileTouchControlStore;
  close(): MaybePromise<void>;
  nowMs(): bigint;
  beginRestoreStream?(stream: string): MaybePromise<void>;
  commitRestoreStream?(stream: string): MaybePromise<void>;
  rollbackRestoreStream?(stream: string): MaybePromise<void>;
  restoreStreamRow(row: StreamReadRow): MaybePromise<void>;
  upsertStreamProfile(stream: string, profileJson: string): MaybePromise<void>;
  deleteStreamProfile(stream: string): MaybePromise<void>;
  upsertSegmentMeta(stream: string, count: number, offsets: Uint8Array, blocks: Uint8Array, lastTs: Uint8Array): MaybePromise<void>;
  upsertManifestRow(
    stream: string,
    generation: number,
    uploadedGeneration: number,
    uploadedAtMs: bigint | null,
    etag: string | null,
    sizeBytes: number | null
  ): MaybePromise<void>;
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
  }): MaybePromise<void>;
  markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): MaybePromise<void>;
  upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): MaybePromise<void>;
  insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): MaybePromise<void>;
  insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): MaybePromise<void>;
  insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): MaybePromise<void>;
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
  ): MaybePromise<void>;
  upsertSchemaRegistry(stream: string, registryJson: string): MaybePromise<void>;
  setSchemaUploadedSizeBytes(stream: string, sizeBytes: number): MaybePromise<void>;
}
