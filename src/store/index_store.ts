import type {
  IndexRunRow,
  IndexStateRow,
  LexiconIndexRunRow,
  LexiconIndexStateRow,
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SecondaryIndexRunRow,
  SecondaryIndexStateRow,
  SegmentRow,
  StreamRow,
} from "./rows";
import type { MaybePromise } from "./capabilities";
import type { WalReadRow } from "./wal_store";

export type ManifestGenerationRow = {
  generation: number;
};

export interface IndexSharedReadStore {
  nowMs(): bigint;
  getStream(stream: string): MaybePromise<StreamRow | null>;
  getSegmentByIndex(stream: string, segmentIndex: number): MaybePromise<SegmentRow | null>;
  countUploadedSegments(stream: string): MaybePromise<number>;
  getManifestRow(stream: string): MaybePromise<ManifestGenerationRow>;
}

export interface RoutingIndexStore extends IndexSharedReadStore {
  getIndexState(stream: string): MaybePromise<IndexStateRow | null>;
  upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): MaybePromise<void>;
  updateIndexedThrough(stream: string, indexedThrough: number): MaybePromise<void>;
  listIndexRuns(stream: string): MaybePromise<IndexRunRow[]>;
  listIndexRunsAll(stream: string): MaybePromise<IndexRunRow[]>;
  listRetiredIndexRuns(stream: string): MaybePromise<IndexRunRow[]>;
  insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  deleteIndexRuns(runIds: string[]): MaybePromise<void>;
  deleteIndex(stream: string): MaybePromise<void>;
}

export interface SecondaryIndexStore extends IndexSharedReadStore {
  countSegmentsForStream(stream: string): MaybePromise<number>;
  getSecondaryIndexState(stream: string, indexName: string): MaybePromise<SecondaryIndexStateRow | null>;
  listSecondaryIndexStates(stream: string): MaybePromise<SecondaryIndexStateRow[]>;
  upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): MaybePromise<void>;
  updateSecondaryIndexedThrough(stream: string, indexName: string, indexedThrough: number): MaybePromise<void>;
  listSecondaryIndexRuns(stream: string, indexName: string): MaybePromise<SecondaryIndexRunRow[]>;
  listRetiredSecondaryIndexRuns(stream: string, indexName: string): MaybePromise<SecondaryIndexRunRow[]>;
  insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  deleteSecondaryIndexRuns(runIds: string[]): MaybePromise<void>;
  deleteSecondaryIndex(stream: string, indexName: string): MaybePromise<void>;
}

export interface LexiconIndexStore extends IndexSharedReadStore {
  countSegmentsForStream(stream: string): MaybePromise<number>;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  getLexiconIndexState(stream: string, sourceKind: string, sourceName: string): MaybePromise<LexiconIndexStateRow | null>;
  upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): MaybePromise<void>;
  updateLexiconIndexedThrough(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): MaybePromise<void>;
  listLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): MaybePromise<LexiconIndexRunRow[]>;
  listLexiconIndexRunsAll(stream: string, sourceKind: string, sourceName: string): MaybePromise<LexiconIndexRunRow[]>;
  listRetiredLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): MaybePromise<LexiconIndexRunRow[]>;
  insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): MaybePromise<void>;
  retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): MaybePromise<void>;
  deleteLexiconIndexRuns(runIds: string[]): MaybePromise<void>;
  deleteLexiconIndexSource(stream: string, sourceKind: string, sourceName: string): MaybePromise<void>;
}

export interface CompanionProgressStore {
  countUploadedSegments(stream: string): MaybePromise<number>;
  getSearchCompanionPlan(stream: string): MaybePromise<SearchCompanionPlanRow | null>;
  getSearchSegmentCompanion(stream: string, segmentIndex: number): MaybePromise<SearchSegmentCompanionRow | null>;
  listSearchSegmentCompanions(stream: string): MaybePromise<SearchSegmentCompanionRow[]>;
}

export interface SearchCompanionIndexStore {
  getSegmentByIndex(stream: string, segmentIndex: number): MaybePromise<SegmentRow | null>;
  countUploadedSegments(stream: string): MaybePromise<number>;
  getSearchCompanionPlan(stream: string): MaybePromise<SearchCompanionPlanRow | null>;
  listSearchCompanionPlanStreams(): MaybePromise<string[]>;
  upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): MaybePromise<void>;
  deleteSearchCompanionPlan(stream: string): MaybePromise<void>;
  getSearchSegmentCompanion(stream: string, segmentIndex: number): MaybePromise<SearchSegmentCompanionRow | null>;
  listSearchSegmentCompanions(stream: string): MaybePromise<SearchSegmentCompanionRow[]>;
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
  deleteSearchSegmentCompanions(stream: string): MaybePromise<void>;
}
