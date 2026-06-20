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
import type { WalReadRow } from "./wal_store";

export type ManifestGenerationRow = {
  generation: number;
};

export interface IndexSharedReadStore {
  nowMs(): bigint;
  getStream(stream: string): StreamRow | null;
  getSegmentByIndex(stream: string, segmentIndex: number): SegmentRow | null;
  countUploadedSegments(stream: string): number;
  getManifestRow(stream: string): ManifestGenerationRow;
}

export interface RoutingIndexStore extends IndexSharedReadStore {
  getIndexState(stream: string): IndexStateRow | null;
  upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): void;
  updateIndexedThrough(stream: string, indexedThrough: number): void;
  listIndexRuns(stream: string): IndexRunRow[];
  listIndexRunsAll(stream: string): IndexRunRow[];
  listRetiredIndexRuns(stream: string): IndexRunRow[];
  insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  deleteIndexRuns(runIds: string[]): void;
  deleteIndex(stream: string): void;
}

export interface SecondaryIndexStore extends IndexSharedReadStore {
  countSegmentsForStream(stream: string): number;
  getSecondaryIndexState(stream: string, indexName: string): SecondaryIndexStateRow | null;
  listSecondaryIndexStates(stream: string): SecondaryIndexStateRow[];
  upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): void;
  updateSecondaryIndexedThrough(stream: string, indexName: string, indexedThrough: number): void;
  listSecondaryIndexRuns(stream: string, indexName: string): SecondaryIndexRunRow[];
  listRetiredSecondaryIndexRuns(stream: string, indexName: string): SecondaryIndexRunRow[];
  insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  deleteSecondaryIndexRuns(runIds: string[]): void;
  deleteSecondaryIndex(stream: string, indexName: string): void;
}

export interface LexiconIndexStore extends IndexSharedReadStore {
  countSegmentsForStream(stream: string): number;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  getLexiconIndexState(stream: string, sourceKind: string, sourceName: string): LexiconIndexStateRow | null;
  upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): void;
  updateLexiconIndexedThrough(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): void;
  listLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): LexiconIndexRunRow[];
  listLexiconIndexRunsAll(stream: string, sourceKind: string, sourceName: string): LexiconIndexRunRow[];
  listRetiredLexiconIndexRuns(stream: string, sourceKind: string, sourceName: string): LexiconIndexRunRow[];
  insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): void;
  retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): void;
  deleteLexiconIndexRuns(runIds: string[]): void;
  deleteLexiconIndexSource(stream: string, sourceKind: string, sourceName: string): void;
}

export interface CompanionProgressStore {
  countUploadedSegments(stream: string): number;
  getSearchCompanionPlan(stream: string): SearchCompanionPlanRow | null;
  getSearchSegmentCompanion(stream: string, segmentIndex: number): SearchSegmentCompanionRow | null;
}

export interface SearchCompanionIndexStore {
  getSegmentByIndex(stream: string, segmentIndex: number): SegmentRow | null;
  countUploadedSegments(stream: string): number;
  getSearchCompanionPlan(stream: string): SearchCompanionPlanRow | null;
  listSearchCompanionPlanStreams(): string[];
  upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): void;
  deleteSearchCompanionPlan(stream: string): void;
  getSearchSegmentCompanion(stream: string, segmentIndex: number): SearchSegmentCompanionRow | null;
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
  deleteSearchSegmentCompanions(stream: string): void;
}
