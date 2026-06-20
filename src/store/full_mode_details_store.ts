import type { MaybePromise } from "./capabilities";
import type {
  IndexRunRow,
  IndexStateRow,
  LexiconIndexRunRow,
  LexiconIndexStateRow,
  SearchCompanionPlanRow,
  SearchSegmentCompanionRow,
  SecondaryIndexRunRow,
  SecondaryIndexStateRow,
} from "./rows";
import type { ManifestRow } from "./segment_manifest_store";
import type { SchemaRegistryRow } from "./schema_profile_store";

export type IndexStorageSummary = {
  object_count: number;
  bytes: bigint;
};

export type SecondaryIndexStorageSummary = IndexStorageSummary & {
  index_name: string;
};

export type LexiconIndexStorageSummary = IndexStorageSummary & {
  source_kind: string;
  source_name: string;
};

export type ExactIndexDetailSnapshot = {
  indexName: string;
  state: SecondaryIndexStateRow | null;
  activeRuns: SecondaryIndexRunRow[];
  retiredRuns: SecondaryIndexRunRow[];
};

export type FullModeDetailsSnapshot = {
  segmentCount: number;
  uploadedSegmentCount: number;
  manifest: ManifestRow;
  schemaRow: SchemaRegistryRow | null;
  uploadedSegmentBytes: bigint;
  pendingSealedSegmentBytes: bigint;
  routingIndexStorage: IndexStorageSummary;
  secondaryIndexStorage: SecondaryIndexStorageSummary[];
  lexiconIndexStorage: LexiconIndexStorageSummary[];
  bundledCompanionStorage: IndexStorageSummary;
  routingState: IndexStateRow | null;
  routingRuns: IndexRunRow[];
  retiredRoutingRuns: IndexRunRow[];
  exactIndexes: ExactIndexDetailSnapshot[];
  routingLexiconState: LexiconIndexStateRow | null;
  routingLexiconRuns: LexiconIndexRunRow[];
  retiredRoutingLexiconRuns: LexiconIndexRunRow[];
  companionPlan: SearchCompanionPlanRow | null;
  companionRows: SearchSegmentCompanionRow[];
};

export type FullModeDetailsSnapshotRequest = {
  stream: string;
  exactIndexNames: string[];
};

export type FullModeLagSnapshotRequest = {
  stream: string;
  segmentIndexes: number[];
};

export interface FullModeDetailsStore {
  getFullModeDetailsSnapshot(request: FullModeDetailsSnapshotRequest): MaybePromise<FullModeDetailsSnapshot>;
  getFullModeLagSnapshot(request: FullModeLagSnapshotRequest): MaybePromise<Map<number, bigint>>;
}
