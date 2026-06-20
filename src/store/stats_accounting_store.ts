export interface StorageStatsStore {
  countStreams(): number;
  getWalDbSizeBytes(): number;
  getMetaDbSizeBytes(): number;
}

export type ObjectStoreRequestSummary = {
  puts: bigint;
  reads: bigint;
  gets: bigint;
  heads: bigint;
  lists: bigint;
  deletes: bigint;
  by_artifact: Array<{
    artifact: string;
    puts: bigint;
    gets: bigint;
    heads: bigint;
    lists: bigint;
    deletes: bigint;
    reads: bigint;
  }>;
};

export interface ObjectStoreAccountingRecorder {
  recordObjectStoreRequestByHash(streamHash: string, artifact: string, op: string, bytes?: number, count?: number): void;
}

export interface ObjectStoreAccountingStore extends ObjectStoreAccountingRecorder {
  getObjectStoreRequestSummaryByHash(streamHash: string): ObjectStoreRequestSummary;
}
