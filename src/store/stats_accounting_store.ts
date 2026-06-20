import type { MaybePromise } from "./capabilities";

export interface StorageStatsStore {
  countStreams(): MaybePromise<number>;
  getWalDbSizeBytes(): MaybePromise<number>;
  getMetaDbSizeBytes(): MaybePromise<number>;
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
  recordObjectStoreRequestByHash(streamHash: string, artifact: string, op: string, bytes?: number, count?: number): MaybePromise<void>;
}

export interface ObjectStoreAccountingStore extends ObjectStoreAccountingRecorder {
  getObjectStoreRequestSummaryByHash(streamHash: string): MaybePromise<ObjectStoreRequestSummary>;
}
