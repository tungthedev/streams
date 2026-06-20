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

export type ObjectStoreRequestCountRow = {
  artifact: string;
  op: string;
  count: bigint;
};

export function summarizeObjectStoreRequestCounts(rows: ObjectStoreRequestCountRow[]): ObjectStoreRequestSummary {
  const byArtifact = new Map<string, { puts: bigint; gets: bigint; heads: bigint; lists: bigint; deletes: bigint; reads: bigint }>();
  let puts = 0n;
  let gets = 0n;
  let heads = 0n;
  let lists = 0n;
  let deletes = 0n;
  for (const row of rows) {
    const artifact = String(row.artifact);
    const op = String(row.op);
    const count = row.count;
    const entry = byArtifact.get(artifact) ?? { puts: 0n, gets: 0n, heads: 0n, lists: 0n, deletes: 0n, reads: 0n };
    if (op === "put") {
      entry.puts += count;
      puts += count;
    } else if (op === "get") {
      entry.gets += count;
      entry.reads += count;
      gets += count;
    } else if (op === "head") {
      entry.heads += count;
      entry.reads += count;
      heads += count;
    } else if (op === "list") {
      entry.lists += count;
      entry.reads += count;
      lists += count;
    } else if (op === "delete") {
      entry.deletes += count;
      deletes += count;
    }
    byArtifact.set(artifact, entry);
  }
  return {
    puts,
    reads: gets + heads + lists,
    gets,
    heads,
    lists,
    deletes,
    by_artifact: Array.from(byArtifact.entries()).map(([artifact, entry]) => ({ artifact, ...entry })),
  };
}

export interface ObjectStoreAccountingRecorder {
  recordObjectStoreRequestByHash(streamHash: string, artifact: string, op: string, bytes?: number, count?: number): MaybePromise<void>;
}

export interface ObjectStoreAccountingStore extends ObjectStoreAccountingRecorder {
  getObjectStoreRequestSummaryByHash(streamHash: string): MaybePromise<ObjectStoreRequestSummary>;
}
