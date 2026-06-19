import type { StoreAppendBatch, StoreAppendTask } from "./append";

export type WalReadRow = {
  offset: bigint;
  tsMs: bigint;
  routingKey: Uint8Array | null;
  contentType: string | null;
  payload: Uint8Array;
};

export interface WalAppendStore {
  appendBatch(tasks: StoreAppendTask[]): Promise<StoreAppendBatch>;
}

export interface WalReadStore {
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  readWalRangeDesc(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  getWalOldestTimestampMsForRead(stream: string): Promise<bigint | null>;
}

export interface WalStore extends WalAppendStore, WalReadStore {}
