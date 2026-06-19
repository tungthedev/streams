import type { StoreAppendBatch, StoreAppendTask } from "./append";

export interface WalStore {
  appendBatch(tasks: StoreAppendTask[]): Promise<StoreAppendBatch>;
}
