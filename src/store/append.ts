import type { Result } from "better-result";

export type AppendRow = {
  routingKey: Uint8Array | null;
  contentType: string | null;
  payload: Uint8Array;
};

export type ProducerInfo = {
  id: string;
  epoch: number;
  seq: number;
};

export type AppendSuccess = {
  lastOffset: bigint;
  appendedRows: number;
  closed: boolean;
  duplicate: boolean;
  producer?: { epoch: number; seq: number };
};

export type StoreAppendError =
  | { kind: "not_found" | "gone" | "content_type_mismatch" }
  | { kind: "stream_seq"; expected: string; received: string }
  | { kind: "closed"; lastOffset: bigint }
  | { kind: "producer_stale_epoch"; producerEpoch: number }
  | { kind: "producer_gap"; expected: number; received: number }
  | { kind: "producer_epoch_seq" };

export type StoreAppendResult = Result<AppendSuccess, StoreAppendError>;

export type StoreAppendTask = {
  stream: string;
  baseAppendMs: bigint;
  rows: AppendRow[];
  contentType: string | null;
  streamSeq: string | null;
  producer: ProducerInfo | null;
  close: boolean;
};

export type StoreAppendBatchResult = {
  results: StoreAppendResult[];
  walBytesCommitted: number;
};

export type StoreAppendBatchError = { kind: "retryable" };

export type StoreAppendBatch = Result<StoreAppendBatchResult, StoreAppendBatchError>;
