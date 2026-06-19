import type { Config } from "./config";
import type { StatsCollector } from "./stats";
import type { BackpressureGate } from "./backpressure";
import type { Metrics } from "./metrics";
import { Result } from "better-result";
import {
  type AppendRow,
  type AppendSuccess,
  type ProducerInfo,
  type StoreAppendBatch,
  type StoreAppendError,
  type StoreAppendResult,
  type StoreAppendTask,
} from "./store/append";
import type { WalStore } from "./store/wal_store";

export type AppendError = StoreAppendError | { kind: "overloaded" | "internal" };
export type AppendResult = Result<AppendSuccess, AppendError>;
export type { AppendRow, AppendSuccess, ProducerInfo } from "./store/append";

type AppendTask = {
  stream: string;
  baseAppendMs: bigint;
  rows: AppendRow[];
  contentType: string | null;
  streamSeq: string | null;
  producer: ProducerInfo | null;
  close: boolean;
  reservedBytes: number;
  enqueuedAtMs?: number;
  resolve: (r: AppendResult) => void;
};

export class IngestQueue {
  private readonly cfg: Config;
  private readonly wal: WalStore;
  private readonly stats?: StatsCollector;
  private readonly gate?: BackpressureGate;
  private readonly metrics?: Metrics;
  private readonly q: AppendTask[] = [];
  private timer: any | null = null;
  private scheduled = false;
  private queuedBytes = 0;
  private lastBacklogWarnMs = 0;
  private flushPromise: Promise<void> | null = null;
  private flushRequested = false;

  constructor(cfg: Config, wal: WalStore, stats?: StatsCollector, gate?: BackpressureGate, metrics?: Metrics) {
    this.cfg = cfg;
    this.wal = wal;
    this.stats = stats;
    this.gate = gate;
    this.metrics = metrics;

    this.timer = setInterval(() => {
      void this.flush();
    }, this.cfg.ingestFlushIntervalMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /**
   * Enqueue an append. This returns after the batch containing it has committed.
   */
  append(args: {
    stream: string;
    baseAppendMs: bigint;
    rows: AppendRow[];
    contentType: string | null;
    streamSeq?: string | null;
    producer?: ProducerInfo | null;
    close?: boolean;
  }, opts?: { bypassBackpressure?: boolean; priority?: "high" | "normal" }): Promise<AppendResult> {
    const bytes = args.rows.reduce((acc, r) => acc + r.payload.byteLength, 0);
    if (!opts?.bypassBackpressure) {
      if (this.q.length >= this.cfg.ingestMaxQueueRequests || this.queuedBytes + bytes > this.cfg.ingestMaxQueueBytes) {
        if (this.metrics) this.metrics.record("tieredstore.backpressure.over_limit", 1, "count", { reason: "queue" });
        return Promise.resolve(Result.err({ kind: "overloaded" }));
      }
      if (this.gate && !this.gate.reserve(bytes)) {
        if (this.metrics) this.metrics.record("tieredstore.backpressure.over_limit", 1, "count", { reason: "backlog" });
        this.warnBacklog();
        return Promise.resolve(Result.err({ kind: "overloaded" }));
      }
    }
    this.queuedBytes += bytes;
    return new Promise((resolve) => {
      const task: AppendTask = {
        stream: args.stream,
        baseAppendMs: args.baseAppendMs,
        rows: args.rows,
        contentType: args.contentType ?? null,
        streamSeq: args.streamSeq ?? null,
        producer: args.producer ?? null,
        close: args.close ?? false,
        reservedBytes: opts?.bypassBackpressure ? 0 : bytes,
        enqueuedAtMs: this.stats ? Date.now() : undefined,
        resolve,
      };
      if (opts?.priority === "high") this.q.unshift(task);
      else this.q.push(task);
      // Opportunistic flush if the queue gets large.
      if (!this.scheduled && this.q.length >= this.cfg.ingestMaxBatchRequests) {
        this.scheduled = true;
        setTimeout(() => {
          this.scheduled = false;
          void this.flush();
        }, 0);
      }
    });
  }

  appendInternal(args: {
    stream: string;
    baseAppendMs: bigint;
    rows: AppendRow[];
    contentType: string | null;
  }): Promise<AppendResult> {
    return this.append(args, { bypassBackpressure: true, priority: "high" });
  }

  getQueueStats(): { requests: number; bytes: number } {
    return { requests: this.q.length, bytes: this.queuedBytes };
  }

  getMemoryStats(): { queuedPayloadBytes: number; queuedRequests: number } {
    return {
      queuedPayloadBytes: this.queuedBytes,
      queuedRequests: this.q.length,
    };
  }

  isQueueFull(): boolean {
    return this.q.length >= this.cfg.ingestMaxQueueRequests || this.queuedBytes >= this.cfg.ingestMaxQueueBytes;
  }

  private warnBacklog(): void {
    if (!this.gate) return;
    const now = Date.now();
    if (now - this.lastBacklogWarnMs < 10_000) return;
    this.lastBacklogWarnMs = now;
    const current = this.gate.getCurrentBytes();
    const max = this.gate.getMaxBytes();
    const msg =
      `[backpressure] local backlog ${formatBytes(current)} exceeds limit ${formatBytes(max)}; rejecting appends (DS_LOCAL_BACKLOG_MAX_BYTES)`;
    // eslint-disable-next-line no-console
    console.warn(msg);
  }

  async flush(): Promise<void> {
    this.flushRequested = true;
    if (this.flushPromise) return this.flushPromise;
    this.flushPromise = this.runFlushLoop().finally(() => {
      this.flushPromise = null;
    });
    return this.flushPromise;
  }

  private async runFlushLoop(): Promise<void> {
    while (this.flushRequested) {
      this.flushRequested = false;
      await this.flushOnce();
    }
  }

  private async flushOnce(): Promise<void> {
    if (this.q.length === 0) return;
    const flushStartMs = Date.now();
    let busyWaitMs = 0;

    // Drain up to limits.
    const batch: AppendTask[] = [];
    let batchBytes = 0;
    let batchReservedBytes = 0;
    let drainCount = 0;
    while (drainCount < this.q.length && batch.length < this.cfg.ingestMaxBatchRequests && batchBytes < this.cfg.ingestMaxBatchBytes) {
      const t = this.q[drainCount]!;
      batch.push(t);
      drainCount += 1;
      for (const r of t.rows) batchBytes += r.payload.byteLength;
      batchReservedBytes += t.reservedBytes;
    }
    if (drainCount > 0) {
      this.q.splice(0, drainCount);
    }
    this.queuedBytes = Math.max(0, this.queuedBytes - batchBytes);

    // Compute queue wait/backpressure stats before executing the batch.
    let bpOverMs = 0;
    if (this.stats) {
      const budgetMs = this.stats.getBackpressureBudgetMs();
      const nowMs = Date.now();
      for (const t of batch) {
        if (t.enqueuedAtMs == null) continue;
        const waitMs = Math.max(0, nowMs - t.enqueuedAtMs);
        if (waitMs > budgetMs) {
          bpOverMs += waitMs - budgetMs;
        }
      }
    }

    let walBytesCommitted = 0;
    let results: StoreAppendResult[] = [];
    const storeBatch: StoreAppendTask[] = batch.map(({ stream, baseAppendMs, rows, contentType, streamSeq, producer, close }) => ({
      stream,
      baseAppendMs,
      rows,
      contentType,
      streamSeq,
      producer,
      close,
    }));

    const sleep = (ms: number): Promise<void> => new Promise((res) => setTimeout(res, ms));

    try {
      const maxBusyMs = Math.max(0, this.cfg.ingestBusyTimeoutMs);
      const startMs = Date.now();
      let attempt = 0;
      let retryLimitExceeded = false;
      while (true) {
        const batchResult = await this.wal.appendBatch(storeBatch);
        if (Result.isOk(batchResult)) {
          results = batchResult.value.results;
          walBytesCommitted = batchResult.value.walBytesCommitted;
          break;
        }
        if (!isRetryableBatchResult(batchResult) || maxBusyMs <= 0) {
          retryLimitExceeded = true;
          break;
        }
        const elapsed = Date.now() - startMs;
        if (elapsed >= maxBusyMs) {
          retryLimitExceeded = true;
          break;
        }
        const delay = Math.min(200, 5 * 2 ** attempt);
        attempt += 1;
        busyWaitMs += delay;
        await sleep(delay);
      }
      if (retryLimitExceeded) {
        if (this.gate && batchReservedBytes > 0) this.gate.release(batchReservedBytes);
        for (const t of batch) t.resolve(Result.err({ kind: "internal" }));
        const elapsedNs = (Date.now() - flushStartMs) * 1_000_000;
        if (this.metrics) {
          this.metrics.record("tieredstore.ingest.flush.latency", elapsedNs, "ns");
          if (busyWaitMs > 0) this.metrics.record("tieredstore.ingest.store_retry.wait", busyWaitMs * 1_000_000, "ns");
        }
        return;
      }
      if (this.gate) {
        const reservedCommitted = Math.min(batchReservedBytes, walBytesCommitted);
        this.gate.commit(walBytesCommitted, reservedCommitted);
        const extra = batchReservedBytes - walBytesCommitted;
        if (extra > 0) this.gate.release(extra);
      }
      if (this.stats && walBytesCommitted > 0) this.stats.recordWalCommitBytes(walBytesCommitted);
      if (this.stats && bpOverMs > 0) this.stats.recordBackpressureOverMs(bpOverMs);
      for (let i = 0; i < batch.length; i++) batch[i].resolve(toAppendResult(results[i]));
      const elapsedNs = (Date.now() - flushStartMs) * 1_000_000;
      if (this.metrics) {
        this.metrics.record("tieredstore.ingest.flush.latency", elapsedNs, "ns");
        if (busyWaitMs > 0) this.metrics.record("tieredstore.ingest.store_retry.wait", busyWaitMs * 1_000_000, "ns");
      }
    } catch (e) {
      // If the whole transaction failed, all tasks are treated as internal errors.
      // eslint-disable-next-line no-console
      console.error("ingest tx failed", e);
      if (this.gate && batchReservedBytes > 0) this.gate.release(batchReservedBytes);
      for (const t of batch) t.resolve(Result.err({ kind: "internal" }));
      const elapsedNs = (Date.now() - flushStartMs) * 1_000_000;
      if (this.metrics) {
        this.metrics.record("tieredstore.ingest.flush.latency", elapsedNs, "ns");
        if (busyWaitMs > 0) this.metrics.record("tieredstore.ingest.store_retry.wait", busyWaitMs * 1_000_000, "ns");
      }
    }
  }
}

function isRetryableBatchResult(result: StoreAppendBatch): boolean {
  return Result.isError(result) && result.error.kind === "retryable";
}

function toAppendResult(result: StoreAppendResult | undefined): AppendResult {
  if (!result) return Result.err({ kind: "internal" });
  if (Result.isError(result)) return Result.err(result.error);
  return Result.ok(result.value);
}

function formatBytes(bytes: number): string {
  const units = ["b", "kb", "mb", "gb"];
  let value = Math.max(0, bytes);
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  const digits = idx === 0 ? 0 : 1;
  return `${value.toFixed(digits)}${units[idx]}`;
}
