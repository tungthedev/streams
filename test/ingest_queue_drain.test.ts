import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../src/app";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { Result } from "better-result";
import { IngestQueue } from "../src/ingest";
import type { StoreAppendBatch, StoreAppendTask } from "../src/store/append";
import type { WalStore } from "../src/store/wal_store";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ingestFlushIntervalMs: 100_000,
    segmentCheckIntervalMs: 100_000,
    uploadIntervalMs: 100_000,
    indexCheckIntervalMs: 100_000,
    touchCheckIntervalMs: 100_000,
    touchWorkers: 0,
    segmenterWorkers: 0,
    ...overrides,
  };
}

describe("ingest queue drain", () => {
  test("flush does not depend on Array.shift for dequeueing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-ingest-drain-"));
    try {
      const app = createApp(makeConfig(root), new MockR2Store());
      app.deps.db.ensureStream("queue_stream", { contentType: "application/octet-stream" });

      const appendPromise = app.deps.ingest.append({
        stream: "queue_stream",
        baseAppendMs: 1n,
        rows: [{ payload: new Uint8Array([1]), routingKey: null, contentType: null }],
        contentType: "application/octet-stream",
      });

      const ingestAny = app.deps.ingest as any;
      const originalShift = ingestAny.q.shift;
      ingestAny.q.shift = () => {
        throw new Error("queue shift should not be called");
      };

      try {
        await app.deps.ingest.flush();
      } finally {
        ingestAny.q.shift = originalShift;
      }

      const res = await appendPromise;
      expect(Result.isOk(res)).toBe(true);
      await app.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("flush serializes async wal submissions in dequeue order", async () => {
    const cfg = makeConfig("unused", {
      ingestMaxBatchRequests: 1,
      ingestMaxBatchBytes: 1024,
    });
    const wal = new ControlledWalStore();
    const ingest = new IngestQueue(cfg, wal);
    try {
      const first = ingest.append({
        stream: "first",
        baseAppendMs: 1n,
        rows: [{ payload: new Uint8Array([1]), routingKey: null, contentType: null }],
        contentType: "application/octet-stream",
      });
      const firstFlush = ingest.flush();
      await wal.waitForCallCount(1);

      const second = ingest.append({
        stream: "second",
        baseAppendMs: 2n,
        rows: [{ payload: new Uint8Array([2]), routingKey: null, contentType: null }],
        contentType: "application/octet-stream",
      });
      const secondFlush = ingest.flush();

      await Promise.resolve();
      expect(wal.calls.map((batch) => batch[0]?.stream)).toEqual(["first"]);

      wal.resolveNext();
      await wal.waitForCallCount(2);
      expect(wal.calls.map((batch) => batch[0]?.stream)).toEqual(["first", "second"]);

      wal.resolveNext();
      await Promise.all([firstFlush, secondFlush]);

      expect(Result.isOk(await first)).toBe(true);
      expect(Result.isOk(await second)).toBe(true);
    } finally {
      ingest.stop();
    }
  });
});

class ControlledWalStore implements WalStore {
  readonly calls: StoreAppendTask[][] = [];
  private pending: Array<(result: StoreAppendBatch) => void> = [];
  private waiters: Array<() => void> = [];

  appendBatch(tasks: StoreAppendTask[]): Promise<StoreAppendBatch> {
    this.calls.push(tasks);
    this.notifyWaiters();
    return new Promise((resolve) => {
      this.pending.push(resolve);
    });
  }

  resolveNext(): void {
    const call = this.calls[this.calls.length - this.pending.length];
    const resolve = this.pending.shift();
    if (!resolve || !call) throw new Error("no pending wal append");
    resolve(Result.ok({
      walBytesCommitted: call.reduce((sum, task) => sum + task.rows.reduce((acc, row) => acc + row.payload.byteLength, 0), 0),
      results: call.map((task) =>
        Result.ok({
          lastOffset: BigInt(task.rows.length - 1),
          appendedRows: task.rows.length,
          closed: task.close,
          duplicate: false,
        })
      ),
    }));
  }

  async waitForCallCount(count: number): Promise<void> {
    while (this.calls.length < count) {
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
      });
    }
  }

  private notifyWaiters(): void {
    const waiters = this.waiters.splice(0);
    for (const waiter of waiters) waiter();
  }
}
