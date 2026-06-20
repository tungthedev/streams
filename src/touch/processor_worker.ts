import { parentPort, workerData } from "node:worker_threads";
import { Result } from "better-result";
import type { Config } from "../config.ts";
import { SqliteDurableStore } from "../db/db.ts";
import type { HostRuntime } from "../runtime/host_runtime.ts";
import { setSqliteRuntimeOverride } from "../sqlite/adapter.ts";
import { initConsoleLogging } from "../util/log.ts";
import { processTouchBatch } from "./process_batch.ts";
import type { ProcessRequest } from "./worker_protocol.ts";

initConsoleLogging();

const data = workerData as { config: Config; hostRuntime?: HostRuntime };
const cfg = data.config;
// Bun worker_threads can miss the Bun globals that the main thread sees.
// Use the parent host runtime hint before the worker opens SQLite.
setSqliteRuntimeOverride(data.hostRuntime ?? null);
// The main server process initializes/migrates schema; workers should avoid
// concurrent migrations on the same sqlite file.
const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.workerSqliteCacheBytes, skipMigrations: true });
const touchStore = db.touch;

async function handleProcess(msg: ProcessRequest): Promise<void> {
  const res = await processTouchBatch({
    db: touchStore,
    stream: msg.stream,
    fromOffset: msg.fromOffset,
    toOffset: msg.toOffset,
    profile: msg.profile,
    maxRows: msg.maxRows,
    maxBytes: msg.maxBytes,
    emitFineTouches: msg.emitFineTouches,
    fineTouchBudget: msg.fineTouchBudget,
    fineGranularity: msg.fineGranularity,
    processingMode: msg.processingMode,
    filterHotTemplates: msg.filterHotTemplates,
    hotTemplateIds: msg.hotTemplateIds,
  });
  if (Result.isError(res)) {
    parentPort?.postMessage({
      type: "error",
      id: msg.id,
      stream: msg.stream,
      message: res.error.message,
    });
    return;
  }
  parentPort?.postMessage({
    ...res.value,
    id: msg.id,
    stream: msg.stream,
  });
}

parentPort?.on("message", (msg: any) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "stop") {
    try {
      db.close();
    } catch {
      // ignore
    }
    try {
      parentPort?.postMessage({ type: "stopped" });
    } catch {
      // ignore
    }
    return;
  }
  if (msg.type === "process") {
    void handleProcess(msg as ProcessRequest).catch((e: any) => {
      try {
        parentPort?.postMessage({
          type: "error",
          id: (msg as any).id,
          stream: (msg as any).stream,
          message: String(e?.message ?? e),
          stack: e?.stack ? String(e.stack) : undefined,
        });
      } catch {
        // ignore
      }
    });
  }
});
