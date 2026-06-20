import { loadConfig } from "./config";
import { createApp, createPostgresApp, createPostgresFullApp, type App } from "./app";
import { StatsCollector, StatsReporter } from "./stats";
import { LatencyHistogramCollector, HistogramReporter } from "./hist";
import { MockR2Store } from "./objectstore/mock_r2";
import type { ObjectStore } from "./objectstore/interface";
import { R2ObjectStore } from "./objectstore/r2";
import { bootstrapFromR2 } from "./bootstrap";
import { bootstrapPostgresFromR2 } from "./postgres/bootstrap";
import { PostgresDurableStore } from "./postgres/store";
import { initConsoleLogging } from "./util/log";
import { applyAutoTune, AutoTuneApplyError, parseAutoTuneArg } from "./server_auto_tune";
import { parseAuthConfigResult, withAuth } from "./auth";
import { Result } from "better-result";

initConsoleLogging();

const args = process.argv.slice(2);
const authConfigResult = parseAuthConfigResult(args);
if (Result.isError(authConfigResult)) {
  console.error(authConfigResult.error.message);
  process.exit(1);
}
const authConfig = authConfigResult.value;

const autoTune = parseAutoTuneArg(args);
if (autoTune.enabled) {
  try {
    applyAutoTune(autoTune.valueMb);
  } catch (error) {
    if (error instanceof AutoTuneApplyError) {
      console.error(error.message);
      process.exit(1);
    }
    throw error;
  }
}

const cfg = loadConfig();

const statsEnabled = args.includes("--stats");
const histEnabled = args.includes("--hist");
const bootstrapEnabled = args.includes("--bootstrap-from-r2");
const bpBudgetRaw = process.env.DS_BACKPRESSURE_BUDGET_MS;
const bpBudgetMs = bpBudgetRaw ? Number(bpBudgetRaw) : cfg.ingestFlushIntervalMs + 1;
if (bpBudgetRaw && !Number.isFinite(bpBudgetMs)) {
  // eslint-disable-next-line no-console
  console.error(`invalid DS_BACKPRESSURE_BUDGET_MS: ${bpBudgetRaw}`);
  process.exit(1);
}
const stats = statsEnabled ? new StatsCollector({ backpressureBudgetMs: bpBudgetMs }) : undefined;
const hist = histEnabled ? new LatencyHistogramCollector() : undefined;

const storeIdx = args.indexOf("--object-store");
const storeChoice = storeIdx >= 0 ? args[storeIdx + 1] : null;

function requireObjectStoreChoice(): "r2" | "local" {
  if (!storeChoice || (storeChoice !== "r2" && storeChoice !== "local")) {
    // eslint-disable-next-line no-console
    console.error("missing or invalid --object-store (expected: r2 | local)");
    process.exit(1);
  }
  return storeChoice;
}

function createConfiguredObjectStore(choice: "r2" | "local"): ObjectStore {
  if (choice === "local") {
    const memBytesRaw = process.env.DS_MOCK_R2_MAX_INMEM_BYTES;
    const memMbRaw = process.env.DS_MOCK_R2_MAX_INMEM_MB;
    const putDelayRaw = process.env.DS_MOCK_R2_PUT_DELAY_MS;
    const getDelayRaw = process.env.DS_MOCK_R2_GET_DELAY_MS;
    const headDelayRaw = process.env.DS_MOCK_R2_HEAD_DELAY_MS;
    const listDelayRaw = process.env.DS_MOCK_R2_LIST_DELAY_MS;
    const memBytes = memBytesRaw ? Number(memBytesRaw) : memMbRaw ? Number(memMbRaw) * 1024 * 1024 : null;
    const putDelayMs = putDelayRaw ? Number(putDelayRaw) : 0;
    const getDelayMs = getDelayRaw ? Number(getDelayRaw) : 0;
    const headDelayMs = headDelayRaw ? Number(headDelayRaw) : 0;
    const listDelayMs = listDelayRaw ? Number(listDelayRaw) : 0;
    if (memBytesRaw && !Number.isFinite(memBytes)) {
      // eslint-disable-next-line no-console
      console.error(`invalid DS_MOCK_R2_MAX_INMEM_BYTES: ${memBytesRaw}`);
      process.exit(1);
    }
    if (memMbRaw && !Number.isFinite(Number(memMbRaw))) {
      // eslint-disable-next-line no-console
      console.error(`invalid DS_MOCK_R2_MAX_INMEM_MB: ${memMbRaw}`);
      process.exit(1);
    }
    for (const [name, value] of [
      ["DS_MOCK_R2_PUT_DELAY_MS", putDelayMs],
      ["DS_MOCK_R2_GET_DELAY_MS", getDelayMs],
      ["DS_MOCK_R2_HEAD_DELAY_MS", headDelayMs],
      ["DS_MOCK_R2_LIST_DELAY_MS", listDelayMs],
    ] as const) {
      if (!Number.isFinite(value) || value < 0) {
        // eslint-disable-next-line no-console
        console.error(`invalid ${name}: ${process.env[name]}`);
        process.exit(1);
      }
    }
    return new MockR2Store({
      maxInMemoryBytes: memBytes ?? undefined,
      spillDir: process.env.DS_MOCK_R2_SPILL_DIR,
      faults: {
        putDelayMs,
        getDelayMs,
        headDelayMs,
        listDelayMs,
      },
    });
  }

  const bucket = process.env.DURABLE_STREAMS_R2_BUCKET;
  const accountId = process.env.DURABLE_STREAMS_R2_ACCOUNT_ID;
  const accessKeyId = process.env.DURABLE_STREAMS_R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.DURABLE_STREAMS_R2_SECRET_ACCESS_KEY;
  const endpoint = process.env.DURABLE_STREAMS_R2_ENDPOINT;
  const region = process.env.DURABLE_STREAMS_R2_REGION;
  if (!bucket || !accountId || !accessKeyId || !secretAccessKey) {
    // eslint-disable-next-line no-console
    console.error("missing R2 env vars: DURABLE_STREAMS_R2_BUCKET, DURABLE_STREAMS_R2_ACCOUNT_ID, DURABLE_STREAMS_R2_ACCESS_KEY_ID, DURABLE_STREAMS_R2_SECRET_ACCESS_KEY");
    process.exit(1);
  }
  return new R2ObjectStore({
    bucket,
    accountId,
    accessKeyId,
    secretAccessKey,
    endpoint,
    region,
  });
}

let app: App;
if (cfg.storage === "postgres") {
  if (cfg.postgresUrl == null) {
    // loadConfig validates this; keep the local guard for future call-site changes.
    // eslint-disable-next-line no-console
    console.error("DS_POSTGRES_URL is required when DS_STORAGE=postgres");
    process.exit(1);
  }
  if (cfg.postgresMode === "wal") {
    if (storeIdx >= 0) {
      // eslint-disable-next-line no-console
      console.error("postgres WAL mode does not support --object-store");
      process.exit(1);
    }
    if (bootstrapEnabled) {
      // eslint-disable-next-line no-console
      console.error("postgres WAL mode does not support --bootstrap-from-r2");
      process.exit(1);
    }
    const postgresStore = await PostgresDurableStore.connect(cfg.postgresUrl);
    app = createPostgresApp(cfg, postgresStore, { stats });
  } else {
    const objectStore = createConfiguredObjectStore(requireObjectStoreChoice());
    if (bootstrapEnabled) {
      await bootstrapPostgresFromR2(cfg, objectStore, cfg.postgresUrl, { clearLocal: true });
    }
    const postgresStore = await PostgresDurableStore.connectFull(cfg.postgresUrl);
    app = createPostgresFullApp(cfg, postgresStore, objectStore, { stats });
  }
} else {
  const store = createConfiguredObjectStore(requireObjectStoreChoice());

  if (bootstrapEnabled) {
    await bootstrapFromR2(cfg, store, { clearLocal: true });
  }

  app = createApp(cfg, store, { stats });
}
const statsIntervalMs = process.env.DS_STATS_INTERVAL_MS ? Number(process.env.DS_STATS_INTERVAL_MS) : 60_000;
if (process.env.DS_STATS_INTERVAL_MS && !Number.isFinite(statsIntervalMs)) {
  // eslint-disable-next-line no-console
  console.error(`invalid DS_STATS_INTERVAL_MS: ${process.env.DS_STATS_INTERVAL_MS}`);
  process.exit(1);
}
const statsReporter =
  statsEnabled && stats && app.deps.storageStats && app.deps.uploader
    ? new StatsReporter(stats, app.deps.storageStats, app.deps.uploader, app.deps.ingest, app.deps.backpressure, app.deps.memory, statsIntervalMs)
    : null;
const histReporter = histEnabled && hist ? new HistogramReporter(hist, statsIntervalMs) : null;

const fetchWithHist = hist
  ? async (req: Request): Promise<Response> => {
      const start = Date.now();
      const resp = await app.fetch(req);
      const url = req.url;
      let path: string | null = null;
      if (url.startsWith("/")) {
        path = url;
      } else {
        const schemeIdx = url.indexOf("://");
        if (schemeIdx !== -1) {
          const pathIdx = url.indexOf("/", schemeIdx + 3);
          path = pathIdx === -1 ? "/" : url.slice(pathIdx);
        }
      }
      if (path) {
        const isStream = path.startsWith("/v1/stream/") || path.startsWith("/v1/streams");
        if (isStream) {
          const ms = Date.now() - start;
          const method = req.method.toUpperCase();
          if (method === "GET" || method === "HEAD") hist.recordRead(ms);
          else if (method === "POST" || method === "PUT" || method === "DELETE") hist.recordWrite(ms);
        }
  }
  return resp;
    }
  : app.fetch;
const fetchWithAuth = withAuth(authConfig, fetchWithHist);

const server = Bun.serve({
  hostname: cfg.host,
  port: cfg.port,
  // Default Bun idleTimeout is 10s, which is too low for long-poll endpoints like /touch/wait.
  // Bun expects seconds here.
  idleTimeout: (() => {
    const raw = process.env.DS_HTTP_IDLE_TIMEOUT_SECONDS;
    if (raw == null || raw.trim() === "") return 180;
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) {
      // eslint-disable-next-line no-console
      console.error(`invalid DS_HTTP_IDLE_TIMEOUT_SECONDS: ${raw}`);
      process.exit(1);
    }
    return n;
  })(),
  fetch: fetchWithAuth,
});

statsReporter?.start();
histReporter?.start();

let shuttingDown = false;
const shutdown = async (signal: NodeJS.Signals) => {
  if (shuttingDown) return;
  shuttingDown = true;
  // eslint-disable-next-line no-console
  console.log(`received ${signal}, shutting down prisma-streams server`);
  statsReporter?.stop();
  histReporter?.stop();
  try {
    server.stop(true);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("failed to stop HTTP server cleanly", err);
  }
  try {
    await app.close();
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error("failed to close application cleanly", err);
    process.exitCode = 1;
  }
};

const listenTarget = cfg.host.includes(":") ? `[${cfg.host}]:${server.port}` : `${cfg.host}:${server.port}`;

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

// eslint-disable-next-line no-console
console.log(`prisma-streams server listening on ${listenTarget}`);
