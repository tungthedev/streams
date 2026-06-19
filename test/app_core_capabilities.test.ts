import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAppCore, type App } from "../src/app_core";
import { loadConfig, type Config } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { StreamReader } from "../src/reader";
import type { WalControlPlaneStore } from "../src/store/capabilities";

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    metricsFlushIntervalMs: 1,
    segmentCheckIntervalMs: 60_000,
    uploadIntervalMs: 60_000,
    expirySweepIntervalMs: 60_000,
    ...overrides,
  };
}

function limitedControlStore(db: SqliteDurableStore): WalControlPlaneStore {
  return new Proxy(db, {
    get(target, prop, receiver) {
      if (prop === "capabilities") {
        return {
          ...target.capabilities,
          indexes: false,
          manifests: false,
          objectStoreAccounting: false,
          storageStats: false,
          schemaPublication: false,
          builtinProfiles: false,
          internalMetrics: false,
          touch: false,
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as WalControlPlaneStore;
}

async function withLimitedApp<T>(fn: (ctx: { app: App; baseUrl: string; sqlite: SqliteDurableStore }) => Promise<T>): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), "ds-app-core-caps-"));
  const cfg = makeConfig(root);
  const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes });
  const controlStore = limitedControlStore(db);
  const app = createAppCore(cfg, {
    store: controlStore,
    createRuntime: ({ config, registry, memorySampler, memory }) => ({
      reader: new StreamReader(config, controlStore, registry, undefined, memorySampler, memory),
      start(): void {},
    }),
  });
  const server = Bun.serve({ port: 0, fetch: app.fetch });
  try {
    await app.ready;
    return await fn({ app, baseUrl: `http://localhost:${server.port}`, sqlite: db });
  } finally {
    server.stop();
    await app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

async function readJson(resp: Response): Promise<any> {
  return resp.json();
}

describe("app core capability gates", () => {
  test("startup rejects stores that advertise full-mode capabilities without matching runtime deps", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-app-core-caps-invalid-"));
    const cfg = makeConfig(root);
    const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.sqliteCacheBytes });
    try {
      expect(() =>
        createAppCore(cfg, {
          db,
          store: db,
          createRuntime: ({ config, registry, memorySampler, memory }) => ({
            reader: new StreamReader(config, db, registry, undefined, memorySampler, memory),
            start(): void {},
          }),
        })
      ).toThrow("index capability requires an index runtime");
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("WAL-only startup does not create or emit the internal metrics stream", async () => {
    await withLimitedApp(async ({ app, baseUrl, sqlite }) => {
      expect(app.deps.db).toBeUndefined();
      expect(sqlite.getStream("__stream_metrics__")).toBeNull();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(sqlite.getStream("__stream_metrics__")).toBeNull();

      const resp = await fetch(`${baseUrl}/v1/stream/__stream_metrics__?offset=0`);
      expect(resp.status).toBe(404);
    });
  });

  test("WAL-only core routes reject unsupported full-mode capabilities before mutation", async () => {
    await withLimitedApp(async ({ sqlite, baseUrl }) => {
      const create = await fetch(`${baseUrl}/v1/stream/caps`, { method: "PUT" });
      expect(create.status).toBe(201);

      const profile = await fetch(`${baseUrl}/v1/stream/caps/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ profile: { kind: "metrics" } }),
      });
      expect(profile.status).toBe(501);
      expect(await readJson(profile)).toMatchObject({
        error: { code: "unsupported_capability" },
      });
      expect(sqlite.getStream("caps")?.profile).toBe("generic");

      const schema = await fetch(`${baseUrl}/v1/stream/caps/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          search: {
            primaryTimestampField: "ts",
            fields: {
              ts: {
                kind: "date",
                bindings: [{ version: 1, jsonPointer: "/ts" }],
                exact: true,
              },
            },
          },
        }),
      });
      expect(schema.status).toBe(501);

      const search = await fetch(`${baseUrl}/v1/stream/caps/_search`);
      expect(search.status).toBe(501);

      const details = await fetch(`${baseUrl}/v1/stream/caps/_details`);
      expect(details.status).toBe(501);
    });
  });

  test("WAL-only reader supports since reads without an object store", async () => {
    await withLimitedApp(async ({ baseUrl }) => {
      const create = await fetch(`${baseUrl}/v1/stream/wal-since`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect(create.status).toBe(201);

      const t1Ms = Date.now() + 1000;
      const t2Ms = t1Ms + 1000;
      const first = await fetch(`${baseUrl}/v1/stream/wal-since`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-timestamp": new Date(t1Ms).toISOString() },
        body: "a",
      });
      expect([200, 204]).toContain(first.status);
      const second = await fetch(`${baseUrl}/v1/stream/wal-since`, {
        method: "POST",
        headers: { "content-type": "text/plain", "stream-timestamp": new Date(t2Ms).toISOString() },
        body: "b",
      });
      expect([200, 204]).toContain(second.status);

      const since = new Date(t1Ms + 500).toISOString();
      const read = await fetch(`${baseUrl}/v1/stream/wal-since?since=${encodeURIComponent(since)}`);
      expect(read.status).toBe(200);
      expect(await read.text()).toBe("b");
    });
  });
});
