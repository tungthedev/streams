import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";
import { describe, expect, test } from "bun:test";
import { createApp, createPostgresFullApp } from "../src/app";
import { bootstrapPostgresFromR2 } from "../src/postgres/bootstrap";
import { PostgresDurableStore } from "../src/postgres/store";
import { loadConfig, type Config } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function schemaConnectionString(schema: string): string {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  return `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
}

async function withPostgresSchema<T>(fn: (ctx: { connectionString: string }) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_boot_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  const setupPool = new Pool({ connectionString: POSTGRES_URL });
  await setupPool.query(`CREATE SCHEMA ${schema};`);
  await setupPool.end();
  try {
    return await fn({ connectionString: schemaConnectionString(schema) });
  } finally {
    const cleanupPool = new Pool({ connectionString: POSTGRES_URL });
    try {
      await cleanupPool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
    } finally {
      await cleanupPool.end();
    }
  }
}

function makeConfig(rootDir: string, overrides: Partial<Config> = {}): Config {
  return {
    ...loadConfig(),
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    ...overrides,
  };
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

maybeDescribe("postgres full-mode bootstrap", () => {
  test(
    "restores sqlite-published object-store metadata into postgres full mode",
    async () => {
      await withPostgresSchema(async ({ connectionString }) => {
        const sourceRoot = mkdtempSync(join(tmpdir(), "ds-pg-bootstrap-src-"));
        const destRoot = mkdtempSync(join(tmpdir(), "ds-pg-bootstrap-dst-"));
        const objectStore = new MockR2Store();
        const stream = "pg-bootstrap";
        const sourceCfg = makeConfig(sourceRoot, {
          segmentMaxBytes: 160,
          segmentCheckIntervalMs: 25,
          uploadIntervalMs: 25,
          indexCheckIntervalMs: 25,
          indexL0SpanSegments: 2,
          searchCompanionBuildBatchSegments: 2,
        });
        const sourceApp = createApp(sourceCfg, objectStore);
        try {
          const createRes = await sourceApp.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
              method: "PUT",
              headers: { "content-type": "application/json" },
            })
          );
          expect([201, 204]).toContain(createRes.status);

          const schemaRes = await sourceApp.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_schema`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                schema: {
                  type: "object",
                  required: ["type", "key", "value", "headers"],
                  properties: {
                    type: { type: "string" },
                    key: { type: "string" },
                    value: {
                      type: "object",
                      required: ["service", "duration"],
                      properties: {
                        service: { type: "string" },
                        duration: { type: "number" },
                      },
                    },
                    headers: {
                      type: "object",
                      required: ["timestamp"],
                      properties: { timestamp: { type: "string" } },
                    },
                  },
                },
                search: {
                  primaryTimestampField: "eventTime",
                  fields: {
                    eventTime: {
                      kind: "date",
                      bindings: [{ version: 1, jsonPointer: "/headers/timestamp" }],
                      exact: true,
                      column: true,
                      sortable: true,
                    },
                    service: {
                      kind: "keyword",
                      bindings: [{ version: 1, jsonPointer: "/value/service" }],
                      normalizer: "lowercase_v1",
                      exact: true,
                      prefix: true,
                    },
                    duration: {
                      kind: "float",
                      bindings: [{ version: 1, jsonPointer: "/value/duration" }],
                      column: true,
                      aggregatable: true,
                    },
                  },
                  rollups: {
                    requests: {
                      dimensions: ["service"],
                      intervals: ["1m"],
                      measures: {
                        requests: { kind: "count" },
                        latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
                      },
                    },
                  },
                },
              }),
            })
          );
          expect(schemaRes.status).toBe(200);

          const profileRes = await sourceApp.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_profile`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                apiVersion: "durable.streams/profile/v1",
                profile: { kind: "state-protocol", touch: { enabled: true } },
              }),
            })
          );
          expect(profileRes.status).toBe(200);

          for (let i = 0; i < 4; i++) {
            const appendRes = await sourceApp.fetch(
              new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                  type: "public.requests",
                  key: String(i),
                  value: {
                    service: i % 2 === 0 ? "api" : "worker",
                    duration: 100 + i,
                  },
                  headers: { timestamp: `2026-03-25T10:0${i}:00.000Z` },
                }),
              })
            );
            expect(appendRes.status).toBe(204);
          }

          const deadline = Date.now() + 10_000;
          let ready = false;
          while (Date.now() < deadline) {
            const row = sourceApp.deps.db.getStream(stream);
            const segments = sourceApp.deps.db.listSegmentsForStream(stream);
            const companionPlan = sourceApp.deps.db.getSearchCompanionPlan(stream);
            const companions = sourceApp.deps.db.listSearchSegmentCompanions(stream);
            if (row && row.uploaded_through >= row.sealed_through && segments.length > 0 && companionPlan && companions.length >= segments.length) {
              ready = true;
              break;
            }
            await sleep(50);
          }
          expect(ready).toBe(true);
        } finally {
          await sourceApp.close();
        }

        const destCfg = makeConfig(destRoot);
        await bootstrapPostgresFromR2(destCfg, objectStore, connectionString, { clearLocal: true });

        const postgresStore = await PostgresDurableStore.connectFull(connectionString);
        const app = createPostgresFullApp(destCfg, postgresStore, objectStore);
        try {
          const row = await postgresStore.getStream(stream);
          expect(row).not.toBeNull();
          expect(row?.profile).toBe("state-protocol");
          expect(row?.uploaded_segment_count).toBeGreaterThan(0);

          const readRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}?offset=0`));
          expect(readRes.status).toBe(200);
          expect((await readRes.text()).length).toBeGreaterThan(0);

          const touchMetaRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/touch/meta`));
          expect(touchMetaRes.status).toBe(200);

          const searchRes = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_search`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ q: "service:api" }),
            })
          );
          expect(searchRes.status).toBe(200);
          expect((await searchRes.json()).total.value).toBe(2);

          const aggregateRes = await app.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_aggregate`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                rollup: "requests",
                from: "2026-03-25T10:00:00.000Z",
                to: "2026-03-25T10:04:00.000Z",
                interval: "1m",
                q: "service:api",
                group_by: ["service"],
              }),
            })
          );
          expect(aggregateRes.status).toBe(200);

          const detailsRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}/_details`));
          expect(detailsRes.status).toBe(200);
          const details = await detailsRes.json();
          expect(Number(details.storage.local_storage.postgres_shared_total_bytes)).toBeGreaterThan(0);
        } finally {
          await app.close();
          rmSync(sourceRoot, { recursive: true, force: true });
          rmSync(destRoot, { recursive: true, force: true });
        }
      });
    },
    30_000
  );

  test(
    "restores deleted stream tombstones",
    async () => {
      await withPostgresSchema(async ({ connectionString }) => {
        const sourceRoot = mkdtempSync(join(tmpdir(), "ds-pg-bootstrap-del-src-"));
        const destRoot = mkdtempSync(join(tmpdir(), "ds-pg-bootstrap-del-dst-"));
        const objectStore = new MockR2Store();
        const stream = "pg-bootstrap-deleted";
        const sourceApp = createApp(makeConfig(sourceRoot), objectStore);
        try {
          const createRes = await sourceApp.fetch(
            new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, {
              method: "PUT",
              headers: { "content-type": "text/plain" },
            })
          );
          expect([201, 204]).toContain(createRes.status);
          await sourceApp.deps.uploader.publishManifest(stream);
          const deleteRes = await sourceApp.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" }));
          expect(deleteRes.status).toBe(204);
        } finally {
          await sourceApp.close();
        }

        const destCfg = makeConfig(destRoot);
        await bootstrapPostgresFromR2(destCfg, objectStore, connectionString, { clearLocal: true });
        const postgresStore = await PostgresDurableStore.connectFull(connectionString);
        const app = createPostgresFullApp(destCfg, postgresStore, objectStore);
        try {
          const headRes = await app.fetch(new Request(`http://local/v1/stream/${encodeURIComponent(stream)}`, { method: "HEAD" }));
          expect(headRes.status).toBe(404);
          const listRes = await app.fetch(new Request("http://local/v1/streams"));
          expect(listRes.status).toBe(200);
          const list = (await listRes.json()) as Array<{ name: string }>;
          expect(list.find((entry) => entry.name === stream)).toBeUndefined();
        } finally {
          await app.close();
          rmSync(sourceRoot, { recursive: true, force: true });
          rmSync(destRoot, { recursive: true, force: true });
        }
      });
    },
    30_000
  );
});
