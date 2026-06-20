import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";
import { createPostgresFullApp } from "../src/app";
import { loadConfig } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { PostgresDurableStore } from "../src/postgres/store";
import { tableKeyFor, templateIdFor, watchKeyFor } from "../src/touch/live_keys";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function schemaConnectionString(schema: string): string {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  return `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
}

async function withPostgresSchema<T>(fn: (ctx: { connectionString: string }) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_full_touch_${Date.now()}_${Math.random().toString(16).slice(2)}`;
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

async function fetchJson(app: ReturnType<typeof createPostgresFullApp>, path: string, init?: RequestInit): Promise<{ status: number; body: any }> {
  const res = await app.fetch(new Request(`http://local${path}`, init));
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

async function countWalRows(connectionString: string, stream: string): Promise<number> {
  const pool = new Pool({ connectionString });
  try {
    const res = await pool.query<{ cnt: string | number | bigint }>(`SELECT COUNT(*) AS cnt FROM wal WHERE stream = $1;`, [stream]);
    return Number(res.rows[0]?.cnt ?? 0);
  } finally {
    await pool.end();
  }
}

maybeDescribe("postgres full-mode state-protocol touch parity", () => {
  test("supports built-in profile metadata and live touch processing", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-touch-"));
      const objectStore = new MockR2Store();
      const store = await PostgresDurableStore.connectFull(connectionString);
      const app = createPostgresFullApp(
        {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          touchCheckIntervalMs: 0,
          touchWorkers: 0,
          objectStoreRetries: 0,
        },
        store,
        objectStore
      );
      try {
        await app.ready;
        const stream = "pg-state";
        const entity = "posts";
        const fields = ["tenantId", "userId"];
        const templateId = templateIdFor(entity, fields);
        const tableKey = tableKeyFor(entity);
        const beforeKey = watchKeyFor(templateId, ["t1", "123"]);
        const afterKey = watchKeyFor(templateId, ["t1", "456"]);

        let res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(res.status);

        res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/_profile`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            apiVersion: "durable.streams/profile/v1",
            profile: {
              kind: "state-protocol",
              touch: {
                enabled: true,
                onMissingBefore: "coarse",
              },
            },
          }),
        });
        expect(res.status).toBe(200);
        expect(res.body.profile.kind).toBe("state-protocol");
        expect(await store.fullModeTouch().getStreamTouchState(stream)).not.toBeNull();

        res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/touch/templates/activate`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            templates: [
              {
                entity,
                fields: fields.map((name) => ({ name, encoding: "string" })),
              },
            ],
            inactivityTtlMs: 60 * 60 * 1000,
          }),
        });
        expect(res.status).toBe(200);
        expect(res.body.activated.map((row: any) => row.templateId)).toEqual([templateId]);

        const metaBefore = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/touch/meta`, { method: "GET" });
        expect(metaBefore.status).toBe(200);
        expect(metaBefore.body.activeTemplates).toBe(1);

        const tableWaitPromise = fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            cursor: metaBefore.body.cursor,
            keys: [tableKey],
            interestMode: "coarse",
            timeoutMs: 2000,
          }),
        });
        const fineWaitPromises = [beforeKey, afterKey].map((key) =>
          fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/touch/wait`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              cursor: metaBefore.body.cursor,
              keys: [key],
              templateIdsUsed: [templateId],
              timeoutMs: 2000,
            }),
          })
        );

        await new Promise((resolve) => setTimeout(resolve, 40));

        res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            type: entity,
            key: "post:1",
            value: { tenantId: "t1", userId: "456" },
            old_value: { tenantId: "t1", userId: "123" },
            headers: { operation: "update" },
          }),
        });
        expect([201, 204]).toContain(res.status);

        app.deps.touch.notify(stream);
        await app.deps.touch.tick();

        const tableWait = await tableWaitPromise;
        expect(tableWait.status).toBe(200);
        expect(tableWait.body.touched).toBe(true);

        for (const fineWait of await Promise.all(fineWaitPromises)) {
          expect(fineWait.status).toBe(200);
          expect(fineWait.body.touched).toBe(true);
          expect(fineWait.body.effectiveWaitKind).toBe("fineKey");
        }
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });

  test("manifest publication keeps WAL rows until touch processing catches up", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-touch-gc-"));
      const objectStore = new MockR2Store();
      const store = await PostgresDurableStore.connectFull(connectionString);
      const app = createPostgresFullApp(
        {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          segmentCheckIntervalMs: 0,
          uploadIntervalMs: 0,
          segmentTargetRows: 2,
          segmentMaxBytes: 1024 * 1024,
          touchCheckIntervalMs: 0,
          touchWorkers: 0,
          objectStoreRetries: 0,
        },
        store,
        objectStore
      );
      try {
        await app.ready;
        const stream = "pg-state-touch-gc";
        let res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(res.status);

        res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}/_profile`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            apiVersion: "durable.streams/profile/v1",
            profile: {
              kind: "state-protocol",
              touch: { enabled: true },
            },
          }),
        });
        expect(res.status).toBe(200);

        for (const value of ["1", "2"]) {
          res = await fetchJson(app, `/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              type: "posts",
              key: value,
              value: { id: value },
              headers: { operation: "insert" },
            }),
          });
          expect([201, 204]).toContain(res.status);
        }

        await (app.deps.segmenter as any).tick();
        await app.deps.uploader.tick();

        const published = await store.getStream(stream);
        expect(published?.uploaded_through).toBe(1n);
        expect(await countWalRows(connectionString, stream)).toBe(2);

        app.deps.touch.notify(stream);
        await app.deps.touch.tick();
        await app.deps.uploader.publishManifest(stream, { wait: true });

        expect((await store.fullModeTouch().getStreamTouchState(stream))?.processed_through).toBe(1n);
        expect(await countWalRows(connectionString, stream)).toBe(0);
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });
});
