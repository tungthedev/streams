import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";
import { createPostgresFullApp } from "../src/app";
import { loadConfig } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { PostgresDurableStore } from "../src/postgres/store";
import { manifestObjectKey, streamHash16Hex } from "../src/util/stream_paths";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;
const STREAM = "pg_full_search";

function schemaConnectionString(schema: string): string {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  return `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
}

async function withPostgresSchema<T>(fn: (ctx: { connectionString: string }) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_full_search_${Date.now()}_${Math.random().toString(16).slice(2)}`;
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

async function waitFor(predicate: () => Promise<boolean>, label: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timeout waiting for ${label}`);
}

async function fetchJson(app: ReturnType<typeof createPostgresFullApp>, path: string, init?: RequestInit): Promise<{ status: number; body: any }> {
  const res = await app.fetch(new Request(`http://local${path}`, init));
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

const SEARCH_SCHEMA = {
  routingKey: { jsonPointer: "/repo", required: false },
  schema: {
    type: "object",
    additionalProperties: true,
  },
  search: {
    primaryTimestampField: "eventTime",
    fields: {
      eventTime: {
        kind: "date",
        bindings: [{ version: 1, jsonPointer: "/eventTime" }],
        column: true,
        exists: true,
        sortable: true,
      },
      repo: {
        kind: "keyword",
        bindings: [{ version: 1, jsonPointer: "/repo" }],
        normalizer: "lowercase_v1",
        exact: true,
        prefix: true,
        exists: true,
        sortable: true,
      },
      status: {
        kind: "integer",
        bindings: [{ version: 1, jsonPointer: "/status" }],
        exact: true,
        column: true,
        exists: true,
        sortable: true,
      },
      duration: {
        kind: "float",
        bindings: [{ version: 1, jsonPointer: "/duration" }],
        exact: true,
        column: true,
        exists: true,
        sortable: true,
        aggregatable: true,
      },
      message: {
        kind: "text",
        bindings: [{ version: 1, jsonPointer: "/message" }],
        analyzer: "unicode_word_v1",
        exists: true,
        positions: true,
      },
    },
    rollups: {
      requests: {
        dimensions: ["repo"],
        intervals: ["1m"],
        measures: {
          requests: { kind: "count" },
          latency: { kind: "summary", field: "duration", histogram: "log2_v1" },
        },
      },
    },
  },
};

maybeDescribe("postgres full-mode index and search parity", () => {
  test("supports routing key lexicon, search, aggregate, and manifest index catalogs", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-search-"));
      const objectStore = new MockR2Store();
      const store = await PostgresDurableStore.connectFull(connectionString);
      const app = createPostgresFullApp(
        {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          segmentMaxBytes: 260,
          segmentTargetRows: 2,
          segmentCheckIntervalMs: 10,
          uploadIntervalMs: 10,
          indexCheckIntervalMs: 10,
          indexL0SpanSegments: 1,
          searchWalOverlayQuietPeriodMs: 0,
          segmentCacheMaxBytes: 0,
          segmentFooterCacheEntries: 0,
          objectStoreRetries: 0,
        },
        store,
        objectStore
      );
      try {
        await app.ready;
        let res = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        });
        expect([200, 201]).toContain(res.status);

        res = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(SEARCH_SCHEMA),
        });
        expect(res.status).toBe(200);

        const events = [
          { eventTime: "2026-03-25T10:00:10.000Z", repo: "alpha/repo", status: 200, duration: 10, message: "postgres search alpha match" },
          { eventTime: "2026-03-25T10:00:35.000Z", repo: "beta/repo", status: 503, duration: 20, message: "postgres search beta failure" },
          { eventTime: "2026-03-25T10:01:10.000Z", repo: "alpha/repo", status: 200, duration: 30, message: "postgres search alpha stable" },
          { eventTime: "2026-03-25T10:01:40.000Z", repo: "gamma/repo", status: 200, duration: 40, message: "postgres search gamma stable" },
        ];

        for (const event of events) {
          res = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify(event),
          });
          expect([201, 204]).toContain(res.status);
        }

        const indexStores = store.fullModeIndexStores();
        await waitFor(async () => {
          const stream = await store.getStream(STREAM);
          if (!stream || stream.uploaded_segment_count < 1 || stream.uploaded_through < stream.sealed_through) return false;
          const plan = await indexStores.companions.getSearchCompanionPlan(STREAM);
          if (!plan) return false;
          for (let segmentIndex = 0; segmentIndex < stream.uploaded_segment_count; segmentIndex += 1) {
            const row = await indexStores.companions.getSearchSegmentCompanion(STREAM, segmentIndex);
            if (!row || row.plan_generation !== plan.generation) return false;
          }
          const lexiconState = await indexStores.lexicon.getLexiconIndexState(STREAM, "routing_key", "");
          return (lexiconState?.indexed_through ?? 0) >= stream.uploaded_segment_count;
        }, "postgres full-mode index metadata");

        const routingKeys = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}/_routing_keys?limit=10`);
        expect(routingKeys.status).toBe(200);
        expect(routingKeys.body.keys).toEqual(["alpha/repo", "beta/repo", "gamma/repo"]);
        expect(routingKeys.body.coverage.indexed_segments).toBeGreaterThan(0);

        const search = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}/_search`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            q: 'repo:"alpha/repo" message:"postgres search"',
            size: 10,
            sort: ["offset:asc"],
          }),
        });
        expect(search.status).toBe(200);
        expect(search.body.hits.map((hit: any) => hit.value.repo)).toEqual(["alpha/repo", "alpha/repo"]);
        expect(search.body.coverage.indexed_segments).toBeGreaterThan(0);

        const aggregate = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}/_aggregate`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            rollup: "requests",
            interval: "1m",
            from: "2026-03-25T10:00:00.000Z",
            to: "2026-03-25T10:02:00.000Z",
            dimensions: ["repo"],
            measures: ["requests"],
          }),
        });
        expect(aggregate.status).toBe(200);
        expect(aggregate.body.coverage.indexed_segments).toBeGreaterThan(0);
        expect(aggregate.body.rows.some((row: any) => row.repo === "alpha/repo" && row.requests === 2)).toBe(true);

        await waitFor(async () => {
          const bytes = await objectStore.get(manifestObjectKey(streamHash16Hex(STREAM)));
          if (!bytes) return false;
          const manifest = JSON.parse(new TextDecoder().decode(bytes));
          return (
            manifest.active_runs.length > 0 &&
            manifest.lexicon_indexes.length > 0 &&
            manifest.search_companions?.segments?.length >= 1
          );
        }, "manifest index catalogs");

        const deleted = await fetchJson(app, `/v1/stream/${encodeURIComponent(STREAM)}`, { method: "DELETE" });
        expect(deleted.status).toBe(204);

        await waitFor(async () => {
          const bytes = await objectStore.get(manifestObjectKey(streamHash16Hex(STREAM)));
          if (!bytes) return false;
          const manifest = JSON.parse(new TextDecoder().decode(bytes));
          return (
            (manifest.stream_flags & 1) !== 0 &&
            manifest.active_runs.length === 0 &&
            manifest.lexicon_indexes.length === 0 &&
            manifest.search_companions == null
          );
        }, "deleted manifest without stale index catalogs");

        const inspectPool = new Pool({ connectionString });
        try {
          for (const table of [
            "index_runs",
            "index_state",
            "secondary_index_runs",
            "secondary_index_state",
            "lexicon_index_runs",
            "lexicon_index_state",
            "search_segment_companions",
            "search_companion_plans",
          ]) {
            const count = await inspectPool.query<{ count: string }>(`SELECT COUNT(*) AS count FROM ${table} WHERE stream = $1;`, [STREAM]);
            expect(Number(count.rows[0]?.count ?? 0)).toBe(0);
          }
        } finally {
          await inspectPool.end();
        }
      } finally {
        await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });
});
