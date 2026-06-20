import { describe, expect, test } from "bun:test";
import { Pool } from "pg";
import { Result } from "better-result";
import { PostgresDurableStore } from "../src/postgres/store";
import { migratePostgresStore, readPostgresSchemaVersion } from "../src/postgres/schema";
import { SchemaRegistryStore } from "../src/schema/registry";
import { StreamProfileStore } from "../src/profiles";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function schemaConnectionString(schema: string): string {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  return `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
}

async function withPostgresSchema<T>(fn: (ctx: { schema: string; connectionString: string }) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_test_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  const setupPool = new Pool({ connectionString: POSTGRES_URL });
  await setupPool.query(`CREATE SCHEMA ${schema};`);
  await setupPool.end();
  try {
    return await fn({ schema, connectionString: schemaConnectionString(schema) });
  } finally {
    const cleanupPool = new Pool({ connectionString: POSTGRES_URL });
    try {
      await cleanupPool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
    } finally {
      await cleanupPool.end();
    }
  }
}

async function withPostgresStore<T>(fn: (store: PostgresDurableStore, ctx: { schema: string; connectionString: string }) => Promise<T>): Promise<T> {
  return withPostgresSchema(async ({ schema, connectionString }) => {
    const store = await PostgresDurableStore.connect(connectionString);
    try {
      return await fn(store, { schema, connectionString });
    } finally {
      await store.close();
    }
  });
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const rows: T[] = [];
  for await (const row of iterable) rows.push(row);
  return rows;
}

maybeDescribe("postgres durable store", () => {
  test("migration installs baseline version and remains idempotent", async () => {
    await withPostgresSchema(async ({ schema, connectionString }) => {
      const inspectPool = new Pool({ connectionString });
      try {
        await migratePostgresStore(inspectPool);
        await migratePostgresStore(inspectPool);
        expect(await readPostgresSchemaVersion(inspectPool)).toBe(1);
        const tables = await inspectPool.query<{ table_name: string }>(
          `SELECT table_name
           FROM information_schema.tables
           WHERE table_schema = $1
           ORDER BY table_name ASC;`,
          [schema]
        );
        expect(tables.rows.map((row) => row.table_name)).toEqual([
          "producer_state",
          "schema_version",
          "schemas",
          "stream_profiles",
          "streams",
          "wal",
        ]);
      } finally {
        await inspectPool.end();
      }
    });
  });

  test("migration starts from existing version 1 database", async () => {
    await withPostgresStore(async (store, { connectionString }) => {
      const inspectPool = new Pool({ connectionString });
      try {
        await inspectPool.query(`UPDATE schema_version SET version = 1;`);
        await store.migrate();
        expect(await readPostgresSchemaVersion(inspectPool)).toBe(1);
        const row = await inspectPool.query<{ count: string }>(`SELECT COUNT(*) AS count FROM schema_version;`);
        expect(Number(row.rows[0]?.count ?? 0)).toBe(1);
      } finally {
        await inspectPool.end();
      }
    });
  });

  test("migration rejects newer schema versions", async () => {
    await withPostgresStore(async (store, { connectionString }) => {
      const inspectPool = new Pool({ connectionString });
      try {
        await inspectPool.query(`UPDATE schema_version SET version = 999;`);
        await expect(store.migrate()).rejects.toThrow("postgres schema version 999 is not supported by version 1");
      } finally {
        await inspectPool.end();
      }
    });
  });

  test("migration rejects older unknown schema versions", async () => {
    await withPostgresStore(async (store, { connectionString }) => {
      const inspectPool = new Pool({ connectionString });
      try {
        await inspectPool.query(`UPDATE schema_version SET version = 0;`);
        await expect(store.migrate()).rejects.toThrow("postgres schema version 0 is not supported by version 1");
      } finally {
        await inspectPool.end();
      }
    });
  });

  test("migrations are idempotent and stream lifecycle works", async () => {
    await withPostgresStore(async (store, { schema, connectionString }) => {
      await store.migrate();
      const inspectPool = new Pool({ connectionString });
      try {
        const columns = await inspectPool.query<{ column_name: string }>(
          `SELECT column_name
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = 'streams';`,
          [schema]
        );
        const columnNames = new Set(columns.rows.map((row) => row.column_name));
        for (const fullModeColumn of [
          "sealed_through",
          "uploaded_through",
          "uploaded_segment_count",
          "pending_rows",
          "pending_bytes",
          "last_segment_cut_ms",
          "segment_in_progress",
        ]) {
          expect(columnNames.has(fullModeColumn)).toBe(false);
        }

        const schemaColumns = await inspectPool.query<{ column_name: string }>(
          `SELECT column_name
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = 'schemas';`,
          [schema]
        );
        expect(new Set(schemaColumns.rows.map((row) => row.column_name)).has("uploaded_size_bytes")).toBe(false);
      } finally {
        await inspectPool.end();
      }
      const created = await store.ensureStream("s", { contentType: "text/plain", ttlSeconds: 30 });
      expect(created.stream).toBe("s");
      expect(created.content_type).toBe("text/plain");
      expect(created.profile).toBe("generic");

      const listed = await store.listStreams(10, 0);
      expect(listed.map((row) => row.stream)).toEqual(["s"]);

      expect(await store.deleteStream("s")).toBe(true);
      expect(await store.deleteStream("s")).toBe(false);
      expect(await store.listStreams(10, 0)).toEqual([]);
      expect(await store.hardDeleteStream("s")).toBe(true);
      expect(await store.getStream("s")).toBeNull();

      try {
        await store.ensureStream("bad-profile", { profile: "metrics" });
        throw new Error("expected non-generic profile rejection");
      } catch (error) {
        expect((error as Error).message).toContain("postgres storage supports generic profiles only");
      }
    });
  });

  test("full-mode migration installs segment, manifest, index, and touch capability tables", async () => {
    await withPostgresSchema(async ({ schema, connectionString }) => {
      const store = await PostgresDurableStore.connectFull(connectionString);
      try {
        expect(store.capabilities.segmentReads).toBe(true);
        expect(store.capabilities.manifests).toBe(true);
        expect(store.capabilities.schemaPublication).toBe(true);
        expect(store.capabilities.indexes).toBe(true);
        expect(store.capabilities.builtinProfiles).toBe(true);
        expect(store.capabilities.internalMetrics).toBe(true);
        expect(store.capabilities.touch).toBe(true);
        expect(store.capabilities.storageStats).toBe(false);

        const inspectPool = new Pool({ connectionString });
        try {
          const tables = await inspectPool.query<{ table_name: string }>(
            `SELECT table_name
             FROM information_schema.tables
             WHERE table_schema = $1
             ORDER BY table_name ASC;`,
            [schema]
          );
          expect(tables.rows.map((row) => row.table_name)).toEqual([
            "index_runs",
            "index_state",
            "lexicon_index_runs",
            "lexicon_index_state",
            "live_templates",
            "manifests",
            "producer_state",
            "schema_version",
            "schemas",
            "search_companion_plans",
            "search_segment_companions",
            "secondary_index_runs",
            "secondary_index_state",
            "segments",
            "stream_profiles",
            "stream_segment_meta",
            "stream_touch_state",
            "streams",
            "wal",
          ]);

          const streamColumns = await inspectPool.query<{ column_name: string }>(
            `SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = 'streams';`,
            [schema]
          );
          const columnNames = new Set(streamColumns.rows.map((row) => row.column_name));
          for (const column of [
            "sealed_through",
            "uploaded_through",
            "uploaded_segment_count",
            "pending_rows",
            "pending_bytes",
            "last_segment_cut_ms",
            "segment_in_progress",
            "segment_claim_token",
            "segment_claimed_at_ms",
          ]) {
            expect(columnNames.has(column)).toBe(true);
          }
        } finally {
          await inspectPool.end();
        }
      } finally {
        await store.close();
      }
    });
  });

  test("append assigns contiguous offsets and reads wal ranges", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "application/json" });
      const appendMs = store.nowMs() + 100n;
      const res = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: appendMs,
          rows: [
            { routingKey: new TextEncoder().encode("a"), contentType: "application/json", payload: new TextEncoder().encode(`{"n":1}`) },
            { routingKey: new TextEncoder().encode("b"), contentType: "application/json", payload: new TextEncoder().encode(`{"n":2}`) },
          ],
          contentType: "application/json",
          streamSeq: "001",
          producer: null,
          close: false,
        },
      ]);
      expect(Result.isOk(res)).toBe(true);
      if (Result.isError(res)) return;
      expect(res.value.results).toHaveLength(1);
      expect(Result.isOk(res.value.results[0]!)).toBe(true);
      if (Result.isOk(res.value.results[0]!)) {
        expect(res.value.results[0]!.value.lastOffset).toBe(1n);
        expect(res.value.results[0]!.value.appendedRows).toBe(2);
      }
      const stream = await store.getStream("s");
      expect(stream?.next_offset).toBe(2n);
      expect(stream?.wal_rows).toBe(2n);
      expect(stream?.wal_bytes).toBe(14n);

      const allRows = await collect(store.readWalRange("s", 0n, 10n));
      expect(allRows.map((row) => row.offset)).toEqual([0n, 1n]);
      expect(allRows.map((row) => new TextDecoder().decode(row.payload))).toEqual([`{"n":1}`, `{"n":2}`]);

      const keyed = await collect(store.readWalRange("s", 0n, 10n, new TextEncoder().encode("b")));
      expect(keyed.map((row) => row.offset)).toEqual([1n]);

      const desc = await collect(store.readWalRangeDesc("s", 0n, 10n));
      expect(desc.map((row) => row.offset)).toEqual([1n, 0n]);
      expect(await store.getWalOldestTimestampMsForRead("s")).toBe(appendMs);
    });
  });

  test("wal reads page through ranges larger than one chunk", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "text/plain" });
      const count = 1100;
      const res = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: store.nowMs() + 100n,
          rows: Array.from({ length: count }, (_, idx) => ({
            routingKey: idx % 2 === 0 ? new TextEncoder().encode("even") : new TextEncoder().encode("odd"),
            contentType: "text/plain",
            payload: new TextEncoder().encode("x"),
          })),
          contentType: "text/plain",
          streamSeq: null,
          producer: null,
          close: false,
        },
      ]);
      expect(Result.isOk(res)).toBe(true);
      const allRows = await collect(store.readWalRange("s", 0n, BigInt(count + 10)));
      expect(allRows).toHaveLength(count);
      expect(allRows[0]?.offset).toBe(0n);
      expect(allRows[count - 1]?.offset).toBe(BigInt(count - 1));

      const evenRows = await collect(store.readWalRange("s", 0n, BigInt(count + 10), new TextEncoder().encode("even")));
      expect(evenRows).toHaveLength(count / 2);
      expect(evenRows[0]?.offset).toBe(0n);

      const descRows = await collect(store.readWalRangeDesc("s", 0n, BigInt(count + 10)));
      expect(descRows).toHaveLength(count);
      expect(descRows[0]?.offset).toBe(BigInt(count - 1));
      expect(descRows[count - 1]?.offset).toBe(0n);
    });
  });

  test("producer sequencing is transactional and idempotent", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "text/plain" });
      const first = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: 1n,
          rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode("a") }],
          contentType: "text/plain",
          streamSeq: "001",
          producer: { id: "p1", epoch: 0, seq: 0 },
          close: false,
        },
      ]);
      expect(Result.isOk(first)).toBe(true);

      const duplicate = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: 2n,
          rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode("dup") }],
          contentType: "text/plain",
          streamSeq: null,
          producer: { id: "p1", epoch: 0, seq: 0 },
          close: false,
        },
      ]);
      expect(Result.isOk(duplicate)).toBe(true);
      if (Result.isOk(duplicate) && Result.isOk(duplicate.value.results[0]!)) {
        expect(duplicate.value.results[0]!.value.duplicate).toBe(true);
        expect(duplicate.value.results[0]!.value.appendedRows).toBe(0);
      }

      const gap = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: 3n,
          rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode("gap") }],
          contentType: "text/plain",
          streamSeq: null,
          producer: { id: "p1", epoch: 0, seq: 2 },
          close: false,
        },
      ]);
      expect(Result.isOk(gap)).toBe(true);
      if (Result.isOk(gap)) {
        expect(gap.value.results[0]).toEqual(Result.err({ kind: "producer_gap", expected: 1, received: 2 }));
      }

      const rows = await collect(store.readWalRange("s", 0n, 10n));
      expect(rows.map((row) => new TextDecoder().decode(row.payload))).toEqual(["a"]);

      const staleSeq = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: 4n,
          rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode("bad-seq") }],
          contentType: "text/plain",
          streamSeq: "001",
          producer: { id: "p1", epoch: 0, seq: 1 },
          close: false,
        },
      ]);
      expect(Result.isOk(staleSeq)).toBe(true);
      if (Result.isOk(staleSeq)) {
        expect(staleSeq.value.results[0]).toEqual(Result.err({ kind: "stream_seq", expected: "001", received: "001" }));
      }

      const retry = await store.appendBatch([
        {
          stream: "s",
          baseAppendMs: 5n,
          rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode("b") }],
          contentType: "text/plain",
          streamSeq: "002",
          producer: { id: "p1", epoch: 0, seq: 1 },
          close: false,
        },
      ]);
      expect(Result.isOk(retry)).toBe(true);
      if (Result.isOk(retry) && Result.isOk(retry.value.results[0]!)) {
        expect(retry.value.results[0]!.value.duplicate).toBe(false);
        expect(retry.value.results[0]!.value.appendedRows).toBe(1);
      }
    });
  });

  test("concurrent appends keep offsets contiguous", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "text/plain" });
      const batches = await Promise.all(
        Array.from({ length: 12 }, (_, idx) =>
          store.appendBatch([
            {
              stream: "s",
              baseAppendMs: store.nowMs() + BigInt(idx + 1),
              rows: [{ routingKey: null, contentType: "text/plain", payload: new TextEncoder().encode(String(idx)) }],
              contentType: "text/plain",
              streamSeq: null,
              producer: null,
              close: false,
            },
          ])
        )
      );
      expect(batches.every(Result.isOk)).toBe(true);
      const stream = await store.getStream("s");
      expect(stream?.next_offset).toBe(12n);
      const rows = await collect(store.readWalRange("s", 0n, 20n));
      expect(rows.map((row) => row.offset)).toEqual(Array.from({ length: 12 }, (_, idx) => BigInt(idx)));
    });
  });

  test("schema metadata persists and rejects search configuration", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "application/json" });
      const registry = new SchemaRegistryStore(store);
      const update = await registry.updateRegistryResult("s", {
        schema: {
          type: "object",
          properties: { name: { type: "string" } },
          required: ["name"],
          additionalProperties: false,
        },
      });
      expect(Result.isOk(update)).toBe(true);
      const stored = await store.getSchemaRegistryForRead("s");
      expect(stored).not.toBeNull();
      expect(stored?.uploaded_size_bytes).toBe(0n);

      const search = await registry.updateSearchResult("s", {
        primaryTimestampField: "ts",
        fields: {
          ts: { kind: "date", bindings: [{ version: 1, jsonPointer: "/ts" }], exact: true },
        },
      });
      expect(Result.isError(search)).toBe(true);
      if (Result.isError(search)) {
        expect(search.error.message).toContain("postgres storage does not support schema search configuration yet");
      }
    });
  });

  test("profile metadata supports generic only", async () => {
    await withPostgresStore(async (store) => {
      await store.ensureStream("s", { contentType: "application/json" });
      const profiles = new StreamProfileStore(store);
      const generic = await profiles.updateProfileResult("s", { kind: "generic" });
      expect(Result.isOk(generic)).toBe(true);
      expect((await store.getStream("s"))?.profile).toBe("generic");
      expect(await store.getStreamProfileForRead("s")).toBeNull();

      const unsupported = await store.commitProfileMetadataMutation("s", ({ streamRow }) => {
        expect(streamRow).not.toBeNull();
        return Result.ok({
          metadata: {
            streamProfile: "state-protocol",
            profileJson: JSON.stringify({ kind: "state-protocol" }),
            schemaRegistry: null,
          },
          value: "unsupported",
        });
      });
      expect(Result.isError(unsupported)).toBe(true);
      if (Result.isError(unsupported)) {
        expect(unsupported.error.message).toContain("postgres storage supports generic profiles only");
      }
    });
  });
});

if (!POSTGRES_URL) {
  test("postgres durable store tests require DS_TEST_POSTGRES_URL", () => {
    expect(POSTGRES_URL).toBeUndefined();
  });
}
