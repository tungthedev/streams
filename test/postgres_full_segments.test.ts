import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";
import { createPostgresFullApp } from "../src/app";
import { loadConfig } from "../src/config";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { manifestObjectKey, streamHash16Hex } from "../src/util/stream_paths";
import { PostgresDurableStore } from "../src/postgres/store";
import { Segmenter } from "../src/segment/segmenter";
import { Result } from "better-result";
import { STREAM_FLAG_TOUCH } from "../src/store/rows";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function schemaConnectionString(schema: string): string {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  return `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
}

async function withPostgresSchema<T>(fn: (ctx: { connectionString: string }) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_full_${Date.now()}_${Math.random().toString(16).slice(2)}`;
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

async function waitFor(predicate: () => Promise<boolean>, label: string, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timeout waiting for ${label}`);
}

maybeDescribe("postgres full-mode segment and manifest store", () => {
  test("segment candidates exclude touch-flagged streams", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const store = await PostgresDurableStore.connectFull(connectionString);
      const segmentStore = store.fullModeSegments();
      try {
        await store.ensureStream("touch_metrics", { contentType: "application/json", streamFlags: STREAM_FLAG_TOUCH });
        await store.appendBatch([
          {
            stream: "touch_metrics",
            baseAppendMs: store.nowMs(),
            rows: ["a", "b", "c"].map((value) => ({
              routingKey: null,
              contentType: "application/json",
              payload: new TextEncoder().encode(JSON.stringify({ value })),
            })),
            contentType: "application/json",
            streamSeq: null,
            producer: null,
            close: false,
          },
        ]);

        const candidates = await segmentStore.candidates(1n, 1n, 0n, 10);
        expect(candidates.map((candidate) => candidate.stream)).not.toContain("touch_metrics");
      } finally {
        await store.close();
      }
    });
  });

  test("segment claims are token-fenced and stale claims can be reclaimed", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const store = await PostgresDurableStore.connectFull(connectionString);
      const segmentStore = store.fullModeSegments();
      const inspectPool = new Pool({ connectionString });
      try {
        await store.ensureStream("claims", { contentType: "text/plain" });
        const first = await segmentStore.tryClaimSegment("claims");
        expect(first).not.toBeNull();
        expect(await segmentStore.tryClaimSegment("claims")).toBeNull();

        await inspectPool.query(
          `UPDATE streams
           SET segment_claimed_at_ms = $1
           WHERE stream = $2;`,
          [String(Date.now() - 10 * 60 * 1000), "claims"]
        );
        const reclaimed = await segmentStore.tryClaimSegment("claims");
        expect(reclaimed).not.toBeNull();
        expect(reclaimed?.token).not.toBe(first?.token);

        await segmentStore.setSegmentInProgress("claims", 0, first!);
        const stillClaimed = await inspectPool.query<{ segment_in_progress: number | string }>(
          `SELECT segment_in_progress FROM streams WHERE stream = $1;`,
          ["claims"]
        );
        expect(Number(stillClaimed.rows[0]?.segment_in_progress ?? 0)).toBe(1);

        await segmentStore.setSegmentInProgress("claims", 0, reclaimed!);
        const released = await inspectPool.query<{ segment_in_progress: number | string }>(
          `SELECT segment_in_progress FROM streams WHERE stream = $1;`,
          ["claims"]
        );
        expect(Number(released.rows[0]?.segment_in_progress ?? 1)).toBe(0);
      } finally {
        await inspectPool.end();
        await store.close();
      }
    });
  });

  test("segmenter recovers a stale Postgres claim and cuts from fresh stream bounds", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-claim-recover-"));
      const store = await PostgresDurableStore.connectFull(connectionString);
      const segmentStore = store.fullModeSegments();
      const inspectPool = new Pool({ connectionString });
      try {
        const cfg = {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          segmentCheckIntervalMs: 5,
          segmentMaxBytes: 1024 * 1024,
          segmentTargetRows: 3,
        };
        await store.ensureStream("stale_claim_cut", { contentType: "text/plain" });
        const append = await store.appendBatch([
          {
            stream: "stale_claim_cut",
            baseAppendMs: store.nowMs() + 1n,
            rows: ["a", "b", "c", "d"].map((value) => ({
              routingKey: null,
              contentType: "text/plain",
              payload: new TextEncoder().encode(value),
            })),
            contentType: "text/plain",
            streamSeq: null,
            producer: null,
            close: false,
          },
        ]);
        expect(Result.isOk(append)).toBe(true);
        await inspectPool.query(
          `UPDATE streams
           SET segment_in_progress = 1,
               segment_claim_token = $1,
               segment_claimed_at_ms = $2
           WHERE stream = $3;`,
          ["stale-token", String(Date.now() - 10 * 60 * 1000), "stale_claim_cut"]
        );

        const segmenter = new Segmenter(cfg, segmentStore, { candidatesPerTick: 1 });
        segmenter.start();
        await waitFor(async () => (await store.getStream("stale_claim_cut"))?.sealed_through === 2n, "stale claim segment cut");
        segmenter.stop();

        const streamRow = await store.getStream("stale_claim_cut");
        expect(streamRow?.segment_in_progress).toBe(0);
        expect(await segmentStore.countSegmentsForRead("stale_claim_cut")).toBe(1);
      } finally {
        await inspectPool.end();
        await store.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });

  test("manifest publication snapshots are locked across publishers", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const store = await PostgresDurableStore.connectFull(connectionString);
      const segmentStore = store.fullModeSegments();
      try {
        await store.ensureStream("manifest_lock", { contentType: "text/plain" });
        const first = await segmentStore.loadManifestPublicationSnapshot("manifest_lock");
        expect(typeof first?.publicationToken).toBe("string");
        expect(await segmentStore.loadManifestPublicationSnapshot("manifest_lock")).toBeNull();
        await segmentStore.releaseManifestPublication(first!.publicationToken!);
        const second = await segmentStore.loadManifestPublicationSnapshot("manifest_lock");
        expect(typeof second?.publicationToken).toBe("string");
        await segmentStore.releaseManifestPublication(second!.publicationToken!);
      } finally {
        await store.close();
      }
    });
  });

  test("cuts, uploads, publishes manifest, restarts, and reads segment plus WAL tail", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-seg-"));
      const objectStore = new MockR2Store();
      const stream = "pg_full_segments";
      const streamHash = streamHash16Hex(stream);
      let server: ReturnType<typeof Bun.serve> | null = null;
      let app: ReturnType<typeof createPostgresFullApp> | null = null;
      try {
        const cfg = {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          segmentCheckIntervalMs: 5,
          uploadIntervalMs: 5,
          segmentMaxBytes: 1024 * 1024,
          segmentTargetRows: 3,
          objectStoreRetries: 0,
        };
        const store = await PostgresDurableStore.connectFull(connectionString);
        const segmentStore = store.fullModeSegments();
        app = createPostgresFullApp(cfg, store, objectStore);
        server = Bun.serve({ port: 0, fetch: app.fetch });
        const baseUrl = `http://localhost:${server.port}`;

        await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        });
        for (const body of ["a", "b", "c", "d", "e"]) {
          const res = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
            method: "POST",
            headers: { "content-type": "text/plain" },
            body,
          });
          expect(res.status).toBe(200);
        }

        await waitFor(async () => (await store.getStream(stream))?.sealed_through === 2n, "segment seal");
        await waitFor(async () => (await store.getStream(stream))?.uploaded_through === 2n, "manifest publish");
        expect(await segmentStore.countPendingSegments()).toBe(0);
        expect(await segmentStore.countSegmentsForRead(stream)).toBe(1);
        expect((await store.getStream(stream))?.wal_rows).toBe(2n);

        const manifestBytes = await objectStore.get(manifestObjectKey(streamHash));
        expect(manifestBytes).not.toBeNull();
        const manifest = JSON.parse(new TextDecoder().decode(manifestBytes!));
        expect(manifest.segment_count).toBe(1);
        expect(manifest.uploaded_through).toBe(1);
        expect(manifest.stream_flags).toBe(0);

        server.stop();
        server = null;
        await app.close();
        app = null;

        const restartedStore = await PostgresDurableStore.connectFull(connectionString);
        app = createPostgresFullApp(cfg, restartedStore, objectStore);
        server = Bun.serve({ port: 0, fetch: app.fetch });
        const restartedUrl = `http://localhost:${server.port}`;
        const read = await fetch(`${restartedUrl}/v1/stream/${encodeURIComponent(stream)}?offset=0`);
        expect(read.status).toBe(200);
        expect(await read.text()).toBe("abcde");
      } finally {
        if (server) server.stop();
        if (app) await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });

  test("deleted streams publish deleted manifests", async () => {
    await withPostgresSchema(async ({ connectionString }) => {
      const root = mkdtempSync(join(tmpdir(), "ds-pg-full-delete-"));
      const objectStore = new MockR2Store();
      const stream = "pg_full_deleted";
      let server: ReturnType<typeof Bun.serve> | null = null;
      let app: ReturnType<typeof createPostgresFullApp> | null = null;
      try {
        const cfg = {
          ...loadConfig(),
          rootDir: root,
          dbPath: `${root}/unused.sqlite`,
          port: 0,
          segmentCheckIntervalMs: 60_000,
          uploadIntervalMs: 60_000,
        };
        const store = await PostgresDurableStore.connectFull(connectionString);
        app = createPostgresFullApp(cfg, store, objectStore);
        app.deps.segmenter?.stop();
        app.deps.uploader?.stop();
        server = Bun.serve({ port: 0, fetch: app.fetch });
        const baseUrl = `http://localhost:${server.port}`;

        await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
          method: "PUT",
          headers: { "content-type": "text/plain" },
        });
        const deleted = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, { method: "DELETE" });
        expect(deleted.status).toBe(204);

        const manifestBytes = await objectStore.get(manifestObjectKey(streamHash16Hex(stream)));
        expect(manifestBytes).not.toBeNull();
        const manifest = JSON.parse(new TextDecoder().decode(manifestBytes!));
        expect((manifest.stream_flags & 1) !== 0).toBe(true);
        expect(manifest.segment_count).toBe(0);
      } finally {
        if (server) server.stop();
        if (app) await app.close();
        rmSync(root, { recursive: true, force: true });
      }
    });
  });
});
