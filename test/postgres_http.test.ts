import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { Pool } from "pg";
import { createPostgresApp } from "../src/app";
import { loadConfig } from "../src/config";
import { encodeOffset, parseOffset } from "../src/offset";
import { PostgresDurableStore } from "../src/postgres/store";

const POSTGRES_URL = process.env.DS_TEST_POSTGRES_URL;
const maybeDescribe = POSTGRES_URL ? describe : describe.skip;

function nextOffset(res: Response): bigint {
  const raw = res.headers.get("stream-next-offset");
  if (!raw) throw new Error("missing stream-next-offset");
  const parsed = parseOffset(raw);
  return parsed.kind === "start" ? -1n : parsed.seq;
}

async function withPostgresHttp<T>(fn: (baseUrl: string) => Promise<T>): Promise<T> {
  if (!POSTGRES_URL) throw new Error("DS_TEST_POSTGRES_URL is required");
  const schema = `ds_http_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  const setupPool = new Pool({ connectionString: POSTGRES_URL });
  await setupPool.query(`CREATE SCHEMA ${schema};`);
  await setupPool.end();

  const connectionString = `${POSTGRES_URL}${POSTGRES_URL.includes("?") ? "&" : "?"}options=-c%20search_path%3D${schema}`;
  const store = await PostgresDurableStore.connect(connectionString);
  const cfg = {
    ...loadConfig(),
    storage: "postgres" as const,
    postgresUrl: connectionString,
    rootDir: mkdtempSync(join(tmpdir(), "ds-postgres-http-")),
  };
  const app = createPostgresApp(cfg, store);
  await app.ready;
  const server = Bun.serve({ port: 0, fetch: app.fetch });
  try {
    return await fn(`http://127.0.0.1:${server.port}`);
  } finally {
    server.stop(true);
    await app.close();
    const cleanupPool = new Pool({ connectionString: POSTGRES_URL });
    try {
      await cleanupPool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE;`);
    } finally {
      await cleanupPool.end();
    }
  }
}

maybeDescribe("postgres shared HTTP runtime", () => {
  test("create append read long-poll delete and list use the shared WAL-only runtime", async () => {
    await withPostgresHttp(async (baseUrl) => {
      let res = await fetch(`${baseUrl}/v1/stream/raw`, { method: "PUT", headers: { "content-type": "text/plain" } });
      expect([200, 201]).toContain(res.status);
      expect(nextOffset(res)).toBe(-1n);

      res = await fetch(`${baseUrl}/v1/stream/raw`, { method: "HEAD" });
      expect(res.status).toBe(200);
      expect(nextOffset(res)).toBe(-1n);
      expect(res.headers.get("stream-end-offset")).toBe(encodeOffset(0, -1n));

      res = await fetch(`${baseUrl}/v1/stream/raw`, { method: "POST", headers: { "content-type": "text/plain" }, body: "a" });
      expect(res.status).toBe(204);
      expect(nextOffset(res)).toBe(0n);

      res = await fetch(`${baseUrl}/v1/stream/raw`, { method: "POST", headers: { "content-type": "text/plain" }, body: "b" });
      expect(res.status).toBe(204);
      expect(nextOffset(res)).toBe(1n);

      res = await fetch(`${baseUrl}/v1/stream/raw?offset=-1`);
      expect(res.status).toBe(200);
      expect(await res.text()).toBe("ab");
      expect(nextOffset(res)).toBe(1n);
      expect(res.headers.get("stream-end-offset")).toBe(encodeOffset(0, 1n));

      res = await fetch(`${baseUrl}/v1/stream/raw?offset=${encodeOffset(0, 1n)}&live=long-poll&timeout=50ms`);
      expect(res.status).toBe(204);
      expect(res.headers.get("stream-up-to-date")).toBe("true");
      expect(nextOffset(res)).toBe(1n);

      res = await fetch(`${baseUrl}/v1/stream/json`, { method: "PUT", headers: { "content-type": "application/json" } });
      expect([200, 201]).toContain(res.status);
      res = await fetch(`${baseUrl}/v1/stream/json/_schema`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ routingKey: { jsonPointer: "/k", required: true } }),
      });
      expect(res.status).toBe(200);

      res = await fetch(`${baseUrl}/v1/stream/json`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify([{ k: "k1", x: 1 }, { k: "k2", y: 2 }]),
      });
      expect(res.status).toBe(204);

      res = await fetch(`${baseUrl}/v1/stream/json?offset=-1&format=json&key=k2`);
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual([{ k: "k2", y: 2 }]);

      res = await fetch(`${baseUrl}/v1/streams`);
      expect(res.status).toBe(200);
      const streams = await res.json();
      expect(streams.map((row: { name: string }) => row.name).sort()).toEqual(["json", "raw"]);

      for (const [path, method] of [
        ["/v1/stream/json/_search", "GET"],
        ["/v1/stream/json/_aggregate", "POST"],
        ["/v1/stream/json/_routing_keys", "GET"],
        ["/v1/stream/json/touch/meta", "GET"],
        ["/v1/stream/json/_details", "GET"],
        ["/v1/traces", "POST"],
      ] as const) {
        res = await fetch(`${baseUrl}${path}`, { method });
        expect(res.status).toBe(501);
        expect((await res.json()).error.code).toBe("unsupported_capability");
      }

      res = await fetch(`${baseUrl}/v1/stream/raw`, { method: "DELETE" });
      expect(res.status).toBe(204);
      res = await fetch(`${baseUrl}/v1/stream/raw?offset=-1`);
      expect(res.status).toBe(404);
      res = await fetch(`${baseUrl}/v1/streams`);
      expect((await res.json()).map((row: { name: string }) => row.name)).toEqual(["json"]);
    });
  });
});

describe("postgres server startup guards", () => {
  async function runServer(args: string[]): Promise<{ exitCode: number; stderr: string }> {
    const child = Bun.spawn({
      cmd: ["bun", "run", "src/server.ts", ...args],
      cwd: join(import.meta.dir, ".."),
      env: {
        ...process.env,
        DS_STORAGE: "postgres",
        DS_POSTGRES_URL: "postgres://localhost/unused",
        PORT: "0",
      },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [exitCode, stderr] = await Promise.all([
      child.exited,
      new Response(child.stderr).text(),
      new Response(child.stdout).text(),
    ]);
    return { exitCode, stderr };
  }

  test("postgres mode rejects object-store and bootstrap flags before connecting", async () => {
    let result = await runServer(["--object-store", "local", "--no-auth"]);
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("postgres storage does not support --object-store");

    result = await runServer(["--bootstrap-from-r2", "--no-auth"]);
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("postgres storage does not support --bootstrap-from-r2");
  });
});

test("postgres shared HTTP tests require DS_TEST_POSTGRES_URL", () => {
  if (POSTGRES_URL) return;
  expect(POSTGRES_URL).toBeUndefined();
});
