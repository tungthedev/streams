import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { loadConfig } from "../src/config";
import { SqliteDurableStore } from "../src/db/db";
import { encodeOffset } from "../src/offset";
import { StreamReader } from "../src/reader";
import { SchemaRegistryStore } from "../src/schema/registry";

describe("reader store boundary", () => {
  test("wal-only reader works without segment reads but sealed streams fail loudly", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-reader-boundary-"));
    const db = new SqliteDurableStore(`${root}/wal.sqlite`);
    try {
      db.ensureStream("s", "application/octet-stream");
      const appendRes = db.appendWalRows({
        stream: "s",
        startOffset: 0n,
        baseAppendMs: 1n,
        rows: [{ routingKey: null, contentType: "application/octet-stream", payload: new Uint8Array([1, 2, 3]), appendMs: 1n }],
      });
      expect(Result.isOk(appendRes)).toBe(true);

      const cfg = {
        ...loadConfig(),
        rootDir: root,
        dbPath: `${root}/wal.sqlite`,
      };
      const reader = new StreamReader(cfg, db, new SchemaRegistryStore(db));

      const walOnly = await reader.readResult({
        stream: "s",
        offset: encodeOffset(0, -1n),
        key: null,
        format: "raw",
      });
      expect(Result.isOk(walOnly)).toBe(true);
      if (Result.isOk(walOnly)) {
        expect(walOnly.value.records.map((row) => Array.from(row.payload))).toEqual([[1, 2, 3]]);
      }

      db.commitSealedSegment({
        segmentId: "seg-0",
        stream: "s",
        segmentIndex: 0,
        startOffset: 0n,
        endOffset: 0n,
        blockCount: 1,
        lastAppendMs: 1n,
        payloadBytes: 3n,
        sizeBytes: 1,
        localPath: "",
        rowsSealed: 1n,
      });

      const sealed = await reader.readResult({
        stream: "s",
        offset: encodeOffset(0, -1n),
        key: null,
        format: "raw",
      });
      expect(Result.isError(sealed)).toBe(true);
      if (Result.isError(sealed)) {
        expect(sealed.error).toEqual({
          kind: "internal",
          message: "segment read capability required for sealed stream data",
        });
      }
    } finally {
      db.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
