import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import type { TouchProcessorStore } from "../src/store/touch_store";
import { processTouchBatch } from "../src/touch/process_batch";
import type { StreamProfileSpec } from "../src/profiles/profile";
import type { WalReadRow } from "../src/store/wal_store";

const encoder = new TextEncoder();

const stateProfile: StreamProfileSpec = {
  kind: "state-protocol",
  touch: { enabled: true },
};

function makeStore(overrides: Partial<TouchProcessorStore>): TouchProcessorStore {
  return {
    ...overrides,
  } as TouchProcessorStore;
}

function walRow(value: unknown, offset = 0n): WalReadRow {
  return {
    stream: "state",
    offset,
    tsMs: 1000n,
    payload: encoder.encode(JSON.stringify(value)),
  } as WalReadRow;
}

describe("processTouchBatch", () => {
  test("fails closed on malformed durable live-template metadata", async () => {
    const store = makeStore({
      async listActiveLiveTemplates() {
        return [
          {
            stream: "state",
            template_id: "0123456789abcdef",
            entity: "posts",
            fields_json: "{",
            encodings_json: `["string"]`,
            state: "active",
            created_at_ms: 1n,
            last_seen_at_ms: 1n,
            inactivity_ttl_ms: 60000n,
            active_from_source_offset: 0n,
            retired_at_ms: null,
            retired_reason: null,
          },
        ];
      },
      async *readWalRange() {
        yield walRow({
          type: "posts",
          key: "1",
          value: { tenantId: "t1" },
          headers: { operation: "insert" },
        });
      },
    });

    const res = await processTouchBatch({
      db: store,
      stream: "state",
      fromOffset: 0n,
      toOffset: 0n,
      profile: stateProfile,
      maxRows: 100,
      maxBytes: 1024 * 1024,
    });

    expect(Result.isError(res)).toBe(true);
    if (Result.isError(res)) {
      expect(res.error.message).toContain("invalid live template metadata");
      expect(res.error.message).toContain("fields_json");
    }
  });

  test("keeps valid cold templates filtered without failing the batch", async () => {
    const store = makeStore({
      async listActiveLiveTemplates() {
        return [
          {
            stream: "state",
            template_id: "0123456789abcdef",
            entity: "posts",
            fields_json: `["tenantId"]`,
            encodings_json: `["string"]`,
            state: "active",
            created_at_ms: 1n,
            last_seen_at_ms: 1n,
            inactivity_ttl_ms: 60000n,
            active_from_source_offset: 0n,
            retired_at_ms: null,
            retired_reason: null,
          },
        ];
      },
      async *readWalRange() {
        yield walRow({
          type: "posts",
          key: "1",
          value: { tenantId: "t1" },
          headers: { operation: "insert" },
        });
      },
    });

    const res = await processTouchBatch({
      db: store,
      stream: "state",
      fromOffset: 0n,
      toOffset: 0n,
      profile: stateProfile,
      maxRows: 100,
      maxBytes: 1024 * 1024,
      filterHotTemplates: true,
      hotTemplateIds: [],
    });

    expect(Result.isOk(res)).toBe(true);
    if (Result.isError(res)) throw new Error(res.error.message);
    expect(res.value.processedThrough).toBe(0n);
    expect(res.value.stats.tableTouchesEmitted).toBe(1);
    expect(res.value.stats.templateTouchesEmitted).toBe(0);
    expect(res.value.stats.fineTouchesSkippedColdTemplate).toBe(1);
  });
});
