import { describe, expect, test } from "bun:test";
import type { LiveTemplateStore, LiveTemplateStoreRow } from "../src/store/touch_store";
import { LiveTemplateRegistry } from "../src/touch/live_templates";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeConcurrentActivationStore(): LiveTemplateStore {
  const rows = new Map<string, LiveTemplateStoreRow>();
  return {
    async activateLiveTemplates(args) {
      await delay(20);
      const activated: string[] = [];
      const invalid: string[] = [];
      const rateLimited: string[] = [];
      let activationTokensUsed = 0;
      for (const template of args.templates) {
        const existing = rows.get(template.templateId);
        if (existing && (existing.entity !== template.entity || existing.fields_json !== template.fieldsJson || existing.encodings_json !== template.encodingsJson)) {
          invalid.push(template.templateId);
          continue;
        }
        const alreadyActive = existing?.state === "active";
        if (!alreadyActive && activationTokensUsed >= args.maxActivationTokens) {
          rateLimited.push(template.templateId);
          continue;
        }
        if (!alreadyActive) activationTokensUsed += 1;
        rows.set(template.templateId, {
          stream: args.stream,
          template_id: template.templateId,
          entity: template.entity,
          fields_json: template.fieldsJson,
          encodings_json: template.encodingsJson,
          state: "active",
          created_at_ms: BigInt(template.nowMs),
          last_seen_at_ms: BigInt(template.nowMs),
          inactivity_ttl_ms: BigInt(template.inactivityTtlMs),
          active_from_source_offset: template.activeFromSourceOffset,
          retired_at_ms: null,
          retired_reason: null,
        });
        activated.push(template.templateId);
      }
      return { activated, invalid, rateLimited, activationTokensUsed, evicted: [] };
    },
    countActiveLiveTemplates() {
      return rows.size;
    },
    listActiveLiveTemplates() {
      return Array.from(rows.values());
    },
    updateLiveTemplateLastSeenBatch() {},
    listExpiredLiveTemplates() {
      return [];
    },
    retireLiveTemplatesForInactivity() {},
    listActiveLiveTemplateEntitiesByIds() {
      return [];
    },
  };
}

describe("LiveTemplateRegistry", () => {
  test("serializes activation rate tokens per stream across concurrent requests", async () => {
    const registry = new LiveTemplateRegistry(makeConcurrentActivationStore());
    const limits = {
      maxActiveTemplatesPerStream: 100,
      maxActiveTemplatesPerEntity: 100,
      activationRateLimitPerMinute: 1,
    };
    const common = {
      stream: "state",
      activeFromTouchOffset: "0",
      baseStreamNextOffset: 0n,
      inactivityTtlMs: 60000,
      limits,
      nowMs: 1000,
    };

    const [first, second] = await Promise.all([
      registry.activate({
        ...common,
        templates: [{ entity: "posts", fields: [{ name: "tenantId", encoding: "string" }] }],
      }),
      registry.activate({
        ...common,
        templates: [{ entity: "comments", fields: [{ name: "tenantId", encoding: "string" }] }],
      }),
    ]);

    const activated = first.activated.length + second.activated.length;
    const rateLimited = first.denied.filter((row) => row.reason === "rate_limited").length + second.denied.filter((row) => row.reason === "rate_limited").length;

    expect(activated).toBe(1);
    expect(rateLimited).toBe(1);
  });
});
