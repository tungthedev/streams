import type { LiveTemplateActivationInput, LiveTemplateStore } from "../store/touch_store";
import { canonicalizeTemplateFields, templateIdFor, type TemplateEncoding } from "./live_keys";

export type TemplateFieldSpec = { name: string; encoding: TemplateEncoding };
export type TemplateDecl = { entity: string; fields: TemplateFieldSpec[] };

export type ActivatedTemplate = {
  templateId: string;
  state: "active";
  activeFromTouchOffset: string;
};

export type DeniedTemplate = {
  templateId: string;
  reason: "rate_limited" | "invalid";
};

export type TemplateLifecycleEvent =
  | {
      type: "live.template_activated";
      ts: string;
      stream: string;
      templateId: string;
      entity: string;
      fields: string[];
      encodings: TemplateEncoding[];
      reason: "declared" | "heartbeat";
      activeFromTouchOffset: string;
      inactivityTtlMs: number;
    }
  | {
      type: "live.template_retired";
      ts: string;
      stream: string;
      templateId: string;
      entity: string;
      fields: string[];
      encodings: TemplateEncoding[];
      lastSeenAt: string;
      inactiveForMs: number;
      reason: "inactivity";
    }
  | {
      type: "live.template_evicted";
      ts: string;
      stream: string;
      templateId: string;
      reason: "cap_exceeded";
      cap: number;
    };

type RateState = { tokens: number; lastRefillMs: number };

export type LiveTemplateRegistryMemoryStats = {
  lastSeenEntries: number;
  dirtyLastSeenEntries: number;
  rateStateStreams: number;
};

function nowIso(ms: number): string {
  return new Date(ms).toISOString();
}

export class LiveTemplateRegistry {
  private readonly db: LiveTemplateStore;

  // In-memory last-seen tracking to avoid sqlite writes on every wait call.
  private readonly lastSeenMem = new Map<string, { lastSeenMs: number; lastPersistMs: number }>();
  private readonly dirtyLastSeen = new Set<string>();

  private readonly rate = new Map<string, RateState>();
  private readonly activationLocks = new Map<string, Promise<void>>();

  constructor(db: LiveTemplateStore) {
    this.db = db;
  }

  private key(stream: string, templateId: string): string {
    return `${stream}\n${templateId}`;
  }

  getMemoryStats(): LiveTemplateRegistryMemoryStats {
    return {
      lastSeenEntries: this.lastSeenMem.size,
      dirtyLastSeenEntries: this.dirtyLastSeen.size,
      rateStateStreams: this.rate.size,
    };
  }

  private availableActivationTokens(stream: string, nowMs: number, limitPerMinute: number): number {
    if (limitPerMinute <= 0) return Number.MAX_SAFE_INTEGER;
    const ratePerMs = limitPerMinute / 60_000;
    const st = this.rate.get(stream) ?? { tokens: limitPerMinute, lastRefillMs: nowMs };
    const elapsed = Math.max(0, nowMs - st.lastRefillMs);
    st.tokens = Math.min(limitPerMinute, st.tokens + elapsed * ratePerMs);
    st.lastRefillMs = nowMs;
    this.rate.set(stream, st);
    return Math.max(0, Math.floor(st.tokens));
  }

  private consumeActivationTokens(stream: string, nowMs: number, limitPerMinute: number, count: number): void {
    if (limitPerMinute <= 0 || count <= 0) return;
    this.availableActivationTokens(stream, nowMs, limitPerMinute);
    const st = this.rate.get(stream);
    if (!st) return;
    st.tokens = Math.max(0, st.tokens - count);
    this.rate.set(stream, st);
  }

  private async withActivationLock<T>(stream: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.activationLocks.get(stream) ?? Promise.resolve();
    let release!: () => void;
    const current = new Promise<void>((resolve) => {
      release = resolve;
    });
    const chained = previous.then(() => current, () => current);
    this.activationLocks.set(stream, chained);
    await previous.catch(() => {});
    try {
      return await fn();
    } finally {
      release();
      if (this.activationLocks.get(stream) === chained) this.activationLocks.delete(stream);
    }
  }

  async getActiveTemplateCount(stream: string): Promise<number> {
    try {
      return await this.db.countActiveLiveTemplates(stream);
    } catch {
      return 0;
    }
  }

  async listActiveTemplates(stream: string): Promise<Array<{ templateId: string; entity: string; fields: string[]; encodings: TemplateEncoding[]; lastSeenAtMs: number }>> {
    try {
      const rows = await this.db.listActiveLiveTemplates(stream);
      const out: Array<{ templateId: string; entity: string; fields: string[]; encodings: TemplateEncoding[]; lastSeenAtMs: number }> = [];
      for (const row of rows) {
        const templateId = row.template_id;
        const entity = row.entity;
        const fields = JSON.parse(row.fields_json);
        const encodings = JSON.parse(row.encodings_json);
        if (!Array.isArray(fields) || !Array.isArray(encodings) || fields.length !== encodings.length) continue;
        const lastSeenAtMs = Number(row.last_seen_at_ms);
        out.push({ templateId, entity, fields: fields.map(String), encodings: encodings.map(String) as any, lastSeenAtMs });
      }
      return out;
    } catch {
      return [];
    }
  }

  /**
   * Activate templates (idempotent). Returns lifecycle events to be emitted.
   *
   * `baseStreamNextOffset` is used to set `active_from_source_offset` so we do
   * not backfill fine touches for history when a template is activated while
   * touch processing is behind.
   */
  async activate(args: {
    stream: string;
    activeFromTouchOffset: string;
    baseStreamNextOffset: bigint;
    templates: TemplateDecl[];
    inactivityTtlMs: number;
    limits: {
      maxActiveTemplatesPerStream: number;
      maxActiveTemplatesPerEntity: number;
      activationRateLimitPerMinute: number;
    };
    nowMs: number;
  }): Promise<{ activated: ActivatedTemplate[]; denied: DeniedTemplate[]; lifecycle: TemplateLifecycleEvent[] }> {
    return this.withActivationLock(args.stream, () => this.activateLocked(args));
  }

  private async activateLocked(args: {
    stream: string;
    activeFromTouchOffset: string;
    baseStreamNextOffset: bigint;
    templates: TemplateDecl[];
    inactivityTtlMs: number;
    limits: {
      maxActiveTemplatesPerStream: number;
      maxActiveTemplatesPerEntity: number;
      activationRateLimitPerMinute: number;
    };
    nowMs: number;
  }): Promise<{ activated: ActivatedTemplate[]; denied: DeniedTemplate[]; lifecycle: TemplateLifecycleEvent[] }> {
    const { stream, templates, inactivityTtlMs, nowMs } = args;
    const { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity, activationRateLimitPerMinute } = args.limits;

    const activated: ActivatedTemplate[] = [];
    const denied: DeniedTemplate[] = [];
    const lifecycle: TemplateLifecycleEvent[] = [];

    const atomicInputs: LiveTemplateActivationInput[] = [];
    const atomicLifecycleById = new Map<string, TemplateLifecycleEvent>();

    for (const t of templates) {
      const entity = typeof t?.entity === "string" ? t.entity.trim() : "";
      if (entity === "") {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }
      if (!Array.isArray(t.fields) || t.fields.length === 0 || t.fields.length > 3) {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }

      const rawFields: Array<{ name: string; encoding: TemplateEncoding }> = [];
      for (const f of t.fields) {
        const name = typeof (f as any)?.name === "string" ? String((f as any).name).trim() : "";
        const encoding = (f as any)?.encoding as TemplateEncoding;
        if (name === "") continue;
        if (encoding !== "string" && encoding !== "int64" && encoding !== "bool" && encoding !== "datetime" && encoding !== "bytes") continue;
        rawFields.push({ name, encoding });
      }
      if (rawFields.length !== t.fields.length) {
        denied.push({ templateId: "0000000000000000", reason: "invalid" });
        continue;
      }
      {
        const seen = new Set<string>();
        let ok = true;
        for (const f of rawFields) {
          if (seen.has(f.name)) ok = false;
          seen.add(f.name);
        }
        if (!ok) {
          denied.push({ templateId: "0000000000000000", reason: "invalid" });
          continue;
        }
      }

      const fields = canonicalizeTemplateFields(rawFields);
      const fieldNames = fields.map((f) => f.name);
      const encodings = fields.map((f) => f.encoding);

      const templateId = templateIdFor(entity, fieldNames);
      const fieldsJson = JSON.stringify(fieldNames);
      const encodingsJson = JSON.stringify(encodings);

      if (atomicLifecycleById.has(templateId)) continue;

      atomicInputs.push({
        templateId,
        entity,
        fieldsJson,
        encodingsJson,
        nowMs,
        inactivityTtlMs,
        activeFromSourceOffset: args.baseStreamNextOffset,
      });
      atomicLifecycleById.set(templateId, {
        type: "live.template_activated",
        ts: nowIso(nowMs),
        stream,
        templateId,
        entity,
        fields: fieldNames,
        encodings,
        reason: "declared",
        activeFromTouchOffset: args.activeFromTouchOffset,
        inactivityTtlMs,
      });
    }

    if (atomicInputs.length === 0) return { activated, denied, lifecycle };

    const atomicRes = await this.db.activateLiveTemplates({
      stream,
      templates: atomicInputs,
      maxActiveTemplatesPerStream,
      maxActiveTemplatesPerEntity,
      maxActivationTokens: this.availableActivationTokens(stream, nowMs, activationRateLimitPerMinute),
    });
    const invalidIds = new Set(atomicRes.invalid);
    const rateLimitedIds = new Set(atomicRes.rateLimited);
    for (const input of atomicInputs) {
      if (invalidIds.has(input.templateId)) {
        denied.push({ templateId: input.templateId, reason: "invalid" });
        continue;
      }
      if (rateLimitedIds.has(input.templateId)) {
        denied.push({ templateId: input.templateId, reason: "rate_limited" });
        continue;
      }
      if (!atomicRes.activated.includes(input.templateId)) continue;
      activated.push({ templateId: input.templateId, state: "active", activeFromTouchOffset: args.activeFromTouchOffset });
      const event = atomicLifecycleById.get(input.templateId);
      if (event) lifecycle.push(event);
      this.markSeen(stream, input.templateId, nowMs);
    }
    this.consumeActivationTokens(stream, nowMs, activationRateLimitPerMinute, atomicRes.activationTokensUsed);
    for (const evicted of atomicRes.evicted) {
      lifecycle.push({
        type: "live.template_evicted",
        ts: nowIso(nowMs),
        stream,
        templateId: evicted.templateId,
        reason: evicted.reason,
        cap: evicted.cap,
      });
      this.lastSeenMem.delete(this.key(stream, evicted.templateId));
      this.dirtyLastSeen.delete(this.key(stream, evicted.templateId));
    }
    return { activated, denied, lifecycle };
  }

  heartbeat(stream: string, templateIdsUsed: string[], nowMs: number): void {
    for (const id of templateIdsUsed) {
      const templateId = typeof id === "string" ? id.trim() : "";
      if (!/^[0-9a-f]{16}$/.test(templateId)) continue;
      this.markSeen(stream, templateId, nowMs);
    }
  }

  async flushLastSeen(nowMs: number, persistIntervalMs: number): Promise<void> {
    if (this.dirtyLastSeen.size === 0) return;

    const updates: Array<{ key: string; item: { lastSeenMs: number; lastPersistMs: number }; stream: string; templateId: string }> = [];
    for (const k of this.dirtyLastSeen) {
      const item = this.lastSeenMem.get(k);
      if (!item) {
        this.dirtyLastSeen.delete(k);
        continue;
      }
      if (nowMs - item.lastPersistMs < persistIntervalMs) continue;
      const [stream, templateId] = k.split("\n");
      updates.push({ key: k, item, stream, templateId });
    }
    if (updates.length === 0) return;

    await this.db.updateLiveTemplateLastSeenBatch(
      updates.map((update) => ({
        stream: update.stream,
        templateId: update.templateId,
        lastSeenAtMs: update.item.lastSeenMs,
      }))
    );
    for (const update of updates) {
      update.item.lastPersistMs = nowMs;
      this.dirtyLastSeen.delete(update.key);
    }
  }

  async gcRetireExpired(stream: string, nowMs: number): Promise<{ retired: TemplateLifecycleEvent[] }> {
    const expired: any[] = [];
    try {
      const rows = await this.db.listExpiredLiveTemplates(stream, nowMs, 1000);
      expired.push(...rows);
    } catch {
      return { retired: [] };
    }
    if (expired.length === 0) return { retired: [] };

    // If a client is heartbeating frequently but last-seen persistence is
    // configured with a longer interval than the inactivity TTL, DB state can
    // look expired even though in-memory last-seen is fresh. Prefer in-memory
    // last-seen and opportunistically refresh DB to avoid incorrect retirement.
    const effectiveExpired: any[] = [];
    const refreshes: Array<{ key: string; item: { lastSeenMs: number; lastPersistMs: number }; stream: string; templateId: string }> = [];
    for (const row of expired) {
      const templateId = String(row.template_id);
      const dbLastSeenAtMs = Number(row.last_seen_at_ms);
      const ttlMs = Number(row.inactivity_ttl_ms);
      const mem = this.lastSeenMem.get(this.key(stream, templateId));
      const memLastSeen = mem ? mem.lastSeenMs : 0;
      const lastSeenAtMs = Math.max(dbLastSeenAtMs, memLastSeen);
      if (lastSeenAtMs + ttlMs >= nowMs) {
        // Not expired when considering in-memory last-seen. Refresh DB so it
        // doesn't get re-selected on the next GC tick.
        if (mem && memLastSeen > dbLastSeenAtMs) {
          refreshes.push({ key: this.key(stream, templateId), item: mem, stream, templateId });
        }
        continue;
      }
      effectiveExpired.push(row);
    }

    if (refreshes.length > 0) {
      await this.db.updateLiveTemplateLastSeenBatch(
        refreshes.map((refresh) => ({
          stream: refresh.stream,
          templateId: refresh.templateId,
          lastSeenAtMs: refresh.item.lastSeenMs,
        }))
      );
      for (const refresh of refreshes) {
        refresh.item.lastPersistMs = nowMs;
        this.dirtyLastSeen.delete(refresh.key);
      }
    }

    if (effectiveExpired.length === 0) return { retired: [] };

    const retired: TemplateLifecycleEvent[] = [];
    const retiredIds: string[] = [];
    for (const row of effectiveExpired) {
      const templateId = String(row.template_id);
      const entity = String(row.entity);
      let fields: string[] = [];
      let encodings: TemplateEncoding[] = [];
      try {
        const f = JSON.parse(String(row.fields_json));
        const e = JSON.parse(String(row.encodings_json));
        if (Array.isArray(f)) fields = f.map(String);
        if (Array.isArray(e)) encodings = e.map(String) as any;
      } catch {
        // ignore
      }
      const dbLastSeenAtMs = Number(row.last_seen_at_ms);
      const mem = this.lastSeenMem.get(this.key(stream, templateId));
      const memLastSeen = mem ? mem.lastSeenMs : 0;
      const lastSeenAtMs = Math.max(dbLastSeenAtMs, memLastSeen);
      const inactiveForMs = Math.max(0, nowMs - lastSeenAtMs);
      retiredIds.push(templateId);
      retired.push({
        type: "live.template_retired",
        ts: nowIso(nowMs),
        stream,
        templateId,
        entity,
        fields,
        encodings,
        lastSeenAt: nowIso(lastSeenAtMs),
        inactiveForMs,
        reason: "inactivity",
      });
    }

    await this.db.retireLiveTemplatesForInactivity(stream, retiredIds, nowMs);
    for (const templateId of retiredIds) {
      this.lastSeenMem.delete(this.key(stream, templateId));
      this.dirtyLastSeen.delete(this.key(stream, templateId));
    }

    return { retired };
  }

  private markSeen(stream: string, templateId: string, nowMs: number): void {
    const k = this.key(stream, templateId);
    const item = this.lastSeenMem.get(k) ?? { lastSeenMs: 0, lastPersistMs: 0 };
    if (nowMs > item.lastSeenMs) item.lastSeenMs = nowMs;
    this.lastSeenMem.set(k, item);
    this.dirtyLastSeen.add(k);
  }
}
