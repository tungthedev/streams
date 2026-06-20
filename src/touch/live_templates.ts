import type { LiveTemplateStore, LiveTemplateStoreRow } from "../store/touch_store";
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

export type LiveTemplateRow = {
  stream: string;
  template_id: string;
  entity: string;
  fields_json: string;
  encodings_json: string;
  state: string;
  created_at_ms: bigint;
  last_seen_at_ms: bigint;
  inactivity_ttl_ms: bigint;
  active_from_source_offset: bigint;
  retired_at_ms: bigint | null;
  retired_reason: string | null;
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

function parseTemplateRow(row: LiveTemplateStoreRow): LiveTemplateRow {
  return {
    stream: String(row.stream),
    template_id: String(row.template_id),
    entity: String(row.entity),
    fields_json: String(row.fields_json),
    encodings_json: String(row.encodings_json),
    state: String(row.state),
    created_at_ms: row.created_at_ms,
    last_seen_at_ms: row.last_seen_at_ms,
    inactivity_ttl_ms: row.inactivity_ttl_ms,
    active_from_source_offset: row.active_from_source_offset,
    retired_at_ms: row.retired_at_ms,
    retired_reason: row.retired_reason == null ? null : String(row.retired_reason),
  };
}

export class LiveTemplateRegistry {
  private readonly db: LiveTemplateStore;

  // In-memory last-seen tracking to avoid sqlite writes on every wait call.
  private readonly lastSeenMem = new Map<string, { lastSeenMs: number; lastPersistMs: number }>();
  private readonly dirtyLastSeen = new Set<string>();

  private readonly rate = new Map<string, RateState>();

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

  private allowActivation(stream: string, nowMs: number, limitPerMinute: number): boolean {
    if (limitPerMinute <= 0) return true;
    const ratePerMs = limitPerMinute / 60_000;
    const st = this.rate.get(stream) ?? { tokens: limitPerMinute, lastRefillMs: nowMs };
    const elapsed = Math.max(0, nowMs - st.lastRefillMs);
    st.tokens = Math.min(limitPerMinute, st.tokens + elapsed * ratePerMs);
    st.lastRefillMs = nowMs;
    if (st.tokens < 1) {
      this.rate.set(stream, st);
      return false;
    }
    st.tokens -= 1;
    this.rate.set(stream, st);
    return true;
  }

  getActiveTemplateCount(stream: string): number {
    try {
      return this.db.countActiveLiveTemplates(stream);
    } catch {
      return 0;
    }
  }

  listActiveTemplates(stream: string): Array<{ templateId: string; entity: string; fields: string[]; encodings: TemplateEncoding[]; lastSeenAtMs: number }> {
    try {
      const rows = this.db.listActiveLiveTemplates(stream);
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
  activate(args: {
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
  }): { activated: ActivatedTemplate[]; denied: DeniedTemplate[]; lifecycle: TemplateLifecycleEvent[] } {
    const { stream, templates, inactivityTtlMs, nowMs } = args;
    const { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity, activationRateLimitPerMinute } = args.limits;

    const activated: ActivatedTemplate[] = [];
    const denied: DeniedTemplate[] = [];
    const lifecycle: TemplateLifecycleEvent[] = [];

    const protectedIds = new Set<string>();

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

      const existing = this.db.getLiveTemplate(stream, templateId);

      const alreadyActive = existing && String(existing.state) === "active";
      const needsToken = !alreadyActive;

      if (needsToken && !this.allowActivation(stream, nowMs, activationRateLimitPerMinute)) {
        denied.push({ templateId, reason: "rate_limited" });
        continue;
      }

      if (existing) {
        const row = parseTemplateRow(existing);
        if (row.entity !== entity) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        let storedFields: any;
        let storedEnc: any;
        try {
          storedFields = JSON.parse(row.fields_json);
          storedEnc = JSON.parse(row.encodings_json);
        } catch {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        if (!Array.isArray(storedFields) || !Array.isArray(storedEnc) || storedFields.length !== storedEnc.length) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        const sf = storedFields.map(String);
        const se = storedEnc.map(String);
        if (sf.join("\0") !== fieldNames.join("\0")) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }
        if (se.join("\0") !== encodings.join("\0")) {
          denied.push({ templateId, reason: "invalid" });
          continue;
        }

        if (row.state === "active") {
          this.db.updateLiveTemplateHeartbeat(stream, templateId, nowMs, inactivityTtlMs);
        } else {
          this.db.reactivateLiveTemplate(stream, templateId, nowMs, inactivityTtlMs, args.baseStreamNextOffset);
        }
      } else {
        this.db.insertLiveTemplate({
          stream,
          templateId,
          entity,
          fieldsJson: JSON.stringify(fieldNames),
          encodingsJson: JSON.stringify(encodings),
          nowMs,
          inactivityTtlMs,
          activeFromSourceOffset: args.baseStreamNextOffset,
        });
      }

      protectedIds.add(templateId);
      activated.push({ templateId, state: "active", activeFromTouchOffset: args.activeFromTouchOffset });
      lifecycle.push({
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
      this.markSeen(stream, templateId, nowMs);
    }

    // Enforce caps with LRU eviction.
    const evicted = this.evictToCaps(stream, nowMs, { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity }, protectedIds);
    for (const e of evicted) lifecycle.push(e);

    return { activated, denied, lifecycle };
  }

  heartbeat(stream: string, templateIdsUsed: string[], nowMs: number): void {
    for (const id of templateIdsUsed) {
      const templateId = typeof id === "string" ? id.trim() : "";
      if (!/^[0-9a-f]{16}$/.test(templateId)) continue;
      this.markSeen(stream, templateId, nowMs);
    }
  }

  flushLastSeen(nowMs: number, persistIntervalMs: number): void {
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

    this.db.updateLiveTemplateLastSeenBatch(
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

  gcRetireExpired(stream: string, nowMs: number): { retired: TemplateLifecycleEvent[] } {
    const expired: any[] = [];
    try {
      const rows = this.db.listExpiredLiveTemplates(stream, nowMs, 1000);
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
      this.db.updateLiveTemplateLastSeenBatch(
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

    this.db.retireLiveTemplatesForInactivity(stream, retiredIds, nowMs);
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

  private evictToCaps(
    stream: string,
    nowMs: number,
    caps: { maxActiveTemplatesPerStream: number; maxActiveTemplatesPerEntity: number },
    protectedIds: Set<string>
  ): TemplateLifecycleEvent[] {
    const out: TemplateLifecycleEvent[] = [];
    const { maxActiveTemplatesPerStream, maxActiveTemplatesPerEntity } = caps;

    // Per-entity cap.
    let entities: Array<{ entity: string; cnt: number }> = [];
    try {
      entities = this.db.listActiveLiveTemplateEntityCounts(stream).map((row) => ({ entity: row.entity, cnt: row.count }));
    } catch {
      // ignore
    }

    for (const e of entities) {
      if (e.cnt <= maxActiveTemplatesPerEntity) continue;
      const extra = e.cnt - maxActiveTemplatesPerEntity;
      const evicted = this.evictLru(stream, nowMs, extra, { entity: e.entity, cap: maxActiveTemplatesPerEntity }, protectedIds);
      out.push(...evicted);
    }

    // Per-stream cap.
    let activeCount = 0;
    try {
      activeCount = this.db.countActiveLiveTemplates(stream);
    } catch {
      activeCount = 0;
    }
    if (activeCount > maxActiveTemplatesPerStream) {
      const extra = activeCount - maxActiveTemplatesPerStream;
      const evicted = this.evictLru(stream, nowMs, extra, { cap: maxActiveTemplatesPerStream }, protectedIds);
      out.push(...evicted);
    }

    return out;
  }

  private evictLru(
    stream: string,
    nowMs: number,
    count: number,
    scope: { entity?: string; cap: number },
    protectedIds: Set<string>
  ): TemplateLifecycleEvent[] {
    if (count <= 0) return [];
    const out: TemplateLifecycleEvent[] = [];

    const pick = (excludeProtected: boolean): string[] => {
      try {
        return this.db.listLiveTemplateLruIds({
          stream,
          entity: scope.entity,
          excludeTemplateIds: excludeProtected ? Array.from(protectedIds) : [],
          limit: count,
        });
      } catch {
        return [];
      }
    };

    let ids = pick(true);
    if (ids.length < count) {
      // Evict protected templates only if we have to.
      const extra = pick(false);
      const merged: string[] = [];
      const seen = new Set<string>();
      for (const id of [...ids, ...extra]) {
        if (seen.has(id)) continue;
        seen.add(id);
        merged.push(id);
        if (merged.length >= count) break;
      }
      ids = merged;
    }
    if (ids.length === 0) return [];

    this.db.retireLiveTemplatesForCap(stream, ids, nowMs);
    for (const id of ids) {
      out.push({
        type: "live.template_evicted",
        ts: nowIso(nowMs),
        stream,
        templateId: id,
        reason: "cap_exceeded",
        cap: scope.cap,
      });
      this.lastSeenMem.delete(this.key(stream, id));
      this.dirtyLastSeen.delete(this.key(stream, id));
    }

    return out;
  }
}
