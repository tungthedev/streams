import type { SqliteDatabase, SqliteStatement } from "../sqlite/adapter";
import type { StreamRow } from "../store/rows";
import type {
  LiveTemplateIdentityRow,
  LiveTemplateLastSeenUpdate,
  LiveTemplateStoreRow,
  StreamTouchStateRow,
  TouchProcessorStore,
} from "../store/touch_store";
import type { WalReadRow } from "../store/wal_store";

type SqliteTouchStoreDelegates = {
  nowMs(): bigint;
  getStream(stream: string): StreamRow | null;
  ensureStream(stream: string, opts?: { contentType?: string; streamFlags?: number }): StreamRow;
  addStreamFlags(stream: string, flags: number): void;
  isDeleted(row: StreamRow): boolean;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  deleteWalThrough(stream: string, uploadedThrough: bigint): { deletedRows: number; deletedBytes: number };
  getWalOldestOffset(stream: string): bigint | null;
  trimWalByAge(stream: string, maxAgeMs: number): { trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null };
};

function toBigInt(v: unknown): bigint {
  return typeof v === "bigint" ? v : BigInt(v as any);
}

function bindInt(v: bigint): number | string {
  const max = BigInt(Number.MAX_SAFE_INTEGER);
  const min = BigInt(Number.MIN_SAFE_INTEGER);
  if (v <= max && v >= min) return Number(v);
  return v.toString();
}

export class SqliteTouchStore implements TouchProcessorStore {
  private readonly stmts: {
    getStreamTouchState: ReturnType<SqliteDatabase["query"]>;
    upsertStreamTouchState: ReturnType<SqliteDatabase["query"]>;
    deleteStreamTouchState: ReturnType<SqliteDatabase["query"]>;
    listStreamTouchStates: ReturnType<SqliteDatabase["query"]>;
    listStreamsByProfile: ReturnType<SqliteDatabase["query"]>;
    countActiveLiveTemplates: SqliteStatement;
    listActiveLiveTemplates: SqliteStatement;
    getLiveTemplate: SqliteStatement;
    updateLiveTemplateHeartbeat: SqliteStatement;
    reactivateLiveTemplate: SqliteStatement;
    insertLiveTemplate: SqliteStatement;
    updateLiveTemplateLastSeen: SqliteStatement;
    listExpiredLiveTemplates: SqliteStatement;
    retireLiveTemplateForInactivity: SqliteStatement;
    listActiveLiveTemplateEntityCounts: SqliteStatement;
    retireLiveTemplateForCap: SqliteStatement;
  };

  constructor(
    private readonly db: SqliteDatabase,
    private readonly delegates: SqliteTouchStoreDelegates
  ) {
    this.stmts = {
      getStreamTouchState: this.db.query(
        `SELECT stream, processed_through, updated_at_ms
         FROM stream_touch_state WHERE stream=? LIMIT 1;`
      ),
      upsertStreamTouchState: this.db.query(
        `INSERT INTO stream_touch_state(stream, processed_through, updated_at_ms)
         VALUES(?, ?, ?)
         ON CONFLICT(stream) DO UPDATE SET
           processed_through=excluded.processed_through,
           updated_at_ms=excluded.updated_at_ms;`
      ),
      deleteStreamTouchState: this.db.query(`DELETE FROM stream_touch_state WHERE stream=?;`),
      listStreamTouchStates: this.db.query(
        `SELECT stream, processed_through, updated_at_ms
         FROM stream_touch_state
         ORDER BY stream ASC;`
      ),
      listStreamsByProfile: this.db.query(`SELECT stream FROM streams WHERE profile=? ORDER BY stream ASC;`),
      countActiveLiveTemplates: this.db.query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`),
      listActiveLiveTemplates: this.db.query(
        `SELECT stream, template_id, entity, fields_json, encodings_json, state, created_at_ms, last_seen_at_ms,
                inactivity_ttl_ms, active_from_source_offset, retired_at_ms, retired_reason
         FROM live_templates
         WHERE stream=? AND state='active'
         ORDER BY entity ASC, template_id ASC;`
      ),
      getLiveTemplate: this.db.query(
        `SELECT stream, template_id, entity, fields_json, encodings_json, state, created_at_ms, last_seen_at_ms,
                inactivity_ttl_ms, active_from_source_offset, retired_at_ms, retired_reason
         FROM live_templates
         WHERE stream=? AND template_id=? LIMIT 1;`
      ),
      updateLiveTemplateHeartbeat: this.db.query(
        `UPDATE live_templates
         SET last_seen_at_ms=?, inactivity_ttl_ms=?
         WHERE stream=? AND template_id=?;`
      ),
      reactivateLiveTemplate: this.db.query(
        `UPDATE live_templates
         SET state='active',
             last_seen_at_ms=?,
             inactivity_ttl_ms=?,
             active_from_source_offset=?,
             retired_at_ms=NULL,
             retired_reason=NULL
         WHERE stream=? AND template_id=?;`
      ),
      insertLiveTemplate: this.db.query(
        `INSERT INTO live_templates(
           stream, template_id, entity, fields_json, encodings_json,
           state, created_at_ms, last_seen_at_ms, inactivity_ttl_ms, active_from_source_offset,
           retired_at_ms, retired_reason
         ) VALUES(?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, NULL, NULL);`
      ),
      updateLiveTemplateLastSeen: this.db.query(
        `UPDATE live_templates
         SET last_seen_at_ms=?
         WHERE stream=? AND template_id=? AND state='active';`
      ),
      listExpiredLiveTemplates: this.db.query(
        `SELECT template_id, entity, fields_json, encodings_json, last_seen_at_ms, inactivity_ttl_ms
         FROM live_templates
         WHERE stream=? AND state='active' AND (last_seen_at_ms + inactivity_ttl_ms) < ?
         ORDER BY last_seen_at_ms ASC
         LIMIT ?;`
      ),
      retireLiveTemplateForInactivity: this.db.query(
        `UPDATE live_templates
         SET state='retired', retired_reason='inactivity', retired_at_ms=?
         WHERE stream=? AND template_id=? AND state='active';`
      ),
      listActiveLiveTemplateEntityCounts: this.db.query(
        `SELECT entity, COUNT(*) as cnt
         FROM live_templates
         WHERE stream=? AND state='active'
         GROUP BY entity;`
      ),
      retireLiveTemplateForCap: this.db.query(
        `UPDATE live_templates
         SET state='retired', retired_reason='cap_exceeded', retired_at_ms=?
         WHERE stream=? AND template_id=? AND state='active';`
      ),
    };
  }

  nowMs(): bigint {
    return this.delegates.nowMs();
  }

  getStream(stream: string): StreamRow | null {
    return this.delegates.getStream(stream);
  }

  ensureStream(stream: string, opts?: { contentType?: string; streamFlags?: number }): StreamRow {
    return this.delegates.ensureStream(stream, opts);
  }

  addStreamFlags(stream: string, flags: number): void {
    this.delegates.addStreamFlags(stream, flags);
  }

  isDeleted(row: StreamRow): boolean {
    return this.delegates.isDeleted(row);
  }

  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    return this.delegates.readWalRange(stream, startOffset, endOffset, routingKey);
  }

  deleteWalThrough(stream: string, uploadedThrough: bigint): { deletedRows: number; deletedBytes: number } {
    return this.delegates.deleteWalThrough(stream, uploadedThrough);
  }

  getWalOldestOffset(stream: string): bigint | null {
    return this.delegates.getWalOldestOffset(stream);
  }

  trimWalByAge(stream: string, maxAgeMs: number): { trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null } {
    return this.delegates.trimWalByAge(stream, maxAgeMs);
  }

  async updateProfileTouchState(stream: string, plan: "ensure" | "delete"): Promise<void> {
    if (plan === "ensure") this.ensureStreamTouchState(stream);
    else this.deleteStreamTouchState(stream);
  }

  getStreamTouchState(stream: string): StreamTouchStateRow | null {
    const row = this.stmts.getStreamTouchState.get(stream) as any;
    if (!row) return null;
    return {
      stream: String(row.stream),
      processed_through: toBigInt(row.processed_through),
      updated_at_ms: toBigInt(row.updated_at_ms),
    };
  }

  listStreamTouchStates(): StreamTouchStateRow[] {
    const rows = this.stmts.listStreamTouchStates.all() as any[];
    return rows.map((row) => ({
      stream: String(row.stream),
      processed_through: toBigInt(row.processed_through),
      updated_at_ms: toBigInt(row.updated_at_ms),
    }));
  }

  listStreamsByProfile(kind: string): string[] {
    const rows = this.stmts.listStreamsByProfile.all(kind) as any[];
    return rows.map((row) => String(row.stream));
  }

  ensureStreamTouchState(stream: string): void {
    const existing = this.getStreamTouchState(stream);
    if (existing) return;
    const srow = this.getStream(stream);
    const initialThrough = srow ? srow.next_offset - 1n : -1n;
    this.stmts.upsertStreamTouchState.run(stream, bindInt(initialThrough), this.nowMs());
  }

  updateStreamTouchStateThrough(stream: string, processedThrough: bigint): void {
    this.stmts.upsertStreamTouchState.run(stream, bindInt(processedThrough), this.nowMs());
  }

  deleteStreamTouchState(stream: string): void {
    this.stmts.deleteStreamTouchState.run(stream);
  }

  countActiveLiveTemplates(stream: string): number {
    const row = this.stmts.countActiveLiveTemplates.get(stream) as any;
    return Number(row?.cnt ?? 0);
  }

  listActiveLiveTemplates(stream: string): LiveTemplateStoreRow[] {
    const rows = this.stmts.listActiveLiveTemplates.all(stream) as any[];
    return rows.map((row) => this.coerceLiveTemplateRow(row));
  }

  getLiveTemplate(stream: string, templateId: string): LiveTemplateStoreRow | null {
    const row = this.stmts.getLiveTemplate.get(stream, templateId) as any;
    return row ? this.coerceLiveTemplateRow(row) : null;
  }

  updateLiveTemplateHeartbeat(stream: string, templateId: string, nowMs: number, inactivityTtlMs: number): void {
    this.stmts.updateLiveTemplateHeartbeat.run(nowMs, inactivityTtlMs, stream, templateId);
  }

  reactivateLiveTemplate(stream: string, templateId: string, nowMs: number, inactivityTtlMs: number, activeFromSourceOffset: bigint): void {
    this.stmts.reactivateLiveTemplate.run(nowMs, inactivityTtlMs, activeFromSourceOffset, stream, templateId);
  }

  insertLiveTemplate(args: {
    stream: string;
    templateId: string;
    entity: string;
    fieldsJson: string;
    encodingsJson: string;
    nowMs: number;
    inactivityTtlMs: number;
    activeFromSourceOffset: bigint;
  }): void {
    this.stmts.insertLiveTemplate.run(
      args.stream,
      args.templateId,
      args.entity,
      args.fieldsJson,
      args.encodingsJson,
      args.nowMs,
      args.nowMs,
      args.inactivityTtlMs,
      args.activeFromSourceOffset
    );
  }

  updateLiveTemplateLastSeen(stream: string, templateId: string, lastSeenAtMs: number): void {
    this.stmts.updateLiveTemplateLastSeen.run(lastSeenAtMs, stream, templateId);
  }

  updateLiveTemplateLastSeenBatch(updates: LiveTemplateLastSeenUpdate[]): void {
    for (const update of updates) {
      this.stmts.updateLiveTemplateLastSeen.run(update.lastSeenAtMs, update.stream, update.templateId);
    }
  }

  listExpiredLiveTemplates(stream: string, nowMs: number, limit: number): LiveTemplateIdentityRow[] {
    const rows = this.stmts.listExpiredLiveTemplates.all(stream, nowMs, Math.max(1, Math.floor(limit))) as any[];
    return rows.map((row) => ({
      template_id: String(row.template_id),
      entity: String(row.entity),
      fields_json: String(row.fields_json),
      encodings_json: String(row.encodings_json),
      last_seen_at_ms: toBigInt(row.last_seen_at_ms),
      inactivity_ttl_ms: toBigInt(row.inactivity_ttl_ms),
    }));
  }

  retireLiveTemplateForInactivity(stream: string, templateId: string, nowMs: number): void {
    this.stmts.retireLiveTemplateForInactivity.run(nowMs, stream, templateId);
  }

  retireLiveTemplatesForInactivity(stream: string, templateIds: string[], nowMs: number): void {
    for (const templateId of templateIds) {
      this.stmts.retireLiveTemplateForInactivity.run(nowMs, stream, templateId);
    }
  }

  listActiveLiveTemplateEntityCounts(stream: string): Array<{ entity: string; count: number }> {
    const rows = this.stmts.listActiveLiveTemplateEntityCounts.all(stream) as any[];
    return rows.map((row) => ({ entity: String(row.entity), count: Number(row.cnt) }));
  }

  listLiveTemplateLruIds(args: { stream: string; entity?: string; excludeTemplateIds?: string[]; limit: number }): string[] {
    const params: any[] = [args.stream];
    let where = `stream=? AND state='active'`;
    if (args.entity) {
      where += ` AND entity=?`;
      params.push(args.entity);
    }
    const excludeTemplateIds = args.excludeTemplateIds ?? [];
    if (excludeTemplateIds.length > 0) {
      const placeholders = excludeTemplateIds.map(() => "?").join(", ");
      where += ` AND template_id NOT IN (${placeholders})`;
      params.push(...excludeTemplateIds);
    }
    const q = `SELECT template_id FROM live_templates WHERE ${where} ORDER BY last_seen_at_ms ASC, template_id ASC LIMIT ?;`;
    params.push(Math.max(1, Math.floor(args.limit)));
    const rows = this.db.query(q).all(...params) as any[];
    return rows.map((row) => String(row.template_id));
  }

  retireLiveTemplateForCap(stream: string, templateId: string, nowMs: number): void {
    this.stmts.retireLiveTemplateForCap.run(nowMs, stream, templateId);
  }

  retireLiveTemplatesForCap(stream: string, templateIds: string[], nowMs: number): void {
    for (const templateId of templateIds) {
      this.stmts.retireLiveTemplateForCap.run(nowMs, stream, templateId);
    }
  }

  listActiveLiveTemplateEntitiesByIds(stream: string, templateIds: string[]): string[] {
    if (templateIds.length === 0) return [];
    const entities = new Set<string>();
    const chunkSize = 200;
    for (let i = 0; i < templateIds.length; i += chunkSize) {
      const chunk = templateIds.slice(i, i + chunkSize);
      const placeholders = chunk.map(() => "?").join(",");
      const rows = this.db
        .query(
          `SELECT DISTINCT entity
           FROM live_templates
           WHERE stream=? AND state='active' AND template_id IN (${placeholders});`
        )
        .all(stream, ...chunk) as any[];
      for (const row of rows) {
        const entity = String(row?.entity ?? "").trim();
        if (entity !== "") entities.add(entity);
      }
    }
    return Array.from(entities);
  }

  private coerceLiveTemplateRow(row: any): LiveTemplateStoreRow {
    return {
      stream: String(row.stream),
      template_id: String(row.template_id),
      entity: String(row.entity),
      fields_json: String(row.fields_json),
      encodings_json: String(row.encodings_json),
      state: String(row.state),
      created_at_ms: toBigInt(row.created_at_ms),
      last_seen_at_ms: toBigInt(row.last_seen_at_ms),
      inactivity_ttl_ms: toBigInt(row.inactivity_ttl_ms),
      active_from_source_offset: toBigInt(row.active_from_source_offset),
      retired_at_ms: row.retired_at_ms == null ? null : toBigInt(row.retired_at_ms),
      retired_reason: row.retired_reason == null ? null : String(row.retired_reason),
    };
  }
}
