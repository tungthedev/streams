import type { Pool, PoolClient } from "pg";
import type { StreamRow } from "../store/rows";
import type {
  LiveTemplateActivationInput,
  LiveTemplateActivationResult,
  LiveTemplateIdentityRow,
  LiveTemplateLastSeenUpdate,
  LiveTemplateStoreRow,
  StreamTouchStateRow,
  TouchProcessorStore,
} from "../store/touch_store";
import type { WalReadRow } from "../store/wal_store";
import type { PgExecutor } from "./types";

type PostgresTouchStoreDelegates = {
  nowMs(): bigint;
  getStream(stream: string): Promise<StreamRow | null>;
  ensureStream(stream: string, opts?: { contentType?: string | null; streamFlags?: number }): Promise<StreamRow>;
  isDeleted(row: StreamRow): boolean;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
};

type LiveTemplatePgRow = {
  stream: string;
  template_id: string;
  entity: string;
  fields_json: string;
  encodings_json: string;
  state: string;
  created_at_ms: string | number | bigint;
  last_seen_at_ms: string | number | bigint;
  inactivity_ttl_ms: string | number | bigint;
  active_from_source_offset: string | number | bigint;
  retired_at_ms: string | number | bigint | null;
  retired_reason: string | null;
};

function pgInt(value: bigint): string {
  return value.toString();
}

function toBigInt(value: unknown): bigint {
  return typeof value === "bigint" ? value : BigInt(value as any);
}

function clampNumber(value: bigint): number {
  return value > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(value);
}

export class PostgresTouchStore implements TouchProcessorStore {
  constructor(
    private readonly pool: Pool,
    private readonly delegates: PostgresTouchStoreDelegates
  ) {}

  nowMs(): bigint {
    return this.delegates.nowMs();
  }

  getStream(stream: string): Promise<StreamRow | null> {
    return this.delegates.getStream(stream);
  }

  async ensureStream(stream: string, opts?: { contentType?: string; streamFlags?: number }): Promise<StreamRow> {
    const row = await this.delegates.ensureStream(stream, { contentType: opts?.contentType ?? null, streamFlags: opts?.streamFlags });
    if (opts?.streamFlags && opts.streamFlags > 0) await this.addStreamFlags(stream, opts.streamFlags);
    return (await this.getStream(stream)) ?? row;
  }

  async addStreamFlags(stream: string, flags: number): Promise<void> {
    if (!Number.isFinite(flags) || flags <= 0) return;
    await this.pool.query(
      `UPDATE streams
       SET stream_flags = (stream_flags | $1), updated_at_ms = $2
       WHERE stream = $3;`,
      [Math.floor(flags), pgInt(this.nowMs()), stream]
    );
  }

  isDeleted(row: StreamRow): boolean {
    return this.delegates.isDeleted(row);
  }

  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    return this.delegates.readWalRange(stream, startOffset, endOffset, routingKey);
  }

  async activateLiveTemplates(args: {
    stream: string;
    templates: LiveTemplateActivationInput[];
    maxActiveTemplatesPerStream: number;
    maxActiveTemplatesPerEntity: number;
    maxActivationTokens: number;
  }): Promise<LiveTemplateActivationResult> {
    if (args.templates.length === 0) return { activated: [], invalid: [], rateLimited: [], activationTokensUsed: 0, evicted: [] };
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`SELECT stream FROM streams WHERE stream = $1 FOR UPDATE;`, [args.stream]);
      const activated: string[] = [];
      const invalid: string[] = [];
      const rateLimited: string[] = [];
      let activationTokensUsed = 0;

      const templateIds = args.templates.map((template) => template.templateId);
      const entities = args.templates.map((template) => template.entity);
      const fieldsJson = args.templates.map((template) => template.fieldsJson);
      const encodingsJson = args.templates.map((template) => template.encodingsJson);
      const nowMs = args.templates.map((template) => template.nowMs);
      const ttlMs = args.templates.map((template) => template.inactivityTtlMs);
      const activeFrom = args.templates.map((template) => pgInt(template.activeFromSourceOffset));

      await client.query(
        `WITH input AS (
           SELECT *
           FROM unnest($2::text[]) WITH ORDINALITY AS i(template_id, ord)
         )
         SELECT t.template_id
         FROM live_templates AS t
         JOIN input AS i ON i.template_id = t.template_id
         WHERE t.stream = $1
         FOR UPDATE OF t;`,
        [args.stream, templateIds]
      );

      const classified = await client.query<{
        ord: string | number | bigint;
        template_id: string;
        entity: string;
        fields_json: string;
        encodings_json: string;
        now_ms: string | number | bigint;
        inactivity_ttl_ms: string | number | bigint;
        active_from_source_offset: string | number | bigint;
        existing_entity: string | null;
        existing_fields_json: string | null;
        existing_encodings_json: string | null;
        existing_state: string | null;
      }>(
        `WITH input AS (
           SELECT *
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::bigint[])
             WITH ORDINALITY AS i(template_id, entity, fields_json, encodings_json, now_ms, inactivity_ttl_ms, active_from_source_offset, ord)
         )
         SELECT
           i.ord,
           i.template_id,
           i.entity,
           i.fields_json,
           i.encodings_json,
           i.now_ms,
           i.inactivity_ttl_ms,
           i.active_from_source_offset,
           t.entity AS existing_entity,
           t.fields_json AS existing_fields_json,
           t.encodings_json AS existing_encodings_json,
           t.state AS existing_state
         FROM input AS i
         LEFT JOIN live_templates AS t ON t.stream = $1 AND t.template_id = i.template_id
         ORDER BY i.ord ASC;`,
        [args.stream, templateIds, entities, fieldsJson, encodingsJson, nowMs, ttlMs, activeFrom]
      );

      const accepted: typeof classified.rows = [];
      for (const row of classified.rows) {
        const hasExisting = row.existing_state != null;
        if (
          hasExisting &&
          (row.existing_entity !== row.entity || row.existing_fields_json !== row.fields_json || row.existing_encodings_json !== row.encodings_json)
        ) {
          invalid.push(row.template_id);
          continue;
        }
        const alreadyActive = row.existing_state === "active";
        if (!alreadyActive && activationTokensUsed >= args.maxActivationTokens) {
          rateLimited.push(row.template_id);
          continue;
        }
        if (!alreadyActive) activationTokensUsed += 1;
        accepted.push(row);
        activated.push(row.template_id);
      }

      if (accepted.length > 0) {
        await client.query(
          `INSERT INTO live_templates(
             stream, template_id, entity, fields_json, encodings_json,
             state, created_at_ms, last_seen_at_ms, inactivity_ttl_ms, active_from_source_offset,
             retired_at_ms, retired_reason
           )
           SELECT $1, u.template_id, u.entity, u.fields_json, u.encodings_json,
                  'active', u.now_ms, u.now_ms, u.inactivity_ttl_ms, u.active_from_source_offset,
                  NULL, NULL
           FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::bigint[])
             AS u(template_id, entity, fields_json, encodings_json, now_ms, inactivity_ttl_ms, active_from_source_offset)
           ON CONFLICT(stream, template_id) DO UPDATE SET
             state = 'active',
             last_seen_at_ms = excluded.last_seen_at_ms,
             inactivity_ttl_ms = excluded.inactivity_ttl_ms,
             active_from_source_offset = CASE
               WHEN live_templates.state = 'active' THEN live_templates.active_from_source_offset
               ELSE excluded.active_from_source_offset
             END,
             retired_at_ms = NULL,
             retired_reason = NULL;`,
          [
            args.stream,
            accepted.map((row) => row.template_id),
            accepted.map((row) => row.entity),
            accepted.map((row) => row.fields_json),
            accepted.map((row) => row.encodings_json),
            accepted.map((row) => toBigInt(row.now_ms).toString()),
            accepted.map((row) => toBigInt(row.inactivity_ttl_ms).toString()),
            accepted.map((row) => toBigInt(row.active_from_source_offset).toString()),
          ]
        );
      }
      const evicted = await this.evictToCapsInTransaction(client, {
        stream: args.stream,
        protectedIds: new Set(activated),
        maxActiveTemplatesPerStream: args.maxActiveTemplatesPerStream,
        maxActiveTemplatesPerEntity: args.maxActiveTemplatesPerEntity,
        nowMs: args.templates.reduce((max, template) => Math.max(max, template.nowMs), 0),
      });
      await client.query("COMMIT");
      return { activated, invalid, rateLimited, activationTokensUsed, evicted };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async ensureStreamTouchState(stream: string): Promise<void> {
    const srow = await this.getStream(stream);
    const initialThrough = srow ? srow.next_offset - 1n : -1n;
    await ensurePostgresStreamTouchState(this.pool, this.nowMs(), stream, initialThrough);
  }

  async deleteStreamTouchState(stream: string): Promise<void> {
    await deletePostgresStreamTouchState(this.pool, stream);
  }

  async getStreamTouchState(stream: string): Promise<StreamTouchStateRow | null> {
    const res = await this.pool.query<{ stream: string; processed_through: string | number | bigint; updated_at_ms: string | number | bigint }>(
      `SELECT stream, processed_through, updated_at_ms
       FROM stream_touch_state
       WHERE stream = $1
       LIMIT 1;`,
      [stream]
    );
    const row = res.rows[0];
    return row
      ? {
          stream: row.stream,
          processed_through: toBigInt(row.processed_through),
          updated_at_ms: toBigInt(row.updated_at_ms),
        }
      : null;
  }

  async listStreamTouchStates(): Promise<StreamTouchStateRow[]> {
    const res = await this.pool.query<{ stream: string; processed_through: string | number | bigint; updated_at_ms: string | number | bigint }>(
      `SELECT stream, processed_through, updated_at_ms
       FROM stream_touch_state
       ORDER BY stream ASC;`
    );
    return res.rows.map((row) => ({
      stream: row.stream,
      processed_through: toBigInt(row.processed_through),
      updated_at_ms: toBigInt(row.updated_at_ms),
    }));
  }

  async listStreamsByProfile(kind: string): Promise<string[]> {
    const res = await this.pool.query<{ stream: string }>(
      `SELECT stream FROM streams
       WHERE profile = $1
         AND (stream_flags & 1) = 0
       ORDER BY stream ASC;`,
      [kind]
    );
    return res.rows.map((row) => row.stream);
  }

  async updateStreamTouchStateThrough(stream: string, processedThrough: bigint): Promise<void> {
    await this.pool.query(
      `INSERT INTO stream_touch_state(stream, processed_through, updated_at_ms)
       VALUES($1, $2, $3)
       ON CONFLICT(stream) DO UPDATE SET
         processed_through = excluded.processed_through,
         updated_at_ms = excluded.updated_at_ms;`,
      [stream, pgInt(processedThrough), pgInt(this.nowMs())]
    );
  }

  async deleteWalThrough(stream: string, uploadedThrough: bigint): Promise<{ deletedRows: number; deletedBytes: number }> {
    const res = await this.pool.query<{ deleted_rows: string | number | bigint; deleted_bytes: string | number | bigint }>(
      `WITH deleted AS (
         DELETE FROM wal
         WHERE stream = $1 AND "offset" <= $2
         RETURNING payload_len
       ),
       totals AS (
         SELECT COUNT(*)::bigint AS deleted_rows, COALESCE(SUM(payload_len), 0)::bigint AS deleted_bytes FROM deleted
       ),
       updated AS (
         UPDATE streams
         SET wal_rows = GREATEST(0, wal_rows - totals.deleted_rows),
             wal_bytes = GREATEST(0, wal_bytes - totals.deleted_bytes),
             updated_at_ms = $3
         FROM totals
         WHERE stream = $1
         RETURNING totals.deleted_rows, totals.deleted_bytes
       )
       SELECT deleted_rows, deleted_bytes FROM updated
       UNION ALL
       SELECT deleted_rows, deleted_bytes FROM totals
       WHERE NOT EXISTS (SELECT 1 FROM updated)
       LIMIT 1;`,
      [stream, pgInt(uploadedThrough), pgInt(this.nowMs())]
    );
    const row = res.rows[0];
    const deletedRows = row ? toBigInt(row.deleted_rows) : 0n;
    const deletedBytes = row ? toBigInt(row.deleted_bytes) : 0n;
    return { deletedRows: clampNumber(deletedRows), deletedBytes: clampNumber(deletedBytes) };
  }

  async getWalOldestOffset(stream: string): Promise<bigint | null> {
    const res = await this.pool.query<{ min_off: string | number | bigint | null }>(
      `SELECT MIN("offset") AS min_off FROM wal WHERE stream = $1;`,
      [stream]
    );
    const value = res.rows[0]?.min_off;
    return value == null ? null : toBigInt(value);
  }

  async trimWalByAge(stream: string, maxAgeMs: number): Promise<{ trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null }> {
    const ageMs = Math.max(0, Math.floor(maxAgeMs));
    if (!Number.isFinite(ageMs)) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: null };

    const lastRes = await this.pool.query<{ offset: string | number | bigint }>(
      `SELECT "offset" FROM wal WHERE stream = $1 ORDER BY "offset" DESC LIMIT 1;`,
      [stream]
    );
    const lastOffsetRaw = lastRes.rows[0]?.offset;
    if (lastOffsetRaw == null) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: null };
    const lastOffset = toBigInt(lastOffsetRaw);

    let keepFromOffset: bigint;
    if (ageMs === 0) {
      keepFromOffset = lastOffset;
    } else {
      const cutoff = this.nowMs() - BigInt(ageMs);
      const keepRes = await this.pool.query<{ offset: string | number | bigint }>(
        `SELECT "offset" FROM wal
         WHERE stream = $1 AND ts_ms >= $2
         ORDER BY "offset" ASC
         LIMIT 1;`,
        [stream, pgInt(cutoff)]
      );
      keepFromOffset = keepRes.rows[0]?.offset == null ? lastOffset : toBigInt(keepRes.rows[0]!.offset);
    }

    if (keepFromOffset <= 0n) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: keepFromOffset };
    const deleted = await this.deleteWalThrough(stream, keepFromOffset - 1n);
    if (deleted.deletedRows <= 0) return { trimmedRows: 0, trimmedBytes: 0, keptFromOffset: keepFromOffset };
    await this.pool.query(
      `UPDATE streams
       SET pending_rows = GREATEST(0, pending_rows - $1::bigint),
           pending_bytes = GREATEST(0, pending_bytes - $2::bigint),
           updated_at_ms = $3
       WHERE stream = $4;`,
      [deleted.deletedRows, deleted.deletedBytes, pgInt(this.nowMs()), stream]
    );
    return { trimmedRows: deleted.deletedRows, trimmedBytes: deleted.deletedBytes, keptFromOffset: keepFromOffset };
  }

  async countActiveLiveTemplates(stream: string): Promise<number> {
    const res = await this.pool.query<{ cnt: string | number | bigint }>(
      `SELECT COUNT(*) AS cnt FROM live_templates WHERE stream = $1 AND state = 'active';`,
      [stream]
    );
    return Number(res.rows[0]?.cnt ?? 0);
  }

  async listActiveLiveTemplates(stream: string): Promise<LiveTemplateStoreRow[]> {
    const res = await this.pool.query<LiveTemplatePgRow>(
      `SELECT stream, template_id, entity, fields_json, encodings_json, state, created_at_ms, last_seen_at_ms,
              inactivity_ttl_ms, active_from_source_offset, retired_at_ms, retired_reason
       FROM live_templates
       WHERE stream = $1 AND state = 'active'
       ORDER BY entity ASC, template_id ASC;`,
      [stream]
    );
    return res.rows.map(coerceLiveTemplateRow);
  }

  async updateLiveTemplateLastSeenBatch(updates: LiveTemplateLastSeenUpdate[]): Promise<void> {
    if (updates.length === 0) return;
    await this.pool.query(
      `UPDATE live_templates AS t
       SET last_seen_at_ms = GREATEST(t.last_seen_at_ms, v.last_seen_at_ms)
       FROM (
         SELECT *
         FROM unnest($1::text[], $2::text[], $3::bigint[]) AS u(stream, template_id, last_seen_at_ms)
       ) AS v
       WHERE t.stream = v.stream
         AND t.template_id = v.template_id
         AND t.state = 'active';`,
      [updates.map((update) => update.stream), updates.map((update) => update.templateId), updates.map((update) => update.lastSeenAtMs)]
    );
  }

  async listExpiredLiveTemplates(stream: string, nowMs: number, limit: number): Promise<LiveTemplateIdentityRow[]> {
    const res = await this.pool.query<{
      template_id: string;
      entity: string;
      fields_json: string;
      encodings_json: string;
      last_seen_at_ms: string | number | bigint;
      inactivity_ttl_ms: string | number | bigint;
    }>(
      `SELECT template_id, entity, fields_json, encodings_json, last_seen_at_ms, inactivity_ttl_ms
       FROM live_templates
       WHERE stream = $1 AND state = 'active' AND (last_seen_at_ms + inactivity_ttl_ms) < $2
       ORDER BY last_seen_at_ms ASC
       LIMIT $3;`,
      [stream, nowMs, Math.max(1, Math.floor(limit))]
    );
    return res.rows.map((row) => ({
      template_id: row.template_id,
      entity: row.entity,
      fields_json: row.fields_json,
      encodings_json: row.encodings_json,
      last_seen_at_ms: toBigInt(row.last_seen_at_ms),
      inactivity_ttl_ms: toBigInt(row.inactivity_ttl_ms),
    }));
  }

  async retireLiveTemplatesForInactivity(stream: string, templateIds: string[], nowMs: number): Promise<void> {
    if (templateIds.length === 0) return;
    await this.pool.query(
      `UPDATE live_templates
       SET state = 'retired', retired_reason = 'inactivity', retired_at_ms = $1
       WHERE stream = $2 AND state = 'active' AND template_id = ANY($3::text[]);`,
      [nowMs, stream, templateIds]
    );
  }

  async listActiveLiveTemplateEntitiesByIds(stream: string, templateIds: string[]): Promise<string[]> {
    if (templateIds.length === 0) return [];
    const res = await this.pool.query<{ entity: string }>(
      `SELECT DISTINCT entity
       FROM live_templates
       WHERE stream = $1 AND state = 'active' AND template_id = ANY($2::text[]);`,
      [stream, templateIds]
    );
    return res.rows.map((row) => row.entity.trim()).filter((entity) => entity !== "");
  }

  private async evictToCapsInTransaction(
    client: PoolClient,
    args: {
      stream: string;
      protectedIds: Set<string>;
      maxActiveTemplatesPerStream: number;
      maxActiveTemplatesPerEntity: number;
      nowMs: number;
    }
  ): Promise<Array<{ templateId: string; reason: "cap_exceeded"; cap: number }>> {
    const evicted: Array<{ templateId: string; reason: "cap_exceeded"; cap: number }> = [];
    const protectedIds = Array.from(args.protectedIds);
    const entityRes = await client.query<{ template_id: string }>(
      `WITH ranked AS (
         SELECT
           template_id,
           COUNT(*) OVER (PARTITION BY entity) AS total_count,
           ROW_NUMBER() OVER (
             PARTITION BY entity
             ORDER BY
               CASE WHEN template_id = ANY($4::text[]) THEN 1 ELSE 0 END ASC,
               last_seen_at_ms ASC,
               template_id ASC
           ) AS rn
         FROM live_templates
         WHERE stream = $1 AND state = 'active'
       ),
       selected AS (
         SELECT template_id
         FROM ranked
         WHERE rn <= GREATEST(total_count - $2, 0)
       ),
       updated AS (
         UPDATE live_templates AS t
         SET state = 'retired', retired_reason = 'cap_exceeded', retired_at_ms = $3
         FROM selected
         WHERE t.stream = $1
           AND t.state = 'active'
           AND t.template_id = selected.template_id
         RETURNING t.template_id
       )
       SELECT template_id FROM updated
       ORDER BY template_id ASC;`,
      [args.stream, args.maxActiveTemplatesPerEntity, args.nowMs, protectedIds]
    );
    for (const row of entityRes.rows) evicted.push({ templateId: row.template_id, reason: "cap_exceeded", cap: args.maxActiveTemplatesPerEntity });

    const streamRes = await client.query<{ template_id: string }>(
      `WITH ranked AS (
         SELECT
           template_id,
           COUNT(*) OVER () AS total_count,
           ROW_NUMBER() OVER (
             ORDER BY
               CASE WHEN template_id = ANY($4::text[]) THEN 1 ELSE 0 END ASC,
               last_seen_at_ms ASC,
               template_id ASC
           ) AS rn
         FROM live_templates
         WHERE stream = $1 AND state = 'active'
       ),
       selected AS (
         SELECT template_id
         FROM ranked
         WHERE rn <= GREATEST(total_count - $2, 0)
       ),
       updated AS (
         UPDATE live_templates AS t
         SET state = 'retired', retired_reason = 'cap_exceeded', retired_at_ms = $3
         FROM selected
         WHERE t.stream = $1
           AND t.state = 'active'
           AND t.template_id = selected.template_id
         RETURNING t.template_id
       )
       SELECT template_id FROM updated
       ORDER BY template_id ASC;`,
      [args.stream, args.maxActiveTemplatesPerStream, args.nowMs, protectedIds]
    );
    for (const row of streamRes.rows) evicted.push({ templateId: row.template_id, reason: "cap_exceeded", cap: args.maxActiveTemplatesPerStream });
    return evicted;
  }
}

export async function ensurePostgresStreamTouchState(
  executor: PgExecutor,
  nowMs: bigint,
  stream: string,
  initialThrough: bigint
): Promise<void> {
  await executor.query(
    `INSERT INTO stream_touch_state(stream, processed_through, updated_at_ms)
     VALUES($1, $2, $3)
     ON CONFLICT(stream) DO UPDATE SET
       processed_through = stream_touch_state.processed_through,
       updated_at_ms = stream_touch_state.updated_at_ms;`,
    [stream, pgInt(initialThrough), pgInt(nowMs)]
  );
}

export async function ensurePostgresStreamTouchStateFromStream(executor: PgExecutor, nowMs: bigint, stream: string): Promise<void> {
  const res = await executor.query<{ next_offset: string | number | bigint }>(
    `SELECT next_offset FROM streams WHERE stream = $1 LIMIT 1;`,
    [stream]
  );
  const nextOffset = toBigInt(res.rows[0]?.next_offset ?? 0);
  await ensurePostgresStreamTouchState(executor, nowMs, stream, nextOffset - 1n);
}

export async function deletePostgresStreamTouchState(executor: PgExecutor, stream: string): Promise<void> {
  await executor.query(`DELETE FROM stream_touch_state WHERE stream = $1;`, [stream]);
}

function coerceLiveTemplateRow(row: LiveTemplatePgRow): LiveTemplateStoreRow {
  return {
    stream: row.stream,
    template_id: row.template_id,
    entity: row.entity,
    fields_json: row.fields_json,
    encodings_json: row.encodings_json,
    state: row.state,
    created_at_ms: toBigInt(row.created_at_ms),
    last_seen_at_ms: toBigInt(row.last_seen_at_ms),
    inactivity_ttl_ms: toBigInt(row.inactivity_ttl_ms),
    active_from_source_offset: toBigInt(row.active_from_source_offset),
    retired_at_ms: row.retired_at_ms == null ? null : toBigInt(row.retired_at_ms),
    retired_reason: row.retired_reason == null ? null : row.retired_reason,
  };
}
