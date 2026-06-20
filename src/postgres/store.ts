import { Pool, type PoolClient } from "pg";
import { Result } from "better-result";
import type {
  EnsureStreamOptions,
  DurableStoreCapabilities,
  WalControlPlaneStore,
} from "../store/capabilities";
import type { StreamReadRow } from "../store/segment_read_store";
import type {
  SchemaMetadataCommit,
  SchemaMetadataMutationContext,
  SchemaMetadataMutationPlan,
  SchemaRegistryRow,
  ProfileMetadataCommit,
  ProfileMetadataMutationContext,
  ProfileMetadataMutationPlan,
  StoredProfileRow,
} from "../store/schema_profile_store";
import type {
  StoreAppendBatch,
  StoreAppendError,
  StoreAppendResult,
  StoreAppendTask,
} from "../store/append";
import type { WalReadRow } from "../store/wal_store";
import { dsError } from "../util/ds_error";
import { migratePostgresStore } from "./schema";
import { PostgresSegmentManifestStore } from "./segments";
import type { PgExecutor, PgStreamRow } from "./types";

const STREAM_FLAG_DELETED = 1 << 0;
const STREAM_FLAG_TOUCH = 1 << 1;
const WAL_READ_CHUNK_ROWS = 1024;

type StreamState = {
  nextOffset: bigint;
  lastAppendMs: bigint;
  expiresAtMs: bigint | null;
  contentType: string;
  streamSeq: string | null;
  closed: boolean;
  closedProducerId: string | null;
  closedProducerEpoch: number | null;
  closedProducerSeq: number | null;
};

type ProducerState = { epoch: number; lastSeq: number };
type ProducerCheck = { duplicate: boolean; update: boolean; epoch: number; seq: number };
type PostgresStoreOptions = { fullMode?: boolean };

export class PostgresDurableStore implements WalControlPlaneStore {
  readonly kind = "postgres" as const;
  readonly capabilities: DurableStoreCapabilities;
  private readonly segments?: PostgresSegmentManifestStore;

  constructor(private readonly pool: Pool, private readonly opts: PostgresStoreOptions = {}) {
    this.capabilities = {
      wal: true,
      schemas: true,
      profiles: true,
      streamLifecycle: true,
      segmentReads: opts.fullMode === true,
      indexes: false,
      manifests: opts.fullMode === true,
      objectStoreAccounting: false,
      storageStats: false,
      schemaPublication: opts.fullMode === true,
      builtinProfiles: false,
      internalMetrics: false,
      touch: false,
    };
    if (opts.fullMode) {
      this.segments = new PostgresSegmentManifestStore(this.pool, () => this.nowMs(), (stream, startOffset, endOffset, routingKey) =>
        this.readWalRange(stream, startOffset, endOffset, routingKey)
      );
    }
  }

  static async connect(connectionString: string): Promise<PostgresDurableStore> {
    const store = new PostgresDurableStore(new Pool({ connectionString }));
    await store.migrate();
    return store;
  }

  static async connectFull(connectionString: string): Promise<PostgresDurableStore> {
    const store = new PostgresDurableStore(new Pool({ connectionString }), { fullMode: true });
    await store.migrate();
    return store;
  }

  async migrate(): Promise<void> {
    await migratePostgresStore(this.pool, { fullMode: this.opts.fullMode });
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  nowMs(): bigint {
    return BigInt(Date.now());
  }

  async nowMsForRead(): Promise<bigint> {
    return this.nowMs();
  }

  isDeleted(row: StreamReadRow): boolean {
    return (row.stream_flags & STREAM_FLAG_DELETED) !== 0;
  }

  async getStream(stream: string): Promise<StreamReadRow | null> {
    return this.getStreamWithExecutor(this.pool, stream, false);
  }

  async getStreamForRead(stream: string): Promise<StreamReadRow | null> {
    return this.getStream(stream);
  }

  async ensureStream(stream: string, opts?: EnsureStreamOptions | null): Promise<StreamReadRow> {
    if (opts?.profile != null && opts.profile !== "generic") {
      throw dsError("postgres storage supports generic profiles only", { code: "unsupported_capability" });
    }
    const now = this.nowMs();
    const sql = this.opts.fullMode
      ? `INSERT INTO streams(
         stream, created_at_ms, updated_at_ms,
         content_type, profile, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
         epoch, next_offset, sealed_through, uploaded_through, uploaded_segment_count,
         pending_rows, pending_bytes, logical_size_bytes, wal_rows, wal_bytes, last_append_ms, last_segment_cut_ms, segment_in_progress,
         expires_at_ms, stream_flags
       )
       VALUES($1, $2, $2, $3, $4, $5, $6, NULL, NULL, NULL, $7, 0, 0, -1, -1, 0, 0, 0, 0, 0, 0, $2, $2, 0, $8, 0)
       ON CONFLICT (stream) DO NOTHING
       RETURNING *;`
      : `INSERT INTO streams(
         stream, created_at_ms, updated_at_ms,
         content_type, profile, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq, ttl_seconds,
         epoch, next_offset, logical_size_bytes, wal_rows, wal_bytes, last_append_ms,
         expires_at_ms, stream_flags
       )
       VALUES($1, $2, $2, $3, $4, $5, $6, NULL, NULL, NULL, $7, 0, 0, 0, 0, 0, $2, $8, 0)
       ON CONFLICT (stream) DO NOTHING
       RETURNING *;`;
    const row = await this.pool.query<PgStreamRow>(
      sql,
      [
        stream,
        pgInt(now),
        opts?.contentType ?? "application/octet-stream",
        opts?.profile ?? "generic",
        opts?.streamSeq ?? null,
        opts?.closed ? 1 : 0,
        opts?.ttlSeconds ?? null,
        opts?.expiresAtMs == null ? null : pgInt(opts.expiresAtMs),
      ]
    );
    if (row.rows[0]) return coerceStreamRow(row.rows[0]);
    const existing = await this.getStream(stream);
    if (!existing) throw dsError("failed to ensure stream");
    return existing;
  }

  async listStreams(limit: number, offset: number): Promise<StreamReadRow[]> {
    const now = this.nowMs();
    const res = await this.pool.query<PgStreamRow>(
      `SELECT * FROM streams
       WHERE (stream_flags & $1) = 0
         AND (expires_at_ms IS NULL OR expires_at_ms > $2)
       ORDER BY created_at_ms ASC, stream ASC
       LIMIT $3 OFFSET $4;`,
      [STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, pgInt(now), limit, offset]
    );
    return res.rows.map(coerceStreamRow);
  }

  async listExpiredStreams(limit: number): Promise<string[]> {
    const now = this.nowMs();
    const res = await this.pool.query<{ stream: string }>(
      `SELECT stream FROM streams
       WHERE (stream_flags & $1) = 0
         AND expires_at_ms IS NOT NULL
         AND expires_at_ms <= $2
       ORDER BY expires_at_ms ASC
       LIMIT $3;`,
      [STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH, pgInt(now), limit]
    );
    return res.rows.map((row) => row.stream);
  }

  async deleteStream(stream: string): Promise<boolean> {
    const now = this.nowMs();
    const res = await this.pool.query(
      `UPDATE streams
       SET stream_flags = (stream_flags | $1), updated_at_ms = $2
       WHERE stream = $3 AND (stream_flags & $1) = 0;`,
      [STREAM_FLAG_DELETED, pgInt(now), stream]
    );
    return (res.rowCount ?? 0) > 0;
  }

  async hardDeleteStream(stream: string): Promise<boolean> {
    const res = await this.pool.query(`DELETE FROM streams WHERE stream = $1;`, [stream]);
    return (res.rowCount ?? 0) > 0;
  }

  async appendBatch(batch: StoreAppendTask[]): Promise<StoreAppendBatch> {
    if (batch.length === 0) return Result.ok({ results: [], walBytesCommitted: 0 });
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await this.appendBatchInTransaction(client, batch);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      if (isRetryablePostgresError(error)) return Result.err({ kind: "retryable" });
      throw error;
    } finally {
      client.release();
    }
  }

  async *readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    for await (const row of this.readWalRows(stream, startOffset, endOffset, "ASC", routingKey)) yield row;
  }

  async *readWalRangeDesc(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    for await (const row of this.readWalRows(stream, startOffset, endOffset, "DESC", routingKey)) yield row;
  }

  async getWalOldestTimestampMsForRead(stream: string): Promise<bigint | null> {
    const res = await this.pool.query<{ min_ts: string | number | bigint | null }>(
      `SELECT MIN(ts_ms) AS min_ts FROM wal WHERE stream = $1;`,
      [stream]
    );
    const value = res.rows[0]?.min_ts;
    return value == null ? null : toBigInt(value);
  }

  fullModeSegments(): PostgresSegmentManifestStore {
    return this.requireSegments();
  }

  async getSchemaRegistryForRead(stream: string): Promise<SchemaRegistryRow | null> {
    return this.getSchemaRegistryWithExecutor(this.pool, stream);
  }

  async commitSchemaMetadataMutation<T, E>(
    stream: string,
    mutation: (ctx: SchemaMetadataMutationContext) => Result<SchemaMetadataMutationPlan<T>, E>
  ): Promise<Result<SchemaMetadataCommit<T>, E>> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const streamRow = await this.getStreamWithExecutor(client, stream, true);
      const registryRow = await this.getSchemaRegistryWithExecutor(client, stream);
      const mutationRes = mutation({ streamRow, registryRow });
      if (Result.isError(mutationRes)) {
        await client.query("ROLLBACK");
        return mutationRes;
      }
      if (mutationRes.value.registry.search) {
        await client.query("ROLLBACK");
        return Result.err(unsupportedSchemaSearchError() as E);
      }
      const updatedAtMs = this.nowMs();
      await client.query(
        `INSERT INTO schemas(stream, schema_json, updated_at_ms)
         VALUES($1, $2, $3)
         ON CONFLICT(stream) DO UPDATE SET
           schema_json = excluded.schema_json,
           updated_at_ms = excluded.updated_at_ms;`,
        [stream, mutationRes.value.registryJson, pgInt(updatedAtMs)]
      );
      await client.query("COMMIT");
      return Result.ok({
        registry: mutationRes.value.registry,
        updatedAtMs,
        value: mutationRes.value.value,
      });
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async getStreamProfileForRead(stream: string): Promise<StoredProfileRow | null> {
    return this.getProfileWithExecutor(this.pool, stream);
  }

  async commitProfileMetadataMutation<T, E>(
    stream: string,
    mutation: (ctx: ProfileMetadataMutationContext) => Result<ProfileMetadataMutationPlan<T>, E>
  ): Promise<Result<ProfileMetadataCommit<T>, E>> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const streamRow = await this.getStreamWithExecutor(client, stream, true);
      const profileRow = await this.getProfileWithExecutor(client, stream);
      const mutationRes = mutation({ streamRow, profileRow });
      if (Result.isError(mutationRes)) {
        await client.query("ROLLBACK");
        return mutationRes;
      }
      const metadata = mutationRes.value.metadata;
      if ((metadata.streamProfile != null && metadata.streamProfile !== "generic") || metadata.profileJson != null) {
        await client.query("ROLLBACK");
        return Result.err(unsupportedProfileError() as E);
      }
      const updatedAtMs = this.nowMs();
      await client.query(
        `UPDATE streams SET profile = $1, updated_at_ms = $2 WHERE stream = $3;`,
        [metadata.streamProfile, pgInt(updatedAtMs), stream]
      );
      await client.query(`DELETE FROM stream_profiles WHERE stream = $1;`, [stream]);
      if (metadata.schemaRegistry) {
        await client.query(
          `INSERT INTO schemas(stream, schema_json, updated_at_ms)
           VALUES($1, $2, $3)
           ON CONFLICT(stream) DO UPDATE SET
             schema_json = excluded.schema_json,
             updated_at_ms = excluded.updated_at_ms;`,
          [stream, JSON.stringify(metadata.schemaRegistry), pgInt(updatedAtMs)]
        );
      }
      await client.query("COMMIT");
      return Result.ok({
        schemaRegistry: metadata.schemaRegistry,
        profileUpdatedAtMs: updatedAtMs,
        value: mutationRes.value.value,
      });
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  private async appendBatchInTransaction(client: PoolClient, batch: StoreAppendTask[]): Promise<StoreAppendBatch> {
    const nowMs = this.nowMs();
    let walBytesCommitted = 0;
    const results: StoreAppendResult[] = new Array(batch.length);
    const perStream = new Map<string, StreamState>();
    const perProducer = new Map<string, ProducerState | null>();

    const loadStream = async (stream: string): Promise<StreamState | null> => {
      const cached = perStream.get(stream);
      if (cached) return cached;
      const res = await client.query<PgStreamRow>(
        `SELECT stream, epoch, next_offset, last_append_ms, expires_at_ms, stream_flags,
                content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq
         FROM streams
         WHERE stream = $1
         FOR UPDATE;`,
        [stream]
      );
      const row = res.rows[0];
      if (!row || (Number(row.stream_flags) & STREAM_FLAG_DELETED) !== 0) return null;
      const st: StreamState = {
        nextOffset: toBigInt(row.next_offset),
        lastAppendMs: toBigInt(row.last_append_ms),
        expiresAtMs: row.expires_at_ms == null ? null : toBigInt(row.expires_at_ms),
        contentType: String(row.content_type),
        streamSeq: row.stream_seq == null ? null : String(row.stream_seq),
        closed: Number(row.closed) !== 0,
        closedProducerId: row.closed_producer_id == null ? null : String(row.closed_producer_id),
        closedProducerEpoch: row.closed_producer_epoch == null ? null : Number(row.closed_producer_epoch),
        closedProducerSeq: row.closed_producer_seq == null ? null : Number(row.closed_producer_seq),
      };
      perStream.set(stream, st);
      return st;
    };

    const loadProducerState = async (stream: string, producerId: string): Promise<ProducerState | null> => {
      const key = `${stream}\u0000${producerId}`;
      if (perProducer.has(key)) return perProducer.get(key)!;
      const res = await client.query<{ epoch: number; last_seq: number }>(
        `SELECT epoch, last_seq FROM producer_state WHERE stream = $1 AND producer_id = $2 FOR UPDATE;`,
        [stream, producerId]
      );
      const row = res.rows[0];
      const state = row ? { epoch: Number(row.epoch), lastSeq: Number(row.last_seq) } : null;
      perProducer.set(key, state);
      return state;
    };

    const checkProducer = async (task: StoreAppendTask): Promise<Result<ProducerCheck, StoreAppendError>> => {
      const producer = task.producer!;
      const state = await loadProducerState(task.stream, producer.id);
      if (!state) {
        if (producer.seq !== 0) return Result.err({ kind: "producer_epoch_seq" });
        return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
      }
      if (producer.epoch < state.epoch) return Result.err({ kind: "producer_stale_epoch", producerEpoch: state.epoch });
      if (producer.epoch > state.epoch) {
        if (producer.seq !== 0) return Result.err({ kind: "producer_epoch_seq" });
        return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
      }
      if (producer.seq <= state.lastSeq) return Result.ok({ duplicate: true, update: false, epoch: state.epoch, seq: state.lastSeq });
      if (producer.seq === state.lastSeq + 1) {
        return Result.ok({ duplicate: false, update: true, epoch: state.epoch, seq: producer.seq });
      }
      return Result.err({ kind: "producer_gap", expected: state.lastSeq + 1, received: producer.seq });
    };

    const persistProducerUpdate = async (task: StoreAppendTask, check: ProducerCheck): Promise<void> => {
      if (!task.producer || !check.update) return;
      await upsertProducerState(client, task.stream, task.producer.id, check.epoch, check.seq, nowMs);
      perProducer.set(`${task.stream}\u0000${task.producer.id}`, { epoch: check.epoch, lastSeq: check.seq });
    };

    const checkStreamSeq = (task: StoreAppendTask, st: StreamState): Result<{ nextSeq: string | null }, StoreAppendError> => {
      if (task.streamSeq == null) return Result.ok({ nextSeq: st.streamSeq });
      if (st.streamSeq != null && task.streamSeq <= st.streamSeq) {
        return Result.err({ kind: "stream_seq", expected: st.streamSeq, received: task.streamSeq });
      }
      return Result.ok({ nextSeq: task.streamSeq });
    };

    for (let idx = 0; idx < batch.length; idx++) {
      const task = batch[idx]!;
      const st = await loadStream(task.stream);
      if (!st) {
        results[idx] = Result.err({ kind: "not_found" });
        continue;
      }
      if (st.expiresAtMs != null && nowMs > st.expiresAtMs) {
        results[idx] = Result.err({ kind: "gone" });
        continue;
      }
      const tailOffset = st.nextOffset - 1n;
      const isCloseOnly = task.close && task.rows.length === 0;
      if (st.closed) {
        if (isCloseOnly) {
          results[idx] = Result.ok({ lastOffset: tailOffset, appendedRows: 0, closed: true, duplicate: true });
          continue;
        }
        if (
          task.producer &&
          task.close &&
          st.closedProducerId === task.producer.id &&
          st.closedProducerEpoch === task.producer.epoch &&
          st.closedProducerSeq === task.producer.seq
        ) {
          results[idx] = Result.ok({
            lastOffset: tailOffset,
            appendedRows: 0,
            closed: true,
            duplicate: true,
            producer: { epoch: task.producer.epoch, seq: task.producer.seq },
          });
          continue;
        }
        results[idx] = Result.err({ kind: "closed", lastOffset: tailOffset });
        continue;
      }
      if (isCloseOnly) {
        results[idx] = await this.applyCloseOnly(client, task, st, checkProducer, persistProducerUpdate, checkStreamSeq, nowMs, tailOffset);
        continue;
      }
      if (!task.contentType || task.contentType !== st.contentType) {
        results[idx] = Result.err({ kind: "content_type_mismatch" });
        continue;
      }
      const appendRes = await this.applyAppend(client, task, st, checkProducer, persistProducerUpdate, checkStreamSeq, nowMs, tailOffset);
      results[idx] = appendRes.result;
      walBytesCommitted += appendRes.walBytesCommitted;
    }

    return Result.ok({ results, walBytesCommitted });
  }

  private async applyCloseOnly(
    client: PoolClient,
    task: StoreAppendTask,
    st: StreamState,
    checkProducer: (task: StoreAppendTask) => Promise<Result<ProducerCheck, StoreAppendError>>,
    persistProducerUpdate: (task: StoreAppendTask, check: ProducerCheck) => Promise<void>,
    checkStreamSeq: (task: StoreAppendTask, st: StreamState) => Result<{ nextSeq: string | null }, StoreAppendError>,
    nowMs: bigint,
    tailOffset: bigint
  ): Promise<StoreAppendResult> {
    let producerInfo: { epoch: number; seq: number } | undefined;
    let duplicate = false;
    if (task.producer) {
      const prodCheck = await checkProducer(task);
      if (Result.isError(prodCheck)) return Result.err(prodCheck.error);
      duplicate = prodCheck.value.duplicate;
      producerInfo = { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq };
    }
    if (!duplicate) {
      const seqCheck = checkStreamSeq(task, st);
      if (Result.isError(seqCheck)) return Result.err(seqCheck.error);
      if (task.producer) await persistProducerUpdate(task, { duplicate, update: true, epoch: producerInfo!.epoch, seq: producerInfo!.seq });
      st.streamSeq = seqCheck.value.nextSeq;
      const closedProducer = task.producer ?? null;
      await client.query(
        `UPDATE streams
         SET closed = 1,
             closed_producer_id = $1,
             closed_producer_epoch = $2,
             closed_producer_seq = $3,
             updated_at_ms = $4,
             stream_seq = $5
         WHERE stream = $6 AND (stream_flags & $7) = 0;`,
        [
          closedProducer ? closedProducer.id : null,
          closedProducer ? closedProducer.epoch : null,
          closedProducer ? closedProducer.seq : null,
          pgInt(nowMs),
          st.streamSeq,
          task.stream,
          STREAM_FLAG_DELETED,
        ]
      );
      st.closed = true;
      st.closedProducerId = closedProducer ? closedProducer.id : null;
      st.closedProducerEpoch = closedProducer ? closedProducer.epoch : null;
      st.closedProducerSeq = closedProducer ? closedProducer.seq : null;
    }
    return Result.ok({ lastOffset: tailOffset, appendedRows: 0, closed: st.closed, duplicate, producer: producerInfo });
  }

  private async applyAppend(
    client: PoolClient,
    task: StoreAppendTask,
    st: StreamState,
    checkProducer: (task: StoreAppendTask) => Promise<Result<ProducerCheck, StoreAppendError>>,
    persistProducerUpdate: (task: StoreAppendTask, check: ProducerCheck) => Promise<void>,
    checkStreamSeq: (task: StoreAppendTask, st: StreamState) => Result<{ nextSeq: string | null }, StoreAppendError>,
    nowMs: bigint,
    tailOffset: bigint
  ): Promise<{ result: StoreAppendResult; walBytesCommitted: number }> {
    let producerInfo: { epoch: number; seq: number } | undefined;
    let producerCheck: ProducerCheck | null = null;
    if (task.producer) {
      const prodCheck = await checkProducer(task);
      if (Result.isError(prodCheck)) return { result: Result.err(prodCheck.error), walBytesCommitted: 0 };
      producerCheck = prodCheck.value;
      if (prodCheck.value.duplicate) {
        return {
          result: Result.ok({
            lastOffset: tailOffset,
            appendedRows: 0,
            closed: false,
            duplicate: true,
            producer: { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq },
          }),
          walBytesCommitted: 0,
        };
      }
      producerInfo = { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq };
    }

    const seqCheck = checkStreamSeq(task, st);
    if (Result.isError(seqCheck)) return { result: Result.err(seqCheck.error), walBytesCommitted: 0 };
    if (producerCheck) await persistProducerUpdate(task, producerCheck);
    st.streamSeq = seqCheck.value.nextSeq;

    let appendMs = task.baseAppendMs;
    if (appendMs <= st.lastAppendMs) appendMs = st.lastAppendMs + 1n;
    let offset = st.nextOffset;
    let totalBytes = 0n;
    for (const row of task.rows) {
      const payload = Buffer.from(row.payload.buffer, row.payload.byteOffset, row.payload.byteLength);
      const payloadLen = row.payload.byteLength;
      totalBytes += BigInt(payloadLen);
      await client.query(
        `INSERT INTO wal(stream, "offset", ts_ms, payload, payload_len, routing_key, content_type, flags)
         VALUES($1, $2, $3, $4, $5, $6, $7, 0);`,
        [
          task.stream,
          pgInt(offset),
          pgInt(appendMs),
          payload,
          payloadLen,
          row.routingKey == null ? null : Buffer.from(row.routingKey.buffer, row.routingKey.byteOffset, row.routingKey.byteLength),
          row.contentType,
        ]
      );
      offset += 1n;
    }

    const lastOffset = offset - 1n;
    st.nextOffset = offset;
    st.lastAppendMs = appendMs;
    if (task.close) {
      st.closed = true;
      st.closedProducerId = task.producer?.id ?? null;
      st.closedProducerEpoch = task.producer?.epoch ?? null;
      st.closedProducerSeq = task.producer?.seq ?? null;
    }

    if (this.opts.fullMode) {
      await client.query(
        `UPDATE streams
         SET next_offset = $1,
             updated_at_ms = $2,
             last_append_ms = $3,
             pending_rows = pending_rows + $4,
             pending_bytes = pending_bytes + $5,
             logical_size_bytes = logical_size_bytes + $5,
             wal_rows = wal_rows + $4,
             wal_bytes = wal_bytes + $5,
             stream_seq = $6,
             closed = CASE WHEN $7 = 1 THEN 1 ELSE closed END,
             closed_producer_id = CASE WHEN $7 = 1 THEN $8 ELSE closed_producer_id END,
             closed_producer_epoch = CASE WHEN $7 = 1 THEN $9 ELSE closed_producer_epoch END,
             closed_producer_seq = CASE WHEN $7 = 1 THEN $10 ELSE closed_producer_seq END
         WHERE stream = $11 AND (stream_flags & $12) = 0;`,
        [
          pgInt(st.nextOffset),
          pgInt(nowMs),
          pgInt(st.lastAppendMs),
          pgInt(BigInt(task.rows.length)),
          pgInt(totalBytes),
          st.streamSeq,
          task.close ? 1 : 0,
          task.producer?.id ?? null,
          task.producer?.epoch ?? null,
          task.producer?.seq ?? null,
          task.stream,
          STREAM_FLAG_DELETED,
        ]
      );
    } else {
      await client.query(
        `UPDATE streams
         SET next_offset = $1,
             updated_at_ms = $2,
             last_append_ms = $3,
             logical_size_bytes = logical_size_bytes + $5,
             wal_rows = wal_rows + $4,
             wal_bytes = wal_bytes + $5,
             stream_seq = $6,
             closed = CASE WHEN $7 = 1 THEN 1 ELSE closed END,
             closed_producer_id = CASE WHEN $7 = 1 THEN $8 ELSE closed_producer_id END,
             closed_producer_epoch = CASE WHEN $7 = 1 THEN $9 ELSE closed_producer_epoch END,
             closed_producer_seq = CASE WHEN $7 = 1 THEN $10 ELSE closed_producer_seq END
         WHERE stream = $11 AND (stream_flags & $12) = 0;`,
        [
          pgInt(st.nextOffset),
          pgInt(nowMs),
          pgInt(st.lastAppendMs),
          pgInt(BigInt(task.rows.length)),
          pgInt(totalBytes),
          st.streamSeq,
          task.close ? 1 : 0,
          task.producer?.id ?? null,
          task.producer?.epoch ?? null,
          task.producer?.seq ?? null,
          task.stream,
          STREAM_FLAG_DELETED,
        ]
      );
    }

    return {
      result: Result.ok({ lastOffset, appendedRows: task.rows.length, closed: task.close, duplicate: false, producer: producerInfo }),
      walBytesCommitted: Number(totalBytes),
    };
  }

  private async *readWalRows(
    stream: string,
    startOffset: bigint,
    endOffset: bigint,
    order: "ASC" | "DESC",
    routingKey?: Uint8Array
  ): AsyncIterable<WalReadRow> {
    if (endOffset < startOffset) return;
    const routingKeyBuffer = routingKey == null ? null : Buffer.from(routingKey.buffer, routingKey.byteOffset, routingKey.byteLength);
    let cursor = order === "ASC" ? startOffset : endOffset;
    while (order === "ASC" ? cursor <= endOffset : cursor >= startOffset) {
      const params: unknown[] =
        order === "ASC"
          ? [stream, pgInt(cursor), pgInt(endOffset), WAL_READ_CHUNK_ROWS]
          : [stream, pgInt(startOffset), pgInt(cursor), WAL_READ_CHUNK_ROWS];
      let routingSql = "";
      if (routingKeyBuffer) {
        params.push(routingKeyBuffer);
        routingSql = ` AND routing_key = $5`;
      }
      const res = await this.pool.query(
        `SELECT "offset", ts_ms, routing_key, content_type, payload
         FROM wal
         WHERE stream = $1 AND "offset" >= $2 AND "offset" <= $3${routingSql}
         ORDER BY "offset" ${order}
         LIMIT $4;`,
        params
      );
      if (res.rows.length === 0) return;
      for (const row of res.rows) yield coerceWalRow(row);
      const lastOffset = toBigInt(res.rows[res.rows.length - 1]!["offset"]);
      cursor = order === "ASC" ? lastOffset + 1n : lastOffset - 1n;
    }
  }

  private async getStreamWithExecutor(executor: PgExecutor, stream: string, forUpdate: boolean): Promise<StreamReadRow | null> {
    const res = await executor.query<PgStreamRow>(
      `SELECT * FROM streams WHERE stream = $1${forUpdate ? " FOR UPDATE" : ""};`,
      [stream]
    );
    return res.rows[0] ? coerceStreamRow(res.rows[0]) : null;
  }

  private async getSchemaRegistryWithExecutor(executor: PgExecutor, stream: string): Promise<SchemaRegistryRow | null> {
    const res = await executor.query<{ stream: string; schema_json: string; updated_at_ms: string | number | bigint; uploaded_size_bytes?: string | number | bigint }>(
      this.opts.fullMode
        ? `SELECT stream, schema_json, updated_at_ms, uploaded_size_bytes FROM schemas WHERE stream = $1;`
        : `SELECT stream, schema_json, updated_at_ms FROM schemas WHERE stream = $1;`,
      [stream]
    );
    const row = res.rows[0];
    return row
      ? {
          stream: row.stream,
          registry_json: row.schema_json,
          updated_at_ms: toBigInt(row.updated_at_ms),
          uploaded_size_bytes: row.uploaded_size_bytes == null ? 0n : toBigInt(row.uploaded_size_bytes),
        }
      : null;
  }

  private async getProfileWithExecutor(executor: PgExecutor, stream: string): Promise<StoredProfileRow | null> {
    const res = await executor.query<{ stream: string; profile_json: string; updated_at_ms: string | number | bigint }>(
      `SELECT stream, profile_json, updated_at_ms FROM stream_profiles WHERE stream = $1;`,
      [stream]
    );
    const row = res.rows[0];
    return row ? { stream: row.stream, profile_json: row.profile_json, updated_at_ms: toBigInt(row.updated_at_ms) } : null;
  }

  private requireSegments(): PostgresSegmentManifestStore {
    if (!this.segments) throw dsError("postgres full-mode segment capability is not enabled", { code: "unsupported_capability" });
    return this.segments;
  }
}

async function upsertProducerState(client: PoolClient, stream: string, producerId: string, epoch: number, seq: number, nowMs: bigint): Promise<void> {
  await client.query(
    `INSERT INTO producer_state(stream, producer_id, epoch, last_seq, updated_at_ms)
     VALUES($1, $2, $3, $4, $5)
     ON CONFLICT(stream, producer_id) DO UPDATE SET
       epoch = excluded.epoch,
       last_seq = excluded.last_seq,
       updated_at_ms = excluded.updated_at_ms;`,
    [stream, producerId, epoch, seq, pgInt(nowMs)]
  );
}

function pgInt(value: bigint): string {
  return value.toString();
}

function toBigInt(value: unknown): bigint {
  return typeof value === "bigint" ? value : BigInt(value as any);
}

function toBytes(value: unknown): Uint8Array | null {
  if (value == null) return null;
  if (value instanceof Uint8Array) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  return new Uint8Array(value as ArrayBuffer);
}

function coerceStreamRow(row: PgStreamRow): StreamReadRow {
  const walRows = toBigInt(row.wal_rows);
  const walBytes = toBigInt(row.wal_bytes);
  const lastAppendMs = toBigInt(row.last_append_ms);
  return {
    stream: String(row.stream),
    created_at_ms: toBigInt(row.created_at_ms),
    updated_at_ms: toBigInt(row.updated_at_ms),
    content_type: String(row.content_type),
    profile: row.profile == null ? null : String(row.profile),
    stream_seq: row.stream_seq == null ? null : String(row.stream_seq),
    closed: Number(row.closed),
    closed_producer_id: row.closed_producer_id == null ? null : String(row.closed_producer_id),
    closed_producer_epoch: row.closed_producer_epoch == null ? null : Number(row.closed_producer_epoch),
    closed_producer_seq: row.closed_producer_seq == null ? null : Number(row.closed_producer_seq),
    ttl_seconds: row.ttl_seconds == null ? null : Number(row.ttl_seconds),
    epoch: Number(row.epoch),
    next_offset: toBigInt(row.next_offset),
    sealed_through: row.sealed_through == null ? -1n : toBigInt(row.sealed_through),
    uploaded_through: row.uploaded_through == null ? -1n : toBigInt(row.uploaded_through),
    uploaded_segment_count: row.uploaded_segment_count == null ? 0 : Number(row.uploaded_segment_count),
    pending_rows: row.pending_rows == null ? walRows : toBigInt(row.pending_rows),
    pending_bytes: row.pending_bytes == null ? walBytes : toBigInt(row.pending_bytes),
    logical_size_bytes: toBigInt(row.logical_size_bytes),
    wal_rows: walRows,
    wal_bytes: walBytes,
    last_append_ms: lastAppendMs,
    last_segment_cut_ms: row.last_segment_cut_ms == null ? lastAppendMs : toBigInt(row.last_segment_cut_ms),
    segment_in_progress: row.segment_in_progress == null ? 0 : Number(row.segment_in_progress),
    expires_at_ms: row.expires_at_ms == null ? null : toBigInt(row.expires_at_ms),
    stream_flags: Number(row.stream_flags),
  };
}

function coerceWalRow(row: any): WalReadRow {
  return {
    offset: toBigInt(row.offset),
    tsMs: toBigInt(row.ts_ms),
    routingKey: toBytes(row.routing_key),
    contentType: row.content_type == null ? null : String(row.content_type),
    payload: toBytes(row.payload) ?? new Uint8Array(),
  };
}

function unsupportedSchemaSearchError(): { kind: "bad_request"; message: string; code: string } {
  return {
    kind: "bad_request",
    message: "postgres storage does not support schema search configuration yet",
    code: "unsupported_capability",
  };
}

function unsupportedProfileError(): { kind: "bad_request"; message: string; code: string } {
  return {
    kind: "bad_request",
    message: "postgres storage supports generic profiles only",
    code: "unsupported_capability",
  };
}

function isRetryablePostgresError(error: unknown): boolean {
  const code = String((error as { code?: unknown })?.code ?? "");
  return code === "40001" || code === "40P01" || code === "55P03";
}
