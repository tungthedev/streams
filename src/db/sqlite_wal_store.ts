import { Result } from "better-result";
import type { SqliteDatabase, SqliteStatement } from "../sqlite/adapter";
import {
  type StoreAppendBatch,
  type StoreAppendError,
  type StoreAppendResult,
  type StoreAppendTask,
} from "../store/append";
import type { WalStore } from "../store/wal_store";

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

export class SqliteWalStore implements WalStore {
  private readonly stmts: {
    getStream: SqliteStatement;
    insertWal: SqliteStatement;
    updateStreamAppend: SqliteStatement;
    updateStreamCloseOnly: SqliteStatement;
    getProducerState: SqliteStatement;
    upsertProducerState: SqliteStatement;
  };

  constructor(
    private readonly db: SqliteDatabase,
    private readonly nowMs: () => bigint,
    private readonly deletedFlag: number
  ) {
    this.stmts = {
      getStream: this.db.query(
        `SELECT stream, epoch, next_offset, last_append_ms, expires_at_ms, stream_flags,
                content_type, stream_seq, closed, closed_producer_id, closed_producer_epoch, closed_producer_seq
         FROM streams WHERE stream=? LIMIT 1;`
      ),
      insertWal: this.db.query(
        `INSERT INTO wal(stream, offset, ts_ms, payload, payload_len, routing_key, content_type, flags)
         VALUES(?, ?, ?, ?, ?, ?, ?, ?);`
      ),
      updateStreamAppend: this.db.query(
        `UPDATE streams
         SET next_offset=?, updated_at_ms=?, last_append_ms=?,
             pending_rows=pending_rows+?, pending_bytes=pending_bytes+?,
             logical_size_bytes=logical_size_bytes+?,
             wal_rows=wal_rows+?, wal_bytes=wal_bytes+?,
             stream_seq=?,
             closed=CASE WHEN ? THEN 1 ELSE closed END,
             closed_producer_id=CASE WHEN ? THEN ? ELSE closed_producer_id END,
             closed_producer_epoch=CASE WHEN ? THEN ? ELSE closed_producer_epoch END,
             closed_producer_seq=CASE WHEN ? THEN ? ELSE closed_producer_seq END
         WHERE stream=? AND (stream_flags & ?) = 0;`
      ),
      updateStreamCloseOnly: this.db.query(
        `UPDATE streams
         SET closed=1,
             closed_producer_id=?,
             closed_producer_epoch=?,
             closed_producer_seq=?,
             updated_at_ms=?,
             stream_seq=?
         WHERE stream=? AND (stream_flags & ?) = 0;`
      ),
      getProducerState: this.db.query(
        `SELECT epoch, last_seq FROM producer_state WHERE stream=? AND producer_id=? LIMIT 1;`
      ),
      upsertProducerState: this.db.query(
        `INSERT INTO producer_state(stream, producer_id, epoch, last_seq, updated_at_ms)
         VALUES(?, ?, ?, ?, ?)
         ON CONFLICT(stream, producer_id) DO UPDATE SET
           epoch=excluded.epoch,
           last_seq=excluded.last_seq,
           updated_at_ms=excluded.updated_at_ms;`
      ),
    };
  }

  async appendBatch(batch: StoreAppendTask[]): Promise<StoreAppendBatch> {
    if (batch.length === 0) return Result.ok({ results: [], walBytesCommitted: 0 });

    const nowMs = this.nowMs();
    let walBytesCommitted = 0;
    const results: StoreAppendResult[] = new Array(batch.length);

    const tx = this.db.transaction(() => {
      const perStream = new Map<string, StreamState>();
      const perProducer = new Map<string, ProducerState | null>();

      const loadStream = (stream: string): StreamState | null => {
        const cached = perStream.get(stream);
        if (cached) return cached;
        const row = this.stmts.getStream.get(stream) as any;
        if (!row || (Number(row.stream_flags) & this.deletedFlag) !== 0) return null;
        const st: StreamState = {
          nextOffset: BigInt(row.next_offset),
          lastAppendMs: BigInt(row.last_append_ms),
          expiresAtMs: row.expires_at_ms == null ? null : BigInt(row.expires_at_ms),
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

      const loadProducerState = (stream: string, producerId: string): ProducerState | null => {
        const key = `${stream}\u0000${producerId}`;
        if (perProducer.has(key)) return perProducer.get(key)!;
        const row = this.stmts.getProducerState.get(stream, producerId) as any;
        const state = row ? { epoch: Number(row.epoch), lastSeq: Number(row.last_seq) } : null;
        perProducer.set(key, state);
        return state;
      };

      const checkProducer = (
        task: StoreAppendTask
      ): Result<{ duplicate: boolean; update: boolean; epoch: number; seq: number }, StoreAppendError> => {
        const producer = task.producer!;
        const key = `${task.stream}\u0000${producer.id}`;
        const state = loadProducerState(task.stream, producer.id);
        if (!state) {
          if (producer.seq !== 0) return Result.err({ kind: "producer_epoch_seq" });
          perProducer.set(key, { epoch: producer.epoch, lastSeq: producer.seq });
          return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
        }
        if (producer.epoch < state.epoch) {
          return Result.err({ kind: "producer_stale_epoch", producerEpoch: state.epoch });
        }
        if (producer.epoch > state.epoch) {
          if (producer.seq !== 0) return Result.err({ kind: "producer_epoch_seq" });
          perProducer.set(key, { epoch: producer.epoch, lastSeq: producer.seq });
          return Result.ok({ duplicate: false, update: true, epoch: producer.epoch, seq: producer.seq });
        }
        if (producer.seq <= state.lastSeq) {
          return Result.ok({ duplicate: true, update: false, epoch: state.epoch, seq: state.lastSeq });
        }
        if (producer.seq === state.lastSeq + 1) {
          perProducer.set(key, { epoch: state.epoch, lastSeq: producer.seq });
          return Result.ok({ duplicate: false, update: true, epoch: state.epoch, seq: producer.seq });
        }
        return Result.err({ kind: "producer_gap", expected: state.lastSeq + 1, received: producer.seq });
      };

      const checkStreamSeq = (
        task: StoreAppendTask,
        st: StreamState
      ): Result<{ nextSeq: string | null }, StoreAppendError> => {
        if (task.streamSeq == null) return Result.ok({ nextSeq: st.streamSeq });
        if (st.streamSeq != null && task.streamSeq <= st.streamSeq) {
          return Result.err({ kind: "stream_seq", expected: st.streamSeq, received: task.streamSeq });
        }
        return Result.ok({ nextSeq: task.streamSeq });
      };

      for (let idx = 0; idx < batch.length; idx++) {
        const task = batch[idx]!;
        const st = loadStream(task.stream);
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
            st.closedProducerId != null &&
            st.closedProducerEpoch != null &&
            st.closedProducerSeq != null &&
            st.closedProducerId === task.producer.id &&
            st.closedProducerEpoch === task.producer.epoch &&
            st.closedProducerSeq === task.producer.seq
          ) {
            results[idx] = Result.ok({
              lastOffset: tailOffset,
              appendedRows: 0,
              closed: true,
              duplicate: true,
              producer: { epoch: st.closedProducerEpoch, seq: st.closedProducerSeq },
            });
            continue;
          }
          results[idx] = Result.err({ kind: "closed", lastOffset: tailOffset });
          continue;
        }

        if (isCloseOnly) {
          const closeRes = this.applyCloseOnly(task, st, checkProducer, checkStreamSeq, nowMs, tailOffset);
          results[idx] = closeRes.result;
          continue;
        }

        if (!task.contentType || task.contentType !== st.contentType) {
          results[idx] = Result.err({ kind: "content_type_mismatch" });
          continue;
        }

        const appendRes = this.applyAppend(task, st, checkProducer, checkStreamSeq, nowMs, tailOffset);
        results[idx] = appendRes.result;
        walBytesCommitted += appendRes.walBytesCommitted;
      }
    });

    try {
      tx();
    } catch (e) {
      if (isSqliteBusy(e)) {
        return Result.err({ kind: "retryable" });
      }
      throw e;
    }
    return Result.ok({ results, walBytesCommitted });
  }

  private applyCloseOnly(
    task: StoreAppendTask,
    st: StreamState,
    checkProducer: (task: StoreAppendTask) => Result<{ duplicate: boolean; update: boolean; epoch: number; seq: number }, StoreAppendError>,
    checkStreamSeq: (task: StoreAppendTask, st: StreamState) => Result<{ nextSeq: string | null }, StoreAppendError>,
    nowMs: bigint,
    tailOffset: bigint
  ): { result: StoreAppendResult } {
    let producerInfo: { epoch: number; seq: number } | undefined;
    let duplicate = false;
    if (task.producer) {
      const prodCheck = checkProducer(task);
      if (Result.isError(prodCheck)) return { result: Result.err(prodCheck.error) };
      duplicate = prodCheck.value.duplicate;
      producerInfo = { epoch: prodCheck.value.epoch, seq: prodCheck.value.seq };
      if (prodCheck.value.update) {
        this.stmts.upsertProducerState.run(task.stream, task.producer.id, prodCheck.value.epoch, prodCheck.value.seq, nowMs);
      }
    }
    if (!duplicate) {
      const seqCheck = checkStreamSeq(task, st);
      if (Result.isError(seqCheck)) return { result: Result.err(seqCheck.error) };
      st.streamSeq = seqCheck.value.nextSeq;
      const closedProducer = task.producer ?? null;
      this.stmts.updateStreamCloseOnly.run(
        closedProducer ? closedProducer.id : null,
        closedProducer ? closedProducer.epoch : null,
        closedProducer ? closedProducer.seq : null,
        nowMs,
        st.streamSeq,
        task.stream,
        this.deletedFlag
      );
      st.closed = true;
      st.closedProducerId = closedProducer ? closedProducer.id : null;
      st.closedProducerEpoch = closedProducer ? closedProducer.epoch : null;
      st.closedProducerSeq = closedProducer ? closedProducer.seq : null;
    }
    return {
      result: Result.ok({
        lastOffset: tailOffset,
        appendedRows: 0,
        closed: st.closed,
        duplicate,
        producer: producerInfo,
      }),
    };
  }

  private applyAppend(
    task: StoreAppendTask,
    st: StreamState,
    checkProducer: (task: StoreAppendTask) => Result<{ duplicate: boolean; update: boolean; epoch: number; seq: number }, StoreAppendError>,
    checkStreamSeq: (task: StoreAppendTask, st: StreamState) => Result<{ nextSeq: string | null }, StoreAppendError>,
    nowMs: bigint,
    tailOffset: bigint
  ): { result: StoreAppendResult; walBytesCommitted: number } {
    let producerInfo: { epoch: number; seq: number } | undefined;
    if (task.producer) {
      const prodCheck = checkProducer(task);
      if (Result.isError(prodCheck)) return { result: Result.err(prodCheck.error), walBytesCommitted: 0 };
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
      if (prodCheck.value.update) {
        this.stmts.upsertProducerState.run(task.stream, task.producer.id, prodCheck.value.epoch, prodCheck.value.seq, nowMs);
      }
    }

    const seqCheck = checkStreamSeq(task, st);
    if (Result.isError(seqCheck)) return { result: Result.err(seqCheck.error), walBytesCommitted: 0 };
    st.streamSeq = seqCheck.value.nextSeq;

    let appendMs = task.baseAppendMs;
    if (appendMs <= st.lastAppendMs) appendMs = st.lastAppendMs + 1n;

    let offset = st.nextOffset;
    let totalBytes = 0n;
    for (const r of task.rows) {
      const payloadLen = r.payload.byteLength;
      totalBytes += BigInt(payloadLen);
      this.stmts.insertWal.run(task.stream, offset, appendMs, r.payload, payloadLen, r.routingKey, r.contentType, 0);
      offset += 1n;
    }

    const lastOffset = offset - 1n;
    st.nextOffset = offset;
    st.lastAppendMs = appendMs;
    if (task.close) {
      st.closed = true;
      if (task.producer) {
        st.closedProducerId = task.producer.id;
        st.closedProducerEpoch = task.producer.epoch;
        st.closedProducerSeq = task.producer.seq;
      } else {
        st.closedProducerId = null;
        st.closedProducerEpoch = null;
        st.closedProducerSeq = null;
      }
    }

    const closedProducer = task.close && task.producer ? task.producer : null;
    const closeFlag = task.close ? 1 : 0;
    this.stmts.updateStreamAppend.run(
      st.nextOffset,
      nowMs,
      st.lastAppendMs,
      BigInt(task.rows.length),
      totalBytes,
      totalBytes,
      BigInt(task.rows.length),
      totalBytes,
      st.streamSeq,
      closeFlag,
      closeFlag,
      closedProducer ? closedProducer.id : null,
      closeFlag,
      closedProducer ? closedProducer.epoch : null,
      closeFlag,
      closedProducer ? closedProducer.seq : null,
      task.stream,
      this.deletedFlag
    );

    return {
      result: Result.ok({
        lastOffset,
        appendedRows: task.rows.length,
        closed: task.close,
        duplicate: false,
        producer: producerInfo,
      }),
      walBytesCommitted: Number(totalBytes),
    };
  }
}

function isSqliteBusy(e: unknown): boolean {
  const err = e as { code?: unknown; errno?: unknown };
  const code = String(err?.code ?? "");
  const errno = Number(err?.errno ?? -1);
  return code === "SQLITE_BUSY" || code === "SQLITE_BUSY_SNAPSHOT" || errno === 5 || errno === 517;
}
