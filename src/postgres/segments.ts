import { createHash, randomUUID } from "node:crypto";
import type { Pool, PoolClient } from "pg";
import type { SegmentReadStore, SegmentReadRow, StreamReadRow, SearchCompanionPlanReadRow, SearchSegmentCompanionReadRow } from "../store/segment_read_store";
import type {
  SegmentClaim,
  ManifestPublicationSnapshot,
  ManifestRow,
  SegmentCandidateRow,
  SegmentMetaRow,
  SegmentRow,
  SegmentStore,
  SealedSegmentCommit,
  ManifestStore,
} from "../store/segment_manifest_store";
import type { WalReadRow } from "../store/wal_store";
import { STREAM_FLAG_DELETED, STREAM_FLAG_TOUCH } from "../store/rows";
import { readU64LE } from "../util/endian";
import { dsError } from "../util/ds_error";
import type { PgExecutor, PgStreamRow } from "./types";
import { loadPostgresRoutingIndexManifest } from "./routing_index";
import { loadPostgresSecondaryIndexManifest } from "./secondary_index";
import { loadPostgresLexiconIndexManifest } from "./lexicon_index";
import {
  getPostgresSearchCompanionPlan,
  getPostgresSearchSegmentCompanion,
  listPostgresSearchSegmentCompanions,
  loadPostgresSearchCompanionManifest,
} from "./companions";

const WAL_GC_CHUNK_OFFSETS = 100_000n;
const SEGMENT_CLAIM_LEASE_MS = 5 * 60 * 1000;

type ManifestLease = {
  client: PoolClient;
  stream: string;
  lockKey: bigint;
};

export class PostgresSegmentManifestStore implements SegmentReadStore, SegmentStore, ManifestStore {
  private readonly manifestLeases = new Map<string, ManifestLease>();

  constructor(
    private readonly pool: Pool,
    private readonly currentTimeMs: () => bigint,
    private readonly readWal: (stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array) => AsyncIterable<WalReadRow>
  ) {}

  nowMs(): bigint {
    return this.currentTimeMs();
  }

  nowMsForRead(): Promise<bigint> {
    return Promise.resolve(this.currentTimeMs());
  }

  async getSegmentStreamState(stream: string): Promise<StreamReadRow | null> {
    return this.getStream(stream);
  }

  isDeleted(row: StreamReadRow): boolean {
    return (row.stream_flags & STREAM_FLAG_DELETED) !== 0;
  }

  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow> {
    return this.readWal(stream, startOffset, endOffset, routingKey);
  }

  async getStreamForRead(stream: string): Promise<StreamReadRow | null> {
    return this.getStream(stream);
  }

  async getStream(stream: string): Promise<StreamReadRow | null> {
    return this.getStreamWithExecutor(this.pool, stream);
  }

  async listSegmentsForRead(stream: string): Promise<SegmentReadRow[]> {
    const res = await this.pool.query(segmentSelectSql(`WHERE stream = $1 ORDER BY segment_index ASC`), [stream]);
    return res.rows.map(coerceSegmentRow);
  }

  async getSegmentByIndexForRead(stream: string, segmentIndex: number): Promise<SegmentReadRow | null> {
    return this.getSegmentByIndex(stream, segmentIndex);
  }

  async findSegmentForOffsetForRead(stream: string, offset: bigint): Promise<SegmentReadRow | null> {
    const res = await this.pool.query(
      segmentSelectSql(`WHERE stream = $1 AND start_offset <= $2 AND end_offset >= $2 ORDER BY segment_index DESC LIMIT 1`),
      [stream, pgInt(offset)]
    );
    return res.rows[0] ? coerceSegmentRow(res.rows[0]) : null;
  }

  async countSegmentsForRead(stream: string): Promise<number> {
    return this.countSegmentsWithExecutor(this.pool, stream);
  }

  getSearchCompanionPlanForRead(_stream: string): Promise<SearchCompanionPlanReadRow | null> {
    return getPostgresSearchCompanionPlan(this.pool, _stream);
  }

  listSearchSegmentCompanionsForRead(_stream: string): Promise<SearchSegmentCompanionReadRow[]> {
    return listPostgresSearchSegmentCompanions(this.pool, _stream);
  }

  getSearchSegmentCompanionForRead(_stream: string, _segmentIndex: number): Promise<SearchSegmentCompanionReadRow | null> {
    return getPostgresSearchSegmentCompanion(this.pool, _stream, _segmentIndex);
  }

  async candidates(minPendingBytes: bigint, minPendingRows: bigint, maxIntervalMs: bigint, limit: number): Promise<SegmentCandidateRow[]> {
    const now = this.currentTimeMs();
    const includeInterval = maxIntervalMs > 0n;
    const sql = includeInterval
      ? `SELECT stream, pending_bytes, pending_rows, last_segment_cut_ms, sealed_through, next_offset, epoch
         FROM streams
         WHERE (stream_flags & $1) = 0
           AND (segment_in_progress = 0 OR segment_claimed_at_ms IS NULL OR segment_claimed_at_ms < $6)
           AND (pending_bytes >= $2 OR pending_rows >= $3 OR ($4 - last_segment_cut_ms) >= $5)
         ORDER BY pending_bytes DESC
         LIMIT $7;`
      : `SELECT stream, pending_bytes, pending_rows, last_segment_cut_ms, sealed_through, next_offset, epoch
         FROM streams
         WHERE (stream_flags & $1) = 0
           AND (segment_in_progress = 0 OR segment_claimed_at_ms IS NULL OR segment_claimed_at_ms < $4)
           AND (pending_bytes >= $2 OR pending_rows >= $3)
         ORDER BY pending_bytes DESC
         LIMIT $5;`;
    const excludedFlags = STREAM_FLAG_DELETED | STREAM_FLAG_TOUCH;
    const params = includeInterval
      ? [excludedFlags, pgInt(minPendingBytes), pgInt(minPendingRows), pgInt(now), pgInt(maxIntervalMs), pgInt(now - BigInt(SEGMENT_CLAIM_LEASE_MS)), limit]
      : [excludedFlags, pgInt(minPendingBytes), pgInt(minPendingRows), pgInt(now - BigInt(SEGMENT_CLAIM_LEASE_MS)), limit];
    const res = await this.pool.query(sql, params);
    return res.rows.map((row) => ({
      stream: String(row.stream),
      pending_bytes: toBigInt(row.pending_bytes),
      pending_rows: toBigInt(row.pending_rows),
      last_segment_cut_ms: toBigInt(row.last_segment_cut_ms),
      sealed_through: toBigInt(row.sealed_through),
      next_offset: toBigInt(row.next_offset),
      epoch: Number(row.epoch),
    }));
  }

  async recentSegmentCompressionRatio(stream: string, limit = 8): Promise<number | null> {
    const res = await this.pool.query<{ payload_total: string | null; size_total: string | null; count: string }>(
      `SELECT
         COALESCE(SUM(payload_bytes), 0) AS payload_total,
         COALESCE(SUM(size_bytes), 0) AS size_total,
         COUNT(*) AS count
       FROM (
         SELECT payload_bytes, size_bytes
         FROM segments
         WHERE stream = $1 AND payload_bytes > 0
         ORDER BY segment_index DESC
         LIMIT $2
       ) recent;`,
      [stream, Math.max(1, limit)]
    );
    const count = Number(res.rows[0]?.count ?? 0);
    if (!Number.isFinite(count) || count <= 0) return null;
    const payloadTotal = toBigInt(res.rows[0]?.payload_total ?? 0);
    const sizeTotal = toBigInt(res.rows[0]?.size_total ?? 0);
    if (payloadTotal <= 0n || sizeTotal <= 0n) return null;
    return Number(sizeTotal) / Number(payloadTotal);
  }

  async tryClaimSegment(stream: string): Promise<SegmentClaim | null> {
    const token = randomUUID();
    const now = this.currentTimeMs();
    const staleBefore = now - BigInt(SEGMENT_CLAIM_LEASE_MS);
    const res = await this.pool.query(
      `UPDATE streams
       SET segment_in_progress = 1,
           segment_claim_token = $1,
           segment_claimed_at_ms = $2,
           updated_at_ms = $2
       WHERE stream = $3
         AND (stream_flags & $4) = 0
         AND (segment_in_progress = 0 OR segment_claimed_at_ms IS NULL OR segment_claimed_at_ms < $5);`,
      [token, pgInt(now), stream, STREAM_FLAG_DELETED, pgInt(staleBefore)]
    );
    return (res.rowCount ?? 0) > 0 ? { token } : null;
  }

  async setSegmentInProgress(stream: string, inProgress: number, claim?: SegmentClaim): Promise<void> {
    if (inProgress === 0 && claim?.token) {
      await this.pool.query(
        `UPDATE streams
         SET segment_in_progress = 0,
             segment_claim_token = NULL,
             segment_claimed_at_ms = NULL,
             updated_at_ms = $1
         WHERE stream = $2 AND segment_claim_token = $3;`,
        [pgInt(this.currentTimeMs()), stream, claim.token]
      );
      return;
    }
    await this.pool.query(
      `UPDATE streams
       SET segment_in_progress = $1,
           segment_claim_token = NULL,
           segment_claimed_at_ms = NULL,
           updated_at_ms = $2
       WHERE stream = $3;`,
      [inProgress, pgInt(this.currentTimeMs()), stream]
    );
  }

  async nextSegmentIndexForStream(stream: string): Promise<number> {
    const res = await this.pool.query<{ next_idx: string | null }>(`SELECT COALESCE(MAX(segment_index) + 1, 0) AS next_idx FROM segments WHERE stream = $1;`, [stream]);
    return Number(res.rows[0]?.next_idx ?? 0);
  }

  async commitSealedSegment(row: SealedSegmentCommit): Promise<void> {
    if (!row.claimToken) throw dsError("postgres segment commit requires a claim token", { code: "unsupported_capability" });
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const claim = await client.query<{ sealed_through: string | number | bigint }>(
        `SELECT sealed_through FROM streams
         WHERE stream = $1
           AND segment_in_progress = 1
           AND segment_claim_token = $2
         FOR UPDATE;`,
        [row.stream, row.claimToken]
      );
      if (claim.rows.length === 0) throw dsError("postgres segment claim is no longer active", { code: "conflict" });
      const currentSealedThrough = toBigInt(claim.rows[0]!.sealed_through);
      if (currentSealedThrough !== row.startOffset - 1n) {
        throw dsError("postgres segment commit start offset is stale", { code: "conflict" });
      }
      const nextIndex = await client.query<{ next_idx: string | number | null }>(
        `SELECT COALESCE(MAX(segment_index) + 1, 0) AS next_idx FROM segments WHERE stream = $1;`,
        [row.stream]
      );
      if (Number(nextIndex.rows[0]?.next_idx ?? 0) !== row.segmentIndex) {
        throw dsError("postgres segment commit index is stale", { code: "conflict" });
      }
      await client.query(
        `INSERT INTO segments(segment_id, stream, segment_index, start_offset, end_offset, block_count,
                              last_append_ms, payload_bytes, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag)
         VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, NULL);`,
        [
          row.segmentId,
          row.stream,
          row.segmentIndex,
          pgInt(row.startOffset),
          pgInt(row.endOffset),
          row.blockCount,
          pgInt(row.lastAppendMs),
          pgInt(row.payloadBytes),
          row.sizeBytes,
          row.localPath,
          pgInt(this.currentTimeMs()),
        ]
      );
      await this.appendSegmentMeta(client, row.stream, row.endOffset + 1n, row.blockCount, row.lastAppendMs * 1_000_000n);
      await client.query(
        `UPDATE streams
         SET sealed_through = $1,
             pending_bytes = GREATEST(pending_bytes - $2, 0),
             pending_rows = GREATEST(pending_rows - $3, 0),
             segment_in_progress = 0,
             segment_claim_token = NULL,
             segment_claimed_at_ms = NULL,
             last_segment_cut_ms = $4,
             updated_at_ms = $4
         WHERE stream = $5 AND segment_claim_token = $6;`,
        [pgInt(row.endOffset), pgInt(row.payloadBytes), pgInt(row.rowsSealed), pgInt(this.currentTimeMs()), row.stream, row.claimToken]
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async countPendingSegments(): Promise<number> {
    const res = await this.pool.query<{ count: string }>(`SELECT COUNT(*) AS count FROM segments WHERE uploaded_at_ms IS NULL;`);
    return Number(res.rows[0]?.count ?? 0);
  }

  async pendingUploadHeads(limit: number): Promise<SegmentRow[]> {
    const res = await this.pool.query(
      segmentSelectSql(
        `WHERE uploaded_at_ms IS NULL
           AND segment_index = (
             SELECT MIN(s2.segment_index)
             FROM segments s2
             WHERE s2.stream = segments.stream AND s2.uploaded_at_ms IS NULL
           )
         ORDER BY created_at_ms ASC, stream ASC
         LIMIT $1`
      ),
      [limit]
    );
    return res.rows.map(coerceSegmentRow);
  }

  async markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): Promise<void> {
    await this.pool.query(`UPDATE segments SET r2_etag = $1, uploaded_at_ms = $2 WHERE segment_id = $3;`, [
      etag,
      pgInt(uploadedAtMs),
      segmentId,
    ]);
  }

  async loadManifestPublicationSnapshot(stream: string, opts: { wait?: boolean } = {}): Promise<ManifestPublicationSnapshot | null> {
    const maxAttempts = opts.wait ? 5 : 3;
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.loadManifestPublicationSnapshotOnce(stream, opts);
      } catch (error) {
        if (attempt >= maxAttempts || !isRetryablePostgresPublicationError(error)) throw error;
        await sleep(Math.min(100, 10 * 2 ** (attempt - 1)));
      }
    }
  }

  private async loadManifestPublicationSnapshotOnce(stream: string, opts: { wait?: boolean } = {}): Promise<ManifestPublicationSnapshot | null> {
    const publication = await this.acquireManifestPublication(stream, { wait: opts.wait });
    if (!publication) return null;
    let keepLease = false;
    try {
      await publication.client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      const streamRow = await this.getStreamWithExecutor(publication.client, stream);
      if (!streamRow) return null;

      const prevUploadedSegmentCount = streamRow.uploaded_segment_count ?? 0;
      let uploadedPrefixCount = await this.advanceUploadedSegmentCount(publication.client, stream, prevUploadedSegmentCount);
      const segmentCount = await this.countSegmentsWithExecutor(publication.client, stream);
      let segmentMeta = await this.getSegmentMeta(publication.client, stream);
      const needsRebuild =
        !segmentMeta ||
        segmentMeta.segment_count !== segmentCount ||
        segmentMeta.segment_offsets.byteLength !== segmentCount * 8 ||
        segmentMeta.segment_blocks.byteLength !== segmentCount * 4 ||
        segmentMeta.segment_last_ts.byteLength !== segmentCount * 8;
      if (needsRebuild) segmentMeta = await this.rebuildSegmentMeta(publication.client, stream);
      if (!segmentMeta) return null;
      if (uploadedPrefixCount > segmentMeta.segment_count) {
        uploadedPrefixCount = segmentMeta.segment_count;
        await this.setUploadedSegmentCount(publication.client, stream, uploadedPrefixCount);
      }

      const uploadedThrough =
        uploadedPrefixCount === 0 ? -1n : readU64LE(segmentMeta.segment_offsets, (uploadedPrefixCount - 1) * 8) - 1n;
      const unpublishedWalBytes = await this.getWalBytesAfterOffset(publication.client, stream, uploadedThrough);
      const publishedLogicalSizeBytes =
        streamRow.logical_size_bytes > unpublishedWalBytes ? streamRow.logical_size_bytes - unpublishedWalBytes : 0n;
      const manifestRow = await this.getManifestRow(publication.client, stream);
      const profileJson = await this.getProfileJson(publication.client, stream);
      const routingIndex = await loadPostgresRoutingIndexManifest(publication.client, stream);
      const secondaryIndex = await loadPostgresSecondaryIndexManifest(publication.client, stream);
      const lexiconIndex = await loadPostgresLexiconIndexManifest(publication.client, stream);
      const searchCompanions = await loadPostgresSearchCompanionManifest(publication.client, stream);
      await publication.client.query("COMMIT");

      keepLease = true;
      return {
        publicationToken: publication.token,
        streamRow,
        prevUploadedSegmentCount,
        uploadedPrefixCount,
        uploadedThrough,
        publishedLogicalSizeBytes,
        generation: manifestRow.generation + 1,
        segmentMeta,
        profileJson,
        indexState: routingIndex.indexState,
        indexRuns: routingIndex.indexRuns,
        retiredRuns: routingIndex.retiredRuns,
        secondaryIndexStates: secondaryIndex.secondaryIndexStates,
        secondaryIndexRuns: secondaryIndex.secondaryIndexRuns,
        retiredSecondaryIndexRuns: secondaryIndex.retiredSecondaryIndexRuns,
        lexiconIndexStates: lexiconIndex.lexiconIndexStates,
        lexiconIndexRuns: lexiconIndex.lexiconIndexRuns,
        retiredLexiconIndexRuns: lexiconIndex.retiredLexiconIndexRuns,
        searchCompanionPlan: searchCompanions.searchCompanionPlan,
        searchSegmentCompanions: searchCompanions.searchSegmentCompanions,
      };
    } finally {
      if (!keepLease) {
        await publication.client.query("ROLLBACK").catch(() => {});
        await this.releaseManifestPublication(publication.token);
      }
    }
  }

  async commitManifest(
    stream: string,
    generation: number,
    etag: string,
    uploadedAtMs: bigint,
    uploadedThrough: bigint,
    sizeBytes: number,
    publicationToken?: string
  ): Promise<void> {
    if (!publicationToken) throw dsError("postgres manifest commit requires a publication token", { code: "unsupported_capability" });
    const lease = this.manifestLeases.get(publicationToken);
    if (!lease || lease.stream !== stream) throw dsError("postgres manifest publication token is not active", { code: "conflict" });
    const client = lease.client;
    try {
      await client.query("BEGIN");
      const current = await client.query<{ generation: number | string | null; uploaded_through: string | number | bigint | null }>(
        `SELECT m.generation, s.uploaded_through
         FROM streams s
         LEFT JOIN manifests m ON m.stream = s.stream
         WHERE s.stream = $1
         FOR UPDATE OF s;`,
        [stream]
      );
      const currentRow = current.rows[0];
      if (!currentRow) throw dsError(`stream not found: ${stream}`, { code: "not_found" });
      const currentGeneration = currentRow.generation == null ? 0 : Number(currentRow.generation);
      const currentUploadedThrough = currentRow.uploaded_through == null ? -1n : toBigInt(currentRow.uploaded_through);
      if (generation <= currentGeneration || uploadedThrough < currentUploadedThrough) {
        throw dsError("postgres manifest publication is stale", { code: "conflict" });
      }
      await client.query(
        `INSERT INTO manifests(stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes)
         VALUES($1, $2, $2, $3, $4, $5)
         ON CONFLICT(stream) DO UPDATE SET
           generation = excluded.generation,
           uploaded_generation = excluded.uploaded_generation,
           last_uploaded_at_ms = excluded.last_uploaded_at_ms,
           last_uploaded_etag = excluded.last_uploaded_etag,
           last_uploaded_size_bytes = excluded.last_uploaded_size_bytes;`,
        [stream, generation, pgInt(uploadedAtMs), etag, sizeBytes]
      );
      await client.query(`UPDATE streams SET uploaded_through = $1, updated_at_ms = $2 WHERE stream = $3;`, [
        pgInt(uploadedThrough),
        pgInt(this.currentTimeMs()),
        stream,
      ]);
      let gcThrough = uploadedThrough;
      const touchState = await client.query<{ processed_through: string | number | bigint }>(
        `SELECT processed_through FROM stream_touch_state WHERE stream = $1;`,
        [stream]
      );
      const processedThrough = touchState.rows[0]?.processed_through;
      if (processedThrough != null) {
        const touchThrough = toBigInt(processedThrough);
        gcThrough = touchThrough < gcThrough ? touchThrough : gcThrough;
      }
      if (gcThrough >= 0n) await this.deleteWalThrough(client, stream, gcThrough);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      await this.releaseManifestPublication(publicationToken);
    }
  }

  async releaseManifestPublication(publicationToken: string): Promise<void> {
    const lease = this.manifestLeases.get(publicationToken);
    if (!lease) return;
    this.manifestLeases.delete(publicationToken);
    try {
      await lease.client.query(`SELECT pg_advisory_unlock($1::bigint);`, [pgInt(lease.lockKey)]);
    } finally {
      lease.client.release();
    }
  }

  getSegmentForManifestCleanup(stream: string, segmentIndex: number): Promise<SegmentRow | null> {
    return this.getSegmentByIndex(stream, segmentIndex);
  }

  private async getStreamWithExecutor(executor: PgExecutor, stream: string): Promise<StreamReadRow | null> {
    const res = await executor.query<PgStreamRow>(`SELECT * FROM streams WHERE stream = $1;`, [stream]);
    return res.rows[0] ? coerceStreamRow(res.rows[0]) : null;
  }

  private async countSegmentsWithExecutor(executor: PgExecutor, stream: string): Promise<number> {
    const res = await executor.query<{ count: string }>(`SELECT COUNT(*) AS count FROM segments WHERE stream = $1;`, [stream]);
    return Number(res.rows[0]?.count ?? 0);
  }

  private async acquireManifestPublication(stream: string, opts: { wait?: boolean } = {}): Promise<{ token: string; client: PoolClient } | null> {
    const client = await this.pool.connect();
    const token = randomUUID();
    const lockKey = manifestLockKey(stream);
    try {
      const sql = opts.wait ? `SELECT pg_advisory_lock($1::bigint) AS locked;` : `SELECT pg_try_advisory_lock($1::bigint) AS locked;`;
      const res = await client.query<{ locked: boolean | null }>(sql, [pgInt(lockKey)]);
      if (!opts.wait && !res.rows[0]?.locked) {
        client.release();
        return null;
      }
      this.manifestLeases.set(token, { client, stream, lockKey });
      return { token, client };
    } catch (error) {
      client.release();
      throw error;
    }
  }

  async setSchemaUploadedSizeBytes(stream: string, sizeBytes: number): Promise<void> {
    await this.pool.query(`UPDATE schemas SET uploaded_size_bytes = $1, updated_at_ms = $2 WHERE stream = $3;`, [
      sizeBytes,
      pgInt(this.currentTimeMs()),
      stream,
    ]);
  }

  private async getSegmentByIndex(stream: string, segmentIndex: number): Promise<SegmentRow | null> {
    return this.getSegmentByIndexWithExecutor(this.pool, stream, segmentIndex);
  }

  private async getSegmentByIndexWithExecutor(executor: PgExecutor, stream: string, segmentIndex: number): Promise<SegmentRow | null> {
    const res = await executor.query(segmentSelectSql(`WHERE stream = $1 AND segment_index = $2 LIMIT 1`), [stream, segmentIndex]);
    return res.rows[0] ? coerceSegmentRow(res.rows[0]) : null;
  }

  private async appendSegmentMeta(executor: PgExecutor, stream: string, offsetPlusOne: bigint, blockCount: number, lastAppendNs: bigint): Promise<void> {
    await executor.query(
      `INSERT INTO stream_segment_meta(stream, segment_count, segment_offsets, segment_blocks, segment_last_ts)
       VALUES($1, 0, ''::bytea, ''::bytea, ''::bytea)
       ON CONFLICT(stream) DO NOTHING;`,
      [stream]
    );
    await executor.query(
      `UPDATE stream_segment_meta
       SET segment_count = segment_count + 1,
           segment_offsets = segment_offsets || $1::bytea,
           segment_blocks = segment_blocks || $2::bytea,
           segment_last_ts = segment_last_ts || $3::bytea
       WHERE stream = $4;`,
      [encodeU64Le(offsetPlusOne), encodeU32Le(blockCount), encodeU64Le(lastAppendNs), stream]
    );
  }

  private async getSegmentMeta(executor: PgExecutor, stream: string): Promise<SegmentMetaRow | null> {
    const res = await executor.query(
      `SELECT stream, segment_count, segment_offsets, segment_blocks, segment_last_ts
       FROM stream_segment_meta WHERE stream = $1 LIMIT 1;`,
      [stream]
    );
    const row = res.rows[0];
    return row ? coerceSegmentMetaRow(row) : null;
  }

  private async rebuildSegmentMeta(executor: PgExecutor, stream: string): Promise<SegmentMetaRow> {
    const rows = await executor.query<{ end_offset: string; block_count: number; last_append_ms: string }>(
      `SELECT end_offset, block_count, last_append_ms
       FROM segments WHERE stream = $1 ORDER BY segment_index ASC;`,
      [stream]
    );
    const count = rows.rows.length;
    const offsets = new Uint8Array(count * 8);
    const blocks = new Uint8Array(count * 4);
    const lastTs = new Uint8Array(count * 8);
    const dvOffsets = new DataView(offsets.buffer, offsets.byteOffset, offsets.byteLength);
    const dvBlocks = new DataView(blocks.buffer, blocks.byteOffset, blocks.byteLength);
    const dvLastTs = new DataView(lastTs.buffer, lastTs.byteOffset, lastTs.byteLength);
    for (let i = 0; i < rows.rows.length; i++) {
      const row = rows.rows[i]!;
      dvOffsets.setBigUint64(i * 8, toBigInt(row.end_offset) + 1n, true);
      dvBlocks.setUint32(i * 4, Number(row.block_count) >>> 0, true);
      dvLastTs.setBigUint64(i * 8, toBigInt(row.last_append_ms) * 1_000_000n, true);
    }
    await executor.query(
      `INSERT INTO stream_segment_meta(stream, segment_count, segment_offsets, segment_blocks, segment_last_ts)
       VALUES($1, $2, $3, $4, $5)
       ON CONFLICT(stream) DO UPDATE SET
         segment_count = excluded.segment_count,
         segment_offsets = excluded.segment_offsets,
         segment_blocks = excluded.segment_blocks,
         segment_last_ts = excluded.segment_last_ts;`,
      [stream, count, Buffer.from(offsets), Buffer.from(blocks), Buffer.from(lastTs)]
    );
    return { stream, segment_count: count, segment_offsets: offsets, segment_blocks: blocks, segment_last_ts: lastTs };
  }

  private async setUploadedSegmentCount(executor: PgExecutor, stream: string, count: number): Promise<void> {
    await executor.query(`UPDATE streams SET uploaded_segment_count = $1, updated_at_ms = $2 WHERE stream = $3;`, [
      count,
      pgInt(this.currentTimeMs()),
      stream,
    ]);
  }

  private async advanceUploadedSegmentCount(executor: PgExecutor, stream: string, currentCount: number): Promise<number> {
    let count = currentCount;
    for (;;) {
      const segment = await this.getSegmentByIndexWithExecutor(executor, stream, count);
      if (!segment || !segment.r2_etag) break;
      count += 1;
    }
    if (count !== currentCount) await this.setUploadedSegmentCount(executor, stream, count);
    return count;
  }

  private async getWalBytesAfterOffset(executor: PgExecutor, stream: string, offset: bigint): Promise<bigint> {
    const res = await executor.query<{ bytes: string | null }>(
      `SELECT COALESCE(SUM(payload_len), 0) AS bytes
       FROM wal
       WHERE stream = $1 AND "offset" > $2;`,
      [stream, pgInt(offset)]
    );
    return toBigInt(res.rows[0]?.bytes ?? 0);
  }

  private async getManifestRow(executor: PgExecutor, stream: string): Promise<ManifestRow> {
    const res = await executor.query(
      `SELECT stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes
       FROM manifests WHERE stream = $1 LIMIT 1;`,
      [stream]
    );
    if (res.rows[0]) return coerceManifestRow(res.rows[0]);
    await executor.query(
      `INSERT INTO manifests(stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes)
       VALUES($1, 0, 0, NULL, NULL, NULL)
       ON CONFLICT(stream) DO NOTHING;`,
      [stream]
    );
    return { stream, generation: 0, uploaded_generation: 0, last_uploaded_at_ms: null, last_uploaded_etag: null, last_uploaded_size_bytes: null };
  }

  private async getProfileJson(executor: PgExecutor, stream: string): Promise<Record<string, any> | null> {
    const res = await executor.query<{ profile_json: string }>(`SELECT profile_json FROM stream_profiles WHERE stream = $1;`, [stream]);
    const raw = res.rows[0]?.profile_json;
    if (raw == null) return null;
    try {
      return JSON.parse(raw);
    } catch {
      throw dsError(`invalid profile_json for ${stream}`);
    }
  }

  private async deleteWalThrough(client: PoolClient, stream: string, uploadedThrough: bigint): Promise<void> {
    const upper = uploadedThrough + 1n;
    for (;;) {
      const res = await client.query<{ rows_deleted: string | number | bigint | null; bytes_deleted: string | number | bigint | null }>(
        `WITH doomed AS (
           SELECT stream, "offset", payload_len
           FROM wal
           WHERE stream = $1 AND "offset" < $2
           ORDER BY "offset" ASC
           LIMIT $3
         ),
         deleted AS (
           DELETE FROM wal
           USING doomed
           WHERE wal.stream = doomed.stream AND wal."offset" = doomed."offset"
           RETURNING doomed.payload_len
         )
         SELECT COUNT(*) AS rows_deleted, COALESCE(SUM(payload_len), 0) AS bytes_deleted FROM deleted;`,
        [stream, pgInt(upper), pgInt(WAL_GC_CHUNK_OFFSETS)]
      );
      const rowsDeleted = toBigInt(res.rows[0]?.rows_deleted ?? 0);
      const bytesDeleted = toBigInt(res.rows[0]?.bytes_deleted ?? 0);
      if (rowsDeleted <= 0n) break;
      await client.query(
        `UPDATE streams
         SET wal_bytes = GREATEST(wal_bytes - $1, 0),
             wal_rows = GREATEST(wal_rows - $2, 0),
             updated_at_ms = $3
         WHERE stream = $4;`,
        [pgInt(bytesDeleted), pgInt(rowsDeleted), pgInt(this.currentTimeMs()), stream]
      );
      if (rowsDeleted < WAL_GC_CHUNK_OFFSETS) break;
    }
  }
}

export type PostgresSegmentRestoreRow = {
  segmentId: string;
  stream: string;
  segmentIndex: number;
  startOffset: bigint;
  endOffset: bigint;
  blockCount: number;
  lastAppendMs: bigint;
  payloadBytes: bigint;
  sizeBytes: number;
  localPath: string;
};

export async function restorePostgresSegmentRow(
  executor: PgExecutor,
  nowMs: bigint,
  row: PostgresSegmentRestoreRow
): Promise<void> {
  await executor.query(
    `INSERT INTO segments(segment_id, stream, segment_index, start_offset, end_offset, block_count,
                          last_append_ms, payload_bytes, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag)
     VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, NULL)
     ON CONFLICT(stream, segment_index) DO UPDATE SET
       segment_id = excluded.segment_id,
       start_offset = excluded.start_offset,
       end_offset = excluded.end_offset,
       block_count = excluded.block_count,
       last_append_ms = excluded.last_append_ms,
       payload_bytes = excluded.payload_bytes,
       size_bytes = excluded.size_bytes,
       local_path = excluded.local_path;`,
    [
      row.segmentId,
      row.stream,
      row.segmentIndex,
      pgInt(row.startOffset),
      pgInt(row.endOffset),
      row.blockCount,
      pgInt(row.lastAppendMs),
      pgInt(row.payloadBytes),
      row.sizeBytes,
      row.localPath,
      pgInt(nowMs),
    ]
  );
}

export async function restorePostgresSegmentMeta(
  executor: PgExecutor,
  stream: string,
  count: number,
  offsets: Uint8Array,
  blocks: Uint8Array,
  lastTs: Uint8Array
): Promise<void> {
  await executor.query(
    `INSERT INTO stream_segment_meta(stream, segment_count, segment_offsets, segment_blocks, segment_last_ts)
     VALUES($1, $2, $3, $4, $5)
     ON CONFLICT(stream) DO UPDATE SET
       segment_count = excluded.segment_count,
       segment_offsets = excluded.segment_offsets,
       segment_blocks = excluded.segment_blocks,
       segment_last_ts = excluded.segment_last_ts;`,
    [stream, count, Buffer.from(offsets), Buffer.from(blocks), Buffer.from(lastTs)]
  );
}

export async function restorePostgresManifestRow(
  executor: PgExecutor,
  stream: string,
  generation: number,
  uploadedGeneration: number,
  uploadedAtMs: bigint | null,
  etag: string | null,
  sizeBytes: number | null
): Promise<void> {
  await executor.query(
    `INSERT INTO manifests(stream, generation, uploaded_generation, last_uploaded_at_ms, last_uploaded_etag, last_uploaded_size_bytes)
     VALUES($1, $2, $3, $4, $5, $6)
     ON CONFLICT(stream) DO UPDATE SET
       generation = excluded.generation,
       uploaded_generation = excluded.uploaded_generation,
       last_uploaded_at_ms = excluded.last_uploaded_at_ms,
       last_uploaded_etag = excluded.last_uploaded_etag,
       last_uploaded_size_bytes = excluded.last_uploaded_size_bytes;`,
    [stream, generation, uploadedGeneration, uploadedAtMs == null ? null : pgInt(uploadedAtMs), etag, sizeBytes]
  );
}

export async function markPostgresSegmentUploaded(
  executor: PgExecutor,
  segmentId: string,
  etag: string,
  uploadedAtMs: bigint
): Promise<void> {
  await executor.query(`UPDATE segments SET r2_etag = $1, uploaded_at_ms = $2 WHERE segment_id = $3;`, [
    etag,
    pgInt(uploadedAtMs),
    segmentId,
  ]);
}

export async function setPostgresSchemaUploadedSizeBytes(
  executor: PgExecutor,
  nowMs: bigint,
  stream: string,
  sizeBytes: number
): Promise<void> {
  await executor.query(`UPDATE schemas SET uploaded_size_bytes = $1, updated_at_ms = $2 WHERE stream = $3;`, [
    sizeBytes,
    pgInt(nowMs),
    stream,
  ]);
}

function segmentSelectSql(whereSql: string): string {
  return `SELECT segment_id, stream, segment_index, start_offset, end_offset, block_count, last_append_ms,
                 payload_bytes, size_bytes, local_path, created_at_ms, uploaded_at_ms, r2_etag
          FROM segments ${whereSql};`;
}

function pgInt(value: bigint): string {
  return value.toString();
}

function toBigInt(value: unknown): bigint {
  return typeof value === "bigint" ? value : BigInt(value as any);
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  return new Uint8Array(value as ArrayBuffer);
}

function encodeU64Le(value: bigint): Buffer {
  const bytes = Buffer.alloc(8);
  bytes.writeBigUInt64LE(value);
  return bytes;
}

function encodeU32Le(value: number): Buffer {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32LE(value >>> 0);
  return bytes;
}

function manifestLockKey(stream: string): bigint {
  return createHash("sha256").update(`manifest:${stream}`).digest().readBigInt64BE(0);
}

function isRetryablePostgresPublicationError(error: unknown): boolean {
  const code = String((error as { code?: unknown })?.code ?? "");
  return code === "40001" || code === "40P01" || code === "55P03";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function coerceSegmentRow(row: any): SegmentRow {
  return {
    segment_id: String(row.segment_id),
    stream: String(row.stream),
    segment_index: Number(row.segment_index),
    start_offset: toBigInt(row.start_offset),
    end_offset: toBigInt(row.end_offset),
    block_count: Number(row.block_count),
    last_append_ms: toBigInt(row.last_append_ms),
    payload_bytes: toBigInt(row.payload_bytes),
    size_bytes: Number(row.size_bytes),
    local_path: String(row.local_path),
    created_at_ms: toBigInt(row.created_at_ms),
    uploaded_at_ms: row.uploaded_at_ms == null ? null : toBigInt(row.uploaded_at_ms),
    r2_etag: row.r2_etag == null ? null : String(row.r2_etag),
  };
}

function coerceSegmentMetaRow(row: any): SegmentMetaRow {
  return {
    stream: String(row.stream),
    segment_count: Number(row.segment_count),
    segment_offsets: toBytes(row.segment_offsets),
    segment_blocks: toBytes(row.segment_blocks),
    segment_last_ts: toBytes(row.segment_last_ts),
  };
}

function coerceManifestRow(row: any): ManifestRow {
  return {
    stream: String(row.stream),
    generation: Number(row.generation),
    uploaded_generation: Number(row.uploaded_generation),
    last_uploaded_at_ms: row.last_uploaded_at_ms == null ? null : toBigInt(row.last_uploaded_at_ms),
    last_uploaded_etag: row.last_uploaded_etag == null ? null : String(row.last_uploaded_etag),
    last_uploaded_size_bytes: row.last_uploaded_size_bytes == null ? null : toBigInt(row.last_uploaded_size_bytes),
  };
}
