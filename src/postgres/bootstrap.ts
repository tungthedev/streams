import { mkdirSync, rmSync } from "node:fs";
import { Pool, type PoolClient } from "pg";
import type { Config } from "../config";
import { bootstrapObjectStoreIntoRestoreStore } from "../bootstrap";
import type { ObjectStore } from "../objectstore/interface";
import type { BootstrapRestoreStore } from "../store/bootstrap_restore_store";
import type { ProfileTouchControlStore } from "../store/touch_store";
import type { IndexRunRow, LexiconIndexRunRow, SecondaryIndexRunRow } from "../store/rows";
import type { StreamReadRow } from "../store/segment_read_store";
import { migratePostgresStore } from "./schema";
import type { PgExecutor } from "./types";
import { dsError } from "../util/ds_error";
import {
  deletePostgresStreamProfile,
  restorePostgresStreamRow,
  upsertPostgresSchemaRegistry,
  upsertPostgresStreamProfile,
} from "./control_restore";
import {
  markPostgresSegmentUploaded,
  restorePostgresManifestRow,
  restorePostgresSegmentMeta,
  restorePostgresSegmentRow,
  setPostgresSchemaUploadedSizeBytes,
} from "./segments";
import { insertPostgresIndexRun, retirePostgresIndexRuns, upsertPostgresIndexState } from "./routing_index";
import {
  insertPostgresSecondaryIndexRun,
  retirePostgresSecondaryIndexRuns,
  upsertPostgresSecondaryIndexState,
} from "./secondary_index";
import {
  insertPostgresLexiconIndexRun,
  retirePostgresLexiconIndexRuns,
  upsertPostgresLexiconIndexState,
} from "./lexicon_index";
import { upsertPostgresSearchCompanionPlan, upsertPostgresSearchSegmentCompanion } from "./companions";
import { deletePostgresStreamTouchState, ensurePostgresStreamTouchStateFromStream } from "./touch";

export async function bootstrapPostgresFromR2(
  cfg: Config,
  objectStore: ObjectStore,
  connectionString: string,
  opts: { clearLocal?: boolean } = {}
): Promise<void> {
  if (opts.clearLocal !== false) {
    rmSync(`${cfg.rootDir}/local`, { recursive: true, force: true });
    rmSync(`${cfg.rootDir}/cache`, { recursive: true, force: true });
  }
  mkdirSync(cfg.rootDir, { recursive: true });

  const pool = new Pool({ connectionString });
  try {
    await migratePostgresStore(pool, { fullMode: true });
    if (opts.clearLocal !== false) await clearPostgresRestoreTarget(pool);
    else await assertPostgresRestoreTargetEmpty(pool);
    await bootstrapObjectStoreIntoRestoreStore(cfg, objectStore, new PostgresBootstrapRestoreStore(pool));
  } finally {
    await pool.end();
  }
}

async function clearPostgresRestoreTarget(executor: PgExecutor): Promise<void> {
  await executor.query(`TRUNCATE TABLE objectstore_request_counts, streams RESTART IDENTITY CASCADE;`);
}

async function assertPostgresRestoreTargetEmpty(executor: PgExecutor): Promise<void> {
  const res = await executor.query<{ count: string | number | bigint }>(
    `SELECT
       (SELECT COUNT(*) FROM streams) +
       (SELECT COUNT(*) FROM objectstore_request_counts) AS count;`
  );
  if (BigInt(res.rows[0]?.count ?? 0) !== 0n) {
    throw dsError("postgres bootstrap target is not empty; rerun with clearLocal enabled");
  }
}

class PostgresBootstrapRestoreStore implements BootstrapRestoreStore {
  private tx: PoolClient | null = null;

  readonly touch: ProfileTouchControlStore = {
    ensureStreamTouchState: (stream) => this.ensureStreamTouchState(stream),
    deleteStreamTouchState: (stream) => this.deleteStreamTouchState(stream),
  };

  constructor(private readonly pool: Pool) {}

  nowMs(): bigint {
    return BigInt(Date.now());
  }

  async close(): Promise<void> {
    await this.rollbackActiveTransaction();
  }

  async beginRestoreStream(_stream: string): Promise<void> {
    if (this.tx) throw dsError("postgres bootstrap restore transaction is already active");
    this.tx = await this.pool.connect();
    await this.tx.query("BEGIN");
  }

  async commitRestoreStream(_stream: string): Promise<void> {
    if (!this.tx) return;
    const client = this.tx;
    try {
      await client.query("COMMIT");
      this.tx = null;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      this.tx = null;
      throw error;
    } finally {
      client.release();
    }
  }

  async rollbackRestoreStream(_stream: string): Promise<void> {
    await this.rollbackActiveTransaction();
  }

  async restoreStreamRow(row: StreamReadRow): Promise<void> {
    await restorePostgresStreamRow(this.executor, row);
  }

  async upsertStreamProfile(stream: string, profileJson: string): Promise<void> {
    await upsertPostgresStreamProfile(this.executor, this.nowMs(), stream, profileJson);
  }

  async deleteStreamProfile(stream: string): Promise<void> {
    await deletePostgresStreamProfile(this.executor, stream);
  }

  async upsertSegmentMeta(stream: string, count: number, offsets: Uint8Array, blocks: Uint8Array, lastTs: Uint8Array): Promise<void> {
    await restorePostgresSegmentMeta(this.executor, stream, count, offsets, blocks, lastTs);
  }

  async upsertManifestRow(
    stream: string,
    generation: number,
    uploadedGeneration: number,
    uploadedAtMs: bigint | null,
    etag: string | null,
    sizeBytes: number | null
  ): Promise<void> {
    await restorePostgresManifestRow(this.executor, stream, generation, uploadedGeneration, uploadedAtMs, etag, sizeBytes);
  }

  async createSegmentRow(row: {
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
  }): Promise<void> {
    await restorePostgresSegmentRow(this.executor, this.nowMs(), row);
  }

  async markSegmentUploaded(segmentId: string, etag: string, uploadedAtMs: bigint): Promise<void> {
    await markPostgresSegmentUploaded(this.executor, segmentId, etag, uploadedAtMs);
  }

  async upsertIndexState(stream: string, indexSecret: Uint8Array, indexedThrough: number): Promise<void> {
    await upsertPostgresIndexState(this.executor, this.nowMs(), stream, indexSecret, indexedThrough);
  }

  async insertIndexRun(row: Omit<IndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await insertPostgresIndexRun(this.executor, row, { idempotent: true });
  }

  async retireIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    await retirePostgresIndexRuns(this.executor, runIds, retiredGen, retiredAtMs);
  }

  async upsertSecondaryIndexState(
    stream: string,
    indexName: string,
    indexSecret: Uint8Array,
    configHash: string,
    indexedThrough: number
  ): Promise<void> {
    await upsertPostgresSecondaryIndexState(this.executor, this.nowMs(), stream, indexName, indexSecret, configHash, indexedThrough);
  }

  async insertSecondaryIndexRun(row: Omit<SecondaryIndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await insertPostgresSecondaryIndexRun(this.executor, row, { idempotent: true });
  }

  async retireSecondaryIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    await retirePostgresSecondaryIndexRuns(this.executor, runIds, retiredGen, retiredAtMs);
  }

  async upsertLexiconIndexState(stream: string, sourceKind: string, sourceName: string, indexedThrough: number): Promise<void> {
    await upsertPostgresLexiconIndexState(this.executor, this.nowMs(), stream, sourceKind, sourceName, indexedThrough);
  }

  async insertLexiconIndexRun(row: Omit<LexiconIndexRunRow, "retired_gen" | "retired_at_ms">): Promise<void> {
    await insertPostgresLexiconIndexRun(this.executor, row, { idempotent: true });
  }

  async retireLexiconIndexRuns(runIds: string[], retiredGen: number, retiredAtMs: bigint): Promise<void> {
    await retirePostgresLexiconIndexRuns(this.executor, runIds, retiredGen, retiredAtMs);
  }

  async upsertSearchCompanionPlan(stream: string, generation: number, planHash: string, planJson: string): Promise<void> {
    await upsertPostgresSearchCompanionPlan(this.executor, this.nowMs(), stream, generation, planHash, planJson);
  }

  async upsertSearchSegmentCompanion(
    stream: string,
    segmentIndex: number,
    objectKey: string,
    planGeneration: number,
    sectionsJson: string,
    sectionSizesJson: string,
    sizeBytes: number,
    primaryTimestampMinMs: bigint | null,
    primaryTimestampMaxMs: bigint | null
  ): Promise<void> {
    await upsertPostgresSearchSegmentCompanion(
      this.executor,
      this.nowMs(),
      stream,
      segmentIndex,
      objectKey,
      planGeneration,
      sectionsJson,
      sectionSizesJson,
      sizeBytes,
      primaryTimestampMinMs,
      primaryTimestampMaxMs
    );
  }

  async upsertSchemaRegistry(stream: string, registryJson: string): Promise<void> {
    await upsertPostgresSchemaRegistry(this.executor, this.nowMs(), stream, registryJson);
  }

  async setSchemaUploadedSizeBytes(stream: string, sizeBytes: number): Promise<void> {
    await setPostgresSchemaUploadedSizeBytes(this.executor, this.nowMs(), stream, sizeBytes);
  }

  private async ensureStreamTouchState(stream: string): Promise<void> {
    await ensurePostgresStreamTouchStateFromStream(this.executor, this.nowMs(), stream);
  }

  private async deleteStreamTouchState(stream: string): Promise<void> {
    await deletePostgresStreamTouchState(this.executor, stream);
  }

  private get executor(): PgExecutor {
    return this.tx ?? this.pool;
  }

  private async rollbackActiveTransaction(): Promise<void> {
    if (!this.tx) return;
    const client = this.tx;
    this.tx = null;
    try {
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  }
}
