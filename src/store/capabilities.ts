import type { WalStore } from "./wal_store";
import type { StreamReadStore } from "./segment_read_store";
import type { SchemaStore } from "./schema_profile_store";
import type { ProfileStore } from "./schema_profile_store";
import { dsError } from "../util/ds_error";

export type MaybePromise<T> = T | Promise<T>;

export type DurableStoreCapabilities = {
  wal: true;
  schemas: true;
  profiles: true;
  streamLifecycle: true;
  segmentReads: boolean;
  indexes: boolean;
  manifests: boolean;
  objectStoreAccounting: boolean;
  storageStats: boolean;
  schemaPublication: boolean;
  builtinProfiles: boolean;
  internalMetrics: boolean;
  touch: boolean;
};

export interface StoreLifecycle {
  readonly kind: "sqlite" | "postgres";
  readonly capabilities: DurableStoreCapabilities;
  close(): MaybePromise<void>;
  nowMs(): bigint;
}

export interface StreamStore {
  getStream(stream: string): MaybePromise<StreamReadStoreRow | null>;
  ensureStream(stream: string, opts?: EnsureStreamOptions | null): MaybePromise<StreamReadStoreRow>;
  listStreams(limit: number, offset: number): MaybePromise<StreamReadStoreRow[]>;
  listExpiredStreams(limit: number): MaybePromise<string[]>;
  deleteStream(stream: string): MaybePromise<boolean>;
  hardDeleteStream(stream: string): MaybePromise<boolean>;
  isDeleted(row: StreamReadStoreRow): boolean;
}

export type StreamReadStoreRow = Awaited<ReturnType<StreamReadStore["getStreamForRead"]>> extends infer Row ? NonNullable<Row> : never;

export type EnsureStreamOptions = {
  contentType?: string | null;
  profile?: string | null;
  streamSeq?: string | null;
  closed?: boolean;
  ttlSeconds?: number | null;
  expiresAtMs?: bigint | null;
};

export interface WalControlPlaneStore extends StoreLifecycle, StreamStore, StreamReadStore, WalStore, SchemaStore, ProfileStore {}

export function requireWalControlPlaneStore(store: StoreLifecycle): WalControlPlaneStore {
  const candidate = store as Partial<WalControlPlaneStore>;
  if (
    store.capabilities.wal !== true ||
    store.capabilities.streamLifecycle !== true ||
    store.capabilities.schemas !== true ||
    store.capabilities.profiles !== true ||
    typeof candidate.getStream !== "function" ||
    typeof candidate.ensureStream !== "function" ||
    typeof candidate.listStreams !== "function" ||
    typeof candidate.listExpiredStreams !== "function" ||
    typeof candidate.deleteStream !== "function" ||
    typeof candidate.hardDeleteStream !== "function" ||
    typeof candidate.isDeleted !== "function" ||
    typeof candidate.close !== "function" ||
    typeof candidate.nowMs !== "function" ||
    typeof candidate.appendBatch !== "function" ||
    typeof candidate.readWalRange !== "function" ||
    typeof candidate.readWalRangeDesc !== "function" ||
    typeof candidate.getStreamForRead !== "function" ||
    typeof candidate.nowMsForRead !== "function" ||
    typeof candidate.getWalOldestTimestampMsForRead !== "function" ||
    typeof candidate.getSchemaRegistryForRead !== "function" ||
    typeof candidate.commitSchemaMetadataMutation !== "function" ||
    typeof candidate.getStreamProfileForRead !== "function" ||
    typeof candidate.commitProfileMetadataMutation !== "function"
  ) {
    throw dsError("store does not provide WAL/control-plane capabilities");
  }
  return candidate as WalControlPlaneStore;
}
