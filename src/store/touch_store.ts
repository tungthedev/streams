import type { StreamRow } from "./rows";
import type { MaybePromise } from "./capabilities";
import type { WalReadRow } from "./wal_store";

export type StreamTouchStateRow = {
  stream: string;
  processed_through: bigint;
  updated_at_ms: bigint;
};

export type LiveTemplateStoreRow = {
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

export type LiveTemplateIdentityRow = {
  template_id: string;
  entity: string;
  fields_json: string;
  encodings_json?: string;
  last_seen_at_ms: bigint;
  inactivity_ttl_ms?: bigint;
};

export type LiveTemplateLastSeenUpdate = {
  stream: string;
  templateId: string;
  lastSeenAtMs: number;
};

export type LiveTemplateActivationInput = {
  templateId: string;
  entity: string;
  fieldsJson: string;
  encodingsJson: string;
  nowMs: number;
  inactivityTtlMs: number;
  activeFromSourceOffset: bigint;
};

export type LiveTemplateActivationResult = {
  activated: string[];
  invalid: string[];
  rateLimited: string[];
  activationTokensUsed: number;
  evicted: Array<{ templateId: string; reason: "cap_exceeded"; cap: number }>;
};

export interface ProfileTouchControlStore {
  ensureStreamTouchState(stream: string): MaybePromise<void>;
  deleteStreamTouchState(stream: string): MaybePromise<void>;
}

export interface TouchRouteStore {
  countActiveLiveTemplates(stream: string): MaybePromise<number>;
  getStreamTouchState(stream: string): MaybePromise<StreamTouchStateRow | null>;
}

export interface LiveTemplateStore {
  activateLiveTemplates(args: {
    stream: string;
    templates: LiveTemplateActivationInput[];
    maxActiveTemplatesPerStream: number;
    maxActiveTemplatesPerEntity: number;
    maxActivationTokens: number;
  }): MaybePromise<LiveTemplateActivationResult>;
  countActiveLiveTemplates(stream: string): MaybePromise<number>;
  listActiveLiveTemplates(stream: string): MaybePromise<LiveTemplateStoreRow[]>;
  updateLiveTemplateLastSeenBatch(updates: LiveTemplateLastSeenUpdate[]): MaybePromise<void>;
  listExpiredLiveTemplates(stream: string, nowMs: number, limit: number): MaybePromise<LiveTemplateIdentityRow[]>;
  retireLiveTemplatesForInactivity(stream: string, templateIds: string[], nowMs: number): MaybePromise<void>;
  listActiveLiveTemplateEntitiesByIds(stream: string, templateIds: string[]): MaybePromise<string[]>;
}

export interface TouchProcessorStore extends ProfileTouchControlStore, TouchRouteStore, LiveTemplateStore {
  nowMs(): bigint;
  getStream(stream: string): MaybePromise<StreamRow | null>;
  ensureStream(stream: string, opts?: { contentType?: string; streamFlags?: number }): MaybePromise<StreamRow>;
  addStreamFlags(stream: string, flags: number): MaybePromise<void>;
  isDeleted(row: StreamRow): boolean;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  listStreamTouchStates(): MaybePromise<StreamTouchStateRow[]>;
  listStreamsByProfile(kind: string): MaybePromise<string[]>;
  updateStreamTouchStateThrough(stream: string, processedThrough: bigint): MaybePromise<void>;
  deleteWalThrough(stream: string, uploadedThrough: bigint): MaybePromise<{ deletedRows: number; deletedBytes: number }>;
  getWalOldestOffset(stream: string): MaybePromise<bigint | null>;
  trimWalByAge(stream: string, maxAgeMs: number): MaybePromise<{ trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null }>;
}
