import type { StreamRow } from "./rows";
import type { ProfileTouchStateStore } from "./profile_touch_store";
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

export interface ProfileTouchControlStore {
  ensureStreamTouchState(stream: string): void;
  deleteStreamTouchState(stream: string): void;
}

export interface TouchRouteStore {
  countActiveLiveTemplates(stream: string): number;
  getStreamTouchState(stream: string): StreamTouchStateRow | null;
}

export interface LiveTemplateStore {
  countActiveLiveTemplates(stream: string): number;
  listActiveLiveTemplates(stream: string): LiveTemplateStoreRow[];
  getLiveTemplate(stream: string, templateId: string): LiveTemplateStoreRow | null;
  updateLiveTemplateHeartbeat(stream: string, templateId: string, nowMs: number, inactivityTtlMs: number): void;
  reactivateLiveTemplate(stream: string, templateId: string, nowMs: number, inactivityTtlMs: number, activeFromSourceOffset: bigint): void;
  insertLiveTemplate(args: {
    stream: string;
    templateId: string;
    entity: string;
    fieldsJson: string;
    encodingsJson: string;
    nowMs: number;
    inactivityTtlMs: number;
    activeFromSourceOffset: bigint;
  }): void;
  updateLiveTemplateLastSeen(stream: string, templateId: string, lastSeenAtMs: number): void;
  updateLiveTemplateLastSeenBatch(updates: LiveTemplateLastSeenUpdate[]): void;
  listExpiredLiveTemplates(stream: string, nowMs: number, limit: number): LiveTemplateIdentityRow[];
  retireLiveTemplateForInactivity(stream: string, templateId: string, nowMs: number): void;
  retireLiveTemplatesForInactivity(stream: string, templateIds: string[], nowMs: number): void;
  listActiveLiveTemplateEntityCounts(stream: string): Array<{ entity: string; count: number }>;
  listLiveTemplateLruIds(args: {
    stream: string;
    entity?: string;
    excludeTemplateIds?: string[];
    limit: number;
  }): string[];
  retireLiveTemplateForCap(stream: string, templateId: string, nowMs: number): void;
  retireLiveTemplatesForCap(stream: string, templateIds: string[], nowMs: number): void;
  listActiveLiveTemplateEntitiesByIds(stream: string, templateIds: string[]): string[];
}

export interface TouchProcessorStore extends ProfileTouchStateStore, ProfileTouchControlStore, TouchRouteStore, LiveTemplateStore {
  nowMs(): bigint;
  getStream(stream: string): StreamRow | null;
  ensureStream(stream: string, opts?: { contentType?: string; streamFlags?: number }): StreamRow;
  addStreamFlags(stream: string, flags: number): void;
  isDeleted(row: StreamRow): boolean;
  readWalRange(stream: string, startOffset: bigint, endOffset: bigint, routingKey?: Uint8Array): AsyncIterable<WalReadRow>;
  listStreamTouchStates(): StreamTouchStateRow[];
  listStreamsByProfile(kind: string): string[];
  updateStreamTouchStateThrough(stream: string, processedThrough: bigint): void;
  deleteWalThrough(stream: string, uploadedThrough: bigint): { deletedRows: number; deletedBytes: number };
  getWalOldestOffset(stream: string): bigint | null;
  trimWalByAge(stream: string, maxAgeMs: number): { trimmedRows: number; trimmedBytes: number; keptFromOffset: bigint | null };
}
