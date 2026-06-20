import { buildRequestObservabilityPairingDescriptor } from "../observe/pairing";
import { hashSecondaryIndexField } from "../index/secondary_schema";
import { buildDesiredSearchCompanionPlan, hashSearchCompanionPlan } from "../search/companion_plan";
import type { SchemaRegistry, SearchConfig } from "../schema/registry";
import type { StreamProfileResource, StreamProfileSpec } from "../profiles";
import type { SearchSegmentCompanionRow, StreamRow } from "../store/rows";
import type { FullModeDetailsSnapshot, FullModeDetailsStore } from "../store/full_mode_details_store";
import type { ObjectStoreAccountingStore, StorageStatsStore } from "../store/stats_accounting_store";
import { streamHash16Hex } from "../util/stream_paths";
import { dsError } from "../util/ds_error";

export type LocalStorageUsage = {
  segment_cache_bytes: number;
  routing_index_cache_bytes: number;
  exact_index_cache_bytes: number;
  lexicon_index_cache_bytes: number;
  companion_cache_bytes: number;
};

export type FullModeDetailsBuilderOptions = {
  detailsStore: FullModeDetailsStore;
  storageBackend?: "sqlite" | "postgres";
  storageStatsStore?: StorageStatsStore;
  objectStoreAccountingStore?: ObjectStoreAccountingStore;
  getLocalStorageUsage?: (stream: string) => Partial<LocalStorageUsage>;
};

export type FullModeDetailsMode = "details" | "index_status";

type ExactIndexStatus = {
  name: string;
  kind: string;
  indexed_segment_count: number;
  lag_segments: number;
  lag_ms: string | null;
  bytes_at_rest: string;
  object_count: number;
  active_run_count: number;
  retired_run_count: number;
  fully_indexed_uploaded_segments: boolean;
  stale_configuration: boolean;
  updated_at: string | null;
};

type SearchFamilyStatus = {
  family: "exact" | "col" | "fts" | "agg" | "mblk";
  fields: string[];
  plan_generation: number;
  covered_segment_count: number;
  contiguous_covered_segment_count: number;
  lag_segments: number;
  lag_ms: string | null;
  bytes_at_rest: string;
  object_count: number;
  stale_segment_count: number;
  fully_indexed_uploaded_segments: boolean;
  updated_at: string | null;
};

type ExactIndexCoverage = {
  name: string;
  kind: string;
  configHash: string;
  configMatches: boolean;
  indexedSegmentCount: number;
};

type SearchFamilyCoverage = {
  family: "exact" | "col" | "fts" | "agg" | "mblk";
  fields: string[];
  coveredSegmentCount: number;
  contiguousCoveredCount: number;
  familyBytes: bigint;
  familyObjectCount: number;
};

type LagLookup = Map<number, bigint>;

type IndexStatusPayload = {
  stream: string;
  profile: string;
  desired_index_plan_generation: number;
  segments: {
    total_count: number;
    uploaded_count: number;
  };
  manifest: {
    generation: number;
    uploaded_generation: number;
    last_uploaded_at: string | null;
    last_uploaded_etag: string | null;
    last_uploaded_size_bytes: string | null;
  };
  routing_key_index: {
    configured: boolean;
    indexed_segment_count: number;
    lag_segments: number;
    lag_ms: string | null;
    bytes_at_rest: string;
    object_count: number;
    active_run_count: number;
    retired_run_count: number;
    fully_indexed_uploaded_segments: boolean;
    updated_at: string | null;
  };
  routing_key_lexicon: {
    configured: boolean;
    indexed_segment_count: number;
    lag_segments: number;
    lag_ms: string | null;
    bytes_at_rest: string;
    object_count: number;
    active_run_count: number;
    retired_run_count: number;
    fully_indexed_uploaded_segments: boolean;
    updated_at: string | null;
  };
  exact_indexes: ExactIndexStatus[];
  bundled_companions: {
    object_count: number;
    bytes_at_rest: string;
    fully_indexed_uploaded_segments: boolean;
  };
  search_families: SearchFamilyStatus[];
};

type IndexStatusSnapshot = {
  payload: IndexStatusPayload;
  currentCompanionRows: SearchSegmentCompanionRow[];
};

function normalizeContentType(value: string | null): string | null {
  if (!value) return null;
  const base = value.split(";")[0]?.trim().toLowerCase();
  return base ? base : null;
}

function timestampToIsoString(value: bigint | null): string | null {
  return value == null ? null : new Date(Number(value)).toISOString();
}

function configuredExactIndexes(search: SearchConfig | undefined): Array<{ name: string; kind: string; configHash: string }> {
  if (!search) return [];
  return Object.entries(search.fields)
    .filter(([, field]) => field.exact === true && field.kind !== "text")
    .map(([name, field]) => ({
      name,
      kind: field.kind,
      configHash: hashSecondaryIndexField({ name, config: field }),
    }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function configuredSearchFamilies(search: SearchConfig | undefined): Array<{ family: "exact" | "col" | "fts" | "agg" | "mblk"; fields: string[] }> {
  if (!search) return [];
  const out: Array<{ family: "exact" | "col" | "fts" | "agg" | "mblk"; fields: string[] }> = [];
  const exactFields = Object.entries(search.fields)
    .filter(([, field]) => field.exact === true && field.kind !== "text")
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  if (exactFields.length > 0) out.push({ family: "exact", fields: exactFields });
  const colFields = Object.entries(search.fields)
    .filter(([, field]) => field.column === true)
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  if (colFields.length > 0) out.push({ family: "col", fields: colFields });
  const ftsFields = Object.entries(search.fields)
    .filter(([, field]) => field.kind === "text" || (field.kind === "keyword" && field.prefix === true))
    .map(([name]) => name)
    .sort((a, b) => a.localeCompare(b));
  if (ftsFields.length > 0) out.push({ family: "fts", fields: ftsFields });
  const aggRollups = Object.keys(search.rollups ?? {}).sort((a, b) => a.localeCompare(b));
  if (aggRollups.length > 0) out.push({ family: "agg", fields: aggRollups });
  if (search.profile === "metrics") out.push({ family: "mblk", fields: ["metrics"] });
  return out;
}

function parseCompanionSections(value: string): Set<string> {
  try {
    const parsed = JSON.parse(value);
    return new Set(Array.isArray(parsed) ? parsed.filter((entry) => typeof entry === "string") : []);
  } catch {
    return new Set();
  }
}

function parseCompanionSectionSizes(value: string): Record<string, number> {
  try {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object") return {};
    const out: Record<string, number> = {};
    for (const [key, raw] of Object.entries(parsed)) {
      if (typeof raw === "number" && Number.isFinite(raw) && raw >= 0) out[key] = raw;
    }
    return out;
  } catch {
    return {};
  }
}

function contiguousCoveredSegmentCount(rows: Array<{ segment_index: number; sections_json: string }>, family: string): number {
  let expected = 0;
  for (const row of rows) {
    if (row.segment_index < expected) continue;
    if (row.segment_index > expected) break;
    if (!parseCompanionSections(row.sections_json).has(family)) break;
    expected += 1;
  }
  return expected;
}

export class FullModeDetailsBuilder {
  constructor(private readonly opts: FullModeDetailsBuilderOptions) {}

  async buildPayload(args: {
    stream: string;
    row: StreamRow;
    registry: SchemaRegistry;
    profileResource: StreamProfileResource;
    mode: FullModeDetailsMode;
  }): Promise<Record<string, unknown>> {
    const profileKind = args.profileResource.profile.kind;
    const configuredExact = configuredExactIndexes(args.registry.search);
    const snapshot = await this.opts.detailsStore.getFullModeDetailsSnapshot({
      stream: args.stream,
      exactIndexNames: configuredExact.map((entry) => entry.name),
    });
    const indexStatus = await this.buildIndexStatus(args.stream, args.row, args.registry, profileKind, snapshot, configuredExact);
    if (args.mode === "index_status") return indexStatus.payload;
    return {
      stream: this.buildStreamSummary(args.stream, args.row, args.profileResource.profile, snapshot),
      profile: args.profileResource,
      schema: args.registry,
      index_status: indexStatus.payload,
      storage: await this.buildStorageBreakdown(args.stream, args.row, snapshot, indexStatus.currentCompanionRows, indexStatus.payload),
      object_store_requests: await this.buildObjectStoreRequestSummary(args.stream),
    };
  }

  private buildStreamSummary(stream: string, row: StreamRow, profile: StreamProfileSpec, snapshot: FullModeDetailsSnapshot) {
    const observability = buildRequestObservabilityPairingDescriptor(stream, profile);
    return {
      name: stream,
      content_type: normalizeContentType(row.content_type) ?? row.content_type,
      profile: profile.kind,
      ...(observability ? { observability } : {}),
      created_at: timestampToIsoString(row.created_at_ms),
      updated_at: timestampToIsoString(row.updated_at_ms),
      expires_at: timestampToIsoString(row.expires_at_ms),
      ttl_seconds: row.ttl_seconds,
      stream_seq: row.stream_seq,
      closed: row.closed !== 0,
      epoch: row.epoch,
      next_offset: row.next_offset.toString(),
      sealed_through: row.sealed_through.toString(),
      uploaded_through: row.uploaded_through.toString(),
      segment_count: snapshot.segmentCount,
      uploaded_segment_count: snapshot.uploadedSegmentCount,
      pending_rows: row.pending_rows.toString(),
      pending_bytes: row.pending_bytes.toString(),
      total_size_bytes: row.logical_size_bytes.toString(),
      wal_rows: row.wal_rows.toString(),
      wal_bytes: row.wal_bytes.toString(),
      last_append_at: timestampToIsoString(row.last_append_ms),
      last_segment_cut_at: timestampToIsoString(row.last_segment_cut_ms),
    };
  }

  private buildIndexLagMsFromLookup(lagLookup: LagLookup, headRow: StreamRow, coveredSegmentCount: number): string | null {
    if (coveredSegmentCount <= 0) return null;
    const coveredLastAppendMs = lagLookup.get(coveredSegmentCount - 1) ?? null;
    if (coveredLastAppendMs == null) return null;
    const lagMs = headRow.last_append_ms > coveredLastAppendMs ? headRow.last_append_ms - coveredLastAppendMs : 0n;
    return lagMs.toString();
  }

  private async buildStorageBreakdown(
    stream: string,
    row: StreamRow,
    snapshot: FullModeDetailsSnapshot,
    currentCompanionRows: Array<{
      sections_json: string;
      section_sizes_json: string;
      size_bytes: number;
    }>,
    indexStatus: IndexStatusPayload
  ) {
    const manifest = snapshot.manifest;
    const schemaRow = snapshot.schemaRow;
    const uploadedSegmentBytes = snapshot.uploadedSegmentBytes;
    const pendingSealedSegmentBytes = snapshot.pendingSealedSegmentBytes;
    const routingIndexStorage = snapshot.routingIndexStorage;
    const routingLexiconStorage =
      snapshot.lexiconIndexStorage
        .find((entry) => entry.source_kind === "routing_key" && entry.source_name === "") ?? { object_count: 0, bytes: 0n };
    const companionStorage = snapshot.bundledCompanionStorage;
    const localStorageUsage: LocalStorageUsage = {
      segment_cache_bytes: 0,
      routing_index_cache_bytes: 0,
      exact_index_cache_bytes: 0,
      lexicon_index_cache_bytes: 0,
      companion_cache_bytes: 0,
      ...(this.opts.getLocalStorageUsage?.(stream) ?? {}),
    };
    if (!this.opts.storageStatsStore) throw dsError("storage stats store is not available");
    const sharedBytes = BigInt((await this.opts.storageStatsStore.getWalDbSizeBytes()) + (await this.opts.storageStatsStore.getMetaDbSizeBytes()));
    const sharedDbStorage = {
      shared_db_total_bytes: sharedBytes.toString(),
      ...(this.opts.storageBackend === "postgres"
        ? { postgres_shared_total_bytes: sharedBytes.toString() }
        : { sqlite_shared_total_bytes: sharedBytes.toString() }),
    };
    const exactIndexBytes = indexStatus.exact_indexes.reduce((sum: bigint, entry) => sum + BigInt(entry.bytes_at_rest), 0n);
    const familyBytes = new Map<string, bigint>();
    for (const row of currentCompanionRows) {
      const sizes = parseCompanionSectionSizes(row.section_sizes_json);
      for (const [kind, size] of Object.entries(sizes)) {
        familyBytes.set(kind, (familyBytes.get(kind) ?? 0n) + BigInt(size));
      }
    }
    return {
      object_storage: {
        total_bytes: (
          uploadedSegmentBytes +
          routingIndexStorage.bytes +
          routingLexiconStorage.bytes +
          exactIndexBytes +
          companionStorage.bytes +
          (manifest.last_uploaded_size_bytes ?? 0n) +
          (schemaRow?.uploaded_size_bytes ?? 0n)
        ).toString(),
        segments_bytes: uploadedSegmentBytes.toString(),
        indexes_bytes: (routingIndexStorage.bytes + routingLexiconStorage.bytes + exactIndexBytes + companionStorage.bytes).toString(),
        manifest_and_meta_bytes: ((manifest.last_uploaded_size_bytes ?? 0n) + (schemaRow?.uploaded_size_bytes ?? 0n)).toString(),
        manifest_bytes: (manifest.last_uploaded_size_bytes ?? 0n).toString(),
        schema_registry_bytes: (schemaRow?.uploaded_size_bytes ?? 0n).toString(),
        segment_object_count: indexStatus.segments.uploaded_count,
        routing_index_object_count: routingIndexStorage.object_count,
        routing_lexicon_object_count: routingLexiconStorage.object_count,
        exact_index_object_count: indexStatus.exact_indexes.reduce((sum: number, entry) => sum + entry.object_count, 0),
        bundled_companion_object_count: companionStorage.object_count,
      },
      local_storage: {
        total_bytes: (
          row.wal_bytes +
          pendingSealedSegmentBytes +
          BigInt(localStorageUsage.segment_cache_bytes) +
          BigInt(localStorageUsage.routing_index_cache_bytes) +
          BigInt(localStorageUsage.exact_index_cache_bytes) +
          BigInt(localStorageUsage.lexicon_index_cache_bytes) +
          BigInt(localStorageUsage.companion_cache_bytes)
        ).toString(),
        wal_retained_bytes: row.wal_bytes.toString(),
        pending_tail_bytes: row.pending_bytes.toString(),
        pending_sealed_segment_bytes: pendingSealedSegmentBytes.toString(),
        segment_cache_bytes: String(localStorageUsage.segment_cache_bytes),
        routing_index_cache_bytes: String(localStorageUsage.routing_index_cache_bytes),
        exact_index_cache_bytes: String(localStorageUsage.exact_index_cache_bytes),
        lexicon_index_cache_bytes: String(localStorageUsage.lexicon_index_cache_bytes),
        companion_cache_bytes: String(localStorageUsage.companion_cache_bytes),
        ...sharedDbStorage,
      },
      companion_families: {
        exact_bytes: String(familyBytes.get("exact") ?? 0n),
        col_bytes: String(familyBytes.get("col") ?? 0n),
        fts_bytes: String(familyBytes.get("fts") ?? 0n),
        agg_bytes: String(familyBytes.get("agg") ?? 0n),
        mblk_bytes: String(familyBytes.get("mblk") ?? 0n),
      },
    };
  }

  private async buildObjectStoreRequestSummary(stream: string) {
    if (!this.opts.objectStoreAccountingStore) throw dsError("object-store accounting store is not available");
    const summary = await this.opts.objectStoreAccountingStore.getObjectStoreRequestSummaryByHash(streamHash16Hex(stream));
    return {
      puts: summary.puts.toString(),
      reads: summary.reads.toString(),
      gets: summary.gets.toString(),
      heads: summary.heads.toString(),
      lists: summary.lists.toString(),
      deletes: summary.deletes.toString(),
      by_artifact: summary.by_artifact.map((entry) => ({
        artifact: entry.artifact,
        puts: entry.puts.toString(),
        gets: entry.gets.toString(),
        heads: entry.heads.toString(),
        lists: entry.lists.toString(),
        deletes: entry.deletes.toString(),
        reads: entry.reads.toString(),
      })),
    };
  }

  private async buildIndexStatus(
    stream: string,
    row: StreamRow,
    reg: SchemaRegistry,
    profileKind: string,
    snapshot: FullModeDetailsSnapshot,
    configuredExact: Array<{ name: string; kind: string; configHash: string }>
  ): Promise<IndexStatusSnapshot> {
    const segmentCount = snapshot.segmentCount;
    const uploadedSegmentCount = snapshot.uploadedSegmentCount;
    const manifest = snapshot.manifest;

    const routingState = snapshot.routingState;
    const routingRuns = snapshot.routingRuns;
    const retiredRoutingRuns = snapshot.retiredRoutingRuns;
    const routingStorage = snapshot.routingIndexStorage;
    const routingLexiconState = snapshot.routingLexiconState;
    const routingLexiconRuns = snapshot.routingLexiconRuns;
    const retiredRoutingLexiconRuns = snapshot.retiredRoutingLexiconRuns;
    const routingLexiconStorage =
      snapshot.lexiconIndexStorage
        .find((entry) => entry.source_kind === "routing_key" && entry.source_name === "") ?? { object_count: 0, bytes: 0n };
    const secondaryIndexStorage = new Map(snapshot.secondaryIndexStorage.map((entry) => [entry.index_name, entry]));
    const exactSnapshots = new Map(snapshot.exactIndexes.map((entry) => [entry.indexName, entry]));

    const exactCoverages: ExactIndexCoverage[] = [];
    for (const { name, kind, configHash } of configuredExact) {
      const exactSnapshot = exactSnapshots.get(name);
      const state = exactSnapshot?.state ?? null;
      const configMatches = state?.config_hash === configHash;
      const indexedSegmentCount = configMatches ? (state?.indexed_through ?? 0) : 0;
      exactCoverages.push({ name, kind, configHash, configMatches, indexedSegmentCount });
    }

    const desiredCompanionPlan = buildDesiredSearchCompanionPlan(reg);
    const desiredCompanionHash = hashSearchCompanionPlan(desiredCompanionPlan);
    const companionPlanRow = snapshot.companionPlan;
    const desiredIndexPlanGeneration =
      Object.values(desiredCompanionPlan.families).some(Boolean)
        ? companionPlanRow
          ? companionPlanRow.plan_hash === desiredCompanionHash
            ? companionPlanRow.generation
            : companionPlanRow.generation + 1
          : 1
        : 0;
    const companionRows = snapshot.companionRows;
    const currentCompanionRows = companionRows.filter((row) => row.plan_generation === desiredIndexPlanGeneration);
    const currentCompanionBytes = currentCompanionRows.reduce((sum, entry) => sum + BigInt(entry.size_bytes), 0n);
    const searchFamilyCoverages: SearchFamilyCoverage[] = [];
    for (const { family, fields } of configuredSearchFamilies(reg.search)) {
      const coveredSegmentCount = currentCompanionRows.filter((row) => parseCompanionSections(row.sections_json).has(family)).length;
      const contiguousCoveredCount = contiguousCoveredSegmentCount(currentCompanionRows, family);
      let familyBytes = 0n;
      let familyObjectCount = 0;
      for (const row of currentCompanionRows) {
        const size = parseCompanionSectionSizes(row.section_sizes_json)[family];
        if (size == null) continue;
        familyBytes += BigInt(size);
        familyObjectCount += 1;
      }
      searchFamilyCoverages.push({
        family,
        fields,
        coveredSegmentCount,
        contiguousCoveredCount,
        familyBytes,
        familyObjectCount,
      });
    }

    const lagSegmentIndexes = new Set<number>();
    const addLagPoint = (coveredSegmentCount: number): void => {
      if (coveredSegmentCount > 0) lagSegmentIndexes.add(coveredSegmentCount - 1);
    };
    addLagPoint(routingState?.indexed_through ?? 0);
    addLagPoint(routingLexiconState?.indexed_through ?? 0);
    for (const coverage of exactCoverages) addLagPoint(coverage.indexedSegmentCount);
    for (const coverage of searchFamilyCoverages) addLagPoint(coverage.contiguousCoveredCount);
    const lagLookup = await this.opts.detailsStore.getFullModeLagSnapshot({
      stream,
      segmentIndexes: Array.from(lagSegmentIndexes),
    });

    const exactIndexes: ExactIndexStatus[] = [];
    for (const { name, kind, configMatches, indexedSegmentCount } of exactCoverages) {
      const exactSnapshot = exactSnapshots.get(name);
      const state = exactSnapshot?.state ?? null;
      const storage = secondaryIndexStorage.get(name);
      exactIndexes.push({
        name,
        kind,
        indexed_segment_count: indexedSegmentCount,
        lag_segments: Math.max(0, uploadedSegmentCount - indexedSegmentCount),
        lag_ms: this.buildIndexLagMsFromLookup(lagLookup, row, indexedSegmentCount),
        bytes_at_rest: String(storage?.bytes ?? 0n),
        object_count: storage?.object_count ?? 0,
        active_run_count: exactSnapshot?.activeRuns.length ?? 0,
        retired_run_count: exactSnapshot?.retiredRuns.length ?? 0,
        fully_indexed_uploaded_segments: configMatches && indexedSegmentCount >= uploadedSegmentCount,
        stale_configuration: !configMatches,
        updated_at: timestampToIsoString(state?.updated_at_ms ?? null),
      });
    }

    const searchFamilies: SearchFamilyStatus[] = [];
    for (const { family, fields, coveredSegmentCount, contiguousCoveredCount, familyBytes, familyObjectCount } of searchFamilyCoverages) {
      searchFamilies.push({
        family,
        fields,
        plan_generation: desiredIndexPlanGeneration,
        covered_segment_count: coveredSegmentCount,
        contiguous_covered_segment_count: contiguousCoveredCount,
        lag_segments: Math.max(0, uploadedSegmentCount - contiguousCoveredCount),
        lag_ms: this.buildIndexLagMsFromLookup(lagLookup, row, contiguousCoveredCount),
        bytes_at_rest: familyBytes.toString(),
        object_count: familyObjectCount,
        stale_segment_count: Math.max(0, uploadedSegmentCount - coveredSegmentCount),
        fully_indexed_uploaded_segments: coveredSegmentCount >= uploadedSegmentCount,
        updated_at: timestampToIsoString(companionPlanRow?.updated_at_ms ?? null),
      });
    }

    return {
      currentCompanionRows,
      payload: {
        stream,
        profile: profileKind,
        desired_index_plan_generation: desiredIndexPlanGeneration,
        segments: {
          total_count: segmentCount,
          uploaded_count: uploadedSegmentCount,
        },
        manifest: {
          generation: manifest.generation,
          uploaded_generation: manifest.uploaded_generation,
          last_uploaded_at: timestampToIsoString(manifest.last_uploaded_at_ms),
          last_uploaded_etag: manifest.last_uploaded_etag,
          last_uploaded_size_bytes: manifest.last_uploaded_size_bytes?.toString() ?? null,
        },
        routing_key_index: {
          configured: reg.routingKey != null,
          indexed_segment_count: routingState?.indexed_through ?? 0,
          lag_segments: Math.max(0, uploadedSegmentCount - (routingState?.indexed_through ?? 0)),
          lag_ms: this.buildIndexLagMsFromLookup(lagLookup, row, routingState?.indexed_through ?? 0),
          bytes_at_rest: routingStorage.bytes.toString(),
          object_count: routingStorage.object_count,
          active_run_count: routingRuns.length,
          retired_run_count: retiredRoutingRuns.length,
          fully_indexed_uploaded_segments: reg.routingKey == null ? true : (routingState?.indexed_through ?? 0) >= uploadedSegmentCount,
          updated_at: timestampToIsoString(routingState?.updated_at_ms ?? null),
        },
        routing_key_lexicon: {
          configured: reg.routingKey != null,
          indexed_segment_count: routingLexiconState?.indexed_through ?? 0,
          lag_segments: Math.max(0, uploadedSegmentCount - (routingLexiconState?.indexed_through ?? 0)),
          lag_ms: this.buildIndexLagMsFromLookup(lagLookup, row, routingLexiconState?.indexed_through ?? 0),
          bytes_at_rest: routingLexiconStorage.bytes.toString(),
          object_count: routingLexiconStorage.object_count,
          active_run_count: routingLexiconRuns.length,
          retired_run_count: retiredRoutingLexiconRuns.length,
          fully_indexed_uploaded_segments: reg.routingKey == null ? true : (routingLexiconState?.indexed_through ?? 0) >= uploadedSegmentCount,
          updated_at: timestampToIsoString(routingLexiconState?.updated_at_ms ?? null),
        },
        exact_indexes: exactIndexes,
        bundled_companions: {
          object_count: currentCompanionRows.length,
          bytes_at_rest: currentCompanionBytes.toString(),
          fully_indexed_uploaded_segments: currentCompanionRows.length >= uploadedSegmentCount,
        },
        search_families: searchFamilies,
      },
    };
  }
}
