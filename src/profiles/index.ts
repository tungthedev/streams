import { Result } from "better-result";
import type { SchemaRegistry } from "../schema/registry";
import type { StreamReadRow as StreamRow, StreamReadStore } from "../store/segment_read_store";
import type { ProfileStore } from "../store/schema_profile_store";
import { LruCache } from "../util/lru";
import { dsError } from "../util/ds_error.ts";
import { GENERIC_STREAM_PROFILE_DEFINITION } from "./generic";
import { EVLOG_STREAM_PROFILE_DEFINITION } from "./evlog";
import { METRICS_STREAM_PROFILE_DEFINITION } from "./metrics";
import { OTEL_TRACES_STREAM_PROFILE_DEFINITION } from "./otelTraces";
import {
  buildStreamProfileResource,
  cloneStreamProfileSpec,
  DEFAULT_STREAM_PROFILE,
  parseProfileUpdateEnvelopeResult,
  readProfileKindResult,
  type CachedStreamProfile,
  type StoredProfileRow,
  type StreamProfileJsonIngestCapability,
  type StreamProfileOtlpTracesCapability,
  type StreamProfileCorrelationCapability,
  type StreamProfileDefinition,
  type StreamProfileMetricsCapability,
  type StreamProfilePersistResult,
  type StreamProfileReadError,
  type StreamProfileResource,
  type StreamProfileSpec,
  type StreamProfileMutationError,
  type StreamTouchCapability,
} from "./profile";
import { STATE_PROTOCOL_STREAM_PROFILE_DEFINITION } from "./stateProtocol";

export * from "./profile";
export { EVLOG_STREAM_PROFILE_DEFINITION } from "./evlog";
export { GENERIC_STREAM_PROFILE_DEFINITION } from "./generic";
export { METRICS_STREAM_PROFILE_DEFINITION } from "./metrics";
export { OTEL_TRACES_STREAM_PROFILE_DEFINITION } from "./otelTraces";
export { STATE_PROTOCOL_STREAM_PROFILE_DEFINITION } from "./stateProtocol";

const STREAM_PROFILE_DEFINITIONS: Record<string, StreamProfileDefinition> = {
  [EVLOG_STREAM_PROFILE_DEFINITION.kind]: EVLOG_STREAM_PROFILE_DEFINITION,
  [GENERIC_STREAM_PROFILE_DEFINITION.kind]: GENERIC_STREAM_PROFILE_DEFINITION,
  [METRICS_STREAM_PROFILE_DEFINITION.kind]: METRICS_STREAM_PROFILE_DEFINITION,
  [OTEL_TRACES_STREAM_PROFILE_DEFINITION.kind]: OTEL_TRACES_STREAM_PROFILE_DEFINITION,
  [STATE_PROTOCOL_STREAM_PROFILE_DEFINITION.kind]: STATE_PROTOCOL_STREAM_PROFILE_DEFINITION,
};
// New built-in profiles are wired here. Core runtime paths must resolve the
// definition and dispatch through its hooks rather than branching on profile
// kinds directly.

function supportedProfileKindsMessage(): string {
  return listSupportedStreamProfileKinds().join("|");
}

export function listSupportedStreamProfileKinds(): string[] {
  return Object.keys(STREAM_PROFILE_DEFINITIONS);
}

export function resolveStreamProfileDefinition(kind: string | null | undefined): StreamProfileDefinition | null {
  const normalized = typeof kind === "string" && kind !== "" ? kind : DEFAULT_STREAM_PROFILE;
  return STREAM_PROFILE_DEFINITIONS[normalized] ?? null;
}

function resolveStreamProfileDefinitionResult(
  kind: string | null | undefined
): Result<StreamProfileDefinition, StreamProfileReadError> {
  const normalized = typeof kind === "string" && kind !== "" ? kind : DEFAULT_STREAM_PROFILE;
  const definition = resolveStreamProfileDefinition(normalized);
  if (!definition) {
    return Result.err({ kind: "invalid_profile", message: `unknown stream profile: ${normalized}` });
  }
  return Result.ok(definition);
}

function cloneCachedProfile(cache: CachedStreamProfile | null): CachedStreamProfile | null {
  if (!cache) return null;
  return {
    profile: cloneStreamProfileSpec(cache.profile),
    updatedAtMs: cache.updatedAtMs,
  };
}

export function parseProfileUpdateResult(body: unknown): Result<StreamProfileSpec, { message: string }> {
  const envelopeRes = parseProfileUpdateEnvelopeResult(body);
  if (Result.isError(envelopeRes)) return envelopeRes;
  const kindRes = readProfileKindResult(envelopeRes.value, "profile");
  if (Result.isError(kindRes)) return kindRes;
  const definition = resolveStreamProfileDefinition(kindRes.value);
  if (!definition) {
    return Result.err({ message: `profile.kind must be ${supportedProfileKindsMessage()}` });
  }
  return definition.validateResult(envelopeRes.value, "profile");
}

export function resolveTouchCapability(profile: StreamProfileSpec | null | undefined): StreamTouchCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.touch ?? null;
}

export function resolveJsonIngestCapability(profile: StreamProfileSpec | null | undefined): StreamProfileJsonIngestCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.jsonIngest ?? null;
}

export function resolveMetricsCapability(profile: StreamProfileSpec | null | undefined): StreamProfileMetricsCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.metrics ?? null;
}

export function resolveOtlpTracesCapability(profile: StreamProfileSpec | null | undefined): StreamProfileOtlpTracesCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.otlpTraces ?? null;
}

export function resolveCorrelationCapability(profile: StreamProfileSpec | null | undefined): StreamProfileCorrelationCapability | null {
  if (!profile) return null;
  return resolveStreamProfileDefinition(profile.kind)?.correlation ?? null;
}

export function resolveEnabledTouchCapability(
  profile: StreamProfileSpec | null | undefined
): { capability: StreamTouchCapability; touchCfg: NonNullable<ReturnType<StreamTouchCapability["getTouchConfig"]>> } | null {
  const capability = resolveTouchCapability(profile);
  if (!profile || !capability) return null;
  const touchCfg = capability.getTouchConfig(profile);
  if (!touchCfg) return null;
  return { capability, touchCfg };
}

export function listTouchCapableProfileKinds(): string[] {
  return Object.values(STREAM_PROFILE_DEFINITIONS)
    .filter((definition) => !!definition.touch)
    .map((definition) => definition.kind);
}

export type StreamProfileUpdateResult = {
  resource: StreamProfileResource;
  schemaRegistry: SchemaRegistry | null;
};

export class StreamProfileStore {
  private readonly store: ProfileStore & StreamReadStore;
  private readonly touchEnabled: boolean;
  private readonly cache: LruCache<string, CachedStreamProfile>;

  constructor(store: ProfileStore & StreamReadStore, opts?: { cacheEntries?: number; touchEnabled?: boolean }) {
    this.store = store;
    this.touchEnabled = opts?.touchEnabled === true;
    this.cache = new LruCache(opts?.cacheEntries ?? 1024);
  }

  private async loadRow(stream: string): Promise<StoredProfileRow | null> {
    return this.store.getStreamProfileForRead(stream);
  }

  async getProfile(stream: string, streamRow?: StreamRow | null): Promise<StreamProfileSpec> {
    const res = await this.getProfileResult(stream, streamRow);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  async getProfileResult(stream: string, streamRow?: StreamRow | null): Promise<Result<StreamProfileSpec, StreamProfileReadError>> {
    const srow = streamRow ?? await this.store.getStreamForRead(stream);
    if (!srow) return Result.ok(GENERIC_STREAM_PROFILE_DEFINITION.defaultProfile());

    const definitionRes = resolveStreamProfileDefinitionResult(srow.profile);
    if (Result.isError(definitionRes)) return definitionRes;

    const row = definitionRes.value.usesStoredProfileRow ? await this.loadRow(stream) : null;
    const cached = cloneCachedProfile(this.cache.get(stream) ?? null);
    const readRes = definitionRes.value.readProfileResult({
      row,
      cached: cached && cached.profile.kind === definitionRes.value.kind ? cached : null,
    });
    if (Result.isError(readRes)) {
      return Result.err({ kind: "invalid_profile", message: readRes.error.message });
    }

    if (readRes.value.cache) this.cache.set(stream, cloneCachedProfile(readRes.value.cache)!);
    else this.cache.delete(stream);
    return Result.ok(cloneStreamProfileSpec(readRes.value.profile));
  }

  async getProfileResource(stream: string, streamRow?: StreamRow | null): Promise<StreamProfileResource> {
    const res = await this.getProfileResourceResult(stream, streamRow);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value;
  }

  async getProfileResourceResult(stream: string, streamRow?: StreamRow | null): Promise<Result<StreamProfileResource, StreamProfileReadError>> {
    const profileRes = await this.getProfileResult(stream, streamRow);
    if (Result.isError(profileRes)) return profileRes;
    return Result.ok(buildStreamProfileResource(profileRes.value));
  }

  async updateProfile(stream: string, profile: StreamProfileSpec): Promise<StreamProfileResource> {
    const res = await this.updateProfileResult(stream, profile);
    if (Result.isError(res)) throw dsError(res.error.message, { code: res.error.code });
    return res.value.resource;
  }

  async updateProfileResult(stream: string, profile: StreamProfileSpec): Promise<Result<StreamProfileUpdateResult, StreamProfileMutationError>> {
    const definition = resolveStreamProfileDefinition(profile.kind);
    if (!definition) {
      return Result.err({ kind: "bad_request", message: `profile.kind must be ${supportedProfileKindsMessage()}` });
    }

    const commitRes = await this.store.commitProfileMetadataMutation<StreamProfilePersistResult, StreamProfileMutationError>(stream, ({ streamRow }) => {
      if (!streamRow) return Result.err({ kind: "bad_request", message: "stream not found" });
      const persistRes = definition.persistProfileResult({ stream, streamRow, profile });
      if (Result.isError(persistRes)) return persistRes;
      if (persistRes.value.touchState !== "preserve" && !this.touchEnabled) {
        return Result.err({ kind: "bad_request", message: `${persistRes.value.profile.kind} profile requires touch capability` });
      }
      return Result.ok({
        metadata: {
          streamProfile: persistRes.value.streamProfile,
          profileJson: persistRes.value.profileJson,
          schemaRegistry: persistRes.value.schemaRegistry ?? null,
          touchState: persistRes.value.touchState,
        },
        value: persistRes.value,
      });
    });
    if (Result.isError(commitRes)) return commitRes;

    const persist = commitRes.value.value;
    const cache = persist.cache ? { ...persist.cache, updatedAtMs: commitRes.value.profileUpdatedAtMs } : null;

    if (cache) this.cache.set(stream, cloneCachedProfile(cache)!);
    else this.cache.delete(stream);
    return Result.ok({
      resource: buildStreamProfileResource(cloneStreamProfileSpec(persist.profile)),
      schemaRegistry: commitRes.value.schemaRegistry,
    });
  }
}
