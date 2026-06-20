import { Result } from "better-result";
import type {
  StreamProfileDefinition,
  StreamProfilePersistResult,
  StreamProfileReadResult,
  StreamTouchCapability,
} from "./profile";
import { normalizeProfileContentType, parseStoredProfileJsonResult } from "./profile";
import { deriveStateProtocolChanges } from "./stateProtocol/changes";
import { handleStateProtocolTouchRoute } from "./stateProtocol/routes";
import type { StateProtocolStreamProfile } from "./stateProtocol/types";
import {
  cloneStateProtocolCache,
  cloneStateProtocolProfile,
  getStateProtocolTouchConfig,
  isStateProtocolProfile,
  validateStateProtocolProfileResult,
} from "./stateProtocol/validation";
import { validateStateProtocolRecordResult } from "./stateProtocol/ingest";

const STATE_PROTOCOL_TOUCH_CAPABILITY: StreamTouchCapability = {
  getTouchConfig(profile) {
    return getStateProtocolTouchConfig(profile);
  },

  syncState({ db, stream, profile }) {
    if (getStateProtocolTouchConfig(profile)) return db.ensureStreamTouchState(stream);
    return db.deleteStreamTouchState(stream);
  },

  deriveCanonicalChanges(record) {
    return deriveStateProtocolChanges(record);
  },

  async handleRoute(args) {
    return handleStateProtocolTouchRoute(args);
  },
};

export const STATE_PROTOCOL_STREAM_PROFILE_DEFINITION: StreamProfileDefinition = {
  kind: "state-protocol",
  usesStoredProfileRow: true,
  touch: STATE_PROTOCOL_TOUCH_CAPABILITY,

  defaultProfile(): StateProtocolStreamProfile {
    return { kind: "state-protocol" };
  },

  validateResult(raw, path) {
    return validateStateProtocolProfileResult(raw, path);
  },

  readProfileResult({ row, cached }): Result<StreamProfileReadResult, { message: string }> {
    if (!row) {
      return Result.ok({ profile: { kind: "state-protocol" }, cache: null });
    }
    const cachedCopy = cloneStateProtocolCache(cached);
    if (cachedCopy && cachedCopy.updatedAtMs === row.updated_at_ms) {
      return Result.ok({
        profile: cloneStateProtocolProfile(cachedCopy.profile as StateProtocolStreamProfile),
        cache: cachedCopy,
      });
    }
    const parsedRes = parseStoredProfileJsonResult(row.profile_json);
    if (Result.isError(parsedRes)) return parsedRes;
    const profileRes = validateStateProtocolProfileResult(parsedRes.value, "profile");
    if (Result.isError(profileRes)) return profileRes;
    const profile = cloneStateProtocolProfile(profileRes.value);
    return Result.ok({
      profile: cloneStateProtocolProfile(profile),
      cache: { profile, updatedAtMs: row.updated_at_ms },
    });
  },

  persistProfileResult({ streamRow, profile }): Result<StreamProfilePersistResult, { kind: "bad_request"; message: string; code?: string }> {
    if (!isStateProtocolProfile(profile)) {
      return Result.err({ kind: "bad_request", message: "invalid state-protocol profile" });
    }
    const contentType = normalizeProfileContentType(streamRow.content_type);
    if (contentType !== "application/json") {
      return Result.err({
        kind: "bad_request",
        message: "state-protocol profile requires application/json stream content-type",
      });
    }

    const persistedProfile = cloneStateProtocolProfile(profile);
    return Result.ok({
      profile: cloneStateProtocolProfile(persistedProfile),
      cache: {
        profile: persistedProfile,
        updatedAtMs: 0n,
      },
      schemaRegistry: null,
      streamProfile: persistedProfile.kind,
      profileJson: JSON.stringify(persistedProfile),
      touchState: getStateProtocolTouchConfig(persistedProfile) ? "ensure" : "delete",
    });
  },

  jsonIngest: {
    prepareRecordResult({ profile, value }) {
      if (!isStateProtocolProfile(profile)) return Result.err({ message: "invalid state-protocol profile" });
      return validateStateProtocolRecordResult(value);
    },
  },
};
