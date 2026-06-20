import type { Result } from "better-result";
import type { StreamReadRow } from "./segment_read_store";
import type { SchemaRegistry } from "../schema/registry";
import type { ProfileTouchStatePlan } from "./profile_touch_store";

export type SchemaRegistryRow = {
  stream: string;
  registry_json: string;
  updated_at_ms: bigint;
  uploaded_size_bytes: bigint;
};

export type StoredProfileRow = {
  stream: string;
  profile_json: string;
  updated_at_ms: bigint;
};

export type SchemaMetadataMutationContext = {
  streamRow: StreamReadRow | null;
  registryRow: SchemaRegistryRow | null;
};

export type SchemaMetadataMutationPlan<T> = {
  registry: SchemaRegistry;
  registryJson: string;
  value: T;
};

export type SchemaMetadataCommit<T> = {
  registry: SchemaRegistry;
  updatedAtMs: bigint;
  value: T;
};

export interface SchemaStore {
  getSchemaRegistryForRead(stream: string): Promise<SchemaRegistryRow | null>;
  commitSchemaMetadataMutation<T, E>(
    stream: string,
    mutation: (ctx: SchemaMetadataMutationContext) => Result<SchemaMetadataMutationPlan<T>, E>
  ): Promise<Result<SchemaMetadataCommit<T>, E>>;
}

export type ProfileMetadataMutationContext = {
  streamRow: StreamReadRow | null;
  profileRow: StoredProfileRow | null;
};

export type ProfileMetadataPlan = {
  streamProfile: string | null;
  profileJson: string | null;
  schemaRegistry: SchemaRegistry | null;
  touchState?: ProfileTouchStatePlan;
};

export type ProfileMetadataMutationPlan<T> = {
  metadata: ProfileMetadataPlan;
  value: T;
};

export type ProfileMetadataCommit<T> = {
  schemaRegistry: SchemaRegistry | null;
  profileUpdatedAtMs: bigint;
  value: T;
};

export interface ProfileStore {
  getStreamProfileForRead(stream: string): Promise<StoredProfileRow | null>;
  commitProfileMetadataMutation<T, E>(
    stream: string,
    mutation: (ctx: ProfileMetadataMutationContext) => Result<ProfileMetadataMutationPlan<T>, E>
  ): Promise<Result<ProfileMetadataCommit<T>, E>>;
}
