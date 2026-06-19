import type { SchemaRegistry } from "../schema/registry";

export interface SchemaPublicationStore {
  uploadSchemaRegistry(stream: string, registry: SchemaRegistry): Promise<void>;
  publishProfileSchemaRegistry(stream: string, registry: SchemaRegistry): Promise<void>;
}
