import { describe, expect, test } from "bun:test";
import { Uploader } from "../src/uploader";
import { loadConfig } from "../src/config";
import type { ManifestPublicationSnapshot, ManifestStore, SegmentRow } from "../src/store/segment_manifest_store";
import type { ObjectStore, PutResult } from "../src/objectstore/interface";

function emptySnapshot(stream: string, wait: boolean): ManifestPublicationSnapshot {
  return {
    publicationToken: wait ? "forced-token" : undefined,
    streamRow: {
      stream,
      created_at_ms: 1n,
      updated_at_ms: 1n,
      content_type: "text/plain",
      profile: "generic",
      stream_seq: null,
      closed: 0,
      closed_producer_id: null,
      closed_producer_epoch: null,
      closed_producer_seq: null,
      ttl_seconds: null,
      epoch: 0,
      next_offset: 0n,
      sealed_through: -1n,
      uploaded_through: -1n,
      uploaded_segment_count: 0,
      pending_rows: 0n,
      pending_bytes: 0n,
      logical_size_bytes: 0n,
      wal_rows: 0n,
      wal_bytes: 0n,
      last_append_ms: 1n,
      last_segment_cut_ms: 1n,
      segment_in_progress: 0,
      expires_at_ms: null,
      stream_flags: 1,
    },
    prevUploadedSegmentCount: 0,
    uploadedPrefixCount: 0,
    uploadedThrough: -1n,
    publishedLogicalSizeBytes: 0n,
    generation: 1,
    segmentMeta: {
      stream,
      segment_count: 0,
      segment_offsets: new Uint8Array(),
      segment_blocks: new Uint8Array(),
      segment_last_ts: new Uint8Array(),
    },
    profileJson: null,
    indexState: null,
    indexRuns: [],
    retiredRuns: [],
    secondaryIndexStates: [],
    secondaryIndexRuns: [],
    retiredSecondaryIndexRuns: [],
    lexiconIndexStates: [],
    lexiconIndexRuns: [],
    retiredLexiconIndexRuns: [],
    searchCompanionPlan: null,
    searchSegmentCompanions: [],
  };
}

class WaitingManifestStore implements ManifestStore {
  snapshotCalls: boolean[] = [];
  commits: Array<{ stream: string; token?: string }> = [];

  nowMs(): bigint {
    return 10n;
  }

  countPendingSegments(): number {
    return 0;
  }

  async pendingUploadHeads(_limit: number): Promise<SegmentRow[]> {
    return [];
  }

  async markSegmentUploaded(_segmentId: string, _etag: string, _uploadedAtMs: bigint): Promise<void> {}

  async loadManifestPublicationSnapshot(stream: string, opts: { wait?: boolean } = {}): Promise<ManifestPublicationSnapshot | null> {
    this.snapshotCalls.push(opts.wait === true);
    return emptySnapshot(stream, opts.wait === true);
  }

  async commitManifest(
    stream: string,
    _generation: number,
    _etag: string,
    _uploadedAtMs: bigint,
    _uploadedThrough: bigint,
    _sizeBytes: number,
    publicationToken?: string
  ): Promise<void> {
    this.commits.push({ stream, token: publicationToken });
  }

  async getSegmentForManifestCleanup(_stream: string, _segmentIndex: number): Promise<SegmentRow | null> {
    return null;
  }
}

class CapturingObjectStore implements ObjectStore {
  puts = 0;

  async put(_key: string, _data: Uint8Array): Promise<PutResult> {
    this.puts += 1;
    return { etag: "etag" };
  }

  async get(): Promise<Uint8Array | null> {
    return null;
  }

  async head(): Promise<{ etag: string; size: number } | null> {
    return null;
  }

  async delete(): Promise<void> {}

  async list(): Promise<string[]> {
    return [];
  }
}

describe("uploader manifest publication", () => {
  test("forced publish waits behind an in-flight publish and requests a waiting snapshot", async () => {
    const store = new WaitingManifestStore();
    const objectStore = new CapturingObjectStore();
    const uploader = new Uploader(loadConfig(), store, objectStore);

    const normal = uploader.publishManifest("deleted");
    const forced = uploader.publishManifest("deleted", { wait: true });
    await Promise.all([normal, forced]);

    expect(store.snapshotCalls).toEqual([false, true]);
    expect(store.commits).toEqual([
      { stream: "deleted", token: undefined },
      { stream: "deleted", token: "forced-token" },
    ]);
    expect(objectStore.puts).toBe(2);
  });
});
