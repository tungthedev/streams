import type { SqliteDurableStore } from "./db";
import type { ManifestPublicationSnapshot } from "../store/segment_manifest_store";
import { dsError } from "../util/ds_error.ts";
import { readU64LE } from "../util/endian";

export function loadSqliteManifestPublicationSnapshot(db: SqliteDurableStore, stream: string): ManifestPublicationSnapshot | null {
  const streamRow = db.getStream(stream);
  if (!streamRow) return null;

  const prevUploadedSegmentCount = streamRow.uploaded_segment_count ?? 0;
  let uploadedPrefixCount = db.advanceUploadedSegmentCount(stream);

  const segmentCount = db.countSegmentsForStream(stream);
  let segmentMeta = db.getSegmentMeta(stream);
  const needsRebuild =
    !segmentMeta ||
    segmentMeta.segment_count !== segmentCount ||
    segmentMeta.segment_offsets.byteLength !== segmentCount * 8 ||
    segmentMeta.segment_blocks.byteLength !== segmentCount * 4 ||
    segmentMeta.segment_last_ts.byteLength !== segmentCount * 8;
  if (needsRebuild) {
    segmentMeta = db.rebuildSegmentMeta(stream);
  }
  if (!segmentMeta) return null;
  if (uploadedPrefixCount > segmentMeta.segment_count) {
    uploadedPrefixCount = segmentMeta.segment_count;
    db.setUploadedSegmentCount(stream, uploadedPrefixCount);
  }

  const uploadedThrough =
    uploadedPrefixCount === 0 ? -1n : readU64LE(segmentMeta.segment_offsets, (uploadedPrefixCount - 1) * 8) - 1n;
  const unpublishedWalBytes = db.getWalBytesAfterOffset(stream, uploadedThrough);
  const publishedLogicalSizeBytes =
    streamRow.logical_size_bytes > unpublishedWalBytes ? streamRow.logical_size_bytes - unpublishedWalBytes : 0n;

  const manifestRow = db.getManifestRow(stream);
  const secondaryIndexStates = db.listSecondaryIndexStates(stream);
  const secondaryIndexRuns = secondaryIndexStates.flatMap((state) => db.listSecondaryIndexRuns(stream, state.index_name));
  const retiredSecondaryIndexRuns = secondaryIndexStates.flatMap((state) =>
    db.listRetiredSecondaryIndexRuns(stream, state.index_name)
  );
  const lexiconIndexStates = db.listLexiconIndexStates(stream);
  const lexiconIndexRuns = lexiconIndexStates.flatMap((state) =>
    db.listLexiconIndexRuns(stream, state.source_kind, state.source_name)
  );
  const retiredLexiconIndexRuns = lexiconIndexStates.flatMap((state) =>
    db.listRetiredLexiconIndexRuns(stream, state.source_kind, state.source_name)
  );

  let profileJson: Record<string, any> | null = null;
  const profileRow = db.getStreamProfile(stream);
  if (profileRow) {
    try {
      profileJson = JSON.parse(profileRow.profile_json);
    } catch {
      throw dsError(`invalid profile_json for ${stream}`);
    }
  }

  return {
    streamRow,
    prevUploadedSegmentCount,
    uploadedPrefixCount,
    uploadedThrough,
    publishedLogicalSizeBytes,
    generation: manifestRow.generation + 1,
    segmentMeta,
    profileJson,
    indexState: db.getIndexState(stream),
    indexRuns: db.listIndexRuns(stream),
    retiredRuns: db.listRetiredIndexRuns(stream),
    secondaryIndexStates,
    secondaryIndexRuns,
    retiredSecondaryIndexRuns,
    lexiconIndexStates,
    lexiconIndexRuns,
    retiredLexiconIndexRuns,
    searchCompanionPlan: db.getSearchCompanionPlan(stream),
    searchSegmentCompanions: db.listSearchSegmentCompanions(stream),
  };
}
