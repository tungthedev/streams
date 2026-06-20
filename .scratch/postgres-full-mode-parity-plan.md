# Postgres Full-Mode Parity Design / Port Plan

> **For agentic workers:** Use `executing-plans-with-thermal-review` when implementing this plan.

**Goal:** Add a true Postgres full mode that supports segmenting, manifest publication, search, aggregate, touch/live, metrics, and object-store recovery after the async storage capability boundaries are complete.

**Architecture:** SQLite full mode remains the behavioral source of truth until each capability is ported intentionally. Slices 6-10 of `.scratch/async-durable-store-refactor-plan.md` moved worker/runtime consumers toward capability interfaces, but Slice 0 below found that `app_core.ts` still owns SQLite-specific full-mode details/index-status assembly. This plan first finishes that remaining runtime boundary, then implements Postgres capability modules and wires Postgres through the same full-mode runtime as SQLite. Postgres full mode must use Postgres for durable WAL/control-plane and capability metadata, object storage for published segment/index/schema/manifest objects, and the existing capability guards for startup and route dispatch.

**Tech Stack:** Bun + TypeScript, Postgres via `pg`, existing `ObjectStore` interface, TieredStore segment/manifest formats, existing search companion/index formats, `better-result`, current async store capability interfaces.

---

## Current Shape / Baseline

Postgres currently supports only the WAL/control-plane subset:

- `src/postgres/schema.ts` schema version 1 creates `streams`, `wal`, `schemas`, `stream_profiles`, and `producer_state`.
- `src/postgres/store.ts` implements `WalControlPlaneStore` and explicitly sets `segmentReads`, `indexes`, `manifests`, `objectStoreAccounting`, `storageStats`, `schemaPublication`, `builtinProfiles`, `internalMetrics`, and `touch` to `false`.
- `docs/postgres-store.md`, `docs/overview.md`, `docs/architecture.md`, and `docs/conformance.md` document Postgres as WAL-only.
- `src/server.ts` rejects `DS_STORAGE=postgres` with `--object-store` and `--bootstrap-from-r2`.

SQLite full mode owns the full behavior today:

- `src/app.ts` wires `SqliteDurableStore`, `AccountingObjectStore`, `Segmenter`, `Uploader`, index managers, `StreamReader`, and schema publication.
- `src/db/db.ts` stores WAL/control-plane, segment rows, manifest rows, schema/profile metadata, producer state, touch state, index state/runs, search companion plan/catalog rows, object-store request counters, and storage stats helpers.
- `src/segment/segmenter.ts` builds local segment files from WAL rows and records segment metadata.
- `src/uploader.ts` uploads segment objects, builds manifests, publishes manifest generations, advances `uploaded_through`, and deletes WAL rows only after manifest publication.
- `src/index/*` and `src/search/companion_manager.ts` build routing, lexicon, exact, `.col`, `.fts`, `.agg`, and `.mblk` object-backed search companions.
- `src/touch/*` and `src/profiles/stateProtocol*` own state-protocol touch/live invalidation and live template runtime state.
- `src/bootstrap.ts` reconstructs SQLite metadata from object-store manifests, segment object heads, schema objects, profile JSON, touch profile state, index runs, lexicon runs, and search companion catalogs.

This plan must not grow `src/postgres/store.ts` into a second monolithic `SqliteDurableStore`.

## Prerequisite Gate

Do not start Postgres full-mode implementation until Slices 6-10 of `.scratch/async-durable-store-refactor-plan.md` are complete and the Slice 0 reconfirmation below has no unresolved runtime-boundary blockers:

- Slice 6: Segment and manifest worker capability migration.
- Slice 7: Index capability migration.
- Slice 8: Touch capability migration.
- Slice 9: Stats and accounting capability migration.
- Slice 10: Documentation reconciliation.

The initial prerequisite grep gates are:

```bash
rg "SqliteDurableStore" src/app_core.ts src/segment src/uploader.ts src/index src/search/companion_manager.ts src/touch src/stats.ts src/objectstore/accounting.ts
rg "\\.db\\.query|\\.db\\.transaction" src/app_core.ts src/segment src/uploader.ts src/index src/search/companion_manager.ts src/touch src/stats.ts src/objectstore/accounting.ts
```

Expected result after Slice 1 below: no production hits for capability-owned work except explicitly documented worker bootstrap code that still constructs a store from config.

### Slice 0 Reconfirmation Result

Reconfirmed after commits:

- `7bea487 refactor: move segment manifest workers to capabilities`
- `f736ea9 refactor: move index workers to capabilities`
- `e350755 refactor: move touch runtime to capability store`
- `1220cfd refactor: move stats accounting to capabilities`
- `5bbc5bf docs: reconcile storage capability boundaries`

Current capability interface names:

- WAL/control-plane: `WalControlPlaneStore`, `StoreLifecycle`, `StreamStore`, `WalStore`, `SchemaStore`, and `ProfileStore` in `src/store/capabilities.ts`, `src/store/wal_store.ts`, and `src/store/schema_profile_store.ts`.
- Segment/manifest: `SegmentReadStore` in `src/store/segment_read_store.ts`; `SegmentStore` and `ManifestStore` in `src/store/segment_manifest_store.ts`.
- Index/search metadata: `RoutingIndexStore`, `SecondaryIndexStore`, `LexiconIndexStore`, `SearchCompanionIndexStore`, and `CompanionProgressStore` in `src/store/index_store.ts`.
- Touch/live: `ProfileTouchStateStore` in `src/store/profile_touch_store.ts`; `ProfileTouchControlStore`, `TouchRouteStore`, `LiveTemplateStore`, and `TouchProcessorStore` in `src/store/touch_store.ts`.
- Stats/accounting: `StorageStatsStore`, `ObjectStoreAccountingRecorder`, and `ObjectStoreAccountingStore` in `src/store/stats_accounting_store.ts`.
- Schema publication: `SchemaPublicationStore` in `src/store/schema_publication.ts`.

Gate results:

- `rg "\\.db\\.query|\\.db\\.transaction" ...`: no hits.
- `rg "SqliteDurableStore" ...`: remaining hits in `src/app_core.ts` and worker entrypoints.
- Worker hits in `src/segment/segmenter_worker.ts` and `src/touch/processor_worker.ts` are expected SQLite worker bootstrap code.
- `src/app_core.ts` still imports `SqliteDurableStore` for SQLite-only full-mode helpers, including `requireSqliteFullModeStore`, `buildIndexStatus`, `buildIndexLagMs`, and parts of `buildStorageBreakdown`.

Conclusion: Slices 6-10 are complete, but Postgres full-mode implementation must not start at segment/schema capability work yet. Slice 1 below must first move the remaining full-mode details/index-status assembly out of `app_core.ts` and behind a narrow full-mode details capability.

Startup matrix decision for later implementation:

- `DS_STORAGE=postgres` without `DS_POSTGRES_MODE` remains the existing WAL-only public mode.
- `DS_STORAGE=postgres DS_POSTGRES_MODE=wal` is an explicit spelling for the existing WAL-only mode.
- `DS_STORAGE=postgres DS_POSTGRES_MODE=full` becomes the future full-mode entrypoint after the full capability slices pass.
- WAL mode continues to reject `--object-store` and `--bootstrap-from-r2`.
- Full mode requires `--object-store local|r2`; it must reject `--bootstrap-from-r2` until the object-store recovery slice lands.
- Startup tests should cover default WAL behavior, explicit WAL behavior, full mode without object store, full mode with local object store, full mode with R2 object store env validation, and bootstrap rejection until recovery is implemented.

## Target Shape

Postgres full mode should reuse the same runtime composition as SQLite full mode:

```text
server.ts
  DS_STORAGE=sqlite   -> createApp(...)
  DS_STORAGE=postgres -> createPostgresApp(...)      # default WAL-only mode
  DS_STORAGE=postgres DS_POSTGRES_MODE=wal
                      -> createPostgresApp(...)
  DS_STORAGE=postgres DS_POSTGRES_MODE=full
                      -> createPostgresFullApp(...)

createPostgresFullApp(...)
  PostgresDurableStore with full capabilities
  ObjectStore wrapper with capability-owned accounting
  Segmenter / Uploader through SegmentStore + ManifestStore
  Index managers through IndexStore
  TouchProcessorManager through TouchStore
  StreamReader through WAL + SegmentReadStore
```

Keep implementation ownership split:

- `src/postgres/schema.ts`: migration DDL only.
- `src/postgres/store.ts`: constructor, lifecycle, WAL/control-plane delegation, capability composition.
- `src/postgres/segments.ts`: segment, segment-meta, manifest, upload-prefix, and WAL-GC SQL.
- `src/postgres/routing_index.ts`: routing-key index state/run SQL.
- `src/postgres/secondary_index.ts`: exact secondary index state/run SQL.
- `src/postgres/lexicon_index.ts`: routing-key lexicon state/run SQL.
- `src/postgres/companions.ts`: bundled companion plan/catalog SQL.
- `src/postgres/touch.ts`: touch state and live template SQL.
- `src/postgres/stats.ts`: storage stats and object-store request accounting SQL.
- `src/postgres/bootstrap.ts`: Postgres object-store recovery orchestration.
- Shared runtime modules stay database-agnostic and depend on capability interfaces.

## Non-Goals

- Do not replace object storage with Postgres blobs for segment, index, companion, schema, or manifest objects.
- Do not fork the HTTP route stack or add `src/postgres/app.ts` with duplicate route behavior.
- Do not implement Postgres capabilities as no-ops just to flip booleans.
- Do not move segment building, uploads, index compaction, search scans, or bootstrap reconstruction into request handlers.
- Do not add logical decoding, triggers, or database-specific WAL notification as a requirement for core runtime.
- Do not change Durable Streams protocol semantics while adding parity.

## Ownership Rules

Avoid:

- Adding generic SQL escape hatches to capability interfaces.
- Adding Postgres branches in `src/app_core.ts` route handlers.
- Growing `src/postgres/store.ts` beyond lifecycle/composition and short delegating methods.
- Growing one Postgres index module into a catch-all for routing, secondary, lexicon, and bundled companions.
- Copying SQLite sync transaction style into async Postgres runtime code.

## Source Inventory

| Category | Source | Use |
| --- | --- | --- |
| Copy / Adapt | SQLite table set in `src/db/schema.ts` | Port table semantics to Postgres DDL with explicit types, constraints, and indexes. |
| Copy / Adapt | `src/uploader.ts` manifest commit rule | Preserve segment upload -> manifest upload -> `uploaded_through` advancement -> WAL GC order. |
| Copy / Adapt | `src/manifest.ts` JSON shape | Keep manifest compatibility so SQLite and Postgres full modes can recover from the same object-store layout. |
| Copy / Adapt | Segment/index/search binary object formats | Reuse existing object formats and object key layout unchanged. |
| Copy / Adapt | Bootstrap manifest parsing in `src/bootstrap.ts` | Move reconstruction semantics behind a database-agnostic bootstrap writer or Postgres-specific writer using shared helpers. |
| Reference Only | Current `src/db/db.ts` method list | Defines behavior, not the final Postgres file shape. Split by capability module. |
| Reference Only | `src/app.ts` runtime wiring | Reuse composition pattern, but avoid duplicating the full factory if a store-agnostic full app factory can be extracted. |
| Reject | `PostgresDurableStore` implementing every SQLite method inline | Recreates a catch-all store and blocks maintainability. |
| Reject | Postgres-only HTTP stack | Would fork protocol semantics. |
| Reject | Placeholder capabilities | Capabilities must be true only when backing tables, object-store flow, tests, and docs exist. |

## Architecture Safety Checklist

- **Source of truth:** Postgres is authoritative for WAL/control-plane and capability metadata in Postgres full mode; object storage is authoritative for published segments, companions, schema objects, and manifests.
- **Public contract:** The startup matrix keeps the existing WAL-only Postgres mode stable and starts full Postgres mode only through `DS_POSTGRES_MODE=full` after all advertised capabilities pass integration coverage.
- **Runtime vs persisted state:** Runtime caches and local disk caches remain rebuildable. Segment/index/touch progress metadata that must survive restart lives in Postgres.
- **Object-store visibility:** `uploaded_through` must advance only after manifest upload succeeds.
- **Lifecycle:** `close()` must stop Postgres pools and all background loops. Segmenter/uploader/index/touch startup and teardown should stay owned by the app runtime.
- **Errors:** Expected unsupported/misconfigured mode failures use `better-result` or existing `dsError` conventions; request-path unsupported capability responses stay explicit.
- **Migration policy:** Postgres schema version 1 is the WAL-only baseline. Full mode uses additive, idempotent migrations. No silent destructive migration.
- **Deletion:** Stream tombstone must scrub Postgres-owned acceleration state in one durable delete path, matching SQLite startup re-enforcement semantics.
- **Deleted-stream visibility:** Full-mode delete must publish a manifest after tombstoning so object-store recovery does not resurrect deleted streams.
- **File growth guard:** New Postgres capability modules must stay focused; `src/postgres/store.ts` should not absorb SQL for segments, indexes, touch, stats, or bootstrap.

## Implementation Slices

### Slice 0: Reconfirm Boundary Prerequisites

**Scope:** Verify async plan Slices 6-10 are actually complete and update this plan if capability names differ from the final implementation.

**Files:**
- Modify: `.scratch/postgres-full-mode-parity-plan.md`

**Checklist:**
- [ ] Run the prerequisite grep gates.
- [ ] Record final capability interface names for segment, manifest, index, touch, stats, accounting, and schema publication.
- [ ] Confirm Slices 6-10 removed SQLite ownership from `validateRuntimeCapabilityBundle`, details snapshots, touch route dependencies, and full-mode runtime dependencies in `src/app_core.ts`; record any residual ownership and add a pre-implementation boundary slice before starting Postgres parity implementation.
- [ ] Confirm `createAppCore` still has no Postgres-specific route branches.
- [ ] Confirm docs still describe Postgres as WAL-only before this parity work starts.
- [ ] Decide the public startup matrix before implementation.
- [ ] Add planned startup matrix tests for allowed and rejected combinations.
- [ ] Split or revise slices if Slices 6-10 chose different ownership boundaries.

**Done when:**
- The plan references current interface names and no implementation slice depends on stale Slices 6-10 assumptions.
- The public mode contract is written into this plan before any Postgres full-mode schema or runtime code is started.

### Slice 1: Complete Full-Mode Runtime Boundary

**Scope:** Remove the remaining SQLite-owned full-mode runtime boundary from `src/app_core.ts` before implementing Postgres full-mode tables. This includes details/index-status assembly, SQLite lifecycle cleanup hooks, and the `runtime.fullMode` validation that currently requires a concrete SQLite store.

**Files:**
- Create: `src/store/full_mode_details_store.ts`
- Create: `src/details/full_mode_details.ts` or equivalent focused details builder
- Create or modify: a narrow SQLite lifecycle/maintenance hook interface, colocated with the existing store capability modules
- Modify: `src/app_core.ts`
- Modify: `src/app.ts`
- Modify: `src/db/db.ts` only to implement the new narrow interface, not to add behavior
- Test: `test/http_behavior.test.ts`, `test/app_core_capabilities.test.ts`

**Boundary Shape:** `app_core.ts` may validate route capability booleans, serialize responses, and invoke narrow lifecycle hooks, but it must not import `SqliteDurableStore` or call SQLite-specific segment, manifest, index, lexicon, companion, storage, object-store accounting, reset, or reconciliation methods directly. The details/status builder should depend on a narrow capability bundle for stream details, index status, storage breakdown, object-store request summary, and index lag calculations. SQLite-specific startup maintenance such as `resetSegmentInProgress`, deleted-stream acceleration reconciliation, and internal metrics cleanup must move behind SQLite-owned hooks passed from `createApp`.

**Checklist:**
- [ ] Define the smallest details/status read interface needed by `GET /_details` and `GET /_index`.
- [ ] Move `buildIndexStatus`, `buildIndexLagMs`, storage breakdown full-mode reads, and object-store request summary reads out of `app_core.ts`.
- [ ] Replace `CreateAppCoreOptions.db?: SqliteDurableStore` with narrow optional dependencies for lifecycle/maintenance hooks and any remaining SQLite-only full-mode operations.
- [ ] Remove the `runtime.fullMode && !db` validation and replace it with capability/runtime validation that does not require SQLite.
- [ ] Move `resetSegmentInProgress`, deleted-stream acceleration reconciliation, and internal metrics acceleration cleanup behind SQLite-owned hooks or capability-owned startup work.
- [ ] Keep the unsupported route behavior explicit for stores without `storageStats`, `objectStoreAccounting`, or `indexes`.
- [ ] Ensure `createAppCore` receives the details/status capability only from full-mode wiring.
- [ ] Preserve SQLite full-mode `_details` and `_index` response shapes.
- [ ] Keep Postgres WAL-only behavior unchanged and still explicitly unsupported for full-mode details/accounting/index fields.
- [ ] Add grep gates for `SqliteDurableStore` usage in `src/app_core.ts` after the move.

**Done when:**
- `src/app_core.ts` no longer imports or types against `SqliteDurableStore`.
- `CreateAppCoreOptions` and `validateRuntimeCapabilityBundle` no longer require SQLite for `runtime.fullMode`.
- The only remaining `SqliteDurableStore` hits in the prerequisite grep are documented SQLite worker bootstrap entrypoints or non-capability lifecycle helpers.
- `bun test test/http_behavior.test.ts test/app_core_capabilities.test.ts` passes.
- `bun run typecheck`, `bun run check:result-policy`, `bun run check:storage-boundaries`, and `git diff --check` pass.

### Slice 2: Postgres Full-Mode Schema And Capability Skeleton

**Scope:** Add only migration/versioning scaffolding and the first capability module skeletons. Do not create unused full-mode tables ahead of the slice that implements and tests their owner.

**Files:**
- Modify: `src/postgres/schema.ts`
- Modify: `src/postgres/store.ts`
- Create: `src/postgres/segments.ts`
- Test: `test/postgres_store.test.ts`

**Boundary Shape:** Schema migrations are additive and capability-owned. Slice 2 may add shared migration helpers and segment/manifest table DDL only if Slice 3 is implemented in the same review unit; otherwise table DDL moves to the slice that owns the behavior.

**Checklist:**
- [ ] Add idempotent migration helpers with schema version checks and version 1 upgrade tests.
- [ ] Do not add index, touch, stats, or accounting tables before their implementation slices.
- [ ] Keep capability booleans false until implementation and tests for each capability land.
- [ ] Add migration tests that start from schema version 1 and from empty database.
- [ ] Keep `src/postgres/store.ts` as composition/lifecycle only.

**Done when:**
- Postgres migrations pass against `DS_TEST_POSTGRES_URL`.
- Existing WAL/control-plane Postgres tests still pass.
- Public startup behavior remains WAL-only and documented as such.

### Slice 3: Postgres Segment And Manifest Store

**Scope:** Implement Postgres `SegmentReadStore`, `SegmentStore`, `ManifestStore`, and schema publication support so segmenter/uploader can run against Postgres.

**Files:**
- Modify: `src/postgres/segments.ts`
- Modify: `src/postgres/schema.ts`
- Modify: `src/postgres/store.ts`
- Modify: app full-mode wiring helper
- Test: `test/postgres_full_segments.test.ts`, `test/segmenter_behavior.test.ts`, `test/segment_recovery.test.ts`

**Boundary Shape:** Segmenter and uploader use the same capability interfaces as SQLite. Postgres methods use transactions and row locks for segment claims, uploaded-prefix advancement, manifest commits, deleted-stream manifest publication, and WAL GC.

**Checklist:**
- [ ] Add only the segment, segment-meta, and manifest DDL needed by this slice.
- [ ] Implement candidate selection from Postgres stream counters without WAL scans.
- [ ] Implement segment claim/release so concurrent workers cannot build the same stream.
- [ ] Implement segment row insert, segment-meta append/rebuild, pending counters, and `sealed_through`.
- [ ] Implement upload-head selection preserving per-stream prefix order.
- [ ] Implement manifest row upsert and `commitManifest` transaction that advances `uploaded_through` and deletes WAL only after object-store manifest upload.
- [ ] Implement `publishDeletedStreamManifest` parity for tombstoned streams.
- [ ] Implement schema registry object upload accounting through `SchemaPublicationStore`.
- [ ] Keep local segment file paths and object keys compatible with existing object-store layout.

**Done when:**
- A Postgres full-mode smoke test can append enough data to seal a segment, upload it, publish a manifest, restart, and read from the segment plus WAL tail.
- WAL rows below `uploaded_through` are deleted only after manifest publication.
- A deleted Postgres stream publishes a deleted manifest with the expected object-store shape.
- SQLite full-mode segment tests still pass.

### Slice 4: Postgres Index And Search Capability

**Scope:** Implement Postgres index metadata capabilities for routing-key reads, routing-key lexicon, exact secondary indexes, bundled companions, `_search`, and `_aggregate`.

**Files:**
- Create: `src/postgres/routing_index.ts`
- Create: `src/postgres/secondary_index.ts`
- Create: `src/postgres/lexicon_index.ts`
- Create: `src/postgres/companions.ts`
- Modify: `src/postgres/schema.ts`
- Modify: `src/postgres/store.ts`
- Test: `test/postgres_full_search.test.ts`, `test/search_http.test.ts`, `test/aggregate_http.test.ts`, `test/routing_key_lexicon.test.ts`, `test/index_compaction.test.ts`, `test/secondary_indexer.test.ts`

**Boundary Shape:** Existing index managers continue to build binary object-store runs and companion objects; only metadata persistence changes to Postgres.

**Checklist:**
- [ ] Add only the index and companion DDL needed by this slice.
- [ ] Implement routing index state/runs, retired runs, compaction metadata, and storage summaries.
- [ ] Implement secondary index state/runs and config hash handling.
- [ ] Implement lexicon index state/runs and list-routing-keys behavior.
- [ ] Implement search companion plan and segment companion catalog persistence.
- [ ] Preserve foreground activity yielding and async index concurrency gates.
- [ ] Publish manifests after index/companion metadata changes so object-store recovery sees current index catalogs.
- [ ] Add line-count gates for `src/postgres/routing_index.ts`, `src/postgres/secondary_index.ts`, `src/postgres/lexicon_index.ts`, and `src/postgres/companions.ts`; split further before any module becomes a broad `indexes.ts`.

**Done when:**
- Postgres full mode supports `_routing_keys`, `_search`, and `_aggregate` for published segments and WAL tail behavior matches SQLite.
- Index run and companion objects use the same object key layout and formats as SQLite.
- SQLite index/search suites still pass.

### Slice 5: Postgres Built-In Profiles And Touch/Live

**Scope:** Enable built-in profile side effects and state-protocol touch/live for Postgres full mode.

**Files:**
- Modify: `src/postgres/schema.ts`
- Modify: `src/postgres/touch.ts`
- Modify: `src/postgres/store.ts`
- Modify: `src/profiles/*` only if final capability APIs require database-agnostic adjustments
- Test: `test/postgres_full_touch.test.ts`, `test/profile_state_protocol.test.ts`, `test/touch_processor.test.ts`, `test/touch_wait_timeout_reliability.test.ts`, `test/profile_evlog.test.ts`, `test/profile_metrics.test.ts`, `test/profile_otel_traces.test.ts`

**Boundary Shape:** `TouchProcessorManager`, live templates, and profile hooks use `TouchStore` and profile capability interfaces, not concrete SQLite calls.

**Checklist:**
- [ ] Add only the touch/live-template DDL needed by this slice.
- [ ] Implement `stream_touch_state` and `live_templates` operations in Postgres.
- [ ] Enable `builtinProfiles` only after `evlog`, `metrics`, `otel-traces`, and `state-protocol` profile side effects work with Postgres capabilities.
- [ ] Enable `touch` only after state-protocol live routes and background processing work.
- [ ] Preserve in-memory touch journal behavior; Postgres stores durable/rebuildable helper state, not every in-memory journal record.
- [ ] Keep live metrics and internal metrics stream behavior explicit.

**Done when:**
- Postgres full mode supports profile updates for `evlog`, `metrics`, `otel-traces`, and `state-protocol`.
- `/touch/*` routes work in Postgres full mode.
- Existing SQLite touch/profile tests still pass.

### Slice 6: Postgres Metrics, Stats, And Accounting

**Scope:** Implement runtime metrics stream startup, storage stats, object-store request accounting, and `_details` parity for Postgres full mode.

**Files:**
- Modify: `src/postgres/schema.ts`
- Modify: `src/postgres/stats.ts`
- Modify: `src/postgres/store.ts`
- Modify: `src/stats.ts` only if the final stats reporter capability requires database-agnostic injection
- Test: `test/postgres_full_segments.test.ts`, `test/stats.test.ts`, `test/objectstore_accounting.test.ts`, `test/metrics.test.ts`, `test/metrics_emitter.test.ts`

**Boundary Shape:** Stats reporter and `_details` use capability interfaces. Postgres does not expose SQLite runtime internals as fake values; database-specific fields are named honestly or omitted according to docs.

**Checklist:**
- [ ] Add only the stats/accounting DDL needed by this slice.
- [ ] Implement object-store request counters keyed by stream hash/artifact/op.
- [ ] Implement uploaded segment bytes, pending sealed segment bytes, index bytes, companion bytes, WAL bytes, and metadata bytes from Postgres tables.
- [ ] Enable `internalMetrics` after the metrics profile stream can initialize and emit without SQLite-only assumptions.
- [ ] Preserve `/metrics` process/runtime metric behavior.
- [ ] Document any stats fields that are SQLite-specific versus storage-mode-neutral.

**Done when:**
- `GET /v1/stream/{name}/_details` has parity for meaningful Postgres full-mode storage breakdown.
- Stats reporter runs in Postgres full mode without `SqliteDurableStore`.
- Metrics profile stream and `/metrics` tests pass.

### Slice 7: Postgres Object-Store Recovery

**Scope:** Add `--bootstrap-from-r2` support for Postgres full mode after segment, index, touch, and stats capability writers exist. Recovery uses per-capability restore writers with a thin orchestration layer.

**Files:**
- Create: `src/postgres/bootstrap.ts`
- Modify: `src/bootstrap.ts` or extract shared manifest parsing helpers
- Modify: `src/server.ts`
- Test: `test/postgres_full_bootstrap.test.ts`, `test/bootstrap_from_r2.test.ts`

**Boundary Shape:** Recovery reads existing manifest/schema/segment/index objects and reconstructs Postgres metadata without constructing `SqliteDurableStore`. `src/postgres/bootstrap.ts` orchestrates per-capability restore methods; it must not become the owner of segment, index, touch, or stats SQL.

**Checklist:**
- [ ] Extract manifest parsing and validation helpers shared by SQLite and Postgres bootstrap.
- [ ] Restore stream rows, segment metadata, manifest rows, schema registries, profile rows, index states/runs, lexicon states/runs, and search companion catalogs through their capability writers.
- [ ] Restore deleted-stream tombstones so object-store recovery does not resurrect deleted streams.
- [ ] Restore only published profile/touch configuration, then recreate or reseed rebuildable touch helper state cold through profile capability ownership.
- [ ] Do not restore touch journals or live-template runtime state from object storage; those are not mirrored to manifests and must be rebuilt or relearned after recovery.
- [ ] Keep local cache cleanup behavior explicit for Postgres mode.
- [ ] Keep `--bootstrap-from-r2` rejected for Postgres until this slice passes aggregate recovery tests.

**Done when:**
- A SQLite-published object-store fixture can bootstrap into Postgres and serve reads, search, aggregates, routing keys, profile metadata, and touch metadata through the shared HTTP surface.
- Deleted stream bootstrap fixtures remain tombstoned in Postgres.
- Existing SQLite bootstrap tests remain green.

### Slice 8: Public Startup Mode And Documentation

**Scope:** Make Postgres full mode a documented public mode with object-store support and remove WAL-only wording where no longer true.

**Files:**
- Modify: `src/server.ts`
- Modify: `src/config.ts`
- Modify: `docs/postgres-store.md`
- Modify: `docs/overview.md`
- Modify: `docs/architecture.md`
- Modify: `docs/conformance.md`
- Modify: `docs/index.md`
- Test: `test/postgres_http.test.ts`, all Postgres full-mode tests

**Boundary Shape:** Use explicit startup modes. `DS_STORAGE=postgres` remains WAL-only by default unless `DS_POSTGRES_MODE=full` is set. Do not replace or silently reinterpret existing WAL-only public behavior.

**Checklist:**
- [ ] Implement `DS_POSTGRES_MODE=wal|full` with WAL as the default when `DS_STORAGE=postgres`.
- [ ] Allow object-store flags only for the full Postgres mode and keep startup rejection for unsupported combinations.
- [ ] Update support matrix for segmenting, manifests, search, aggregate, touch/live, metrics, object-store recovery, and built-in profiles.
- [ ] Update conformance expectations and test commands with `DS_TEST_POSTGRES_URL`.
- [ ] Remove obsolete WAL-only claims from docs or scope them to explicit WAL-only mode.

**Done when:**
- A user can start documented Postgres full mode with Postgres plus local/R2 object store.
- Docs match behavior in startup gates and HTTP smoke tests.

### Slice 9: Final Parity Verification And Cleanup

**Scope:** Run full parity verification, delete temporary gates, and tighten regression checks.

**Files:**
- Modify: tests and docs only as needed

**Checklist:**
- [ ] Run full SQLite verification baseline.
- [ ] Run full Postgres-gated verification against disposable Postgres.
- [ ] Run black-box HTTP smoke for create, append, read from WAL, segment read after upload, long-poll, delete, list, schema/profile, search, aggregate, routing keys, touch/live, metrics, details, and bootstrap.
- [ ] Run `bun test test/conformance.test.ts` and a Postgres full-mode black-box conformance run, for example `CONFORMANCE_TEST_URL=... bun run test:conformance` or the exact equivalent documented in `docs/conformance.md`.
- [ ] Add grep gates for forbidden concrete imports and Postgres no-op capability methods.
- [ ] Confirm no stale docs still describe Postgres as WAL-only unless an explicit WAL-only mode remains.
- [ ] Confirm migration from schema version 1 to full-mode schema version works.

**Done when:**
- `bun run typecheck`
- `bun run check:result-policy`
- `bun test`
- `DS_TEST_POSTGRES_URL=... bun test test/postgres_store.test.ts test/postgres_http.test.ts test/postgres_full_segments.test.ts test/postgres_full_search.test.ts test/postgres_full_touch.test.ts test/postgres_full_bootstrap.test.ts`
- `bun test test/conformance.test.ts`
- Postgres full-mode `CONFORMANCE_TEST_URL=... bun run test:conformance` or documented equivalent
- `git diff --check`

## Verification

- Every slice runs `bun run typecheck`, `bun run check:result-policy`, and the focused SQLite plus Postgres-gated tests named in that slice.
- Segment/recovery slices include `test/segmenter_behavior.test.ts`, `test/segment_recovery.test.ts`, `test/bootstrap_from_r2.test.ts`, and the matching Postgres full-mode suites.
- Search/index slices include `test/search_http.test.ts`, `test/aggregate_http.test.ts`, `test/routing_key_lexicon.test.ts`, `test/index_compaction.test.ts`, `test/secondary_indexer.test.ts`, and `test/postgres_full_search.test.ts`.
- Touch/profile/metrics slices include state-protocol, touch processor, built-in profile, metrics, metrics-emitter, and Postgres full-touch suites.
- Final baseline: `bun run typecheck`, `bun run check:result-policy`, `bun test`, Postgres full-mode gated suites with `DS_TEST_POSTGRES_URL`, and `git diff --check`.

## Risks / Open Questions

- **Mode naming:** The plan chooses explicit `DS_POSTGRES_MODE=wal|full`, with WAL as the default for `DS_STORAGE=postgres`; implementation must avoid silently switching existing WAL-only deployments into full mode.
- **Bootstrap compatibility:** Object-store manifests are currently built from SQLite row types. Shared manifest row contracts may need to move out of `src/db/db.ts`.
- **Worker construction:** Segmenter and touch worker pools still construct SQLite stores directly. Postgres full mode needs either database-agnostic worker store factories or a documented first pass with in-process workers only.
- **Stats parity:** SQLite runtime memory stats cannot be faked for Postgres. Docs and API fields should distinguish storage-neutral stats from SQLite-specific runtime counters.
- **Migration safety:** Schema version 1 Postgres databases may contain retained WAL rows for streams that would be GC'd after full-mode manifest publication. The first full-mode segment pass must handle retained WAL safely.
- **Performance:** Postgres row locking and async iteration must preserve bounded memory and not introduce per-row await patterns in hot loops.

## Review Status

- Initial draft was reviewed before Slices 6-10 completed.
- Slice 0 reconfirmation updated this plan after current code inspection and discovered the remaining `app_core.ts` full-mode details/status boundary.
- Current revision still requires thermal review and fresh clean-pass review before implementation starts.

## Recommended Execution Mode

Use inline implementation with `executing-plans-with-thermal-review` per slice. Commit after each slice. Do not parallelize slices that touch `src/postgres/schema.ts`, `src/postgres/store.ts`, app startup, or docs; parallel work is only reasonable for disjoint test fixture preparation or focused explorer audits.
