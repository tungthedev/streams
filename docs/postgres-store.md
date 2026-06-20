# Postgres Store

Postgres storage runs the normal Prisma Streams HTTP server with Postgres as the
durable WAL/control-plane store. It has two explicit modes:

- `wal` mode: the default when `DS_STORAGE=postgres`; stores stream metadata,
  schemas, profiles, producer state, and WAL rows in Postgres.
- `full` mode: enabled with `DS_POSTGRES_MODE=full`; uses Postgres for durable
  metadata and object storage for published segments, manifests, schemas,
  search companions, and index artifacts.

## Startup

WAL mode is the default:

```bash
DS_STORAGE=postgres \
DS_POSTGRES_URL=postgres://user:pass@host:5432/database \
bun run src/server.ts --no-auth
```

`DS_POSTGRES_MODE=wal` is accepted as an explicit spelling of the same mode.
WAL mode rejects `--object-store` and `--bootstrap-from-r2`.

Full mode requires an object store:

```bash
DS_STORAGE=postgres \
DS_POSTGRES_MODE=full \
DS_POSTGRES_URL=postgres://user:pass@host:5432/database \
bun run src/server.ts --object-store local --no-auth
```

For R2-backed full mode, use `--object-store r2` and the standard
`DURABLE_STREAMS_R2_*` environment variables. To rebuild Postgres full-mode
metadata from published object-store state:

```bash
DS_STORAGE=postgres \
DS_POSTGRES_MODE=full \
DS_POSTGRES_URL=postgres://user:pass@host:5432/database \
bun run src/server.ts --object-store r2 --bootstrap-from-r2 --no-auth
```

`--bootstrap-from-r2` clears local cache files and the Postgres restore target
before restoring. Published manifests are the recovery source. Unuploaded WAL
tail records, producer dedupe state, touch journals, and live-template runtime
state are not restored from object storage.

Choose exactly one auth mode as usual: `--no-auth` for trusted perimeters or
`--auth-strategy api-key` with `API_KEY`.

## Supported HTTP Surface

Both Postgres modes use the same route handlers as the SQLite server for the
base WAL/control-plane surface:

- `PUT /v1/stream/{name}`
- `POST /v1/stream/{name}`
- `GET /v1/stream/{name}` with `offset`, `since`, `format=json`, `key`, and
  `live=long-poll`
- `HEAD /v1/stream/{name}`
- `DELETE /v1/stream/{name}`
- `GET /v1/streams`
- `GET|POST /v1/stream/{name}/_schema` for base schema, lenses, routing-key
  derivation, and full-mode search configuration
- `GET|POST /v1/stream/{name}/_profile`

Append acknowledgements are returned only after the Postgres transaction
commits. Offsets are protocol offsets stored explicitly in the application WAL
table; they do not depend on Postgres row ids.

Full mode additionally supports:

- segment building, manifest publication, and WAL garbage collection after the
  manifest visibility point
- historical reads from published segment objects plus WAL-tail overlay
- `_details` storage/accounting responses, including
  `postgres_shared_total_bytes`
- `_search`, `_aggregate`, and `_routing_keys`
- built-in profile side effects for `evlog`, `metrics`, `otel-traces`, and
  `state-protocol`
- `touch/*` routes and state-protocol live invalidation
- `POST /v1/traces` and `/_otlp/v1/traces`
- object-store recovery with `--bootstrap-from-r2`

## WAL Mode Limits

Postgres WAL mode returns explicit unsupported-capability errors for features
that need full-mode storage or built-in profile runtimes:

- segment building and object-store upload
- R2 bootstrap and manifest publication
- WAL garbage collection based on uploaded manifests
- `_details` storage accounting
- `_search`, `_aggregate`, and `_routing_keys`
- `touch/*` routes and state-protocol live invalidation
- `POST /v1/traces` and `/_otlp/v1/traces`
- built-in profile side effects for `evlog`, `metrics`, `otel-traces`, and
  `state-protocol`
- schema-owned `search` fields and `search.rollups`

WAL mode supports `generic` profiles only. Non-generic profile updates are
rejected until full mode is enabled.

## Durability And Retention

In WAL mode, Postgres is the only durability point. WAL rows are retained in
Postgres because there is no object-store manifest commit point.

In full mode, append ACK still means the write committed to Postgres. Object
storage becomes durable only after segment upload plus manifest publication.
`uploaded_through` advances after manifest publication, and WAL garbage
collection follows that visibility commit point.

## Verification

Postgres store, HTTP, and full-mode parity tests are gated by
`DS_TEST_POSTGRES_URL`:

```bash
DS_TEST_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/streams_test \
bun test \
  test/postgres_store.test.ts \
  test/postgres_http.test.ts \
  test/postgres_full_segments.test.ts \
  test/postgres_full_search.test.ts \
  test/postgres_full_touch.test.ts \
  test/postgres_full_bootstrap.test.ts
```

Without `DS_TEST_POSTGRES_URL`, the Postgres integration tests skip the
database cases and keep small guard tests active so the missing environment
variable is visible in test output.
