# Postgres Store

Postgres storage runs the normal Prisma Streams HTTP server with a Postgres
WAL/control-plane store instead of the full SQLite plus object-store pipeline.

It is a supported WAL-only mode. It does not implement segmenting, object-store
publication, search companions, touch/live invalidation, or built-in profile
side effects.

## Startup

```bash
DS_STORAGE=postgres \
DS_POSTGRES_URL=postgres://user:pass@host:5432/database \
bun run src/server.ts --no-auth
```

`DS_POSTGRES_URL` is required when `DS_STORAGE=postgres`.

Postgres mode rejects full-mode object-store options:

- `--object-store`
- `--bootstrap-from-r2`

Choose exactly one auth mode as usual: `--no-auth` for trusted perimeters or
`--auth-strategy api-key` with `API_KEY`.

## Supported HTTP Surface

Postgres mode uses the same route handlers as the SQLite server for:

- `PUT /v1/stream/{name}`
- `POST /v1/stream/{name}`
- `GET /v1/stream/{name}` with `offset`, `since`, `format=json`, `key`, and
  `live=long-poll`
- `HEAD /v1/stream/{name}`
- `DELETE /v1/stream/{name}`
- `GET /v1/streams`
- `GET|POST /v1/stream/{name}/_schema` for base schema, lenses, and
  routing-key derivation
- `GET|POST /v1/stream/{name}/_profile` for `generic`

Append acknowledgements are returned only after the Postgres transaction commits.
Offsets are protocol offsets stored explicitly in the application WAL table; they
do not depend on Postgres row ids.

## Unsupported Features

Postgres mode returns explicit unsupported-capability errors for features that
need full-mode storage or built-in profile runtimes:

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

Postgres v1 supports `generic` profiles only. Non-generic profile updates are
rejected until their required storage, index, or touch capabilities are ported.

## Durability And Retention

Postgres mode stores stream metadata, schema/profile metadata, producer state,
and WAL rows in Postgres. There is no object-store visibility point in this
mode, and no segment manifest is published.

Because there is no object-store commit point, WAL rows are retained in Postgres.
Age- or count-based retention can be added later as a separate documented
feature.

## Verification

Store and HTTP tests are gated by `DS_TEST_POSTGRES_URL`:

```bash
DS_TEST_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/streams_test \
bun test test/postgres_store.test.ts test/postgres_http.test.ts
```

Without `DS_TEST_POSTGRES_URL`, the Postgres integration tests skip the database
cases and keep a small guard test active so the missing environment variable is
visible in test output.
