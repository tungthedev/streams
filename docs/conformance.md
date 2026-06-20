# Prisma Streams Conformance

This repository uses two layers of protocol verification:

1. Local black-box tests in `test/conformance.test.ts`
2. The upstream suite from `@durable-streams/server-conformance-tests`

## Commands

```bash
bun test test/conformance.test.ts
bun run test:conformance
bun run test:conformance:local
DS_TEST_POSTGRES_URL=postgres://postgres:postgres@127.0.0.1:5432/streams_test \
  bun test test/postgres_store.test.ts test/postgres_http.test.ts
```

Default behavior:

- `test:conformance` starts a temporary full server on `127.0.0.1:8787`
- `test:conformance:local` starts a temporary local-mode server on `127.0.0.1:8787`

Manual target mode:

```bash
DS_HOST=127.0.0.1 PORT=8787 DS_ROOT=/tmp/ds-conformance bun run src/server.ts --object-store local --no-auth
CONFORMANCE_TEST_URL=http://127.0.0.1:8787 bun run test:conformance
CONFORMANCE_TEST_URL=http://127.0.0.1:8787 bun run test:conformance:local
```

## Current Status

Last verified on `2026-03-14`:

- Local repository suite: `bun test` passed
- Upstream full-server suite: `239/239` passing
- Upstream local-mode suite: `239/239` passing

Prisma Streams currently passes the upstream black-box suite for the full and
local modes.

Postgres mode has focused store and HTTP smoke coverage for its supported
WAL/control-plane capability bundle. It does not claim full upstream
conformance because the first Postgres mode intentionally excludes full-mode
segmenting, manifest publication, search, aggregate, routing-key lexicon
listing, `_details` storage/accounting, touch/live, object-store recovery, and
built-in profile side effects.

## Notes

- The upstream suite is black-box and only drives HTTP endpoints.
- The conformance runners remain valuable as regression detectors and should continue to run in CI or release verification.
