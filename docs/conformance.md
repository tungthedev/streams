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
  bun test \
    test/postgres_store.test.ts \
    test/postgres_http.test.ts \
    test/postgres_full_segments.test.ts \
    test/postgres_full_search.test.ts \
    test/postgres_full_touch.test.ts \
    test/postgres_full_bootstrap.test.ts
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

Postgres WAL mode has focused store and HTTP smoke coverage for its supported
WAL/control-plane capability bundle. Postgres full mode has focused parity
coverage for segmenting, manifest publication, search, aggregate, routing-key
lexicon listing, `_details` storage/accounting, touch/live, object-store
recovery, and built-in profile side effects. Upstream conformance should be run
against an explicitly started Postgres full server when validating that mode
black-box.

## Notes

- The upstream suite is black-box and only drives HTTP endpoints.
- The conformance runners remain valuable as regression detectors and should continue to run in CI or release verification.
