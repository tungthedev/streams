# Evlog And Tracing Guide

This guide describes how to use the `evlog` and `otel-traces` profiles
together in an application that does not already have logging or tracing.

It is written as an implementation handoff for an agent adding observability to
an ecommerce app. The same pattern applies to other request-oriented services.

## Mental Model

Use two streams:

- `app-events`: one `evlog` record per request, job, webhook, or important
  workflow outcome
- `app-traces`: one `otel-traces` record per OpenTelemetry span

Do not put span graphs into `evlog`, and do not use traces as the only request
log. They answer different questions:

- `evlog` is the compact request ledger: what happened, who/what it affected,
  status, duration, why it failed, and how to fix it.
- `otel-traces` is the execution graph: which services, DB calls, cache calls,
  queues, HTTP clients, and internal functions contributed to that request.
- `POST /v1/observe/request` joins them at query time by `requestId`,
  `traceId`, or `spanId`.

Every app request should have a stable `requestId`. The active root/server span
must also carry that value as an OpenTelemetry attribute named `request.id`.
The evlog event should store the same `requestId`, plus the active `traceId`
and root `spanId`.

## Stream Setup

Choose stream names before instrumenting the app. For a single ecommerce app,
use stable environment-specific names:

- `ecommerce-production-events`
- `ecommerce-production-traces`

Use separate stream pairs when retention, tenant isolation, access control, or
deployment lifecycle differs. Do not rely on UI filters for tenant isolation.

The examples below use:

```bash
STREAMS_URL=http://127.0.0.1:8080
EVENTS_STREAM=ecommerce-production-events
TRACES_STREAM=ecommerce-production-traces

# Only needed when the full server runs with --auth-strategy api-key.
STREAMS_AUTH_HEADER="Authorization: Bearer ${STREAMS_API_KEY}"
```

If the server runs with `--no-auth`, omit the `-H "$STREAMS_AUTH_HEADER"`
lines from the curl examples.

### 1. Create The Evlog Stream

Create the stream as `application/json`, then install the `evlog` profile before
appending any records.

```bash
curl -X PUT "$STREAMS_URL/v1/stream/$EVENTS_STREAM" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER"
```

Install the profile:

```bash
curl -X POST "$STREAMS_URL/v1/stream/$EVENTS_STREAM/_profile" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER" \
  --data-binary @- <<JSON
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "evlog",
    "redactKeys": [
      "authorization",
      "cookie",
      "password",
      "sessionToken",
      "stripeSecret",
      "token"
    ],
    "correlation": {
      "requestIdFields": [
        "requestId",
        "context.requestId",
        "headers.x-request-id"
      ],
      "traceContextFields": [
        "traceId",
        "spanId",
        "traceContext.traceId",
        "traceContext.spanId"
      ],
      "parseTraceparent": true
    },
    "observability": {
      "request": {
        "tracesStream": "ecommerce-production-traces"
      }
    }
  }
}
JSON
```

`observability.request.tracesStream` is important. It lets clients discover the
correct trace stream from `GET /v1/streams` or
`GET /v1/stream/{name}/_details` instead of guessing by stream name.

### 2. Create The Trace Stream

Create the trace stream as `application/json`, then install `otel-traces`.

```bash
curl -X PUT "$STREAMS_URL/v1/stream/$TRACES_STREAM" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER"
```

Install the profile:

```bash
curl -X POST "$STREAMS_URL/v1/stream/$TRACES_STREAM/_profile" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER" \
  --data-binary @- <<JSON
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "otel-traces",
    "redactKeys": [
      "authorization",
      "cookie",
      "password",
      "sessionToken",
      "stripeSecret",
      "token"
    ],
    "requestIdAttributes": [
      "request.id",
      "http.request.header.x-request-id",
      "http.request_id",
      "correlation.id"
    ],
    "dbStatementMode": "drop",
    "urlMode": "drop_query",
    "store": {
      "rawResourceAttributes": true,
      "rawSpanAttributes": true,
      "rawEvents": true,
      "rawLinks": true
    },
    "observability": {
      "request": {
        "eventsStream": "ecommerce-production-events"
      }
    }
  }
}
JSON
```

`observability.request.eventsStream` declares the reverse pairing.

Use `dbStatementMode: "drop"` and `urlMode: "drop_query"` by default for
production ecommerce apps. Enable raw SQL statements or raw query strings only
after deciding they cannot contain customer, payment, session, or cart secrets.

### 3. Optional Default OTLP Endpoint

If you want OpenTelemetry SDKs to export to the shared endpoint
`POST /v1/traces`, start the Streams server with:

```bash
DS_OTLP_TRACES_STREAM=ecommerce-production-traces \
  bun run src/server.ts --object-store local --no-auth
```

For production, include the normal auth and object-store configuration. If the
stream is already created and profiled, keep `DS_OTLP_AUTO_CREATE` unset or
false. Auto-create is useful for local development, but explicit setup is
clearer for production.

The explicit endpoint also works and does not need `DS_OTLP_TRACES_STREAM`:

```text
POST /v1/stream/ecommerce-production-traces/_otlp/v1/traces
```

Both OTLP endpoints accept `application/x-protobuf`, `application/json`, and
`Content-Encoding: gzip`.

## App Implementation

An ecommerce app with no logging or tracing needs five pieces:

1. A request ID middleware.
2. OpenTelemetry tracing initialization with an OTLP HTTP exporter.
3. Framework instrumentation for incoming requests.
4. Manual spans around ecommerce domain work that auto-instrumentation cannot
   see.
5. An evlog append at the end of each request or job.

The examples below are TypeScript-oriented pseudocode. Adapt framework hooks to
Express, Fastify, Hono, Next.js route handlers, Remix, workers, or the app's
actual server.

### 1. Configuration

Use environment variables so the same code works in local and production:

```ts
export const observabilityConfig = {
  serviceName: process.env.OTEL_SERVICE_NAME ?? "ecommerce-web",
  serviceVersion: process.env.APP_VERSION ?? "dev",
  environment: process.env.NODE_ENV ?? "development",
  streamsUrl: requiredEnv("STREAMS_URL"),
  eventsStream:
    process.env.STREAMS_EVENTS_STREAM ?? "ecommerce-production-events",
  tracesStream:
    process.env.STREAMS_TRACES_STREAM ?? "ecommerce-production-traces",
  tracesEndpoint:
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ??
    `${requiredEnv("STREAMS_URL")}/v1/traces`,
  streamsApiKey: process.env.STREAMS_API_KEY ?? null,
};

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}
```

When the Streams server uses API-key auth, every app-side Streams request must
include:

```http
Authorization: Bearer <API_KEY>
```

### 2. Initialize OpenTelemetry

Initialize tracing once at process startup before loading the app routes.

Use the OpenTelemetry SDK for the app runtime and configure an OTLP HTTP trace
exporter with:

- URL: `${STREAMS_URL}/v1/traces`, or the explicit stream endpoint
- headers: `Authorization: Bearer ${STREAMS_API_KEY}` when required
- protocol/content type supported by the chosen exporter

Example shape:

```ts
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";

const headers = observabilityConfig.streamsApiKey
  ? { Authorization: `Bearer ${observabilityConfig.streamsApiKey}` }
  : {};

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    "service.name": observabilityConfig.serviceName,
    "service.version": observabilityConfig.serviceVersion,
    "deployment.environment": observabilityConfig.environment,
  }),
  traceExporter: new OTLPTraceExporter({
    url: observabilityConfig.tracesEndpoint,
    headers,
  }),
  instrumentations: [
    // Add the runtime/framework instrumentations used by the app:
    // HTTP server/client, framework router, database driver, Redis/cache,
    // message queues, fetch/undici, and Prisma/client instrumentation if used.
  ],
});

sdk.start();
```

The exact OpenTelemetry package imports vary by runtime. Keep the behavior the
same even if the app uses a different SDK surface:

- create server/root spans for incoming requests
- export traces to Streams over OTLP HTTP
- attach service/environment resource attributes
- instrument DB, cache, HTTP client, and queue libraries

### 3. Request ID Middleware

Each incoming request needs a stable ID. Reuse a trusted inbound
`x-request-id` when present, otherwise generate one. Echo it in the response.

```ts
import { randomUUID } from "node:crypto";

export function getOrCreateRequestId(headers: Headers): string {
  const inbound = headers.get("x-request-id");

  if (inbound && inbound.length <= 128) {
    return inbound;
  }

  return `req_${randomUUID()}`;
}
```

Store the request ID in request-local context so application code and evlog
append code can read it. In Node, `AsyncLocalStorage` is a common choice. Store
the active root/server span context there too while the span is active; some
framework response-finished hooks run after OpenTelemetry has left the request
context.

### 4. Attach Correlation To The Active Span

At the start of request handling, add the request ID and route metadata to the
active root/server span:

```ts
import { trace } from "@opentelemetry/api";

export function annotateRequestSpan(args: {
  method: string;
  path: string;
  requestId: string;
  route?: string;
}) {
  const span = trace.getActiveSpan();

  if (!span) return;

  span.setAttribute("request.id", args.requestId);
  span.setAttribute("http.request.method", args.method);
  span.setAttribute("url.path", args.path);
  if (args.route) span.setAttribute("http.route", args.route);
}
```

This is the main correlation requirement. The `otel-traces` profile derives
`requestId` from `request.id` and related configured attributes.

### 5. Add Domain Spans

Auto-instrumentation will catch many HTTP, DB, and cache operations. Add manual
spans around business operations so the trace is useful to an ecommerce
operator.

Recommended span names:

- `GET /api/products`
- `POST /api/cart/items`
- `POST /api/checkout`
- `reserve inventory`
- `authorize payment`
- `create order`
- `publish order.created`
- `send confirmation email`

Example:

```ts
import { SpanStatusCode, trace } from "@opentelemetry/api";

const tracer = trace.getTracer("ecommerce-app");

export async function checkout(input: CheckoutInput): Promise<CheckoutResult> {
  return tracer.startActiveSpan("checkout workflow", async (span) => {
    try {
      span.setAttribute("cart.id", input.cartId);
      span.setAttribute("customer.id", input.customerId);

      const inventory = await tracer.startActiveSpan(
        "reserve inventory",
        async (child) => {
          try {
            return await reserveInventory(input.cartId);
          } catch (error) {
            child.recordException(error as Error);
            child.setStatus({
              code: SpanStatusCode.ERROR,
              message: "inventory reservation failed",
            });
            throw error;
          } finally {
            child.end();
          }
        },
      );

      const payment = await tracer.startActiveSpan(
        "authorize payment",
        async (child) => {
          try {
            return await authorizePayment(input.payment);
          } finally {
            child.end();
          }
        },
      );

      return await createOrder({ inventory, payment });
    } catch (error) {
      span.recordException(error as Error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message : "checkout failed",
      });
      throw error;
    } finally {
      span.end();
    }
  });
}
```

Do not add card numbers, CVV values, raw payment tokens, session cookies, or
authorization headers as span attributes.

### 6. Append One Evlog Event Per Request

When the request finishes, append one event to the evlog stream. For HTTP
servers this usually belongs in a `finally` block or response-finished hook.
For jobs and webhooks, append when the unit of work succeeds or fails.

```ts
import { trace } from "@opentelemetry/api";

export async function appendEvlog(event: Record<string, unknown>) {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (observabilityConfig.streamsApiKey) {
    headers.Authorization = `Bearer ${observabilityConfig.streamsApiKey}`;
  }

  const response = await fetch(
    `${observabilityConfig.streamsUrl}/v1/stream/${encodeURIComponent(
      observabilityConfig.eventsStream,
    )}`,
    {
      method: "POST",
      headers,
      body: JSON.stringify(event),
    },
  );

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error(`evlog append failed: ${response.status} ${detail}`);
  }
}

export async function logRequestOutcome(args: {
  durationMs: number;
  error?: unknown;
  method: string;
  path: string;
  requestId: string;
  route?: string;
  spanId?: string | null;
  status: number;
  traceId?: string | null;
  userId?: string;
}) {
  const spanContext = trace.getActiveSpan()?.spanContext();
  const isError = args.status >= 500 || args.error != null;

  await appendEvlog({
    timestamp: new Date().toISOString(),
    level: isError ? "error" : args.status >= 400 ? "warn" : "info",
    service: observabilityConfig.serviceName,
    environment: observabilityConfig.environment,
    version: observabilityConfig.serviceVersion,
    requestId: args.requestId,
    traceId: args.traceId ?? spanContext?.traceId ?? null,
    spanId: args.spanId ?? spanContext?.spanId ?? null,
    method: args.method,
    path: args.path,
    route: args.route ?? null,
    status: args.status,
    duration: args.durationMs,
    message: requestMessage(args.method, args.path, args.status),
    why: isError ? explainError(args.error) : null,
    fix: isError ? suggestFix(args.error) : null,
    context: {
      userId: args.userId ?? null,
      cartId: getCurrentCartIdOrNull(),
      orderId: getCurrentOrderIdOrNull(),
    },
  });
}
```

The `evlog` profile normalizes unknown top-level fields into `context`, but new
integrations should write `context` explicitly so the event shape stays clear.

### 7. Ecommerce Event Shape

Use consistent `message`, `why`, `fix`, and `context` fields for common
workflows.

Successful product listing:

```json
{
  "level": "info",
  "service": "storefront",
  "environment": "production",
  "requestId": "req_...",
  "traceId": "742714b03a0c467fb728d2a4f6429378",
  "spanId": "086e83747d0e381e",
  "method": "GET",
  "path": "/api/products",
  "route": "/api/products",
  "status": 200,
  "duration": 56,
  "message": "Products listed",
  "context": {
    "categoryId": "cat_123",
    "resultCount": 18
  }
}
```

Payment failure:

```json
{
  "level": "error",
  "service": "checkout",
  "environment": "production",
  "requestId": "req_...",
  "traceId": "5b8efff798038103d269b633813fc60c",
  "spanId": "22dd83747d0e3822",
  "method": "POST",
  "path": "/api/checkout",
  "route": "/api/checkout",
  "status": 402,
  "duration": 234,
  "message": "Payment failed",
  "why": "Card declined by issuer",
  "fix": "Ask the customer to retry with a different card.",
  "context": {
    "cartId": "cart_551",
    "orderId": null,
    "paymentProvider": "stripe",
    "userId": "user_910"
  }
}
```

Slow search:

```json
{
  "level": "warn",
  "service": "storefront",
  "environment": "production",
  "requestId": "req_...",
  "method": "GET",
  "path": "/api/search",
  "route": "/api/search",
  "status": 200,
  "duration": 1230,
  "message": "Product search exceeded latency budget",
  "why": "Sequential scan on products for an unindexed sort.",
  "fix": "Add a covering index for category and price ordering.",
  "context": {
    "query": "running shoes",
    "resultCount": 42
  }
}
```

## Querying Correlated Data

After setup, clients should discover the stream pair from metadata.

```bash
curl "$STREAMS_URL/v1/stream/$EVENTS_STREAM/_details" \
  -H "$STREAMS_AUTH_HEADER"
```

The response should include:

```json
{
  "stream": {
    "profile": "evlog",
    "observability": {
      "request": {
        "events_stream": "ecommerce-production-events",
        "traces_stream": "ecommerce-production-traces"
      }
    }
  }
}
```

Lookup a request:

```bash
curl -X POST "$STREAMS_URL/v1/observe/request" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER" \
  --data-binary @- <<JSON
{
  "streams": {
    "events": "ecommerce-production-events",
    "traces": "ecommerce-production-traces"
  },
  "lookup": {
    "requestId": "req_123"
  },
  "include": {
    "events": true,
    "trace": true,
    "timeline": true,
    "raw": false
  },
  "limits": {
    "events": 100,
    "spans": 5000
  }
}
JSON
```

The response contains:

- `summary`: method, path, status, duration, service, level, and error summary
- `evlog.primary`: the best matching request event
- `trace.spans`: deduplicated spans
- `trace.tree`: parent/child waterfall data
- `trace.serviceMap`: service edges
- `trace.errors`: span errors and exceptions
- `timeline`: merged event/span timeline
- `coverage.warnings`: partial-result warnings that a UI or agent must surface

For an app handoff, this is the main acceptance test: one known checkout
request should produce both an evlog event and a multi-span trace in the same
`/v1/observe/request` response.

## Search Examples

Use the evlog stream as the default operational list:

```bash
curl -X POST "$STREAMS_URL/v1/stream/$EVENTS_STREAM/_search" \
  -H "Content-Type: application/json" \
  -H "$STREAMS_AUTH_HEADER" \
  --data-binary '{"q":"level:error service:checkout","size":50,"sort":["timestamp:desc","offset:desc"]}'
```

Useful ecommerce searches:

- `level:error`
- `service:checkout status:>=400`
- `duration:>1000`
- `path:/api/checkout`
- `req:req_123`
- `trace:5b8efff798038103d269b633813fc60c`
- `message:"Payment failed"`
- `why:"Sequential scan"`

Use the trace stream for span-centric searches:

- `svc:checkout error:true`
- `op:"authorize payment"`
- `db:postgresql duration_ms:>200`
- `route:/api/products`
- `trace:5b8efff798038103d269b633813fc60c`

## Sampling Policy

For a first ecommerce integration:

- Write one evlog event for every request and job.
- Always write evlog events for errors, failed payments, failed checkout,
  inventory reservation failures, and webhooks.
- Start with trace sampling at 100 percent in local and staging.
- In production, reduce trace sampling only after verifying that
  `/v1/observe/request` still gives enough detail for high-value workflows.
- If traces are sampled, evlog still provides a complete request ledger, and
  the request detail response may include a warning or missing trace side.

If the app samples traces, keep `request.id` on every sampled root/server span
so sampled traces still join correctly.

## Privacy And Safety

Default posture for ecommerce:

- Do not store card numbers, CVV values, raw payment tokens, session cookies,
  bearer tokens, password reset tokens, or full authorization headers.
- Prefer stable IDs such as `userId`, `cartId`, `orderId`, `paymentProvider`,
  `productId`, `sku`, `tenantId`, and `region`.
- Avoid raw query strings in spans unless reviewed. Use `urlMode:
  "drop_query"` by default.
- Avoid raw SQL statements unless reviewed. Use `dbStatementMode: "drop"` by
  default.
- Put large or sensitive request/response bodies in neither evlog nor spans.
- Configure `redactKeys` on both profiles, because evlog events and span
  attributes are normalized independently.

## Implementation Checklist For An Agent

Use this checklist when handing the work to an implementation agent.

1. Add environment variables:
   - `STREAMS_URL`
   - `STREAMS_API_KEY` when Streams uses API-key auth
   - `STREAMS_EVENTS_STREAM`
   - `STREAMS_TRACES_STREAM`
   - `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
   - `OTEL_SERVICE_NAME`
   - `APP_VERSION`
2. Provision the two streams:
   - create both as `application/json`
   - install `evlog` on the events stream
   - install `otel-traces` on the traces stream
   - set `observability.request` descriptors both ways
3. Initialize OpenTelemetry before loading app routes:
   - resource attributes include service name, version, and environment
   - OTLP exporter points at Streams
   - exporter sends `Authorization: Bearer <API_KEY>` when required
4. Add request ID middleware:
   - accept or generate `x-request-id`
   - store it in request context
   - echo it in the response
5. Annotate root/server spans:
   - set `request.id`
   - set route/method/path/status attributes where available
6. Add manual spans around ecommerce domain operations:
   - product search/listing
   - cart mutation
   - checkout workflow
   - payment authorization/capture
   - inventory reservation
   - order creation
   - email/notification dispatch
   - webhook processing
7. Append one evlog event when each request/job finishes:
   - include `requestId`, `traceId`, and `spanId`
   - include status, duration, service, environment, route, message
   - include `why` and `fix` for failures and slow paths
   - include safe IDs in `context`
8. Validate:
   - `GET /v1/stream/$EVENTS_STREAM/_profile` returns `kind: "evlog"`
   - `GET /v1/stream/$TRACES_STREAM/_profile` returns `kind:
     "otel-traces"`
   - `GET /v1/stream/$EVENTS_STREAM/_details` exposes the observability pair
   - one test request appears in `POST /v1/stream/$EVENTS_STREAM/_search`
   - OTLP export to Streams returns HTTP `200`
   - `POST /v1/observe/request` by `requestId` returns `evlog.primary` and
     `trace.spans`
   - no secrets appear in evlog source records or span attributes

## Common Pitfalls

- Installing profiles after appending data. Profiles must be installed before
  the first append.
- Using one stream for both events and spans. Keep `evlog` and `otel-traces`
  separate.
- Forgetting `request.id` on the root/server span. Without it, request ID
  lookup can still work by trace ID, but request ID correlation is weaker.
- Generating a new request ID for evlog that differs from the request ID stored
  on the span.
- Guessing stream pairs by name. Use `observability.request` descriptors.
- Hiding `coverage.warnings` in request-detail tooling.
- Logging secrets in `context`, raw span attributes, URL query strings, or SQL
  statements.
- Treating trace sampling as log sampling. Evlog should remain the complete
  request ledger even when traces are sampled.

## Related References

- [profile-evlog.md](./profile-evlog.md)
- [profile-otel-traces.md](./profile-otel-traces.md)
- [request-observability.md](./request-observability.md)
- [durable-streams-spec.md](./durable-streams-spec.md)
- [auth.md](./auth.md)
