# HTTP API reference

Base URL: `http://<host>:<API_PORT>` (default `http://localhost:8000`). All
request and response bodies are JSON; send `Content-Type: application/json`.
Paths are versioned under `/v1` (health probes are not). Interactive docs are
served at `/docs` (Swagger UI) and the raw schema at `/openapi.json`.

See the [README](../README.md#signed-deliveries-wire-format) for the outbound
delivery headers and HMAC verification.

## Conventions

### Errors

Every failure uses the same envelope:

```json
{
  "error": {
    "code": "conflict",
    "message": "endpoint is disabled",
    "details": null
  }
}
```

`details` is present only for `validation_error` (a list of
`{"location": [...], "message": "..."}`) . Codes:

| HTTP | `code`             | Raised when |
| ---- | ------------------ | ----------- |
| 400  | `invalid_request`  | Bad query value (e.g. unknown `status`), or body/header idempotency keys disagree. |
| 404  | `not_found`        | Endpoint or event UUID does not exist. |
| 404  | `http_error`       | Any other Starlette-level `HTTPException`, e.g. an unknown route. |
| 409  | `conflict`         | Endpoint disabled/deleted; retry of an in-flight, delivered, or already-queued event. |
| 422  | `validation_error` | Request body, path or query validation failed (pydantic), malformed UUID included. |
| 500  | `internal_error`   | Unexpected server error. |

Request bodies are strict (`extra="forbid"`): unknown fields fail with `422`.

### Pagination

List responses share `{items, total, limit, offset}`. `limit` is capped:
`1..200` for both endpoints and events, default `50`; `offset >= 0`.

- `GET /v1/events`: `total` is the full count of matching rows (ignores
  `limit`/`offset`).
- `GET /v1/endpoints`: `total` is likewise the full count of matching endpoints
  (soft-deleted ones are excluded unless `include_deleted=true`).

### Timestamps and IDs

All timestamps are ISO-8601 UTC with an offset (from the database clock). IDs are
UUIDs; attempt rows also have a bigint `attempt_id` used internally. Issuing a
malformed UUID in a path yields `422`.

## Endpoints

### `POST /v1/endpoints` — register an endpoint

Registers a destination and generates its HMAC secret. **The secret is returned
in this response only** and never appears in any other representation.

Request (`EndpointCreate`):

| Field  | Type   | Constraints |
| ------ | ------ | ----------- |
| `name` | string | 1–200 chars, non-empty after trimming. |
| `url`  | string | Valid `http://` / `https://` URL. |

```sh
curl -sX POST http://localhost:8000/v1/endpoints \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","url":"https://example.com/hooks/orders"}'
```

`201 Created`

```json
{
  "id": "6f1c1c2e-4b1a-4f5e-9a2b-0c3d4e5f6a7b",
  "name": "orders",
  "url": "https://example.com/hooks/orders",
  "state": "active",
  "is_active": true,
  "created_at": "2026-10-02T12:00:00.000000+00:00",
  "updated_at": "2026-10-02T12:00:00.000000+00:00",
  "secret": "Fq2n...URL-safe-32-bytes..."
}
```

### `GET /v1/endpoints` — list endpoints

Query: `limit` (1–200, default 50), `offset` (≥0), `include_deleted` (bool,
default `false`). Newest first. Deleted endpoints are hidden unless
`include_deleted=true`.

```sh
curl -s 'http://localhost:8000/v1/endpoints?limit=10'
```

`200 OK`: `{items: [EndpointOut...], total, limit, offset}` — items omit
`secret`.

### `GET /v1/endpoints/{endpoint_id}` — inspect an endpoint

```sh
curl -s http://localhost:8000/v1/endpoints/6f1c1c2e-4b1a-4f5e-9a2b-0c3d4e5f6a7b
```

`200 OK` `EndpointOut` (no secret) · `404` unknown · `422` malformed UUID.

### `PATCH /v1/endpoints/{endpoint_id}` — update / disable

Request (`EndpointUpdate`, all fields optional): `name`, `url`, `is_active`.
`is_active: false` sets `state` to `disabled` (submissions then return `409`);
`is_active: true` reactivates. A URL change applies to attempts claimed *after*
the update; in-flight attempts already hold the old URL.

```sh
curl -sX PATCH http://localhost:8000/v1/endpoints/6f1c.../ \
  -H 'Content-Type: application/json' \
  -d '{"is_active":false}'
```

`200 OK` `EndpointOut` · `404` · `409` if the endpoint is already deleted · `422`.

### `DELETE /v1/endpoints/{endpoint_id}` — soft delete

Sets `state='deleted'`; the row and its delivery history are retained. New events
to it are rejected with `409`. Deleting twice is idempotent (both return `204`).

```sh
curl -sX DELETE -o /dev/null -w '%{http_code}\n' \
  http://localhost:8000/v1/endpoints/6f1c.../
```

`204 No Content` · `404` unknown.

## Events

### `POST /v1/events` — submit an event

Request (`EventCreate`):

| Field             | Type    | Constraints / notes |
| ----------------- | ------- | ------------------- |
| `endpoint_id`     | UUID    | Must be an existing, active endpoint. |
| `event_type`      | string  | 1–100 chars. |
| `payload`         | object  | Any JSON object. |
| `idempotency_key` | string? | 1–255 chars. Per-endpoint uniqueness. |
| `max_attempts`    | int?    | 1–100; defaults to `DELIVERY_MAX_ATTEMPTS`. |

The idempotency key may instead be sent as the `Idempotency-Key` header. If both
are present and differ, the request fails with `400 invalid_request`.

```sh
curl -sX POST http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"endpoint_id":"6f1c1c2e-4b1a-4f5e-9a2b-0c3d4e5f6a7b",
       "event_type":"order.created",
       "payload":{"order_id":42,"total":1999}}'
```

`201 Created`, with `Location: /v1/events/{id}`:

```json
{
  "id": "8ad3...-event-uuid",
  "endpoint_id": "6f1c1c2e-4b1a-4f5e-9a2b-0c3d4e5f6a7b",
  "event_type": "order.created",
  "payload": {"order_id": 42, "total": 1999},
  "status": "pending",
  "attempt_count": 1,
  "max_attempts": 5,
  "next_attempt_at": "2026-10-02T12:00:01.000000+00:00",
  "last_error": null,
  "delivered_at": null,
  "dead_lettered_at": null,
  "created_at": "2026-10-02T12:00:01.000000+00:00",
  "updated_at": "2026-10-02T12:00:01.000000+00:00",
  "attempts": [
    {
      "attempt_number": 1,
      "state": "pending",
      "scheduled_at": "2026-10-02T12:00:01.000000+00:00",
      "started_at": null,
      "finished_at": null,
      "worker_id": null,
      "recoveries": 0,
      "response_status": null,
      "error": null,
      "duration_ms": null
    }
  ]
}
```

**Idempotent replay.** Resubmitting the same key for the same endpoint returns
`200 OK` with the *original* event unchanged (its payload, not the new one) and
`Idempotent-Replay: true`. A different key, or the same key on a different
endpoint, creates a new event.

`409` when the endpoint is disabled or deleted · `404` unknown endpoint · `422`
validation · `400` idempotency keys disagree.

### `GET /v1/events` — list events (dead-letter view)

Query: `status` (`pending` | `in_progress` | `delivered` | `dead_letter`),
`endpoint_id` (UUID), `limit`, `offset`. Newest first. This is how you page the
DLQ:

```sh
curl -s 'http://localhost:8000/v1/events?status=dead_letter&limit=50'
```

`200 OK` `{items: [EventOut...], total, limit, offset}` where `total` is the full
matching count. An unknown `status` → `400 invalid_request`; `limit` out of range
→ `422`.

### `GET /v1/events/{event_id}` — inspect an event and its attempts

Returns the event with its full attempt history (ordered by attempt number),
including the `worker_id` that handled each attempt, `duration_ms`,
`response_status`, `recoveries` and `error`.

```sh
curl -s http://localhost:8000/v1/events/8ad3...-event-uuid
```

`200 OK` `EventOut` · `404` unknown · `422` malformed UUID.

### `POST /v1/events/{event_id}/retry` — manually retry

Queues a fresh attempt for an event that is not currently in flight. The retry
grants a **new** attempt budget of `DELIVERY_MAX_ATTEMPTS` (or the request's
`max_attempts`) counted from the new attempt number, so a destination fixed after
the event dead-lettered gets the normal policy again.

Request body is optional: `{"max_attempts": <1-100>}` (or no body at all).

```sh
curl -sX POST http://localhost:8000/v1/events/8ad3...-event-uuid/retry
```

`202 Accepted` with the updated `EventOut` (new pending attempt appended) ·
`404` unknown event · `409` if the event is already delivered, has an attempt in
flight, or already has a queued attempt · `422`.

## Health

### `GET /healthz` — liveness

Never touches the database.

```sh
curl -s http://localhost:8000/healthz
# {"status":"ok","version":"0.1.0"}
```

### `GET /readyz` — readiness

Checks the database with `SELECT 1`.

```sh
curl -s -w '\n%{http_code}\n' http://localhost:8000/readyz
# {"status":"ready","database":"ok"}
# 200
```

`503 Service Unavailable` `{"status":"unavailable","database":"error"}` when the
database is unreachable.

## Object schemas

### `EndpointOut`

`id` (uuid), `name`, `url`, `state` (`active` | `disabled` | `deleted`),
`is_active` (bool, `state == "active"`), `created_at`, `updated_at`.
`EndpointCreated` = `EndpointOut` + `secret`.

### `EventOut`

`id`, `endpoint_id`, `event_type`, `payload`, `status`, `attempt_count` (number
of attempts scheduled so far), `max_attempts`, `next_attempt_at` (null when not
scheduled), `last_error`, `delivered_at`, `dead_lettered_at`, `created_at`,
`updated_at`, `attempts` (list of `AttemptOut`).

### `AttemptOut`

`attempt_number`, `state` (`pending` | `in_progress` | `succeeded` | `failed`),
`scheduled_at`, `started_at`, `finished_at`, `worker_id`, `recoveries` (times the
lease expired before this attempt finished), `response_status`, `error`,
`duration_ms`.

### State reference

| Event `status` | Meaning |
| -------------- | ------- |
| `pending` | Queued, or waiting for its next backoff slot. |
| `in_progress` | An attempt is currently claimed by a worker. |
| `delivered` | Terminal success. |
| `dead_letter` | Terminal failure; retries exhausted. Replay via `/retry`. |

| Attempt `state` | Meaning |
| --------------- | ------- |
| `pending` | Scheduled, not yet claimed. |
| `in_progress` | Claimed; a worker holds a lease. |
| `succeeded` | Terminal success. |
| `failed` | Terminal failure. |

See [architecture.md](architecture.md) for the full lifecycle and the recovery
semantics behind these states.

## End-to-end example

```sh
API=http://localhost:8000

# 1. Register a destination and capture id + secret.
ENDPOINT=$(curl -sX POST "$API/v1/endpoints" -H 'Content-Type: application/json' \
  -d '{"name":"demo","url":"https://example.com/hooks/demo"}')
ID=$(printf '%s' "$ENDPOINT" | python -c 'import sys,json;print(json.load(sys.stdin)["id"])')

# 2. Submit an event.
EVENT=$(curl -sX POST "$API/v1/events" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d "{\"endpoint_id\":\"$ID\",\"event_type\":\"demo.ping\",\"payload\":{\"n\":1}}")
EVENT_ID=$(printf '%s' "$EVENT" | python -c 'import sys,json;print(json.load(sys.stdin)["id"])')

# 3. Poll until it is delivered (or dead-lettered).
curl -s "$API/v1/events/$EVENT_ID" | python -m json.tool

# 4. If it dead-lettered, replay it after fixing the destination.
curl -sX POST "$API/v1/events/$EVENT_ID/retry"
```
