# Reliable Webhook Delivery Platform

Durable, at-least-once webhook delivery: a FastAPI service accepts events, a pool
of workers delivers them over signed HTTP with retries, exponential backoff and a
dead-letter queue. PostgreSQL is the only moving part — it is both the datastore
and the job queue, coordinated with `SELECT ... FOR UPDATE SKIP LOCKED` and
leases with worker fencing. No Redis, no broker.

```
Producer ──POST /v1/events──▶ API ──INSERT event + attempt──▶ PostgreSQL
                                                                  ▲   ▲
                                        claim (SKIP LOCKED) ──────┘   │
                                                                     │
Worker ──signed POST (HMAC-SHA256)──▶ your endpoint ──2xx / non-2xx──┘
```

- **Docs**: [architecture](docs/architecture.md) · [HTTP API reference](docs/api.md)
- **Interactive API**: `http://localhost:8000/docs` (Swagger UI), `/openapi.json`

## Features

- Durable event submission with per-endpoint idempotency keys.
- At-least-once delivery to registered endpoints, signed with HMAC-SHA256.
- Exponential backoff with jitter; per-event attempt budget; dead-lettering.
- Crash-safe workers: a lease plus `worker_id` fencing means a reclaimed attempt
  can never be overwritten by the worker that lost it.
- Manual replay/retry of failed or dead-lettered events.
- Structured JSON logs (one object per line) with `event_id` / `endpoint_id` /
  `attempt_number` fields, ready for a log pipeline.
- `/healthz` (liveness) and `/readyz` (readiness, checks the database).

## Quickstart with Docker Compose

The Compose stack runs PostgreSQL, applies migrations once, then starts the API
and two workers. The API is published on port 8000. Compose supplies its own
service configuration, so no `.env` is required for it (`.env` is for local,
non-Compose runs — see [Configuration](#configuration)).

```sh
docker compose up --build
curl -s http://localhost:8000/readyz
```

That starts these services:

| Service    | Role                                                              |
| ---------- | ----------------------------------------------------------------- |
| `postgres` | PostgreSQL 16 with a named data volume and a `pg_isready` healthcheck |
| `migrate`  | One-shot `alembic upgrade head`; other services wait for it to exit 0 |
| `api`      | `webhooks-api`, published on `localhost:8000`                     |
| `worker-1` | `webhooks-worker` with `WORKER_ID=worker-1`                       |
| `worker-2` | `webhooks-worker` with `WORKER_ID=worker-2`                       |

Stop it with `docker compose down` (add `-v` to delete the database volume); Make
equivalents are `make up`, `make down` (keeps the database volume) and
`make logs`.

### End-to-end verification against the running stack

The verify override adds a reference receiver on port 9000 and a one-shot service
that runs the acceptance script:

```sh
docker compose -f docker-compose.yml -f docker-compose.verify.yml run --rm verify
# or, from the Makefile:
make e2e
```

`verify` is gated behind the `verify` profile, so use `--profile verify` if you
prefer to boot it with `docker compose --profile verify up`. Running the service
by name with `run` works with or without the profile flag.

It prints one `[PASS]`/`[FAIL]` line per check and exits non-zero if anything
fails. See [Verification](#verification) for the checks it covers.

## Running locally without Docker

`scripts/local_postgres.py` starts a real PostgreSQL using the `pgserver` dev
dependency (no Docker, no runtime downloads) and prints a `DATABASE_URL`.

```sh
# 1. Python deps (creates .venv and installs the project + dev extras)
uv venv && uv pip install -e '.[dev]'
# or: python -m venv .venv && .venv/bin/pip install -e '.[dev]'

# 2. Start PostgreSQL and capture its URL
python scripts/local_postgres.py           # prints DATABASE_URL, writes .e2e/database-url
export DATABASE_URL="$(cat .e2e/database-url)"

# 3. Create the schema
alembic upgrade head

# 4. Run the three processes (separate shells), each with DATABASE_URL set
webhooks-api          # http://127.0.0.1:8000
webhooks-worker       # long-running claim/deliver loop
webhooks-receiver     # reference receiver on http://127.0.0.1:9000
```

Equivalent Make targets: `make local-db`, `make migrate`, `make api`,
`make worker`, `make receiver`. `make help` lists everything.

If you start more than one worker by hand, give each a distinct `WORKER_ID`
(otherwise they fence each other's attempts): `WORKER_ID=worker-a webhooks-worker`.

## Configuration

All settings are read from the environment (and an optional `.env`) via
pydantic-settings; API and workers share the same object. See `.env.example` for
the annotated list.

| Variable                            | Default                                                     | Meaning |
| ----------------------------------- | ----------------------------------------------------------- | ------- |
| `DATABASE_URL`                      | `postgresql+asyncpg://webhooks:webhooks@localhost:5432/webhooks` | Async SQLAlchemy URL (asyncpg driver). Alembic reuses it. |
| `DB_POOL_SIZE`                      | `10`                                                        | SQLAlchemy connection pool size per process. |
| `DB_MAX_OVERFLOW`                   | `20`                                                        | Extra connections allowed above the pool size. |
| `API_HOST`                          | `0.0.0.0`                                                   | Uvicorn bind host. |
| `API_PORT`                          | `8000`                                                      | Uvicorn bind port. |
| `DELIVERY_TIMEOUT_SECONDS`          | `10`                                                        | Per-attempt outbound HTTP timeout. |
| `DELIVERY_MAX_ATTEMPTS`             | `5`                                                         | Default attempt budget per event. |
| `DELIVERY_BACKOFF_BASE_SECONDS`     | `2`                                                         | Base of the exponential backoff. |
| `DELIVERY_BACKOFF_MAX_SECONDS`      | `300`                                                       | Cap on the computed delay. |
| `DELIVERY_BACKOFF_JITTER_RATIO`     | `0.2`                                                       | Uniform jitter added as a fraction of the delay (`0` disables). |
| `DELIVERY_RESPONSE_SNIPPET_BYTES`   | `512`                                                       | Bytes of a failing response body kept for diagnostics. |
| `WORKER_ID`                         | `<hostname>-<pid>`                                          | Unique per worker process; lease fencing key. |
| `WORKER_BATCH_SIZE`                 | `10`                                                        | Attempts claimed per poll. |
| `WORKER_CONCURRENCY`                | `8`                                                         | In-flight deliveries per worker. |
| `WORKER_POLL_INTERVAL_SECONDS`      | `1`                                                         | Sleep when no work was claimed. |
| `WORKER_LEASE_SECONDS`              | `60`                                                        | Claim lease length; must be ≥ 2 × `DELIVERY_TIMEOUT_SECONDS`. |
| `LOG_LEVEL`                         | `INFO`                                                      | Root log level. |
| `LOG_FORMAT`                        | `json`                                                      | `json` (one JSON object per line) or `console` (human readable). |

The reference receiver reads `RECEIVER_HOST` / `RECEIVER_PORT` (default
`0.0.0.0:9000`) instead.

Two invariants are enforced at startup, and a bad value fails fast:

- `WORKER_LEASE_SECONDS >= 2 * DELIVERY_TIMEOUT_SECONDS` — otherwise a second
  worker could reclaim an attempt while the first is still inside its HTTP call.
- `DELIVERY_BACKOFF_MAX_SECONDS >= DELIVERY_BACKOFF_BASE_SECONDS`.

## HTTP API

Full reference with request/response fields and copy-paste curls:
[docs/api.md](docs/api.md).

```sh
# Register an endpoint; the HMAC secret is returned exactly once.
curl -sX POST http://localhost:8000/v1/endpoints \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","url":"https://example.com/hooks/orders"}'

# Submit an event (idempotency key optional but recommended).
curl -sX POST http://localhost:8000/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"endpoint_id":"<uuid>","event_type":"order.created","payload":{"order_id":42}}'

# Inspect it (status, attempts, worker_id, last error).
curl -s http://localhost:8000/v1/events/<event-uuid>

# The dead-letter queue.
curl -s 'http://localhost:8000/v1/events?status=dead_letter'

# Replay a failed / dead-lettered event.
curl -sX POST http://localhost:8000/v1/events/<event-uuid>/retry
```

Errors are always `{"error": {"code", "message", "details"?}}`; `POST /v1/events`
replays an idempotent submission with `200` and `Idempotent-Replay: true`.

## Signed deliveries (wire format)

Every delivery is an HTTP `POST` with `Content-Type: application/json` and these
headers:

| Header                  | Value                                                        |
| ----------------------- | ------------------------------------------------------------ |
| `X-Webhook-Id`          | Event UUID.                                                  |
| `X-Webhook-Timestamp`   | Unix seconds (integer) at signing time.                      |
| `X-Webhook-Attempt`     | 1-based attempt number.                                      |
| `X-Webhook-Event-Type`  | The `event_type` of the event.                               |
| `X-Webhook-Signature`   | `sha256=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`.    |

The body is sent as compact canonical JSON — `json.dumps(payload,
separators=(",", ":"), sort_keys=True, default=str)` — and the signature is
computed over the exact bytes on the wire, so verify **the raw request body**,
not a re-serialized version. The timestamp is inside the signed string, so a receiver can reject
replays older than a freshness window (the reference receiver uses 300 s).

Verify in Python:

```python
import hashlib, hmac


def verify(secret: str, headers, raw_body: bytes, *, tolerance: int = 300) -> bool:
    """Return True only if the signature is fresh and matches."""
    ts = int(headers["X-Webhook-Timestamp"])
    if abs(int(__import__("time").time()) - ts) > tolerance:
        return False
    signed = f"{ts}.".encode() + raw_body
    expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    provided = headers["X-Webhook-Signature"].removeprefix("sha256=")
    return hmac.compare_digest(expected, provided)  # constant time
```

Or, using the helpers the platform itself ships
(`webhooks.security.verify_signature`):

```python
from webhooks.security import verify_signature

ok = verify_signature(secret, timestamp, raw_body, signature_header)  # tolerance 300 s
```

Shell equivalent of the digest:

```sh
printf '%s.%s' "$TIMESTAMP" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex
```

Deliveries are **at-least-once**: a worker that crashes mid-POST may have its
attempt redelivered once its lease expires, and a `2xx` lost on the way back is
retried. Receivers must be idempotent — dedupe on `X-Webhook-Id` (plus attempt
where you need it). Non-2xx, timeouts, DNS/TLS errors and connection resets are
all failures and are retried under the backoff policy.

## Testing, lint and typecheck

```sh
ruff check .              # lint
ruff format --check .     # formatting
mypy                      # strict; config lives in pyproject.toml
python -m pytest -q       # full suite
```

The test suite needs PostgreSQL. Set `TEST_DATABASE_URL` to point at a database,
otherwise the `pgserver` dev dependency starts an embedded PostgreSQL
automatically — so `python -m pytest` works with or without Docker. Migrations
are applied by the fixtures.

Make targets: `make lint`, `make format`, `make typecheck`, `make test`.

## Verification

`scripts/e2e_verify.py` is an executable acceptance test for a *running* stack.
It exits `0` only when every check passes:

| # | Check |
| - | ----- |
| 1 | API `/healthz` + `/readyz` ready and reference receiver reachable |
| 2 | End-to-end delivery with a verified HMAC signature |
| 3 | Duplicate submission with the same idempotency key delivered exactly once |
| 4 | Retry proven against a receiver that fails twice (`500, 500, 200`) |
| 5 | Exhausted retries → dead letter → manual retry → delivered |
| 6 | Two concurrent workers, no double delivery |
| 7 | Event inspection exposes attempt-level identifiers |

Run it locally (with the API, worker and receiver all up):

```sh
API_URL=http://127.0.0.1:8000 \
RECEIVER_URL=http://127.0.0.1:9000 \
DATABASE_URL="$(cat .e2e/database-url)" \
python scripts/e2e_verify.py
```

`API_URL`/`RECEIVER_URL` default to `http://127.0.0.1:8000` and
`http://127.0.0.1:9000`; `DATABASE_URL` is required for the direct SQL
assertions. Inside Compose the same script runs as the one-shot `verify` service
(see above), with those variables pointing at the service names.

## Project layout

```
src/webhooks/
  api/                 FastAPI app: routes, schemas, error contract, probes
  worker/              claim/deliver loop and outbound HTTP delivery
  testing/             reference receiver + control client (verification helpers)
  models.py            SQLAlchemy models (endpoints, events, delivery_attempts)
  service.py           domain logic and the SQL claim/complete statements
  security.py          HMAC signing/verification and wire-format headers
  backoff.py           exponential backoff with jitter
  config.py            pydantic-settings
  db.py                async engine/session wiring
  logging_setup.py     JSON/console log formatting
migrations/            Alembic environment and revisions
scripts/               local PostgreSQL bootstrap, e2e verification driver
tests/                 pytest suite (real PostgreSQL, real HTTP, no mocks)
```

## Known limitations

These are deliberate scope choices for this iteration, not bugs — but they matter
before you run this in production:

- **Endpoint secrets are stored in plaintext.** The worker must compute the HMAC,
  so the secret is retrievable at claim time. The API returns it exactly once on
  creation and never serializes it again, but anyone with database or process
  access can read it. At-rest encryption / KMS is not implemented.
- **No API authentication or authorization.** Every endpoint (including manage
  endpoints, event payloads and the DLQ view) is open. Terminate auth in front of
  the API (gateway, service mesh) before exposing it.
- **Lease-based recovery, not heartbeat-based.** A crashed worker's attempt is
  recovered only when `lease_expires_at` passes; the recovery latency is
  `WORKER_LEASE_SECONDS` (60 s by default). There is no background renewer, so a
  slow-but-alive delivery longer than the lease can be re-claimed while still
  running (the `worker_id` fence makes the loser's write a no-op, but the
  receiver may see the delivery twice).
- **No `Retry-After` support.** Retry timing is purely the exponential backoff;
  a `429`/`503` with `Retry-After` is treated like any other non-2xx.
- **Soft-deleted endpoints are retained.** `DELETE` sets `state='deleted'` and
  keeps the row (and its delivery history) forever; there is no hard-delete or
  retention job. Deleted endpoints reject new events but existing events remain
  inspectable.
- **The reference receiver keeps state in memory.** Deliveries, counters and
  hook configuration live in process memory: restarting it loses them, and it is
  not safe to run multiple receiver replicas behind a load balancer.
- **At-least-once, not exactly-once.** Duplicate deliveries are possible across
  lease expiry and lost acknowledgements; consumers must deduplicate (the
  `X-Webhook-Id` / `X-Webhook-Attempt` headers make that a keyed upsert).

## License

No license file is included in this repository.
