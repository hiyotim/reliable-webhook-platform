#!/usr/bin/env python
"""End-to-end verification of a running stack.

Proves the acceptance scenarios against real processes:

1. readiness of the API and its database;
2. a webhook delivered end to end to a local test receiver, HMAC signature verified;
3. duplicate submission delivered exactly once (idempotency key);
4. retry proven with an intentionally failing receiver (three attempts: 500, 500, 200);
5. exhausted retries -> dead letter -> manual retry -> delivered;
6. two concurrent workers never double-deliver a claimed attempt.

Usage:
    python scripts/e2e_verify.py                     # against defaults below
    API_URL=http://api:8000 RECEIVER_URL=http://receiver:9000 \
        DATABASE_URL=postgresql+asyncpg://webhooks:webhooks@postgres:5432/webhooks \
        python scripts/e2e_verify.py

Exit code 0 means every check passed; 1 means at least one failed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import asyncpg
import httpx

API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://127.0.0.1:9000")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
EVENT_TIMEOUT = float(os.environ.get("E2E_EVENT_TIMEOUT", "60"))
# Enough events that the queue cannot be drained by a single worker before the
# other one wakes up: with a 0.5s receiver delay, one worker needs ~2s to clear
# 40 events in batches of 10, while a worker that is idle polls within 0.25s.
CONCURRENT_EVENTS = int(os.environ.get("E2E_CONCURRENT_EVENTS", "40"))
CONCURRENT_DELAY_SECONDS = float(os.environ.get("E2E_CONCURRENT_DELAY_SECONDS", "0.5"))


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
@dataclass
class Check:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append(Check(name, passed, detail))
        mark = "PASS" if passed else "FAIL"
        print(f"[{mark}] {name}" + (f" - {detail}" if detail else ""), flush=True)

    @property
    def failed(self) -> list[Check]:
        return [check for check in self.checks if not check.passed]


def asyncpg_kwargs(url: str) -> dict[str, Any]:
    """Translate a SQLAlchemy-style URL into asyncpg.connect kwargs."""
    normalized = url.replace("postgresql+asyncpg://", "postgresql://")
    parsed = urllib.parse.urlsplit(normalized)
    kwargs: dict[str, Any] = {}
    if parsed.hostname:
        kwargs["host"] = parsed.hostname
    if parsed.port:
        kwargs["port"] = parsed.port
    query = urllib.parse.parse_qs(parsed.query)
    if "host" in query:  # unix socket directory (scripts/local_postgres.py)
        kwargs["host"] = query["host"][0]
    if parsed.username:
        kwargs["user"] = parsed.username
    if parsed.password:
        kwargs["password"] = parsed.password
    database = parsed.path.lstrip("/")
    if database:
        kwargs["database"] = database
    return kwargs


class Api:
    """Thin client for the platform API."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._client.get(path, **kwargs)

    async def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self._client.post(path, **kwargs)

    async def create_endpoint(self, name: str, url: str) -> dict[str, Any]:
        response = await self._client.post("/v1/endpoints", json={"name": name, "url": url})
        response.raise_for_status()
        return dict(response.json())

    async def submit(
        self,
        endpoint_id: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
    ) -> httpx.Response:
        body: dict[str, Any] = {
            "endpoint_id": endpoint_id,
            "event_type": "e2e.verification",
            "payload": payload,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if max_attempts is not None:
            body["max_attempts"] = max_attempts
        return await self._client.post("/v1/events", json=body)

    async def event(self, event_id: str) -> dict[str, Any]:
        response = await self._client.get(f"/v1/events/{event_id}")
        response.raise_for_status()
        return dict(response.json())

    async def wait_for_status(
        self, event_id: str, status: str, *, timeout: float = EVENT_TIMEOUT
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last = "unknown"
        while time.monotonic() < deadline:
            event = await self.event(event_id)
            last = event["status"]
            if last == status:
                return event
            if last in {"delivered", "dead_letter"} and last != status:
                return event
            await asyncio.sleep(0.25)
        raise TimeoutError(f"event {event_id} stayed '{last}', expected '{status}'")


class Receiver:
    """Thin client for the reference receiver's control endpoints."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    def hook_url(self, hook_id: str) -> str:
        return f"{RECEIVER_URL}/hooks/{hook_id}"

    async def health(self) -> httpx.Response:
        return await self._client.get("/healthz")

    async def reset(self) -> None:
        response = await self._client.delete("/deliveries")
        response.raise_for_status()

    async def configure(self, hook_id: str, **settings: Any) -> None:
        response = await self._client.put(f"/hooks/{hook_id}/config", json=settings)
        response.raise_for_status()

    async def deliveries(self, hook_id: str | None = None) -> dict[str, Any]:
        params = {"hook_id": hook_id} if hook_id else None
        response = await self._client.get("/deliveries", params=params)
        response.raise_for_status()
        return dict(response.json())

    async def wait_for_count(
        self, hook_id: str, count: int, *, timeout: float = EVENT_TIMEOUT
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            payload = await self.deliveries(hook_id)
            if payload["total"] >= count:
                return payload
            await asyncio.sleep(0.25)
        raise TimeoutError(f"hook {hook_id} received {payload['total']} of {count} deliveries")


async def fetch_all(query: str, *args: Any, url: str = DATABASE_URL) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(**asyncpg_kwargs(url))
    try:
        return list(await connection.fetch(query, *args))
    finally:
        await connection.close()


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
async def check_health(api: Api, receiver: Receiver, report: Report) -> None:
    health = await api.get("/healthz")
    ready = await api.get("/readyz")
    receiver_health = await receiver.health()
    report.record(
        "1. API liveness + readiness + receiver reachable",
        health.status_code == 200
        and ready.status_code == 200
        and ready.json().get("database") == "ok"
        and receiver_health.status_code == 200,
        f"/healthz={health.status_code} /readyz={ready.status_code} readyz={ready.json()}",
    )


async def check_end_to_end(api: Api, receiver: Receiver, report: Report) -> None:
    hook = "e2e-success"
    endpoint = await api.create_endpoint("e2e success", receiver.hook_url(hook))
    await receiver.configure(hook, secret=endpoint["secret"])

    payload = {"order_id": 4711, "kind": "e2e"}
    response = await api.submit(endpoint["id"], payload)
    event_id = response.json()["id"]
    event = await api.wait_for_status(event_id, "delivered")

    deliveries = await receiver.deliveries(hook)
    received = deliveries["items"][0]
    ok = (
        response.status_code == 201
        and event["status"] == "delivered"
        and len(event["attempts"]) == 1
        and event["attempts"][0]["state"] == "succeeded"
        and event["attempts"][0]["response_status"] == 200
        and deliveries["total"] == 1
        and received["signature_valid"] is True
        and received["event_id"] == event_id
        and received["attempt"] == 1
        and json.loads(received["body"]) == payload
        and "secret" not in json.dumps(event)
    )
    report.record(
        "2. End-to-end delivery with verified HMAC signature",
        ok,
        f"status={event['status']} attempts={len(event['attempts'])} "
        f"receiver_requests={deliveries['total']} signature_valid={received['signature_valid']}",
    )


async def check_idempotency(api: Api, receiver: Receiver, report: Report) -> None:
    hook = "e2e-idempotent"
    endpoint = await api.create_endpoint("e2e idempotent", receiver.hook_url(hook))
    await receiver.configure(hook, secret=endpoint["secret"])

    key = f"e2e-key-{int(time.time())}"
    first = await api.submit(endpoint["id"], {"n": 1}, idempotency_key=key)
    second = await api.submit(endpoint["id"], {"n": 2}, idempotency_key=key)
    event_id = first.json()["id"]
    await api.wait_for_status(event_id, "delivered")
    deliveries = await receiver.deliveries(hook)

    ok = (
        first.status_code == 201
        and second.status_code == 200
        and second.headers.get("idempotent-replay") == "true"
        and second.json()["id"] == event_id
        and second.json()["payload"] == {"n": 1}
        and deliveries["total"] == 1
        and len((await api.event(event_id))["attempts"]) == 1
    )
    report.record(
        "3. Duplicate submission delivered exactly once",
        ok,
        f"first={first.status_code} replay={second.status_code} "
        f"receiver_requests={deliveries['total']}",
    )


async def check_retry(api: Api, receiver: Receiver, report: Report) -> None:
    hook = "e2e-retry"
    endpoint = await api.create_endpoint("e2e retry", receiver.hook_url(hook))
    await receiver.configure(hook, secret=endpoint["secret"], fail_first_n=2, failure_status=500)

    response = await api.submit(endpoint["id"], {"retry": True})
    event_id = response.json()["id"]
    started = time.monotonic()
    event = await api.wait_for_status(event_id, "delivered")
    elapsed = time.monotonic() - started

    attempts = event["attempts"]
    deliveries = await receiver.wait_for_count(hook, 3)
    ok = (
        [attempt["response_status"] for attempt in attempts] == [500, 500, 200]
        and [attempt["state"] for attempt in attempts] == ["failed", "failed", "succeeded"]
        and [item["attempt"] for item in deliveries["items"]] == [1, 2, 3]
        and all(item["signature_valid"] is True for item in deliveries["items"])
        and deliveries["duplicates"] == []
    )
    report.record(
        "4. Retry after intentional receiver failures (500, 500, 200)",
        ok,
        f"statuses={[a['response_status'] for a in attempts]} elapsed={elapsed:.1f}s "
        f"receiver_requests={deliveries['total']}",
    )


async def check_dead_letter_and_manual_retry(api: Api, receiver: Receiver, report: Report) -> None:
    hook = "e2e-dead-letter"
    endpoint = await api.create_endpoint("e2e dead letter", receiver.hook_url(hook))
    await receiver.configure(hook, secret=endpoint["secret"], always_fail=True, failure_status=500)

    response = await api.submit(endpoint["id"], {"dlq": True}, max_attempts=2)
    event_id = response.json()["id"]
    event = await api.wait_for_status(event_id, "dead_letter")
    failed_attempts = len(event["attempts"])
    last_error = event["last_error"]

    listing = await api.get("/v1/events", params={"status": "dead_letter", "limit": 200})
    listed = any(item["id"] == event_id for item in listing.json()["items"])

    # Destination is fixed -> manual retry must deliver the event.
    await receiver.configure(hook, always_fail=False, fail_first_n=0)
    retry = await api.post(f"/v1/events/{event_id}/retry")
    retried = await api.wait_for_status(event_id, "delivered")
    deliveries = await receiver.wait_for_count(hook, failed_attempts + 1)

    ok = (
        event["status"] == "dead_letter"
        and failed_attempts == 2
        and last_error == "unexpected status 500"
        and listed
        and retry.status_code == 202
        and retried["status"] == "delivered"
        and len(retried["attempts"]) == failed_attempts + 1
        and retried["attempts"][-1]["state"] == "succeeded"
        and deliveries["duplicates"] == []
    )
    report.record(
        "5. Dead letter after exhausted retries, then manual retry delivers",
        ok,
        f"dead_letter_attempts={failed_attempts} last_error={last_error!r} "
        f"listed_in_dlq={listed} retry_status={retry.status_code} "
        f"final={retried['status']}",
    )


async def check_two_workers_no_double_delivery(
    api: Api, receiver: Receiver, report: Report
) -> None:
    hook = "e2e-concurrent"
    # Slow enough that both workers are busy at the same time.
    await receiver.configure(hook, delay_seconds=CONCURRENT_DELAY_SECONDS)
    endpoint = await api.create_endpoint("e2e concurrent", receiver.hook_url(hook))
    await receiver.configure(
        hook, secret=endpoint["secret"], delay_seconds=CONCURRENT_DELAY_SECONDS
    )

    event_ids = []
    for index in range(CONCURRENT_EVENTS):
        response = await api.submit(endpoint["id"], {"index": index})
        event_ids.append(response.json()["id"])

    for event_id in event_ids:
        await api.wait_for_status(event_id, "delivered", timeout=EVENT_TIMEOUT * 2)

    deliveries = await receiver.wait_for_count(hook, CONCURRENT_EVENTS)
    histogram = await fetch_all(
        "SELECT a.worker_id, count(*) AS delivered FROM delivery_attempts a "
        "JOIN events e ON e.id = a.event_id "
        "WHERE e.endpoint_id = $1::uuid AND a.state = 'succeeded' "
        "GROUP BY a.worker_id ORDER BY a.worker_id",
        endpoint["id"],
    )
    totals = await fetch_all(
        "SELECT count(*) AS attempts, "
        "count(*) FILTER (WHERE a.state = 'succeeded') AS succeeded "
        "FROM delivery_attempts a JOIN events e ON e.id = a.event_id "
        "WHERE e.endpoint_id = $1::uuid",
        endpoint["id"],
    )

    workers = {row["worker_id"]: row["delivered"] for row in histogram}
    attempts_total = int(totals[0]["attempts"])
    succeeded_total = int(totals[0]["succeeded"])

    ok = (
        deliveries["total"] == CONCURRENT_EVENTS
        and deliveries["duplicates"] == []
        and len(workers) >= 2
        and sum(workers.values()) == CONCURRENT_EVENTS
        and attempts_total == CONCURRENT_EVENTS
        and succeeded_total == CONCURRENT_EVENTS
    )
    report.record(
        "6. Two concurrent workers, no double delivery",
        ok,
        f"events={CONCURRENT_EVENTS} receiver_requests={deliveries['total']} "
        f"duplicate_deliveries={len(deliveries['duplicates'])} workers={workers} "
        f"attempts={attempts_total} succeeded={succeeded_total} "
        f"(needs >=2 distinct workers: {sorted(workers)})",
    )


async def check_observability(api: Api, _receiver: Receiver, report: Report) -> None:
    """Delivery records must expose the identifiers operators need."""
    response = await api.get("/v1/events", params={"limit": 1})
    items = response.json()["items"]
    attempt = items[0]["attempts"][0] if items and items[0]["attempts"] else {}
    ok = response.status_code == 200 and all(
        key in attempt for key in ("attempt_number", "state", "worker_id", "duration_ms")
    )
    report.record(
        "7. Event inspection exposes attempt-level identifiers",
        ok,
        f"attempt_fields={sorted(attempt)}",
    )


async def main() -> int:
    report = Report()
    print(f"API: {API_URL}\nReceiver: {RECEIVER_URL}\nDatabase: {DATABASE_URL or '<unset>'}\n")

    async with (
        httpx.AsyncClient(base_url=API_URL, timeout=30.0) as api_client,
        httpx.AsyncClient(base_url=RECEIVER_URL, timeout=30.0) as receiver_client,
    ):
        api = Api(api_client)
        receiver = Receiver(receiver_client)

        await check_health(api, receiver, report)
        if report.failed:
            print("\nStack is not ready; stopping.", file=sys.stderr)
            return 1

        try:
            await receiver_client.delete("/deliveries")
        except httpx.HTTPError as exc:  # receiver must be reachable for the rest
            report.record("0. Receiver control API reachable", False, str(exc))
            return 1

        for scenario in (
            check_end_to_end,
            check_idempotency,
            check_retry,
            check_dead_letter_and_manual_retry,
            check_two_workers_no_double_delivery,
            check_observability,
        ):
            try:
                await scenario(api, receiver, report)
            except Exception as exc:  # report and continue with the next scenario
                report.record(scenario.__name__, False, f"{type(exc).__name__}: {exc}")

    print()
    failed = report.failed
    print(f"{len(report.checks) - len(failed)}/{len(report.checks)} checks passed")
    if failed:
        for check in failed:
            print(f"  FAILED: {check.name} - {check.detail}", file=sys.stderr)
        return 1
    print("E2E verification: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
