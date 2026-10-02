"""Outbound delivery: signed HTTP POST with a hard timeout."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import httpx

from webhooks.security import build_signature_headers
from webhooks.service import ClaimedAttempt
from webhooks.utils import json_dumps_compact

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """Outcome of one HTTP delivery try."""

    ok: bool
    response_status: int | None
    error: str | None
    response_snippet: str | None
    duration_ms: int


def _snippet(response: httpx.Response, limit: int) -> str | None:
    if limit <= 0:
        return None
    text = response.text
    if not text:
        return None
    return text[:limit]


async def deliver(
    client: httpx.AsyncClient,
    claim: ClaimedAttempt,
    *,
    timeout_seconds: float,
    snippet_bytes: int,
) -> DeliveryResult:
    """POST the signed payload to the endpoint and classify the result.

    Any 2xx is a success. Everything else - non-2xx status, timeout, DNS or TLS
    failure, connection reset - is a failure and is retried by the caller under
    the event's backoff policy.
    """
    body = json_dumps_compact(claim.payload).encode("utf-8")
    headers = build_signature_headers(
        claim.secret,
        event_id=str(claim.event_id),
        event_type=claim.event_type,
        attempt_number=claim.attempt_number,
        body=body,
    ).as_dict()
    headers["Content-Type"] = "application/json"

    started = time.perf_counter()
    try:
        response = await client.post(
            claim.url,
            content=body,
            headers=headers,
            timeout=httpx.Timeout(timeout_seconds),
        )
    except httpx.TimeoutException:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return DeliveryResult(
            ok=False,
            response_status=None,
            error=f"timeout after {timeout_seconds:g}s",
            response_snippet=None,
            duration_ms=duration_ms,
        )
    except httpx.HTTPError as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return DeliveryResult(
            ok=False,
            response_status=None,
            error=f"{type(exc).__name__}: {exc}",
            response_snippet=None,
            duration_ms=duration_ms,
        )
    except Exception as exc:  # pragma: no cover - defensive: malformed URL, TLS bugs
        duration_ms = int((time.perf_counter() - started) * 1000)
        return DeliveryResult(
            ok=False,
            response_status=None,
            error=f"unexpected {type(exc).__name__}: {exc}",
            response_snippet=None,
            duration_ms=duration_ms,
        )

    duration_ms = int((time.perf_counter() - started) * 1000)
    if 200 <= response.status_code < 300:
        return DeliveryResult(
            ok=True,
            response_status=response.status_code,
            error=None,
            response_snippet=_snippet(response, snippet_bytes),
            duration_ms=duration_ms,
        )
    return DeliveryResult(
        ok=False,
        response_status=response.status_code,
        error=f"unexpected status {response.status_code}",
        response_snippet=_snippet(response, snippet_bytes),
        duration_ms=duration_ms,
    )
