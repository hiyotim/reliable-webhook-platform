"""Worker process: claim attempts, deliver them, record the outcome."""

from __future__ import annotations

import asyncio
import contextlib
import logging

import httpx

from webhooks.backoff import BackoffPolicy
from webhooks.config import Settings
from webhooks.db import Database
from webhooks.models import EndpointState
from webhooks.service import (
    ClaimedAttempt,
    claim_attempts,
    mark_attempt_failed,
    mark_attempt_succeeded,
)
from webhooks.worker.delivery import deliver

logger = logging.getLogger(__name__)


class Worker:
    """One worker process; several may run against the same database."""

    def __init__(
        self,
        settings: Settings,
        database: Database,
        *,
        worker_id: str | None = None,
    ) -> None:
        self.settings = settings
        self.database = database
        self.worker_id = worker_id or settings.worker_id
        self.backoff = BackoffPolicy(
            base_seconds=settings.delivery_backoff_base_seconds,
            max_seconds=settings.delivery_backoff_max_seconds,
            jitter_ratio=settings.delivery_backoff_jitter_ratio,
        )
        self._slots = asyncio.Semaphore(settings.worker_concurrency)
        self._client: httpx.AsyncClient | None = None

    async def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            limits = httpx.Limits(
                max_connections=self.settings.worker_concurrency * 2,
                max_keepalive_connections=self.settings.worker_concurrency,
            )
            self._client = httpx.AsyncClient(limits=limits, follow_redirects=False)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def claim(self, *, batch_size: int | None = None) -> list[ClaimedAttempt]:
        """Claim up to ``batch_size`` due or lease-expired attempts."""
        async with self.database.session() as session:
            return await claim_attempts(
                session,
                worker_id=self.worker_id,
                batch_size=batch_size or self.settings.worker_batch_size,
                lease_seconds=self.settings.worker_lease_seconds,
            )

    async def process(self, claim: ClaimedAttempt) -> None:
        """Deliver one claimed attempt and persist its outcome."""
        async with self._slots:
            if claim.endpoint_state != EndpointState.ACTIVE.value:
                # The endpoint was disabled/deleted between submission and claim.
                await self._record_failure(
                    claim,
                    error=f"endpoint is {claim.endpoint_state}",
                    response_status=None,
                    response_snippet=None,
                    duration_ms=0,
                    terminal=True,
                )
                return

            client = await self._http_client()
            result = await deliver(
                client,
                claim,
                timeout_seconds=self.settings.delivery_timeout_seconds,
                snippet_bytes=self.settings.delivery_response_snippet_bytes,
            )
            if result.ok:
                await self._record_success(
                    claim, result.response_status, result.response_snippet, result.duration_ms
                )
            else:
                await self._record_failure(
                    claim,
                    error=result.error or "delivery failed",
                    response_status=result.response_status,
                    response_snippet=result.response_snippet,
                    duration_ms=result.duration_ms,
                )

    async def _record_success(
        self,
        claim: ClaimedAttempt,
        response_status: int | None,
        response_snippet: str | None,
        duration_ms: int,
    ) -> None:
        try:
            async with self.database.session() as session:
                await mark_attempt_succeeded(
                    session,
                    attempt_id=claim.attempt_id,
                    worker_id=self.worker_id,
                    response_status=response_status,
                    response_snippet=response_snippet,
                    duration_ms=duration_ms,
                )
        except Exception:
            # The attempt stays in_progress; its lease expires and another worker
            # recovers it. Losing the outcome must never crash the worker loop.
            logger.exception(
                "failed to record delivery success",
                extra=self._log_context(claim),
            )

    async def _record_failure(
        self,
        claim: ClaimedAttempt,
        *,
        error: str,
        response_status: int | None,
        response_snippet: str | None,
        duration_ms: int,
        terminal: bool = False,
    ) -> None:
        try:
            async with self.database.session() as session:
                await mark_attempt_failed(
                    session,
                    attempt_id=claim.attempt_id,
                    worker_id=self.worker_id,
                    error=error,
                    response_status=response_status,
                    response_snippet=response_snippet,
                    duration_ms=duration_ms,
                    backoff=self.backoff,
                    terminal=terminal,
                )
        except Exception:
            logger.exception(
                "failed to record delivery failure",
                extra=self._log_context(claim),
            )

    def _log_context(self, claim: ClaimedAttempt) -> dict[str, object]:
        return {
            "event_id": str(claim.event_id),
            "endpoint_id": str(claim.endpoint_id),
            "attempt_id": claim.attempt_id,
            "attempt_number": claim.attempt_number,
            "worker_id": self.worker_id,
        }

    async def run_once(self, *, batch_size: int | None = None) -> int:
        """Claim and process one batch; returns the number of attempts handled."""
        claims = await self.claim(batch_size=batch_size)
        if not claims:
            return 0
        await asyncio.gather(*(self.process(claim) for claim in claims))
        return len(claims)

    async def run(self, stop_event: asyncio.Event) -> None:
        """Claim/deliver loop until ``stop_event`` is set."""
        logger.info(
            "worker started",
            extra={
                "worker_id": self.worker_id,
                "batch_size": self.settings.worker_batch_size,
                "concurrency": self.settings.worker_concurrency,
                "lease_seconds": self.settings.worker_lease_seconds,
                "poll_interval_seconds": self.settings.worker_poll_interval_seconds,
            },
        )
        try:
            while not stop_event.is_set():
                try:
                    handled = await self.run_once()
                except Exception:
                    logger.exception("worker iteration failed", extra={"worker_id": self.worker_id})
                    handled = 0
                if handled == 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self.settings.worker_poll_interval_seconds,
                        )
        finally:
            await self.aclose()
            logger.info("worker stopped", extra={"worker_id": self.worker_id})


__all__ = ["Worker"]
