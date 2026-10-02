"""Reference webhook receiver used for verification and integration tests.

It implements the wire contract the platform documents, so it doubles as
executable documentation for endpoint owners:

* verifies ``X-Webhook-Signature`` against the endpoint secret,
* records every delivery (event id, attempt number, signature validity),
* can be programmed to fail, fail N times, or stall, which is how retry,
  backoff, dead-letter and timeout behaviour are exercised end to end.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from webhooks.security import (
    ATTEMPT_HEADER,
    DEFAULT_TOLERANCE_SECONDS,
    EVENT_ID_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_signature,
)


@dataclass
class HookConfig:
    """Programmable behaviour for one hook path."""

    secret: str | None = None
    failure_status: int = 500
    fail_first_n: int = 0
    always_fail: bool = False
    delay_seconds: float = 0.0


@dataclass
class ReceiverState:
    configs: dict[str, HookConfig] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    deliveries: list[dict[str, Any]] = field(default_factory=list)

    def reset(self) -> None:
        self.configs.clear()
        self.counts.clear()
        self.deliveries.clear()

    def duplicate_keys(self) -> list[dict[str, Any]]:
        """Deliveries of the same (hook, event, attempt) more than once."""
        seen: dict[tuple[str, str, int], int] = {}
        for delivery in self.deliveries:
            key = (delivery["hook_id"], delivery["event_id"], delivery["attempt"])
            seen[key] = seen.get(key, 0) + 1
        return [
            {"hook_id": hook, "event_id": event, "attempt": attempt, "count": count}
            for (hook, event, attempt), count in seen.items()
            if count > 1
        ]


class HookConfigUpdate(BaseModel):
    secret: str | None = None
    failure_status: int | None = Field(default=None, ge=100, le=599)
    fail_first_n: int | None = Field(default=None, ge=0)
    always_fail: bool | None = None
    delay_seconds: float | None = Field(default=None, ge=0)


def create_receiver_app() -> FastAPI:
    app = FastAPI(title="Webhook test receiver")
    state = ReceiverState()
    app.state.receiver = state

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/hooks/{hook_id}")
    async def receive(hook_id: str, request: Request) -> JSONResponse:
        config = state.configs.setdefault(hook_id, HookConfig())
        raw = await request.body()

        if config.delay_seconds:
            await asyncio.sleep(config.delay_seconds)

        count = state.counts.get(hook_id, 0) + 1
        state.counts[hook_id] = count

        timestamp_header = request.headers.get(TIMESTAMP_HEADER)
        signature_header = request.headers.get(SIGNATURE_HEADER)
        signature_valid: bool | None = None
        if config.secret and timestamp_header and signature_header:
            try:
                timestamp = int(timestamp_header)
            except ValueError:
                signature_valid = False
            else:
                signature_valid = verify_signature(
                    config.secret,
                    timestamp,
                    raw,
                    signature_header,
                    tolerance_seconds=DEFAULT_TOLERANCE_SECONDS,
                )

        state.deliveries.append(
            {
                "hook_id": hook_id,
                "event_id": request.headers.get(EVENT_ID_HEADER),
                "attempt": int(request.headers.get(ATTEMPT_HEADER, "0") or 0),
                "timestamp": timestamp_header,
                "signature": signature_header,
                "signature_valid": signature_valid,
                "content_type": request.headers.get("content-type"),
                "body": raw.decode("utf-8", errors="replace"),
                "received_at": datetime.now(tz=UTC).isoformat(),
                "failure_status": None,
            }
        )

        should_fail = config.always_fail or count <= config.fail_first_n
        status_code = config.failure_status if should_fail else 200
        if should_fail:
            state.deliveries[-1]["failure_status"] = status_code
        return JSONResponse(
            status_code=status_code,
            content={"ok": not should_fail, "received": count},
        )

    @app.put("/hooks/{hook_id}/config")
    async def configure(hook_id: str, update: HookConfigUpdate) -> dict[str, Any]:
        config = state.configs.setdefault(hook_id, HookConfig())
        for name, value in update.model_dump(exclude_none=True).items():
            setattr(config, name, value)
        return {
            "hook_id": hook_id,
            "config": config.__dict__,
            "received": state.counts.get(hook_id, 0),
        }

    @app.get("/hooks/{hook_id}/config")
    async def get_config(hook_id: str) -> dict[str, Any]:
        config = state.configs.setdefault(hook_id, HookConfig())
        return {
            "hook_id": hook_id,
            "config": config.__dict__,
            "received": state.counts.get(hook_id, 0),
        }

    @app.get("/deliveries")
    async def deliveries(hook_id: str | None = None) -> dict[str, Any]:
        items = [
            delivery
            for delivery in state.deliveries
            if hook_id is None or delivery["hook_id"] == hook_id
        ]
        return {
            "items": items,
            "total": len(items),
            "duplicates": state.duplicate_keys(),
            "by_hook": {
                hook: sum(1 for delivery in state.deliveries if delivery["hook_id"] == hook)
                for hook in sorted(state.counts)
            },
        }

    @app.delete("/deliveries")
    async def reset() -> dict[str, str]:
        state.reset()
        return {"status": "reset"}

    return app


def main() -> None:
    import uvicorn

    host = os.environ.get("RECEIVER_HOST", "0.0.0.0")  # noqa: S104 - container default
    port = int(os.environ.get("RECEIVER_PORT", "9000"))
    uvicorn.run(create_receiver_app(), host=host, port=port, log_level="warning")


__all__ = ["HookConfig", "HookConfigUpdate", "ReceiverState", "create_receiver_app", "main"]


if __name__ == "__main__":
    main()
