"""Application configuration.

One settings object is shared by the API and worker processes; both are started
from the same environment so deployments stay symmetric.
"""

from __future__ import annotations

import os
import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Settings(BaseSettings):
    """Runtime settings, populated from environment variables / .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://webhooks:webhooks@localhost:5432/webhooks"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=20, ge=0)

    # API
    api_host: str = "0.0.0.0"  # noqa: S104 - containers must accept outside traffic
    api_port: int = Field(default=8000, ge=1, le=65535)

    # Delivery
    delivery_timeout_seconds: float = Field(default=10.0, gt=0)
    delivery_max_attempts: int = Field(default=5, ge=1)
    delivery_backoff_base_seconds: float = Field(default=2.0, gt=0)
    delivery_backoff_max_seconds: float = Field(default=300.0, gt=0)
    delivery_backoff_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    delivery_response_snippet_bytes: int = Field(default=512, ge=0)

    # Worker
    worker_id: str = Field(default_factory=_default_worker_id)
    worker_batch_size: int = Field(default=10, ge=1)
    worker_concurrency: int = Field(default=8, ge=1)
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0)
    worker_lease_seconds: float = Field(default=60.0, gt=0)

    # Observability
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    @model_validator(mode="after")
    def _validate_lease(self) -> Settings:
        # A lease shorter than the delivery timeout would let a second worker
        # reclaim an attempt while the first one is still inside its HTTP call.
        minimum = self.delivery_timeout_seconds * 2
        if self.worker_lease_seconds < minimum:
            msg = (
                "WORKER_LEASE_SECONDS must be at least twice DELIVERY_TIMEOUT_SECONDS "
                f"(got lease={self.worker_lease_seconds}, timeout={self.delivery_timeout_seconds})"
            )
            raise ValueError(msg)
        if self.delivery_backoff_max_seconds < self.delivery_backoff_base_seconds:
            msg = "DELIVERY_BACKOFF_MAX_SECONDS must be >= DELIVERY_BACKOFF_BASE_SECONDS"
            raise ValueError(msg)
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()
