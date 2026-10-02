"""Backoff policy and configuration guardrails."""

from __future__ import annotations

import random

import pytest

from webhooks.backoff import BackoffPolicy
from webhooks.config import Settings


def test_exponential_growth_and_cap() -> None:
    policy = BackoffPolicy(base_seconds=1.0, max_seconds=5.0, jitter_ratio=0.0)
    assert policy.delay_seconds(1) == 1.0
    assert policy.delay_seconds(2) == 2.0
    assert policy.delay_seconds(3) == 4.0
    assert policy.delay_seconds(4) == 5.0  # capped
    assert policy.delay_seconds(50) == 5.0


def test_jitter_stays_within_bounds_and_is_deterministic_for_a_seed() -> None:
    policy = BackoffPolicy(base_seconds=1.0, max_seconds=100.0, jitter_ratio=0.5)
    rng = random.Random(1234)
    for attempt in range(1, 5):
        delay = policy.delay_seconds(attempt, rng=rng)
        base = 2 ** (attempt - 1)
        assert base <= delay <= base * 1.5

    assert policy.delay_seconds(3, rng=random.Random(7)) == policy.delay_seconds(
        3, rng=random.Random(7)
    )


def test_invalid_attempt_number_is_rejected() -> None:
    with pytest.raises(ValueError):
        BackoffPolicy().delay_seconds(0)


def test_lease_must_exceed_delivery_timeout() -> None:
    with pytest.raises(ValueError, match="WORKER_LEASE_SECONDS"):
        Settings(delivery_timeout_seconds=10, worker_lease_seconds=5)

    # Twice the timeout is the documented minimum.
    settings = Settings(delivery_timeout_seconds=10, worker_lease_seconds=20)
    assert settings.worker_lease_seconds == 20


def test_backoff_max_must_exceed_base() -> None:
    with pytest.raises(ValueError, match="DELIVERY_BACKOFF_MAX_SECONDS"):
        Settings(delivery_backoff_base_seconds=10, delivery_backoff_max_seconds=1)
