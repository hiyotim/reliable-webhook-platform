"""Retry scheduling.

Delay for attempt *n* (1-based) is ``base * 2**(n-1)`` capped at ``max``, plus
optional uniform jitter of ``jitter_ratio * delay``. Jitter spreads retries when
many events fail at once (for example when a destination goes down) instead of
retrying them in lockstep.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    base_seconds: float = 2.0
    max_seconds: float = 300.0
    jitter_ratio: float = 0.2

    def delay_seconds(self, attempt_number: int, *, rng: random.Random | None = None) -> float:
        """Delay before *attempt_number* is retried (attempts are 1-based)."""
        if attempt_number < 1:
            msg = f"attempt_number must be >= 1, got {attempt_number}"
            raise ValueError(msg)
        delay: float = min(self.base_seconds * (2 ** (attempt_number - 1)), self.max_seconds)
        if self.jitter_ratio > 0:
            generator: random.Random = rng or random.SystemRandom()
            jitter: float = float(generator.random())
            delay += delay * self.jitter_ratio * jitter
        return delay
