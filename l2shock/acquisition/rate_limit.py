# l2shock/acquisition/rate_limit.py
"""Asynchronous rolling-window request limiter."""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Final

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]

_DEFAULT_WINDOW_SECONDS: Final[float] = 60.0


class AsyncRollingWindowRateLimiter:
    """Limit acquisition attempts inside a true rolling time window.

    The limiter serializes admission decisions so concurrent manual,
    automatic, and retrying requests cannot each assume the same remaining
    request budget.

    Waiting occurs while the internal lock is held intentionally. This keeps
    request admission FIFO-like and prevents a large group of waiters from all
    waking and consuming the same apparent slot.
    """

    def __init__(
        self,
        limit: int,
        *,
        window_seconds: float = _DEFAULT_WINDOW_SECONDS,
        clock: Clock = time.monotonic,
        sleeper: Sleeper = asyncio.sleep,
    ) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit must be an integer")

        if limit <= 0:
            raise ValueError("limit must be positive")

        try:
            window = float(window_seconds)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("window_seconds must be a finite positive number") from exc

        if not math.isfinite(window) or window <= 0.0:
            raise ValueError("window_seconds must be a finite positive number")

        self._limit = limit
        self._window_seconds = window
        self._clock = clock
        self._sleeper = sleeper

        self._admissions: deque[float] = deque()
        self._lock = asyncio.Lock()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def window_seconds(self) -> float:
        return self._window_seconds

    def _remove_expired(self, now: float) -> None:
        threshold = now - self._window_seconds

        while self._admissions and self._admissions[0] <= threshold:
            self._admissions.popleft()

    async def acquire(self) -> None:
        """Wait until one request admission is available."""
        async with self._lock:
            while True:
                now = float(self._clock())
                self._remove_expired(now)

                if len(self._admissions) < self._limit:
                    self._admissions.append(now)
                    return

                oldest = self._admissions[0]
                wait_seconds = max(
                    0.0,
                    oldest + self._window_seconds - now,
                )

                if wait_seconds <= 0.0:
                    self._remove_expired(float(self._clock()))
                    continue

                await self._sleeper(wait_seconds)

    async def __aenter__(self) -> AsyncRollingWindowRateLimiter:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        _exception_type,
        _exception,
        _traceback,
    ) -> None:
        return None


__all__ = ["AsyncRollingWindowRateLimiter"]
