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


_SHARED_LIMITER_ATTRIBUTE: Final[str] = (
    "_l2shock_cryptohft_shared_rolling_window_limiter"
)


def get_shared_acquisition_rate_limiter(
    limit: int,
) -> AsyncRollingWindowRateLimiter:
    """Return the default acquisition limiter owned by the running loop.

    Downloader replacement must not reset the provider's rolling admission
    history. Keep the limiter on its owning loop rather than in a process-global
    singleton that could bind an asyncio.Lock to the wrong loop.

    A lower requested limit takes effect without discarding history. A higher
    requested limit does not increase admission during this loop's lifetime.
    Explicitly injected downloader limiters bypass this default accessor.
    """
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("limit must be an integer")

    if limit <= 0:
        raise ValueError("limit must be positive")

    loop = asyncio.get_running_loop()
    existing = getattr(loop, _SHARED_LIMITER_ATTRIBUTE, None)

    if existing is None:
        existing = AsyncRollingWindowRateLimiter(limit)
        setattr(loop, _SHARED_LIMITER_ATTRIBUTE, existing)
        return existing

    if not isinstance(existing, AsyncRollingWindowRateLimiter):
        raise RuntimeError(
            "The event loop's shared acquisition limiter has invalid ownership"
        )

    # This synchronous adjustment has no intervening await. It cannot reset
    # admissions or race another coroutine on the same event-loop thread.
    existing._limit = min(existing.limit, limit)

    return existing


__all__ = [
    "AsyncRollingWindowRateLimiter",
    "get_shared_acquisition_rate_limiter",
]
