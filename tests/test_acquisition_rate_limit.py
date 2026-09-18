from __future__ import annotations

import pytest

from l2shock.acquisition import AsyncRollingWindowRateLimiter


class FakeTime:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


@pytest.mark.asyncio
async def test_rate_limiter_allows_requests_within_budget() -> None:
    fake = FakeTime()
    limiter = AsyncRollingWindowRateLimiter(
        3,
        window_seconds=60.0,
        clock=fake.clock,
        sleeper=fake.sleep,
    )

    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert fake.sleeps == []
    assert fake.value == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_rate_limiter_waits_for_oldest_admission() -> None:
    fake = FakeTime()
    limiter = AsyncRollingWindowRateLimiter(
        2,
        window_seconds=60.0,
        clock=fake.clock,
        sleeper=fake.sleep,
    )

    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    assert fake.sleeps == [pytest.approx(60.0)]
    assert fake.value == pytest.approx(60.0)


@pytest.mark.asyncio
async def test_rate_limiter_uses_rolling_not_fixed_window() -> None:
    fake = FakeTime()
    limiter = AsyncRollingWindowRateLimiter(
        2,
        window_seconds=60.0,
        clock=fake.clock,
        sleeper=fake.sleep,
    )

    await limiter.acquire()

    fake.value = 30.0
    await limiter.acquire()

    fake.value = 59.0
    await limiter.acquire()

    assert fake.sleeps == [pytest.approx(1.0)]
    assert fake.value == pytest.approx(60.0)

    fake.value = 61.0
    await limiter.acquire()

    assert fake.sleeps[-1] == pytest.approx(29.0)
    assert fake.value == pytest.approx(90.0)


def test_rate_limiter_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="positive"):
        AsyncRollingWindowRateLimiter(0)

    with pytest.raises(ValueError, match="integer"):
        AsyncRollingWindowRateLimiter(True)

    with pytest.raises(ValueError, match="window_seconds"):
        AsyncRollingWindowRateLimiter(1, window_seconds=0)


@pytest.mark.parametrize(
    "value",
    (
        float("nan"),
        float("inf"),
        float("-inf"),
        "nan",
        "inf",
        None,
    ),
)
def test_rate_limiter_rejects_non_finite_or_non_numeric_window(
    value: object,
) -> None:
    with pytest.raises(
        ValueError,
        match="finite positive number",
    ):
        AsyncRollingWindowRateLimiter(
            1,
            window_seconds=value,  # type: ignore[arg-type]
        )
