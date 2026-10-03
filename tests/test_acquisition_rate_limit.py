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


@pytest.mark.asyncio
async def test_shared_acquisition_limiter_reuses_history_for_running_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    import l2shock.acquisition.rate_limit as rate_module

    fake = FakeTime()
    limiter = AsyncRollingWindowRateLimiter(
        2,
        clock=fake.clock,
        sleeper=fake.sleep,
    )
    loop = asyncio.get_running_loop()

    monkeypatch.setattr(
        loop,
        rate_module._SHARED_LIMITER_ATTRIBUTE,
        limiter,
        raising=False,
    )

    first = rate_module.get_shared_acquisition_rate_limiter(2)
    await first.acquire()
    await first.acquire()

    replacement = rate_module.get_shared_acquisition_rate_limiter(2)
    assert replacement is first

    await replacement.acquire()

    assert fake.sleeps == [pytest.approx(60.0)]
    assert fake.value == pytest.approx(60.0)


@pytest.mark.asyncio
async def test_shared_acquisition_limit_reduction_preserves_admissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    import l2shock.acquisition.rate_limit as rate_module

    fake = FakeTime()
    limiter = AsyncRollingWindowRateLimiter(
        3,
        clock=fake.clock,
        sleeper=fake.sleep,
    )
    loop = asyncio.get_running_loop()

    monkeypatch.setattr(
        loop,
        rate_module._SHARED_LIMITER_ATTRIBUTE,
        limiter,
        raising=False,
    )

    await limiter.acquire()
    await limiter.acquire()

    reduced = rate_module.get_shared_acquisition_rate_limiter(1)

    assert reduced is limiter
    assert reduced.limit == 1

    await reduced.acquire()

    assert fake.sleeps == [pytest.approx(60.0)]

    requested_increase = rate_module.get_shared_acquisition_rate_limiter(60)

    assert requested_increase is limiter
    assert requested_increase.limit == 1


def test_shared_acquisition_limiter_is_not_reused_between_event_loops() -> None:
    import asyncio

    import l2shock.acquisition.rate_limit as rate_module

    async def resolve():
        return rate_module.get_shared_acquisition_rate_limiter(2)

    first = asyncio.run(resolve())
    second = asyncio.run(resolve())

    assert first is not second


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", (True, False, 0, -1, "2", 2.0))
async def test_shared_acquisition_limiter_rejects_invalid_limits(
    limit: object,
) -> None:
    import l2shock.acquisition.rate_limit as rate_module

    with pytest.raises(ValueError):
        rate_module.get_shared_acquisition_rate_limiter(
            limit,  # type: ignore[arg-type]
        )