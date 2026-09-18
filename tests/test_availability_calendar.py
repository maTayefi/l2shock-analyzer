from __future__ import annotations

from datetime import datetime, timedelta, timezone

from l2shock.acquisition import (
    ContiguousAnalysisWindow,
    HourAvailability,
    HourAvailabilityState,
    most_recent_contiguous_analyzable_window,
)
from l2shock.ui.availability_calendar import (
    AnalysisRangeHandoff,
    availability_for_local_date,
    availability_hour_tooltip,
    availability_state_color,
    format_contiguous_window_local,
    local_date_source_hour_query_bounds,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _availability(
    offset: int,
    state: HourAvailabilityState,
    *,
    base: str = "BTC",
) -> HourAvailability:
    analyzable = state is HourAvailabilityState.ANALYZABLE
    materialized = state in {
        HourAvailabilityState.ANALYZABLE,
        HourAvailabilityState.MATERIALIZED,
    }
    downloaded = state in {
        HourAvailabilityState.ANALYZABLE,
        HourAvailabilityState.MATERIALIZED,
        HourAvailabilityState.DOWNLOADED,
    }

    return HourAvailability(
        base=base,
        hour_utc=_hour(offset),
        preset_hash="a" * 64,
        orderbook_status=("processed" if downloaded else None),
        trades_status=("processed" if downloaded else None),
        orderbook_local=downloaded,
        trades_local=downloaded,
        l2_materialized=materialized,
        price_materialized=materialized,
        l2_valid_seconds=1 if analyzable else 0,
        price_valid_seconds=1 if analyzable else 0,
        state=state,
    )


def test_contiguous_window_uses_newest_uninterrupted_run() -> None:
    values = (
        _availability(
            0,
            HourAvailabilityState.ANALYZABLE,
        ),
        _availability(
            1,
            HourAvailabilityState.ANALYZABLE,
        ),
        _availability(
            2,
            HourAvailabilityState.MATERIALIZED,
        ),
        _availability(
            3,
            HourAvailabilityState.ANALYZABLE,
        ),
        _availability(
            4,
            HourAvailabilityState.ANALYZABLE,
        ),
    )

    window = most_recent_contiguous_analyzable_window(values)

    assert window is not None
    assert window.start_utc == _hour(3)
    assert window.end_utc == _hour(5)
    assert window.hour_count == 2


def test_materialized_all_invalid_hour_is_not_analyzable() -> None:
    values = (
        _availability(
            0,
            HourAvailabilityState.ANALYZABLE,
        ),
        _availability(
            1,
            HourAvailabilityState.MATERIALIZED,
        ),
    )

    window = most_recent_contiguous_analyzable_window(values)

    assert window is not None
    assert window.start_utc == _hour(0)
    assert window.end_utc == _hour(1)
    assert window.hour_count == 1


def test_empty_availability_has_no_window() -> None:
    assert most_recent_contiguous_analyzable_window(()) is None


def test_tehran_local_date_query_encloses_exact_utc_hours() -> None:
    start, end = local_date_source_hour_query_bounds(
        "2026-09-03",
        timezone_name="Asia/Tehran",
    )

    assert start == datetime(
        2026,
        9,
        2,
        20,
        tzinfo=timezone.utc,
    )
    assert end == datetime(
        2026,
        9,
        3,
        21,
        tzinfo=timezone.utc,
    )


def test_local_date_filter_keeps_exact_source_hour_identity() -> None:
    values = tuple(
        _availability(
            offset,
            HourAvailabilityState.UNKNOWN,
        )
        for offset in range(-16, 40)
    )

    selected = availability_for_local_date(
        values,
        selected_local_date="2026-09-03",
        timezone_name="Asia/Tehran",
    )

    assert len(selected) == 24
    assert selected[0].hour_utc == datetime(
        2026,
        9,
        2,
        21,
        tzinfo=timezone.utc,
    )
    assert selected[-1].hour_utc == datetime(
        2026,
        9,
        3,
        20,
        tzinfo=timezone.utc,
    )


def test_analysis_handoff_converts_exclusive_hour_end_to_closed_second() -> None:
    window = ContiguousAnalysisWindow(
        base="BTC",
        preset_hash="a" * 64,
        start_utc=_hour(0),
        end_utc=_hour(3),
        hour_count=3,
    )

    handoff = AnalysisRangeHandoff.from_window(window)

    assert handoff.start_utc == _hour(0)
    assert handoff.end_utc == _hour(3)
    assert handoff.closed_end_utc == (_hour(3) - timedelta(seconds=1))


def test_window_label_uses_configured_local_timezone() -> None:
    window = ContiguousAnalysisWindow(
        base="BTC",
        preset_hash="a" * 64,
        start_utc=_hour(0),
        end_utc=_hour(1),
        hour_count=1,
    )

    label = format_contiguous_window_local(
        window,
        timezone_name="Asia/Tehran",
    )

    assert "15:30" in label
    assert "16:30" in label
    assert "Asia/Tehran" in label


def test_every_calendar_state_has_a_color() -> None:
    for state in HourAvailabilityState:
        assert availability_state_color(state)


def test_partial_component_coverage_cannot_be_analyzable() -> None:
    try:
        HourAvailability(
            base="BTC",
            hour_utc=_hour(),
            preset_hash="a" * 64,
            orderbook_status="partial",
            trades_status="processed",
            orderbook_local=False,
            trades_local=True,
            l2_materialized=True,
            price_materialized=True,
            l2_valid_seconds=3_600,
            price_valid_seconds=3_600,
            state=HourAvailabilityState.ANALYZABLE,
            expected_l2_market_count=2,
            materialized_l2_market_count=1,
            valid_l2_market_count=1,
        )
    except ValueError as exc:
        assert "every expected market" in str(exc)
    else:
        raise AssertionError("Partial multi-market coverage was accepted as ANALYZABLE")


def test_complete_component_coverage_is_not_degraded() -> None:
    item = HourAvailability(
        base="ETH",
        hour_utc=_hour(),
        preset_hash="b" * 64,
        orderbook_status="available",
        trades_status="processed",
        orderbook_local=True,
        trades_local=True,
        l2_materialized=True,
        price_materialized=True,
        l2_valid_seconds=3_600,
        price_valid_seconds=3_600,
        state=HourAvailabilityState.ANALYZABLE,
        expected_l2_market_count=2,
        materialized_l2_market_count=2,
        valid_l2_market_count=2,
    )

    assert item.l2_market_coverage_complete is True
    assert item.l2_market_coverage_degraded is False
    assert item.l2_valid_market_coverage_degraded is False


def test_valid_market_count_cannot_exceed_materialized_count() -> None:
    try:
        HourAvailability(
            base="BTC",
            hour_utc=_hour(),
            preset_hash="c" * 64,
            orderbook_status="partial",
            trades_status="processed",
            orderbook_local=False,
            trades_local=True,
            l2_materialized=True,
            price_materialized=True,
            l2_valid_seconds=1,
            price_valid_seconds=1,
            state=HourAvailabilityState.ANALYZABLE,
            expected_l2_market_count=2,
            materialized_l2_market_count=1,
            valid_l2_market_count=2,
        )
    except ValueError:
        pass
    else:
        raise AssertionError(
            "Availability accepted more valid markets than materialized markets"
        )


def test_hour_tooltip_reports_component_market_coverage() -> None:
    item = HourAvailability(
        base="BTC",
        hour_utc=_hour(),
        preset_hash="d" * 64,
        orderbook_status="partial",
        trades_status="processed",
        orderbook_local=False,
        trades_local=True,
        l2_materialized=True,
        price_materialized=True,
        l2_valid_seconds=2_000,
        price_valid_seconds=3_600,
        state=HourAvailabilityState.PARTIAL,
        expected_l2_market_count=2,
        materialized_l2_market_count=1,
        valid_l2_market_count=1,
    )

    tooltip = availability_hour_tooltip(
        item,
        timezone_name="Asia/Tehran",
    )

    assert "L2 markets materialized=1/2" in tooltip
    assert "L2 markets valid=1/2" in tooltip


def test_partial_multi_market_validity_can_keep_strict_handoff_seconds_zero() -> None:
    item = HourAvailability(
        base="BTC",
        hour_utc=_hour(),
        preset_hash="e" * 64,
        orderbook_status="partial",
        trades_status="processed",
        orderbook_local=False,
        trades_local=True,
        l2_materialized=True,
        price_materialized=True,
        l2_valid_seconds=0,
        price_valid_seconds=3_600,
        state=HourAvailabilityState.PARTIAL,
        expected_l2_market_count=3,
        materialized_l2_market_count=2,
        valid_l2_market_count=1,
    )

    assert item.analyzable is False
    assert item.l2_valid_seconds == 0
    assert item.valid_l2_market_count == 1
    assert item.l2_market_coverage_degraded is True
    assert item.l2_valid_market_coverage_degraded is True