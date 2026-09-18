from __future__ import annotations

import inspect
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext

import pytest

import l2shock.liquidity.depth as depth_module
from l2shock.ingest import (
    BookSide,
    OrderBookEvent,
    OrderBookEventType,
    OrderBookLevelChange,
    OrderBookReplayState,
    ReplayBookStructure,
)
from l2shock.liquidity import (
    DepthBand,
    DepthLiquidityCancelledError,
    DepthLiquidityError,
    DepthLiquidityUnavailableError,
    calculate_depth_liquidity,
    depth_price_intervals,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        2,
        12,
        tzinfo=timezone.utc,
    )


def _change(
    side: BookSide | str,
    price: str,
    quantity: str,
) -> OrderBookLevelChange:
    return OrderBookLevelChange(
        side=BookSide(side),
        price=Decimal(price),
        quantity=Decimal(quantity),
        order_count=None,
    )


def _snapshot(
    *,
    changes: tuple[OrderBookLevelChange, ...],
    last_update_id: int = 100,
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.SNAPSHOT,
        received_time_ns=1_788_350_400_100_000_000,
        event_time_ms=1_788_350_400_100,
        transaction_time_ms=1_788_350_400_099,
        first_update_id=None,
        final_update_id=None,
        prev_final_update_id=None,
        last_update_id=last_update_id,
        changes=changes,
        first_row_number=0,
        last_row_number=len(changes) - 1,
    )


def _update(
    *,
    final_update_id: int,
    previous_update_id: int,
    changes: tuple[OrderBookLevelChange, ...],
) -> OrderBookEvent:
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.UPDATE,
        received_time_ns=(1_788_350_400_100_000_000 + final_update_id),
        event_time_ms=1_788_350_400_100 + final_update_id,
        transaction_time_ms=1_788_350_400_099 + final_update_id,
        first_update_id=final_update_id,
        final_update_id=final_update_id,
        prev_final_update_id=previous_update_id,
        last_update_id=None,
        changes=changes,
        first_row_number=final_update_id,
        last_row_number=final_update_id + len(changes) - 1,
    )


def _state(
    changes: tuple[OrderBookLevelChange, ...],
) -> OrderBookReplayState:
    state = OrderBookReplayState(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
    )

    state.apply(
        _snapshot(changes=changes),
        hour_utc=_hour(),
    )

    return state


def test_depth_intervals_follow_locked_formulas() -> None:
    intervals = depth_price_intervals(
        best_bid=Decimal("100"),
        best_ask=Decimal("101"),
        band=DepthBand(
            lower_fraction=Decimal("0.01"),
            upper_fraction=Decimal("0.05"),
        ),
    )

    assert intervals.bid_lower == Decimal("95")
    assert intervals.bid_upper == Decimal("99")

    assert intervals.ask_lower == Decimal("102.01")
    assert intervals.ask_upper == Decimal("106.05")


def test_interval_boundaries_are_inclusive() -> None:
    state = _state(
        (
            _change("bid", "100", "1"),
            _change("bid", "99", "2"),
            _change("bid", "95", "3"),
            _change("bid", "94.99", "1000"),
            _change("ask", "101", "1"),
            _change("ask", "102.01", "4"),
            _change("ask", "106.05", "5"),
            _change("ask", "106.06", "1000"),
        )
    )

    result = calculate_depth_liquidity(
        state,
        DepthBand(
            lower_fraction=Decimal("0.01"),
            upper_fraction=Decimal("0.05"),
        ),
    )

    # Bid band is [95, 99]. Both exact boundaries are included.
    assert result.bid_level_count == 2
    assert result.bid_liquidity == (
        Decimal("99") * Decimal("2") + Decimal("95") * Decimal("3")
    )

    # Ask band is [102.01, 106.05]. Both exact boundaries are included.
    assert result.ask_level_count == 2
    assert result.ask_liquidity == (
        Decimal("102.01") * Decimal("4") + Decimal("106.05") * Decimal("5")
    )

    assert result.levels_examined == 4


def test_zero_lower_fraction_includes_top_of_book() -> None:
    state = _state(
        (
            _change("bid", "100", "2"),
            _change("bid", "99", "3"),
            _change("ask", "101", "4"),
            _change("ask", "102", "5"),
        )
    )

    result = calculate_depth_liquidity(
        state,
        DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0"),
        ),
    )

    assert result.intervals.bid_lower == Decimal("100")
    assert result.intervals.bid_upper == Decimal("100")
    assert result.intervals.ask_lower == Decimal("101")
    assert result.intervals.ask_upper == Decimal("101")

    assert result.bid_level_count == 1
    assert result.ask_level_count == 1
    assert result.bid_liquidity == Decimal("200")
    assert result.ask_liquidity == Decimal("404")
    assert result.total_liquidity == Decimal("604")

    with localcontext(
        Context(
            prec=result.imbalance_decimal_precision,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        expected_imbalance = Decimal("-204") / Decimal("604")
    assert result.bid_ask_imbalance == expected_imbalance


def test_exact_decimal_notional_is_not_float_arithmetic() -> None:
    state = _state(
        (
            _change("bid", "0.3", "0.1"),
            _change("ask", "0.4", "0.2"),
        )
    )

    result = calculate_depth_liquidity(
        state,
        DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0"),
        ),
    )

    assert result.bid_liquidity == Decimal("0.03")
    assert result.ask_liquidity == Decimal("0.08")
    assert result.total_liquidity == Decimal("0.11")


def test_total_and_imbalance_follow_locked_metric_formulas() -> None:
    state = _state(
        (
            _change("bid", "100", "3"),
            _change("ask", "101", "1"),
        )
    )

    result = calculate_depth_liquidity(
        state,
        DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("0"),
        ),
        imbalance_decimal_precision=40,
    )

    assert result.bid_liquidity == Decimal("300")
    assert result.ask_liquidity == Decimal("101")
    assert result.total_liquidity == Decimal("401")

    assert result.bid_ask_imbalance is not None
    with localcontext(
        Context(
            prec=result.imbalance_decimal_precision,
            rounding=ROUND_HALF_EVEN,
        )
    ):
        expected_imbalance = Decimal("199") / Decimal("401")
    assert result.bid_ask_imbalance == expected_imbalance
    assert Decimal("-1") <= result.bid_ask_imbalance <= Decimal("1")
    assert result.imbalance_decimal_precision == 40


def test_empty_depth_band_has_invalid_imbalance_denominator() -> None:
    state = _state(
        (
            _change("bid", "100", "1"),
            _change("ask", "101", "1"),
        )
    )

    # This band deliberately excludes both available top levels.
    result = calculate_depth_liquidity(
        state,
        DepthBand(
            lower_fraction=Decimal("0.10"),
            upper_fraction=Decimal("0.20"),
        ),
    )

    assert result.bid_level_count == 0
    assert result.ask_level_count == 0
    assert result.bid_liquidity == Decimal("0")
    assert result.ask_liquidity == Decimal("0")
    assert result.total_liquidity == Decimal("0")
    assert result.bid_ask_imbalance is None


@pytest.mark.parametrize(
    ("bid", "ask", "expected_structure"),
    [
        ("100", "100", ReplayBookStructure.LOCKED),
        ("101", "100", ReplayBookStructure.CROSSED),
    ],
)
def test_locked_and_crossed_books_refuse_liquidity(
    bid: str,
    ask: str,
    expected_structure: ReplayBookStructure,
) -> None:
    state = _state(
        (
            _change("bid", bid, "1"),
            _change("ask", ask, "1"),
        )
    )

    assert state.valid is True
    assert state.book_structure is expected_structure

    with pytest.raises(
        DepthLiquidityUnavailableError,
        match="NORMAL",
    ):
        calculate_depth_liquidity(
            state,
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
            ),
        )


def test_uninitialized_book_refuses_liquidity() -> None:
    state = OrderBookReplayState(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
    )

    with pytest.raises(
        DepthLiquidityUnavailableError,
        match="valid initialized",
    ):
        calculate_depth_liquidity(
            state,
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
            ),
        )


def test_empty_side_refuses_liquidity() -> None:
    state = _state(
        (
            _change("bid", "100", "1"),
            _change("ask", "101", "1"),
        )
    )

    state.apply(
        _update(
            final_update_id=101,
            previous_update_id=100,
            changes=(_change("ask", "101", "0"),),
        ),
        hour_utc=_hour(),
    )

    assert state.valid is True
    assert state.book_structure is ReplayBookStructure.EMPTY_ASK

    with pytest.raises(
        DepthLiquidityUnavailableError,
        match="NORMAL",
    ):
        calculate_depth_liquidity(
            state,
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.01"),
            ),
        )


def test_live_range_index_tracks_insert_delete_and_snapshot_reset() -> None:
    state = _state(
        (
            _change("bid", "100", "1"),
            _change("bid", "99", "2"),
            _change("ask", "101", "3"),
            _change("ask", "102", "4"),
        )
    )

    initial_bids = tuple(
        state.iter_levels(
            BookSide.BID,
            lower_price=Decimal("99"),
            upper_price=Decimal("100"),
        )
    )

    assert [(level.price, level.quantity) for level in initial_bids] == [
        (Decimal("99"), Decimal("2")),
        (Decimal("100"), Decimal("1")),
    ]

    state.apply(
        _update(
            final_update_id=101,
            previous_update_id=100,
            changes=(
                _change("bid", "99", "0"),
                _change("bid", "99.5", "5"),
                _change("ask", "101.5", "6"),
            ),
        ),
        hour_utc=_hour(),
    )

    bids = tuple(
        state.iter_levels(
            BookSide.BID,
            lower_price=Decimal("99"),
            upper_price=Decimal("100"),
        )
    )
    asks = tuple(
        state.iter_levels(
            BookSide.ASK,
            lower_price=Decimal("101"),
            upper_price=Decimal("102"),
        )
    )

    assert [(level.price, level.quantity) for level in bids] == [
        (Decimal("99.5"), Decimal("5")),
        (Decimal("100"), Decimal("1")),
    ]

    assert [(level.price, level.quantity) for level in asks] == [
        (Decimal("101"), Decimal("3")),
        (Decimal("101.5"), Decimal("6")),
        (Decimal("102"), Decimal("4")),
    ]

    state.apply(
        _snapshot(
            last_update_id=500,
            changes=(
                _change("bid", "90", "7"),
                _change("ask", "110", "8"),
            ),
        ),
        hour_utc=_hour(),
    )

    old_range = tuple(
        state.iter_levels(
            BookSide.BID,
            lower_price=Decimal("99"),
            upper_price=Decimal("100"),
        )
    )
    replacement_range = tuple(
        state.iter_levels(
            BookSide.BID,
            lower_price=Decimal("90"),
            upper_price=Decimal("90"),
        )
    )

    assert old_range == ()
    assert len(replacement_range) == 1
    assert replacement_range[0].quantity == Decimal("7")


def test_cancellation_is_checked_during_large_band_scan() -> None:
    changes = tuple(
        _change(
            "bid",
            str(Decimal("100") - Decimal(index) / Decimal("10000")),
            "1",
        )
        for index in range(2_000)
    ) + (_change("ask", "101", "1"),)

    state = _state(changes)

    checks = 0

    def cancellation_probe() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 4

    with pytest.raises(
        DepthLiquidityCancelledError,
        match="cancelled",
    ):
        calculate_depth_liquidity(
            state,
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.50"),
            ),
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_levels=100,
        )

    assert checks >= 4


def test_depth_band_rejects_float_and_reversed_inputs() -> None:
    with pytest.raises(
        DepthLiquidityError,
        match="exact Decimal",
    ):
        DepthBand(
            lower_fraction=0.0,  # type: ignore[arg-type]
            upper_fraction=Decimal("0.01"),
        )

    with pytest.raises(
        DepthLiquidityError,
        match="cannot exceed",
    ):
        DepthBand(
            lower_fraction=Decimal("0.10"),
            upper_fraction=Decimal("0.05"),
        )

    with pytest.raises(
        DepthLiquidityError,
        match="less than 1",
    ):
        DepthBand(
            lower_fraction=Decimal("0"),
            upper_fraction=Decimal("1"),
        )


def test_mixed_scale_depth_sum_is_exact_under_low_ambient_precision() -> None:
    bid_levels = (
        (
            Decimal("100"),
            Decimal("0.000000001"),
        ),
        (
            Decimal("99.123456789"),
            Decimal("123456789.123456789"),
        ),
    )
    ask_levels = (
        (
            Decimal("101"),
            Decimal("0.000000002"),
        ),
        (
            Decimal("110.999999999"),
            Decimal("987654321.987654321"),
        ),
    )

    state = _state(
        tuple(
            _change(
                "bid",
                str(price),
                str(quantity),
            )
            for price, quantity in bid_levels
        )
        + tuple(
            _change(
                "ask",
                str(price),
                str(quantity),
            )
            for price, quantity in ask_levels
        )
    )

    with localcontext(Context(prec=200)):
        expected_bid = sum(
            (price * quantity for price, quantity in bid_levels),
            Decimal(0),
        )
        expected_ask = sum(
            (price * quantity for price, quantity in ask_levels),
            Decimal(0),
        )
        expected_total = expected_bid + expected_ask

    with localcontext(Context(prec=6)):
        result = calculate_depth_liquidity(
            state,
            DepthBand(
                lower_fraction=Decimal("0"),
                upper_fraction=Decimal("0.10"),
            ),
        )

    assert result.bid_level_count == 2
    assert result.ask_level_count == 2
    assert result.levels_examined == 4

    assert result.bid_liquidity == expected_bid
    assert result.ask_liquidity == expected_ask
    assert result.total_liquidity == expected_total


def test_depth_hot_loop_does_not_enter_decimal_localcontexts() -> None:
    source = inspect.getsource(
        depth_module._sum_side_liquidity,
    )

    assert "localcontext" not in source
    assert "Context(" not in source
    assert "_exact_multiply(" not in source
    assert "_exact_add(" not in source
    assert "_exact_product_parts(" in source


def test_exact_subtraction_ignores_low_ambient_decimal_precision() -> None:
    left = Decimal("123456789.123456789")
    right = Decimal("123456788.987654321")
    expected = Decimal("0.135802468")

    with localcontext(Context(prec=6)):
        observed = depth_module._exact_subtract(
            left,
            right,
        )

    assert observed == expected