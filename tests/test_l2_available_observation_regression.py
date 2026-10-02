from __future__ import annotations

from decimal import Decimal

from l2shock.analysis.l2_view_stream import _BarBuilder
from l2shock.presets import (
    build_binance_bybit_okx_futures_data_preset,
    component_data_presets,
)
from l2shock.remote.contracts import RemoteArtifactKind
from l2shock.ui.remote_import_runtime import plan_remote_import_keys


def test_sparse_l2_seconds_produce_candles_without_filling_gaps() -> None:
    builder = _BarBuilder(
        timeframe_seconds=15,
        start_epoch=0,
        end_epoch=15,
    )

    for second in range(15):
        if second == 2:
            bid, ask = Decimal("10"), Decimal("1")
        elif second == 8:
            bid, ask = Decimal("1"), Decimal("10")
        else:
            bid, ask = None, None

        builder.add(second, bid, ask, None)

    bars = builder.finish()

    assert len(bars) == 1
    bar = bars[0]

    assert bar.source_seconds == 15
    assert bar.valid_l2 is True
    assert bar.bid == (
        Decimal("10"),
        Decimal("10"),
        Decimal("1"),
        Decimal("1"),
    )
    assert bar.ask == (
        Decimal("1"),
        Decimal("10"),
        Decimal("1"),
        Decimal("10"),
    )

    # Total must use same-second values. Separate side highs would give 20.
    assert bar.total == (
        Decimal("11"),
        Decimal("11"),
        Decimal("11"),
        Decimal("11"),
    )
    assert bar.delta == (
        Decimal("9"),
        Decimal("9"),
        Decimal("-9"),
        Decimal("-9"),
    )

    # The final owned second is missing, but earlier numerical observations
    # still own a renderable candle.
    assert bar.price is None


def test_one_available_l2_second_is_enough_for_a_viewing_candle() -> None:
    builder = _BarBuilder(
        timeframe_seconds=15,
        start_epoch=0,
        end_epoch=15,
    )

    for second in range(15):
        if second == 4:
            builder.add(second, Decimal("7"), Decimal("3"), None)
        else:
            builder.add(second, None, None, None)

    bar = builder.finish()[0]

    assert bar.valid_l2 is True
    assert bar.source_seconds == 15
    assert bar.bid == (Decimal("7"),) * 4
    assert bar.ask == (Decimal("3"),) * 4
    assert bar.total == (Decimal("10"),) * 4
    assert bar.delta == (Decimal("4"),) * 4
    assert bar.bid_share_pct == (70.0,) * 4


def test_completely_missing_l2_does_not_suppress_real_price() -> None:
    builder = _BarBuilder(
        timeframe_seconds=15,
        start_epoch=0,
        end_epoch=15,
    )
    price = (
        Decimal("100"),
        Decimal("105"),
        Decimal("99"),
        Decimal("103"),
    )

    for second in range(15):
        builder.add(
            second,
            None,
            None,
            price if second == 2 else None,
        )

    bar = builder.finish()[0]

    assert bar.source_seconds == 15
    assert bar.valid_l2 is False
    assert bar.bid is None
    assert bar.ask is None
    assert bar.total is None
    assert bar.delta is None
    assert bar.bid_share_pct is None
    assert bar.price == price


def test_entirely_missing_bar_does_not_erase_adjacent_available_bars() -> None:
    builder = _BarBuilder(
        timeframe_seconds=15,
        start_epoch=0,
        end_epoch=45,
    )

    for second in range(45):
        if second == 1:
            builder.add(second, Decimal("8"), Decimal("2"), None)
        elif second == 31:
            builder.add(second, Decimal("6"), Decimal("4"), None)
        else:
            builder.add(second, None, None, None)

    bars = builder.finish()

    assert len(bars) == 3
    assert [bar.valid_l2 for bar in bars] == [True, False, True]
    assert bars[0].bid == (Decimal("8"),) * 4
    assert bars[1].bid is None
    assert bars[1].ask is None
    assert bars[2].bid == (Decimal("6"),) * 4


def test_remote_import_keys_match_selected_aggregate_component_depth() -> None:
    from datetime import datetime, timedelta, timezone

    hour = datetime(2026, 10, 1, 10, tzinfo=timezone.utc)

    selected = build_binance_bybit_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.25"),
    )

    expected = {
        (
            component.eligible_markets[0].venue,
            component.eligible_markets[0].instrument,
            component.preset_hash,
        )
        for component in component_data_presets(selected)
    }

    keys = plan_remote_import_keys(
        requested_start_utc=hour,
        requested_end_utc=hour + timedelta(hours=1),
        bases=("BTC",),
        lower_depth_fraction=selected.band.lower_fraction,
        upper_depth_fraction=selected.band.upper_fraction,
    )

    actual = {
        (key.venue, key.instrument, key.preset_hash)
        for key in keys
        if key.kind is RemoteArtifactKind.L2
    }

    assert actual == expected
    assert len(actual) == 3
    assert len(keys) == 4

    # An aggregate hash is an Analysis identity, not a materialized
    # single-market artifact identity.
    assert selected.preset_hash not in {item[2] for item in actual}


def test_different_depths_do_not_share_component_hashes() -> None:
    narrow = build_binance_bybit_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    wide = build_binance_bybit_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.25"),
    )

    narrow_hashes = {
        component.preset_hash for component in component_data_presets(narrow)
    }
    wide_hashes = {component.preset_hash for component in component_data_presets(wide)}

    assert narrow.preset_hash != wide.preset_hash
    assert narrow_hashes.isdisjoint(wide_hashes)
