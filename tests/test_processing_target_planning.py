from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from l2shock.acquisition import (
    SourceDataKind,
    SourceFileSpec,
    SourceHourStatus,
)
from l2shock.processing.target_planning import (
    _should_process_target,
    _single_market_preset_for_target,
)


def _hour() -> datetime:
    return datetime(
        2026,
        9,
        9,
        12,
        tzinfo=timezone.utc,
    )


def _orderbook_target(
    *,
    venue: str = "binance_futures",
    symbol: str = "BTCUSDT",
) -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue=venue,
        symbol=symbol,
        data_kind=SourceDataKind.ORDERBOOK,
        hour_utc=_hour(),
    )


def _trade_target() -> SourceFileSpec:
    return SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind=SourceDataKind.TRADES,
        hour_utc=_hour(),
    )


def test_downloaded_target_is_selected_for_terminal_processing() -> None:
    assert _should_process_target(
        _orderbook_target(),
        source_status=SourceHourStatus.DOWNLOADED,
        has_replayable_local_archive=True,
        l2_materialized=True,
        price_materialized=False,
    )


def test_processed_orderbook_is_selected_when_preset_is_missing() -> None:
    assert _should_process_target(
        _orderbook_target(),
        source_status=SourceHourStatus.PROCESSED,
        has_replayable_local_archive=True,
        l2_materialized=False,
        price_materialized=False,
    )


def test_processed_orderbook_is_skipped_when_preset_exists() -> None:
    assert not _should_process_target(
        _orderbook_target(),
        source_status=SourceHourStatus.PROCESSED,
        has_replayable_local_archive=True,
        l2_materialized=True,
        price_materialized=False,
    )


def test_processed_trade_is_selected_only_when_price_is_missing() -> None:
    target = _trade_target()

    assert _should_process_target(
        target,
        source_status=SourceHourStatus.PROCESSED,
        has_replayable_local_archive=True,
        l2_materialized=False,
        price_materialized=False,
    )

    assert not _should_process_target(
        target,
        source_status=SourceHourStatus.PROCESSED,
        has_replayable_local_archive=True,
        l2_materialized=False,
        price_materialized=True,
    )


@pytest.mark.parametrize(
    "status",
    (
        SourceHourStatus.DISCOVERED,
        SourceHourStatus.DOWNLOADING,
        SourceHourStatus.PROCESSING,
        SourceHourStatus.MISSING,
        SourceHourStatus.INVALID,
        SourceHourStatus.QUARANTINED,
        SourceHourStatus.ERROR,
    ),
)
def test_transient_or_unavailable_status_is_not_selected(
    status: SourceHourStatus,
) -> None:
    assert not _should_process_target(
        _orderbook_target(),
        source_status=status,
        has_replayable_local_archive=True,
        l2_materialized=False,
        price_materialized=False,
    )


def test_pruned_processed_source_is_not_selected() -> None:
    assert not _should_process_target(
        _orderbook_target(),
        source_status=SourceHourStatus.PROCESSED,
        has_replayable_local_archive=False,
        l2_materialized=False,
        price_materialized=False,
    )


def test_target_specific_preset_hashes_match_venue_builders() -> None:
    binance = _single_market_preset_for_target(
        _orderbook_target(),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )
    okx = _single_market_preset_for_target(
        _orderbook_target(
            venue="okx_futures",
            symbol="BTC-USDT-SWAP",
        ),
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )

    assert binance.eligible_markets[0].venue == ("binance_futures")
    assert okx.eligible_markets[0].venue == "okx_futures"
    assert binance.preset_hash != okx.preset_hash


def test_semantic_depth_change_changes_processing_preset_hash() -> None:
    target = _orderbook_target()

    first = _single_market_preset_for_target(
        target,
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.01"),
    )
    second = _single_market_preset_for_target(
        target,
        lower_depth_fraction=Decimal("0"),
        upper_depth_fraction=Decimal("0.02"),
    )

    assert first.preset_hash != second.preset_hash
