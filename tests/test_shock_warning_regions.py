from __future__ import annotations

from datetime import datetime, timedelta, timezone
from fractions import Fraction
from zoneinfo import ZoneInfo

import pytest

from l2shock.analysis.shock_window import ShockWindowSecond
from l2shock.ui.shock_warning_regions import (
    WARNING_REGION_COLOR,
    ShockWarningChannel,
    ShockWarningRegion,
    ShockWarningRegionError,
    clip_warning_regions,
    find_long_outage_regions,
    l2_warning_observations,
    warning_mark_area_items,
    warning_threshold_seconds_from_minutes,
)

_ORIGIN = datetime(2026, 9, 24, 2, 0, 0, tzinfo=timezone.utc)


def _at(position: int) -> datetime:
    return _ORIGIN + timedelta(seconds=position)


def _obs(total, *, invalid=frozenset(), missing=frozenset()):
    return tuple(
        (_at(position), position not in invalid)
        for position in range(total)
        if position not in missing
    )


def _find(observations, total, *, threshold=60, channel="l2"):
    return find_long_outage_regions(
        observations,
        channel=channel,
        window_start_utc=_ORIGIN,
        window_end_utc_exclusive=_at(total),
        minimum_exclusive_seconds=threshold,
    )


def _l2(start, end):
    return ShockWarningRegion(ShockWarningChannel.L2, _at(start), _at(end))


def test_run_equal_to_threshold_is_not_flagged() -> None:
    assert _find(_obs(200, invalid=frozenset(range(10, 70))), 200) == ()


def test_run_longer_than_threshold_is_flagged() -> None:
    regions = _find(_obs(200, invalid=frozenset(range(10, 71))), 200)

    assert regions == (_l2(10, 71),)
    assert regions[0].duration_seconds == 61


def test_missing_and_invalid_seconds_merge_into_one_outage() -> None:
    observations = _obs(
        200,
        invalid=frozenset(range(10, 40)),
        missing=frozenset(range(40, 80)),
    )

    assert _find(observations, 200) == (_l2(10, 80),)


def test_short_runs_separated_by_a_valid_second_do_not_merge() -> None:
    observations = _obs(
        200,
        invalid=frozenset(range(0, 40)) | frozenset(range(41, 90)),
    )

    assert _find(observations, 200) == ()


def test_leading_and_trailing_unavailability_count() -> None:
    observations = tuple((_at(position), True) for position in range(61, 100))

    assert _find(observations, 200) == (_l2(0, 61), _l2(100, 200))


def test_empty_observations_cover_the_whole_window() -> None:
    assert _find((), 61) == (_l2(0, 61),)
    assert _find((), 60) == ()


def test_price_threshold_uses_minutes() -> None:
    threshold = warning_threshold_seconds_from_minutes(3)

    assert threshold == 180
    assert (
        _find(
            _obs(400, missing=frozenset(range(180))),
            400,
            threshold=threshold,
            channel="price",
        )
        == ()
    )

    regions = _find(
        _obs(400, missing=frozenset(range(181))),
        400,
        threshold=threshold,
        channel="price",
    )
    assert regions[0].channel is ShockWarningChannel.PRICE


def test_clipping_keeps_a_long_outage_that_is_partly_visible() -> None:
    regions = _find(_obs(300, invalid=frozenset(range(100, 200))), 300)

    assert clip_warning_regions(
        regions,
        view_start_utc=_at(150),
        view_end_utc_exclusive=_at(250),
    ) == (_l2(150, 200),)

    assert (
        clip_warning_regions(
            regions,
            view_start_utc=_at(200),
            view_end_utc_exclusive=_at(250),
        )
        == ()
    )


def test_l2_observations_read_window_seconds() -> None:
    seconds = tuple(
        ShockWindowSecond(
            dataset_index=position,
            timestamp_utc=_at(position),
            valid_l2=position != 1,
            bid=Fraction(1) if position != 1 else None,
            ask=Fraction(1) if position != 1 else None,
            total=Fraction(2) if position != 1 else None,
            delta=Fraction(0) if position != 1 else None,
        )
        for position in range(3)
    )

    assert l2_warning_observations(seconds) == (
        (_at(0), True),
        (_at(1), False),
        (_at(2), True),
    )


def test_mark_area_items_use_exact_utc_iso_coordinates() -> None:
    assert warning_mark_area_items((_l2(10, 80),)) == [
        [
            {
                "name": "l2shock-warning:l2",
                "xAxis": _at(10).isoformat(),
                "itemStyle": {"color": WARNING_REGION_COLOR},
            },
            {"xAxis": _at(80).isoformat()},
        ]
    ]


@pytest.mark.parametrize("threshold", [0, -1, True, 60.0, "60"])
def test_invalid_thresholds_are_rejected(threshold: object) -> None:
    with pytest.raises(ShockWarningRegionError, match="positive integer"):
        _find((), 10, threshold=threshold)


@pytest.mark.parametrize(
    "observations",
    [
        ((datetime(2026, 9, 24, 2, 0, 0), True),),
        ((_ORIGIN.replace(microsecond=5), True),),
        ((_ORIGIN.astimezone(ZoneInfo("Asia/Tehran")), True),),
        ((_at(1), True), (_at(1), True)),
        ((_at(10), True),),
        ((_at(0), 1),),
    ],
)
def test_malformed_observations_are_rejected(observations) -> None:
    with pytest.raises(ShockWarningRegionError):
        _find(observations, 10)


def test_unknown_channel_and_reversed_window_are_rejected() -> None:
    with pytest.raises(ShockWarningRegionError, match="channel"):
        _find((), 10, channel="funding")

    with pytest.raises(ShockWarningRegionError, match="precedes"):
        find_long_outage_regions(
            (),
            channel="l2",
            window_start_utc=_at(5),
            window_end_utc_exclusive=_at(1),
            minimum_exclusive_seconds=60,
        )


def _price(start, end):
    return ShockWarningRegion(ShockWarningChannel.PRICE, _at(start), _at(end))


def test_dataset_observations_treat_invalid_and_degraded_as_outage() -> None:
    from types import SimpleNamespace

    from l2shock.ingest.sampling import BookSampleQuality
    from l2shock.ui.shock_warning_regions import l2_dataset_warning_observations

    seconds = (
        SimpleNamespace(
            timestamp_utc=_at(0),
            quality=BookSampleQuality.VALID,
            coverage_degraded=False,
        ),
        SimpleNamespace(
            timestamp_utc=_at(1),
            quality=BookSampleQuality.INVALID,
            coverage_degraded=False,
        ),
        SimpleNamespace(
            timestamp_utc=_at(2),
            quality=BookSampleQuality.VALID,
            coverage_degraded=True,
        ),
    )

    assert l2_dataset_warning_observations(seconds) == (
        (_at(0), True),
        (_at(1), False),
        (_at(2), False),
    )


def test_overlay_targets_panels_and_survives_hidden_b_bands() -> None:
    import copy

    from l2shock.ui.shock_annotation_visibility import (
        with_shock_annotation_visibility,
    )
    from l2shock.ui.shock_warning_regions import (
        ShockWarningOverlay,
        with_shock_warning_regions,
    )

    band = [{"name": "L2 B candidate interval", "xAxis": "b0"}, {"xAxis": "b1"}]
    option = {
        "series": [
            {"id": "shock-view-price-context", "markArea": {"data": [band]}},
            {"id": "shock-view-bid", "markArea": {"data": [band]}},
            {"id": "shock-view-total", "markArea": {"data": [band]}},
            {"id": "l2shock-render-identity"},
        ]
    }
    before = copy.deepcopy(option)
    overlay = ShockWarningOverlay(
        l2_regions=(_l2(10, 80),),
        price_regions=(_price(100, 300),),
    )

    shown = with_shock_warning_regions(option, overlay=overlay)

    assert option == before
    price, bid, total, other = shown["series"]
    assert len(price["markArea"]["data"]) == 3
    assert len(bid["markArea"]["data"]) == 2
    assert len(total["markArea"]["data"]) == 2
    assert "markArea" not in other
    assert price["markArea"]["data"][2][0]["name"] == "l2shock-warning:price"
    assert bid["markArea"]["data"][1][0]["name"] == "l2shock-warning:l2"

    # Bands hidden first, warnings appended afterwards: warnings remain.
    bands_hidden = with_shock_annotation_visibility(
        option, show_lines_and_labels=True, show_b_bands=False
    )
    combined = with_shock_warning_regions(bands_hidden, overlay=overlay)
    assert [len(s.get("markArea", {}).get("data", [])) for s in combined["series"]] == [
        2,
        1,
        1,
        0,
    ]


def test_empty_overlay_returns_same_object_and_channels_are_checked() -> None:
    from l2shock.ui.shock_warning_regions import (
        ShockWarningOverlay,
        with_shock_warning_regions,
    )

    option = {"series": [{"id": "shock-view-bid", "markArea": {"data": []}}]}
    assert with_shock_warning_regions(option, overlay=ShockWarningOverlay()) is option

    with pytest.raises(ShockWarningRegionError, match="l2 overlay"):
        ShockWarningOverlay(l2_regions=(_price(0, 100),))


def test_warning_color_is_the_legend_color() -> None:
    from l2shock.ui.shock_chart_options import SHOCK_CHART_COLORS
    from l2shock.ui.shock_legend import SHOCK_LEGEND_ENTRIES

    assert SHOCK_CHART_COLORS["data_outage"] == WARNING_REGION_COLOR
    assert any(entry.color_key == "data_outage" for entry in SHOCK_LEGEND_ENTRIES)


def test_price_regions_use_padded_window_then_clip(monkeypatch) -> None:
    from types import SimpleNamespace

    import l2shock.ui.shock_price_context as price_context
    from l2shock.price import TradeSampleQuality

    quality = [TradeSampleQuality.INVALID] * 200 + [TradeSampleQuality.VALID] * 3_400
    monkeypatch.setattr(
        price_context,
        "decode_hourly_trade_ohlc_blocks",
        lambda _encoded: SimpleNamespace(quality=quality),
    )

    class Repository:
        def list_price_hours(self, *, base, start_utc, end_utc, verify_codec=True):
            assert verify_codec is True
            assert start_utc == _ORIGIN
            return (
                SimpleNamespace(
                    hour_utc=_ORIGIN,
                    base=base,
                    source_venue="binance_futures",
                    encoded=None,
                ),
            )

    observations = price_context.price_second_validity_from_repository(
        Repository(),
        base="BTC",
        start_utc=_at(100),
        end_utc_exclusive=_at(300),
    )
    assert len(observations) == 200
    assert observations[0] == (_at(100), False)
    assert observations[100] == (_at(200), True)

    # Visible part [190, 200) is only 10 s, but the whole run [10, 200)
    # inside the padded window is 190 s > 180 s, so it is flagged.
    regions = price_context.price_warning_regions_from_repository(
        Repository(),
        base="BTC",
        view_start_utc=_at(190),
        view_end_utc_exclusive=_at(400),
        minimum_exclusive_seconds=180,
    )
    assert regions == (_price(190, 200),)

    with pytest.raises(ValueError, match="positive integer"):
        price_context.price_warning_regions_from_repository(
            Repository(),
            base="BTC",
            view_start_utc=_at(190),
            view_end_utc_exclusive=_at(400),
            minimum_exclusive_seconds=True,
        )
