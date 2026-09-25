# tests/test_shock_dataset.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import l2shock.analysis.shock_dataset as module
from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.shock_dataset import (
    ShockDatasetError,
    ShockDatasetRequest,
    load_shock_dataset_from_repository,
)
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)
from l2shock.presets import (
    build_binance_futures_data_preset,
    build_binance_okx_futures_data_preset,
    component_data_presets,
)

_HOUR = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
_NEXT = _HOUR + timedelta(hours=1)
_A = "a" * 64
_B = "b" * 64


def _single():
    return build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0.001"),
        upper_fraction=Decimal("0.01"),
    )


def _aggregate():
    return build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0.001"),
        upper_fraction=Decimal("0.01"),
    )


@dataclass
class FakeRepository:
    preset: object
    rows: dict[str, tuple]
    calls: list

    def __init__(self, preset, rows):
        self.preset = preset
        self.rows = rows
        self.calls = []

    def get_preset(self, preset_hash):
        self.calls.append(("preset", preset_hash))
        if preset_hash != self.preset.preset_hash:
            return None
        return SimpleNamespace(
            base=self.preset.base,
            config_json=self.preset.to_canonical_dict(),
        )

    def list_l2_hours(
        self, *, base, preset_hash, start_utc, end_utc, verify_codec=True
    ):
        self.calls.append(("l2", base, preset_hash, start_utc, end_utc, verify_codec))
        assert verify_codec is True
        return self.rows.get(preset_hash, ())


def _row(preset_hash, hour, digest=_A):
    return SimpleNamespace(
        base="BTC",
        preset_hash=preset_hash,
        hour_utc=hour,
        encoded=SimpleNamespace(content_sha256=digest),
    )


def _request(preset, start, end):
    return ShockDatasetRequest(
        base="BTC",
        preset_hash=preset.preset_hash,
        requested_start_utc=start,
        requested_end_utc=end,
    )


def _decode(row):
    # The fake supplies a complete, timestamp-owned hour, leaving the
    # production codec/repository verification to their existing tests.
    return tuple(
        L2Second(
            timestamp_utc=row.hour_utc + timedelta(seconds=i),
            quality=BookSampleQuality.VALID,
            invalid_reason=None,
            bid_liquidity=Decimal("10"),
            ask_liquidity=Decimal("5"),
            source_count=1,
        )
        for i in range(3_600)
    )


def test_single_market_closed_endpoints_and_no_price_read(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _single()
    repo = FakeRepository(
        preset,
        {preset.preset_hash: (_row(preset.preset_hash, _HOUR),)},
    )

    result = load_shock_dataset_from_repository(
        repo,
        _request(
            preset,
            _HOUR + timedelta(seconds=7, milliseconds=500),
            _HOUR + timedelta(seconds=9, milliseconds=100),
        ),
    )

    assert [x.timestamp_utc for x in result.seconds] == [
        _HOUR + timedelta(seconds=i) for i in (7, 8, 9)
    ]
    assert all(x.quality is BookSampleQuality.VALID for x in result.seconds)
    assert result.valid_second_count == 3
    assert len(result.component_hours) == 1
    assert len(result.input_id) == 64
    assert len([call for call in repo.calls if call[0] == "l2"]) == 1
    assert all(call[0] in {"preset", "l2"} for call in repo.calls)


def test_missing_hour_is_explicit_invalid_gap(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _single()
    repo = FakeRepository(
        preset,
        {preset.preset_hash: (_row(preset.preset_hash, _HOUR),)},
    )

    result = load_shock_dataset_from_repository(
        repo,
        _request(
            preset,
            _HOUR + timedelta(seconds=3598),
            _NEXT + timedelta(seconds=2),
        ),
    )

    assert len(result.seconds) == 5
    assert result.valid_second_count == 2
    assert result.invalid_second_count == 3
    assert result.seconds[2].timestamp_utc == _NEXT
    assert result.seconds[2].invalid_reason is (BookSampleInvalidReason.UNINITIALIZED)
    assert [x.content_sha256 for x in result.component_hours] == [_A, None]


def test_change_in_row_hash_changes_input_identity(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _single()
    request = _request(preset, _HOUR, _HOUR + timedelta(seconds=2))
    first = load_shock_dataset_from_repository(
        FakeRepository(
            preset, {preset.preset_hash: (_row(preset.preset_hash, _HOUR, _A),)}
        ),
        request,
    )
    second = load_shock_dataset_from_repository(
        FakeRepository(
            preset, {preset.preset_hash: (_row(preset.preset_hash, _HOUR, _B),)}
        ),
        request,
    )

    assert first.input_id != second.input_id
    assert first.seconds == second.seconds


def test_wrong_owner_hour_is_rejected(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _single()
    repo = FakeRepository(
        preset,
        {preset.preset_hash: (_row(preset.preset_hash, _NEXT),)},
    )

    with pytest.raises(ShockDatasetError, match="wrong-owner"):
        load_shock_dataset_from_repository(
            repo,
            _request(preset, _HOUR, _HOUR),
        )


def test_multi_market_missing_component_is_not_silent_sum(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _aggregate()
    components = component_data_presets(preset)
    assert len(components) == 2
    repo = FakeRepository(
        preset,
        {
            components[0].preset_hash: (_row(components[0].preset_hash, _HOUR),),
            # Second component has no hour. The first market's 10+5
            # must NOT masquerade as full aggregate coverage.
        },
    )

    with pytest.raises(ShockDatasetError, match="partial coverage"):
        load_shock_dataset_from_repository(
            repo,
            _request(
                preset,
                _HOUR + timedelta(seconds=3),
                _HOUR + timedelta(seconds=5),
            ),
        )


def test_multi_market_sums_bid_and_ask_at_same_second(monkeypatch):
    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _aggregate()
    components = component_data_presets(preset)
    repo = FakeRepository(
        preset,
        {
            component.preset_hash: (_row(component.preset_hash, _HOUR),)
            for component in components
        },
    )

    result = load_shock_dataset_from_repository(
        repo,
        _request(
            preset,
            _HOUR + timedelta(seconds=3),
            _HOUR + timedelta(seconds=5),
        ),
    )

    assert len(result.seconds) == 3
    assert all(
        second.bid_liquidity == Decimal("20")
        and second.ask_liquidity == Decimal("10")
        and second.quality is BookSampleQuality.VALID
        for second in result.seconds
    )
    assert len(result.component_hours) == 2


def test_execution_uses_verified_l2_without_price_and_is_repeatable(
    monkeypatch,
):
    from l2shock.analysis.shock_execution import (
        run_shock_scan_from_repository,
    )
    from l2shock.analysis.shock_start import (
        ShockStartConfig,
        ShockStructuralScale,
    )

    preset = _single()
    hour_row = _row(preset.preset_hash, _HOUR)

    def shaped_hour(row):
        observations = list(_decode(row))
        # This entire 11-second shape lies in the requested interval.
        # Bid changes and Ask is constant; Total has a local low at B=5.
        shape = (50, 45, 40, 30, 20, 10, 20, 35, 50, 65, 75)
        for index, total in enumerate(shape):
            observations[index] = L2Second(
                timestamp_utc=row.hour_utc + timedelta(seconds=index),
                quality=BookSampleQuality.VALID,
                invalid_reason=None,
                bid_liquidity=Decimal(total),
                ask_liquidity=Decimal(0),
                source_count=1,
            )
        return tuple(observations)

    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", shaped_hour)
    request = _request(
        preset,
        _HOUR,
        _HOUR + timedelta(seconds=10),
    )
    config = ShockStartConfig(
        scales=(
            ShockStructuralScale("major", Decimal("0.20"), 3),
            ShockStructuralScale("medium", Decimal("0.10"), 2),
            ShockStructuralScale("minor", Decimal("0.05"), 1),
        )
    )

    def run():
        repo = FakeRepository(
            preset,
            {preset.preset_hash: (hour_row,)},
        )
        scan = run_shock_scan_from_repository(
            repo,
            request,
            config=config,
        )
        assert all(call[0] in {"preset", "l2"} for call in repo.calls)
        assert all(call[-1] is True for call in repo.calls if call[0] == "l2")
        return scan

    first = run()
    second = run()

    assert first.scan_id == second.scan_id
    assert first.hypotheses == second.hypotheses
    assert first.total_range == 65
    assert any(item.b_index == 5 and item.c_index == 10 for item in first.hypotheses)
    assert first.active_scale_names


def test_execution_config_changes_scan_identity(monkeypatch):
    from l2shock.analysis.shock_execution import (
        run_shock_scan_from_repository,
    )
    from l2shock.analysis.shock_start import (
        ShockStartConfig,
        ShockStructuralScale,
    )

    monkeypatch.setattr(module, "decode_l2_hour_to_seconds", _decode)
    preset = _single()
    request = _request(
        preset,
        _HOUR,
        _HOUR + timedelta(seconds=10),
    )

    def run(radius):
        return run_shock_scan_from_repository(
            FakeRepository(
                preset,
                {
                    preset.preset_hash: (_row(preset.preset_hash, _HOUR),),
                },
            ),
            request,
            config=ShockStartConfig(
                scales=(
                    ShockStructuralScale(
                        "major",
                        Decimal("0.20"),
                        radius,
                    ),
                )
            ),
        )

    first = run(2)
    second = run(3)

    assert first.dataset.input_id == second.dataset.input_id
    assert first.scan_id != second.scan_id
    assert first.hypotheses == second.hypotheses == ()
