# tests/test_shock_evidence.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

from l2shock.analysis.aggregation import L2Second
from l2shock.analysis.shock_dataset import (
    ShockDatasetRequest,
    VerifiedShockDataset,
)
from l2shock.analysis.shock_evidence import (
    EvidenceChannel,
    EvidenceOrientation,
    ShockEvidenceConfig,
    describe_shock_evidence,
)
from l2shock.analysis.shock_execution import ShockCandidateScan
from l2shock.analysis.shock_start import (
    ShockStartConfig,
    ShockStartDirection,
    ShockStartHypothesis,
    ShockStartKind,
    ShockStructuralScale,
)
from l2shock.ingest.sampling import (
    BookSampleInvalidReason,
    BookSampleQuality,
)

_START = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)


def _config():
    return ShockStartConfig(
        scales=(
            ShockStructuralScale("major", Decimal("0.20"), 4),
            ShockStructuralScale("minor", Decimal("0.05"), 2),
        )
    )


def _second(index, bid, ask):
    if bid is None:
        return L2Second(
            timestamp_utc=_START + timedelta(seconds=index),
            quality=BookSampleQuality.INVALID,
            invalid_reason=BookSampleInvalidReason.UNINITIALIZED,
            bid_liquidity=None,
            ask_liquidity=None,
            source_count=0,
        )
    return L2Second(
        timestamp_utc=_START + timedelta(seconds=index),
        quality=BookSampleQuality.VALID,
        invalid_reason=None,
        bid_liquidity=Decimal(bid),
        ask_liquidity=Decimal(ask),
        source_count=1,
    )


def _hypothesis(a, b, c, *, scale="major"):
    # These manually owned hypotheses isolate the evidence/partition
    # semantics from the separate pivot detector tests.
    return ShockStartHypothesis(
        scale_name=scale,
        kind=ShockStartKind.TURNING,
        direction=ShockStartDirection.UP,
        a_index=a,
        b_index=b,
        c_index=c,
        a_utc=_START + timedelta(seconds=a),
        b_utc=_START + timedelta(seconds=b),
        c_utc=_START + timedelta(seconds=c),
        a_total=Fraction(60),
        b_total=Fraction(50),
        c_total=Fraction(80),
        scan_range=Fraction(100),
        bc_height=Fraction(30),
        bc_fraction_of_scan_range=Fraction(3, 10),
    )


def _scan(seconds, hypotheses):
    seconds = tuple(seconds)
    request = ShockDatasetRequest(
        base="BTC",
        preset_hash="a" * 64,
        requested_start_utc=seconds[0].timestamp_utc,
        requested_end_utc=seconds[-1].timestamp_utc,
    )
    dataset = VerifiedShockDataset(
        request=request,
        start_utc=seconds[0].timestamp_utc,
        end_utc=seconds[-1].timestamp_utc + timedelta(seconds=1),
        seconds=seconds,
        component_hours=(),
        input_id="b" * 64,
    )
    return ShockCandidateScan(
        dataset=dataset,
        config=_config(),
        hypotheses=tuple(hypotheses),
        scan_id="c" * 64,
    )


def test_channel_matches_can_have_opposite_orientation_and_offsets():
    # Representative B=8, C=14. Ask leads at 7 and rises into C=13.
    # Bid lags at 9 and falls into C=15. Delta is derived, not a vote.
    bids = [40] * 20
    asks = [20] * 20
    for index in range(9, 20):
        bids[index] = 34
    for index in range(15, 20):
        bids[index] = 10
    for index in range(7, 20):
        asks[index] = 28
    for index in range(13, 20):
        asks[index] = 45

    result = describe_shock_evidence(
        _scan(
            (_second(i, bids[i], asks[i]) for i in range(20)),
            (_hypothesis(4, 8, 14),),
        ),
        config=ShockEvidenceConfig(maximum_offset_seconds=1),
    )
    assert len(result.areas) == 1
    area = result.areas[0]
    by_channel = {item.channel: item for item in area.evidence}

    assert by_channel[EvidenceChannel.ASK].orientation is (EvidenceOrientation.UP)
    assert by_channel[EvidenceChannel.BID].orientation is (EvidenceOrientation.DOWN)
    assert abs(by_channel[EvidenceChannel.ASK].b_offset_seconds) <= 1
    assert abs(by_channel[EvidenceChannel.BID].b_offset_seconds) <= 1
    assert by_channel[EvidenceChannel.DELTA].derived_from_bid_ask is True
    assert area.independent_channel_count == 2
    assert all(abs(item.c_offset_seconds) <= 1 for item in area.evidence)


def test_flat_channels_have_no_support():
    result = describe_shock_evidence(
        _scan(
            (_second(i, 40, 20) for i in range(20)),
            (_hypothesis(4, 8, 14),),
        )
    )
    assert result.areas[0].evidence == ()
    assert result.areas[0].independent_channel_count == 0


def test_invalid_second_blocks_channel_match():
    seconds = [_second(i, 40 if i < 9 else 10, 20) for i in range(20)]
    seconds[11] = _second(11, None, None)

    result = describe_shock_evidence(
        _scan(seconds, (_hypothesis(4, 8, 14),)),
        config=ShockEvidenceConfig(maximum_offset_seconds=1),
    )
    assert result.areas[0].evidence == ()


def test_b_area_keeps_members_but_does_not_chain_indefinitely():
    scan = _scan(
        (_second(i, 40, 20) for i in range(30)),
        (
            _hypothesis(2, 6, 12),
            _hypothesis(4, 8, 14, scale="minor"),
            _hypothesis(6, 10, 16),
            _hypothesis(8, 12, 18, scale="minor"),
        ),
    )
    result = describe_shock_evidence(scan)

    assert result.hypothesis_count == 4
    assert [len(area.members) for area in result.areas] == [2, 2]
    assert [(area.first_b_index, area.last_b_index) for area in result.areas] == [
        (6, 8),
        (10, 12),
    ]
    assert result.areas[0].scale_names == ("major", "minor")
