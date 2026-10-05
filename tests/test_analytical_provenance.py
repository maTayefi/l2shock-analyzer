from __future__ import annotations

from datetime import datetime, timezone

import pytest

from l2shock.db import (
    L2_PROVENANCE_SCHEMA,
    L2_PROVENANCE_SCHEMA_VERSION,
    L2HourlyProvenance,
    SourceHourReference,
)


def _hour(value: int = 12) -> datetime:
    return datetime(
        2081,
        1,
        1,
        value,
        tzinfo=timezone.utc,
    )


def _source(
    *,
    venue: str = "binance_futures",
    instrument: str = "BTCUSDT",
    hour: datetime | None = None,
    digest: str = "a" * 64,
) -> SourceHourReference:
    return SourceHourReference(
        provider="cryptohftdata",
        venue=venue,
        instrument=instrument,
        hour_utc=hour or _hour(),
        content_sha256=digest,
    )


def test_source_identity_is_canonicalized() -> None:
    source = SourceHourReference(
        provider=" CryptoHFTData ",
        venue=" BINANCE_FUTURES ",
        instrument=" btcusdt ",
        hour_utc=_hour(),
        content_sha256="a" * 64,
    )

    assert source.provider == "cryptohftdata"
    assert source.venue == "binance_futures"
    assert source.instrument == "BTCUSDT"


def test_provenance_source_order_is_deterministic() -> None:
    first = _source(
        venue="z_venue",
        digest="a" * 64,
    )
    second = _source(
        venue="a_venue",
        digest="b" * 64,
    )

    provenance = L2HourlyProvenance(
        source_hours=(first, second),
    )

    assert [source.venue for source in provenance.source_hours] == [
        "a_venue",
        "z_venue",
    ]


def test_provenance_dictionary_is_json_safe() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(_source(),),
        checkpoint_content_sha256="b" * 64,
    )

    payload = provenance.to_dict()

    assert payload["schema"] == L2_PROVENANCE_SCHEMA
    assert payload["schema_version"] == L2_PROVENANCE_SCHEMA_VERSION
    assert payload["checkpoint_content_sha256"] == "b" * 64

    source = payload["source_hours"][0]

    assert source["data_kind"] == "orderbook"
    assert source["hour_utc"].endswith("Z")
    assert "local_path" not in source


def test_provenance_rejects_future_source_hour() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(_source(hour=_hour(13)),),
    )

    with pytest.raises(
        ValueError,
        match="after the persisted",
    ):
        provenance.validate_for_hour(_hour(12))


def test_provenance_requires_current_hour_source() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(_source(hour=_hour(11)),),
    )

    with pytest.raises(
        ValueError,
        match="persisted analytical hour",
    ):
        provenance.validate_for_hour(_hour(12))


def test_previous_and_current_sources_are_allowed() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(
            _source(
                hour=_hour(11),
                digest="a" * 64,
            ),
            _source(
                hour=_hour(12),
                digest="b" * 64,
            ),
        ),
    )

    provenance.validate_for_hour(_hour(12))


def test_source_hour_must_be_exact_utc_hour() -> None:
    with pytest.raises(ValueError):
        SourceHourReference(
            provider="cryptohftdata",
            venue="binance_futures",
            instrument="BTCUSDT",
            hour_utc=datetime(
                2081,
                1,
                1,
                12,
                30,
                tzinfo=timezone.utc,
            ),
            content_sha256="a" * 64,
        )


def test_checkpoint_hash_is_optional() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(_source(),),
    )

    assert provenance.checkpoint_content_sha256 is None


def test_checkpoint_hash_must_be_canonical() -> None:
    with pytest.raises(
        ValueError,
        match="canonical lowercase",
    ):
        L2HourlyProvenance(
            source_hours=(_source(),),
            checkpoint_content_sha256="B" * 64,
        )


def test_l2_provenance_round_trips_through_typed_json_decoder() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(
            _source(
                hour=_hour(11),
                digest="a" * 64,
            ),
            _source(
                hour=_hour(12),
                digest="b" * 64,
            ),
        ),
        checkpoint_content_sha256="c" * 64,
    )

    decoded = L2HourlyProvenance.from_dict(provenance.to_dict())

    assert decoded == provenance
    decoded.validate_for_hour(_hour(12))


def test_l2_provenance_rejects_conflicting_hashes_for_one_archive() -> None:
    with pytest.raises(
        ValueError,
        match="conflicting content hashes",
    ):
        L2HourlyProvenance(
            source_hours=(
                _source(
                    hour=_hour(),
                    digest="a" * 64,
                ),
                _source(
                    hour=_hour(),
                    digest="b" * 64,
                ),
            ),
        )


def test_l2_provenance_decoder_rejects_unexpected_fields() -> None:
    provenance = L2HourlyProvenance(
        source_hours=(_source(),),
    )
    payload = provenance.to_dict()
    payload["unexpected"] = True

    with pytest.raises(
        ValueError,
        match="fields do not match",
    ):
        L2HourlyProvenance.from_dict(payload)


def test_l2_provenance_requires_exact_component_market() -> None:
    from decimal import Decimal

    from l2shock.presets import build_binance_futures_data_preset

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )

    wrong_markets = (
        ("cryptohftdata", "binance_futures", "ETHUSDT"),
        ("cryptohftdata", "okx_futures", "BTC-USDT-SWAP"),
        ("another_provider", "binance_futures", "BTCUSDT"),
    )

    for provider, venue, instrument in wrong_markets:
        provenance = L2HourlyProvenance(
            source_hours=(
                SourceHourReference(
                    provider=provider,
                    venue=venue,
                    instrument=instrument,
                    hour_utc=_hour(),
                    content_sha256="a" * 64,
                ),
            ),
        )

        with pytest.raises(ValueError, match="component preset"):
            provenance.validate_for_preset(
                preset=preset,
                hour_utc=_hour(),
            )


def test_l2_provenance_checks_predecessor_market_too() -> None:
    from decimal import Decimal

    from l2shock.presets import build_binance_futures_data_preset

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    provenance = L2HourlyProvenance(
        source_hours=(
            _source(
                venue="okx_futures",
                instrument="BTC-USDT-SWAP",
                hour=_hour(11),
            ),
            _source(
                hour=_hour(12),
                digest="b" * 64,
            ),
        ),
    )

    with pytest.raises(ValueError, match="component preset"):
        provenance.validate_for_preset(
            preset=preset,
            hour_utc=_hour(12),
        )


def test_l2_component_provenance_allows_same_market_replay_history() -> None:
    from decimal import Decimal

    from l2shock.presets import build_binance_futures_data_preset

    preset = build_binance_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    provenance = L2HourlyProvenance(
        source_hours=(
            _source(hour=_hour(11)),
            _source(hour=_hour(12), digest="b" * 64),
        ),
    )

    provenance.validate_for_preset(
        preset=preset,
        hour_utc=_hour(12),
    )


def test_l2_provenance_cannot_authorize_materialized_aggregate_row() -> None:
    from decimal import Decimal

    from l2shock.presets import build_binance_okx_futures_data_preset

    aggregate = build_binance_okx_futures_data_preset(
        base="BTC",
        lower_fraction=Decimal("0"),
        upper_fraction=Decimal("0.01"),
    )
    provenance = L2HourlyProvenance(source_hours=(_source(),))

    with pytest.raises(ValueError, match="exactly one component market"):
        provenance.validate_for_preset(
            preset=aggregate,
            hour_utc=_hour(),
        )
