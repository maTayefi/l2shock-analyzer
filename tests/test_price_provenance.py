from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from l2shock.db import (
    PRICE_PROVENANCE_SCHEMA,
    PRICE_PROVENANCE_SCHEMA_VERSION,
    PriceHourlyProvenance,
    PriceSourceHourReference,
)


def _hour(offset: int = 0) -> datetime:
    return datetime(
        2082,
        1,
        1,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(hours=offset)


def _source(
    *,
    hour: datetime | None = None,
    instrument: str = "BTCUSDT",
    digest: str = "a" * 64,
) -> PriceSourceHourReference:
    return PriceSourceHourReference(
        provider="cryptohftdata",
        venue="binance_futures",
        instrument=instrument,
        hour_utc=hour or _hour(),
        content_sha256=digest,
    )


def test_price_source_identity_is_canonicalized() -> None:
    source = PriceSourceHourReference(
        provider=" CryptoHFTData ",
        venue=" BINANCE_FUTURES ",
        instrument=" btcusdt ",
        hour_utc=_hour(),
        content_sha256="a" * 64,
    )

    assert source.provider == "cryptohftdata"
    assert source.venue == "binance_futures"
    assert source.instrument == "BTCUSDT"
    assert source.data_kind == "trades"


def test_price_provenance_source_order_is_deterministic() -> None:
    previous = _source(
        hour=_hour(-1),
        digest="a" * 64,
    )
    current = _source(
        hour=_hour(),
        digest="b" * 64,
    )
    following = _source(
        hour=_hour(1),
        digest="c" * 64,
    )

    provenance = PriceHourlyProvenance(
        source_hours=(
            following,
            previous,
            current,
        ),
    )

    assert [source.hour_utc for source in provenance.source_hours] == [
        _hour(-1),
        _hour(),
        _hour(1),
    ]


def test_price_provenance_dictionary_is_json_safe() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(_source(),),
    )

    payload = provenance.to_dict()

    assert payload["schema"] == PRICE_PROVENANCE_SCHEMA
    assert payload["schema_version"] == PRICE_PROVENANCE_SCHEMA_VERSION
    assert payload["trade_reader_schema_version"] == 1
    assert payload["trade_ohlc_schema_version"] == 1

    source = payload["source_hours"][0]

    assert source["data_kind"] == "trades"
    assert source["hour_utc"].endswith("Z")
    assert "local_path" not in source


def test_price_provenance_requires_current_source_hour() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(
            _source(hour=_hour(-1)),
            _source(
                hour=_hour(1),
                digest="b" * 64,
            ),
        ),
    )

    with pytest.raises(
        ValueError,
        match="current source hour",
    ):
        provenance.validate_for_hour(
            base="BTC",
            hour_utc=_hour(),
        )


def test_price_provenance_allows_immediately_adjacent_sources() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(
            _source(
                hour=_hour(-1),
                digest="a" * 64,
            ),
            _source(
                hour=_hour(),
                digest="b" * 64,
            ),
            _source(
                hour=_hour(1),
                digest="c" * 64,
            ),
        ),
    )

    provenance.validate_for_hour(
        base="BTC",
        hour_utc=_hour(),
    )


def test_price_provenance_rejects_distant_source_hour() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(
            _source(hour=_hour()),
            _source(
                hour=_hour(2),
                digest="b" * 64,
            ),
        ),
    )

    with pytest.raises(
        ValueError,
        match="immediately previous, current, or immediately following",
    ):
        provenance.validate_for_hour(
            base="BTC",
            hour_utc=_hour(),
        )


def test_price_provenance_rejects_wrong_symbol() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(_source(instrument="ETHUSDT"),),
    )

    with pytest.raises(
        ValueError,
        match="instrument does not match",
    ):
        provenance.validate_for_hour(
            base="BTC",
            hour_utc=_hour(),
        )


def test_duplicate_price_source_is_rejected() -> None:
    source = _source()

    with pytest.raises(
        ValueError,
        match="duplicate",
    ):
        PriceHourlyProvenance(
            source_hours=(source, source),
        )


def test_price_source_digest_must_be_canonical_lowercase() -> None:
    with pytest.raises(
        ValueError,
        match="canonical lowercase",
    ):
        _source(digest="A" * 64)


def test_price_source_hour_must_be_exact_utc_hour() -> None:
    with pytest.raises(ValueError):
        _source(
            hour=_hour().replace(minute=30),
        )


def test_price_provenance_round_trips_through_typed_json_decoder() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(
            _source(
                hour=_hour(-1),
                digest="a" * 64,
            ),
            _source(
                hour=_hour(),
                digest="b" * 64,
            ),
            _source(
                hour=_hour(1),
                digest="c" * 64,
            ),
        ),
    )

    decoded = PriceHourlyProvenance.from_dict(provenance.to_dict())

    assert decoded == provenance
    decoded.validate_for_hour(
        base="BTC",
        hour_utc=_hour(),
    )


def test_price_provenance_rejects_conflicting_hashes_for_one_archive() -> None:
    with pytest.raises(
        ValueError,
        match="conflicting content hashes",
    ):
        PriceHourlyProvenance(
            source_hours=(
                _source(
                    digest="a" * 64,
                ),
                _source(
                    digest="b" * 64,
                ),
            ),
        )


def test_price_provenance_decoder_rejects_noncanonical_source_order() -> None:
    provenance = PriceHourlyProvenance(
        source_hours=(
            _source(
                hour=_hour(-1),
                digest="a" * 64,
            ),
            _source(
                hour=_hour(),
                digest="b" * 64,
            ),
        ),
    )

    payload = provenance.to_dict()
    payload["source_hours"] = list(reversed(payload["source_hours"]))

    with pytest.raises(
        ValueError,
        match="not canonical",
    ):
        PriceHourlyProvenance.from_dict(payload)
