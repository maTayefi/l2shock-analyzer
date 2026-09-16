from __future__ import annotations

from datetime import datetime, timezone

import pytest

from l2shock.acquisition import (
    SourceFileSpec,
    download_endpoint,
    download_query_parameters,
    is_retryable_http_status,
    parse_retry_after_seconds,
)


def _spec() -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="orderbook",
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )


def test_download_endpoint_uses_configured_api_v1_base() -> None:
    assert (
        download_endpoint("https://api.cryptohftdata.com/v1")
        == "https://api.cryptohftdata.com/v1/download"
    )


def test_download_endpoint_rejects_query_and_fragment() -> None:
    with pytest.raises(ValueError, match="query or fragment"):
        download_endpoint("https://api.cryptohftdata.com/v1?api_key=secret")


def test_anonymous_parameters_contain_only_file() -> None:
    parameters = download_query_parameters(
        _spec(),
        use_api_key=False,
        api_key="-".join(("must", "not", "be", "used")),
    )

    assert parameters == {
        "file": ("binance_futures/2026-09-02/12/" "BTCUSDT_orderbook.parquet")
    }
    assert "api_key" not in parameters


def test_direct_api_key_mode_is_explicit() -> None:
    parameters = download_query_parameters(
        _spec(),
        use_api_key=True,
        api_key="example-secret",
    )

    assert parameters["file"].endswith("BTCUSDT_orderbook.parquet")
    assert parameters["api_key"] == "example-secret"


def test_direct_api_key_mode_requires_configured_key() -> None:
    with pytest.raises(ValueError, match="no API key"):
        download_query_parameters(
            _spec(),
            use_api_key=True,
            api_key="",
        )


def test_retryable_status_contract() -> None:
    assert is_retryable_http_status(429) is True
    assert is_retryable_http_status(503) is True
    assert is_retryable_http_status(404) is False
    assert is_retryable_http_status(401) is False


def test_retry_after_numeric_seconds_are_bounded() -> None:
    assert parse_retry_after_seconds("12") == pytest.approx(12.0)
    assert parse_retry_after_seconds("9999") == pytest.approx(300.0)


def test_retry_after_http_date() -> None:
    now = datetime(
        2026,
        9,
        6,
        12,
        0,
        tzinfo=timezone.utc,
    )

    result = parse_retry_after_seconds(
        "Sun, 06 Sep 2026 12:00:30 GMT",
        now=now,
    )

    assert result == pytest.approx(30.0)


def test_invalid_retry_after_is_ignored() -> None:
    assert parse_retry_after_seconds("not-a-delay") is None
    assert parse_retry_after_seconds("-10") is None
