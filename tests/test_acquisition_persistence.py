from __future__ import annotations

from datetime import datetime, timezone

import pytest

from l2shock.acquisition import (
    FetchRunKind,
    FetchRunStatus,
    InvalidSourceHourTransitionError,
    SourceFileSpec,
    SourceHourStatus,
    bounded_diagnostic_text,
    nonnegative_counter,
    source_hour_lock_keys,
    validate_source_hour_transition,
)


def _spec(
    *,
    symbol: str = "BTCUSDT",
    data_kind: str = "orderbook",
) -> SourceFileSpec:
    return SourceFileSpec(
        venue="binance_futures",
        symbol=symbol,
        data_kind=data_kind,
        hour_utc=datetime(
            2026,
            9,
            2,
            12,
            tzinfo=timezone.utc,
        ),
    )


def test_normal_download_status_progression_is_valid() -> None:
    transitions = (
        (
            SourceHourStatus.DISCOVERED,
            SourceHourStatus.DOWNLOADING,
        ),
        (
            SourceHourStatus.DOWNLOADING,
            SourceHourStatus.DOWNLOADED,
        ),
        (
            SourceHourStatus.DOWNLOADED,
            SourceHourStatus.PROCESSING,
        ),
        (
            SourceHourStatus.PROCESSING,
            SourceHourStatus.PROCESSED,
        ),
    )

    for current, target in transitions:
        observed_current, observed_target = validate_source_hour_transition(
            current, target
        )

        assert observed_current is current
        assert observed_target is target


def test_retry_from_missing_or_error_is_valid() -> None:
    assert (
        validate_source_hour_transition(
            SourceHourStatus.MISSING,
            SourceHourStatus.DOWNLOADING,
        )[1]
        is SourceHourStatus.DOWNLOADING
    )

    assert (
        validate_source_hour_transition(
            SourceHourStatus.ERROR,
            SourceHourStatus.DOWNLOADING,
        )[1]
        is SourceHourStatus.DOWNLOADING
    )


def test_processed_source_cannot_regress_to_discovered() -> None:
    with pytest.raises(
        InvalidSourceHourTransitionError,
        match="processed.*discovered",
    ):
        validate_source_hour_transition(
            SourceHourStatus.PROCESSED,
            SourceHourStatus.DISCOVERED,
        )


def test_downloaded_source_can_be_recorded_idempotently() -> None:
    current, target = validate_source_hour_transition(
        SourceHourStatus.DOWNLOADED,
        SourceHourStatus.DOWNLOADED,
    )

    assert current is SourceHourStatus.DOWNLOADED
    assert target is SourceHourStatus.DOWNLOADED


def test_diagnostic_text_redacts_common_credentials() -> None:
    explicit_secret = "-".join(
        (
            "private",
            "application",
            "credential",
            "value",
        )
    )

    raw = (
        "GET https://example.test/file?"
        "file=archive&api_key=query-secret "
        "Authorization: Bearer bearer-secret "
        "api_key=assignment-secret "
        "postgresql://user:database-secret@127.0.0.1/db "
        f"configured={explicit_secret}"
    )

    result = bounded_diagnostic_text(
        raw,
        secrets=(explicit_secret,),
    )

    assert result is not None
    assert "query-secret" not in result
    assert "bearer-secret" not in result
    assert "assignment-secret" not in result
    assert "database-secret" not in result
    assert explicit_secret not in result
    assert "***" in result


def test_diagnostic_text_is_single_line_and_bounded() -> None:
    result = bounded_diagnostic_text(
        "first line\nsecond line " + ("x" * 100),
        maximum_length=40,
    )

    assert result is not None
    assert "\n" not in result
    assert len(result) <= 40
    assert result.endswith("...")


def test_blank_diagnostic_text_becomes_none() -> None:
    assert bounded_diagnostic_text("") is None
    assert bounded_diagnostic_text(" \r\n\t ") is None


def test_source_hour_lock_key_is_deterministic() -> None:
    first = source_hour_lock_keys(_spec())
    second = source_hour_lock_keys(_spec())

    assert first == second
    assert len(first) == 2

    for value in first:
        assert -(2**31) <= value <= (2**31) - 1


def test_different_source_identities_have_different_lock_keys() -> None:
    identities = {
        source_hour_lock_keys(_spec()),
        source_hour_lock_keys(_spec(symbol="ETHUSDT")),
        source_hour_lock_keys(_spec(data_kind="trades")),
    }

    assert len(identities) == 3


def test_nonnegative_counter_rejects_bool_and_negative() -> None:
    assert nonnegative_counter("count", 0) == 0
    assert nonnegative_counter("count", 3) == 3

    with pytest.raises(ValueError, match="integer"):
        nonnegative_counter("count", True)

    with pytest.raises(ValueError, match="non-negative"):
        nonnegative_counter("count", -1)


def test_fetch_run_enum_values_match_database_contract() -> None:
    assert {item.value for item in FetchRunKind} == {
        "manual",
        "automatic",
        "backfill",
        "validation",
    }

    assert {item.value for item in FetchRunStatus} == {
        "running",
        "ok",
        "partial_ok",
        "error",
        "stopped",
    }
