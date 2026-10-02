# tests/test_readme_analysis_doc02.py
"""Contracts for active Analysis loading, viewing, and warning documentation."""

from __future__ import annotations

from pathlib import Path

from l2shock.analysis.l2_view_stream import (
    DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB,
    DEFAULT_L2_VIEW_MAX_BARS,
    DEFAULT_MAX_ANALYSIS_DURATION_SECONDS,
    HARD_MAX_ANALYSIS_DURATION_SECONDS,
    L2_VIEW_TIMEFRAMES_SECONDS,
    MAX_ANALYSIS_MEMORY_BUDGET_MIB,
    MAX_L2_VIEW_BARS,
    MIN_ANALYSIS_MEMORY_BUDGET_MIB,
    MIN_MAX_ANALYSIS_DURATION_SECONDS,
)

_ROOT = Path(__file__).resolve().parents[1]


def _readme() -> str:
    return (_ROOT / "README.md").read_text(encoding="utf-8")


def _section(start: str, end: str) -> str:
    text = _readme()
    assert text.count(start) == 1, start
    assert text.count(end) == 1, end

    begin = text.index(start)
    finish = text.index(end, begin + len(start))
    return text[begin:finish]


def test_core_flow_has_no_stray_fence_after_price_paragraph() -> None:
    flow = _section(
        "## Core data flow",
        "## Detector-free Analysis workflow",
    )

    tail = flow.split(
        "Binance real-trade price is independently constructed and stored.",
        1,
    )[1]

    assert "```" not in tail


def test_active_viewing_timeframes_match_documented_list() -> None:
    viewing = _section(
        "#### Detector-free Analysis viewing bars",
        "#### Duration and streaming-budget controls",
    )

    expected = (
        (1, "1s"),
        (5, "5s"),
        (15, "15s"),
        (60, "1m"),
        (300, "5m"),
        (900, "15m"),
        (3600, "1h"),
        (14400, "4h"),
        (86400, "1d"),
    )

    assert tuple(seconds for seconds, _label in expected) == (
        L2_VIEW_TIMEFRAMES_SECONDS
    )

    for _seconds, label in expected:
        assert f"\n{label}\n" in viewing

    assert MAX_L2_VIEW_BARS == 5000
    assert DEFAULT_L2_VIEW_MAX_BARS == 1200
    assert "1-5000" in viewing
    assert "default\nof 1200" in viewing
    assert "never silently coarsened" in viewing
    assert "never truncates the requested source interval" in viewing


def test_duration_and_memory_constants_match_documented_limits() -> None:
    budgets = _section(
        "#### Duration and streaming-budget controls",
        "#### Retained aggregation foundation",
    )

    assert DEFAULT_MAX_ANALYSIS_DURATION_SECONDS == 86400
    assert MIN_MAX_ANALYSIS_DURATION_SECONDS == 3600
    assert HARD_MAX_ANALYSIS_DURATION_SECONDS == 63072000
    assert DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB == 512
    assert MIN_ANALYSIS_MEMORY_BUDGET_MIB == 64
    assert MAX_ANALYSIS_MEMORY_BUDGET_MIB == 16384

    for value in (
        DEFAULT_MAX_ANALYSIS_DURATION_SECONDS,
        MIN_MAX_ANALYSIS_DURATION_SECONDS,
        HARD_MAX_ANALYSIS_DURATION_SECONDS,
    ):
        assert f"{value:,} seconds" in budgets

    for value in (
        DEFAULT_ANALYSIS_MEMORY_BUDGET_MIB,
        MIN_ANALYSIS_MEMORY_BUDGET_MIB,
        MAX_ANALYSIS_MEMORY_BUDGET_MIB,
    ):
        assert f"{value:,} MiB" in budgets

    assert "not a guaranteed total-process RAM ceiling" in budgets
    assert "not to the length of every" in budgets
    assert "Both endpoint seconds count" in budgets


def test_foundation_is_not_documented_as_active_strict_viewing_policy() -> None:
    foundation = _section(
        "#### Retained aggregation foundation",
        "### Verified detector-free Analysis loading",
    )

    assert "l2shock/analysis/timeframes.py" in foundation
    assert "l2shock/analysis/aggregation.py" in foundation
    assert "final one-second\nendpoint state" in foundation
    assert "differs intentionally" in foundation
    assert "Calendar month timeframes" in foundation


def test_verified_loading_renders_partial_coverage_with_warnings() -> None:
    loading = _section(
        "### Verified detector-free Analysis loading",
        "### Price source identity",
    )

    assert "verify_codec=True" in loading
    assert "partial-market second; exact available-market sum rendered" in loading
    assert "does not reject the complete Analysis request" in loading
    assert "diagnostics and warning regions" in loading
    assert "no separately persisted\naggregate L2 rows" in loading
    assert "does not publish a partial replacement" in loading

    assert "### Verified Shock-Start L2 dataset" not in _readme()
    assert "l2shock/analysis/shock_dataset.py" not in loading


def test_warnings_document_current_range_and_failure_policy() -> None:
    warnings = _section(
        "### Data-quality states",
        "### Application-owned Analysis runtime",
    )

    assert "effective requested one-second range" in warnings
    assert "strictly more than" in warnings
    assert "across hour and chunk boundaries" in warnings
    assert "does not use the retired bounded viewport" in warnings
    assert "A run exactly equal to the threshold is not flagged" in warnings
    assert "without rereading" in warnings
    assert "returns no price-outage regions for that load" in warnings
    assert "must not\nfabricate a no-trades conclusion" in warnings

    assert "A Shock-Start hypothesis:" not in warnings


def test_processing_loading_regression_has_no_detector_dependency() -> None:
    source = (_ROOT / "tests/test_processing_l2_coordinator_postgresql.py").read_text(
        encoding="utf-8"
    )

    assert "stream_l2_view(" in source
    assert "open_repositories=open_analysis_repositories" in source
    assert "l2shock.analysis.shock_dataset" not in source
    assert "l2shock.analysis.shock_execution" not in source
    assert "run_verified_shock_scan" not in source
