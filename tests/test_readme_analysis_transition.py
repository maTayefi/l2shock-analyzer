# tests/test_readme_analysis_transition.py
"""Focused contracts for the first Analysis README reconciliation batch."""

from __future__ import annotations

from pathlib import Path

from l2shock.analysis.l2_view_metrics import L2_VIEW_METRIC_SPECS

_ROOT = Path(__file__).resolve().parents[1]


def _readme() -> str:
    return (_ROOT / "README.md").read_text(encoding="utf-8")


def _section(text: str, start: str, end: str) -> str:
    assert text.count(start) == 1, start
    assert text.count(end) == 1, end

    begin = text.index(start)
    finish = text.index(end, begin + len(start))
    return text[begin:finish]


def test_readme_introduction_describes_detector_free_analysis() -> None:
    introduction = _readme().split("## Project identity", 1)[0]

    assert "detector-free Analysis" in introduction
    assert "exactly five synchronized panels" in introduction
    assert "retrospectively locating Shock-Start B areas" not in introduction


def test_readme_price_source_is_fixed_but_availability_is_optional() -> None:
    scope = _section(
        _readme(),
        "## Supported initial scope",
        "## Core data flow",
    )

    assert "Approved price source: Binance USD-M USDT perpetual trades." in scope
    assert "Price availability is optional for Analysis" in scope
    assert "Mandatory price source" not in scope


def test_readme_core_flow_places_component_storage_before_composition() -> None:
    flow = _section(
        _readme(),
        "## Core data flow",
        "## Detector-free Analysis workflow",
    )

    storage = flow.index("independent compact component-market PostgreSQL blocks")
    composition = flow.index("exact-second expected-market composition")

    assert storage < composition
    assert "Shock-Start detection, B-area review, and visualization" not in flow
    assert "Invalid one-second samples remain explicit" in flow


def test_readme_documents_every_current_selectable_metric() -> None:
    workflow = _section(
        _readme(),
        "## Detector-free Analysis workflow",
        "## Default remote preprocessing profile",
    )

    assert len(L2_VIEW_METRIC_SPECS) == 11

    for spec in L2_VIEW_METRIC_SPECS.values():
        assert f"| {spec.label} |" in workflow, spec.label

    assert "Panel A defaults to Signed Imbalance %" in workflow
    assert "Panel B defaults to Order-Book Delta" in workflow
    assert "Undefined denominators produce null values, not zero" in workflow
    assert "No price-matched liquidity-flow mode" in workflow


def test_readme_documents_presentation_and_export_ownership() -> None:
    workflow = _section(
        _readme(),
        "## Detector-free Analysis workflow",
        "## Default remote preprocessing profile",
    )

    assert "without a database reread" in workflow
    assert "blur or Enter" in workflow
    assert "Shift+wheel" in workflow
    assert "full displayed-bar" in workflow
    assert "not only the current browser zoom window" in workflow


def test_readme_active_aggregate_policy_renders_available_values() -> None:
    aggregates = _section(
        _readme(),
        "### Binance, Bybit, and OKX aggregate-liquidity identity",
        "## Architecture Contract for Humans and AI Reviewers",
    )

    assert "partial-market" in aggregates
    assert "do not abort the complete Analysis request" in aggregates
    assert (
        "Partial-market sums are plotted with explicit quality warnings" in aggregates
    )
    assert "at least one numerical L2 second" in aggregates
    assert "provenance verification" in aggregates
    assert "never plotted as partial sums" not in aggregates
    assert "loading fails with a market-coverage error" not in aggregates


def test_readme_runtime_retains_thread_and_shutdown_ownership() -> None:
    runtime = _section(
        _readme(),
        "### Application-owned Analysis runtime",
        "### Timezone contract",
    )

    assert "`l2shock/ui/l2_view_runtime.py`" in runtime
    assert "`manual_analysis_load`" in runtime
    assert "including under repeated cancellation" in runtime
    assert "does not terminate" in runtime
    assert "the engine is not disposed" in runtime
    assert "`l2shock/ui/shock_runtime.py`" not in runtime


def test_readme_timezone_contract_has_independent_owner() -> None:
    timezone = _section(
        _readme(),
        "### Timezone contract",
        "### Preset identity",
    )

    assert "`l2shock/ui/display_timezone.py`" in timezone
    assert "UTC remains authoritative" in timezone
    assert "Ambiguous or nonexistent DST local times must be rejected" in timezone
    assert "Review-table timestamps" not in timezone
