# tests/test_readme_contract.py
"""README contracts for the retained detector-free Analysis architecture."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_RETIRED_HEADINGS = (
    "Price-filter semantics",
    "Segmented filtered timeline",
    "Price-filter context",
    "Liquidity Movement definition",
    "Segment-scoped LM candidate detection",
    "Fundamental LM measurements",
    "Ranking populations",
    "Primary LM evidence",
    "Secondary LM evidence",
    "LM analysis execution and deterministic result identity",
    "Application-owned analysis runtime",
    "Functional Analysis controls and result table",
    "Synchronized Analysis chart workspace",
    "Chart publication and interaction ownership",
    "Selected-LM emphasis and Analysis export",
    "Explicit chart-timeframe rebuilding",
    "Multi-timeframe behavior",
    "Highlight behavior",
    "Verified aligned analysis datasets",
    "Verified Shock-Start L2 dataset",
    "Application-owned Shock-Start runtime",
    "Shock-Start identity and exports",
    "Shock-Start semantic contract (humans and AI models)",
    "Shock-Start review chart publication",
)


def _readme() -> str:
    return (ROOT / "README.md").read_text(encoding="utf-8")


def _flat() -> str:
    return " ".join(_readme().split())


def test_readme_has_no_retired_detector_sections() -> None:
    text = _readme()

    for heading in _RETIRED_HEADINGS:
        pattern = rf"^#{{2,4}} {re.escape(heading)}\s*$"
        assert not re.search(pattern, text, flags=re.MULTILINE), heading


def test_readme_describes_detector_free_analysis_not_review() -> None:
    text = _readme()
    introduction = text.split("## Project identity", 1)[0]

    assert "detector-free Analysis" in introduction
    assert "exactly five synchronized panels" in introduction
    assert "retrospectively locating Shock-Start B areas" not in text
    assert "Shock-Start is the sole Analysis workflow" not in text
    assert "manual_shock_review" not in text
    assert "SHOCK_REVIEW_ORDER_VERSION" not in text
    assert "SHOCK_CHART_COLORS" not in text

    assert "### Detector-free Analysis semantic contract" in text
    assert "### Analysis chart publication and interaction ownership" in text
    assert "### Analysis identity and exports" in text


def test_readme_has_no_retired_detector_controls() -> None:
    text = _readme()

    for label in (
        "Show B/C lines and rank labels",
        "Show B-area bands",
        "within_tier_percentile_mean_v3",
        "total_structure_first_v2",
    ):
        assert label not in text, label


def test_readme_keeps_header_shutdown_and_analysis_runtime() -> None:
    text = _readme()

    assert "global header Shutdown button" in text
    assert "l2shock/ui/shutdown.py" in text
    assert "l2shock/ui/l2_view_runtime.py" in text
    assert "manual_analysis_load" in text
    assert (ROOT / "l2shock/ui/shutdown_control.py").is_file()

    assert "-> stop Shock-Start review" not in text


def test_readme_keeps_warning_toggle_and_utc_ownership() -> None:
    flat = _flat()

    for marker in (
        '"Show data-quality warnings" switch',
        "analysis.l2_long_invalid_warning_seconds",
        "analysis.price_long_invalid_warning_minutes",
        "A run exactly equal to the threshold is not flagged",
        "UTC remains authoritative",
        "Shift+wheel",
        "chart coordinates stay UTC",
    ):
        assert marker in flat, marker


def test_readme_keeps_retired_lm_functionality_absent() -> None:
    text = _readme()

    assert "multi-scale Liquidity Movements" not in text
    assert "Liquidity Movement detection, and population ranking" not in text
    assert re.search(r"\bLM\b", text) is None
    assert re.search(r"Liquidity[ -]Movement", text, re.IGNORECASE) is None
    assert re.search(r"price[ -]filter", text, re.IGNORECASE) is None


def test_readme_does_not_promise_browser_rollback() -> None:
    flat = _flat()

    assert "automatic restoration of the prior browser option is not guaranteed" in flat
    assert (
        "successful Python publication request is not browser acknowledgement" in flat
    )


def test_readme_markdown_fences_are_balanced() -> None:
    assert _readme().count("```") % 2 == 0
