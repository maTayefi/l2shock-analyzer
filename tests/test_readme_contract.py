# tests/test_readme_contract.py
"""README contract must agree with the code it documents (Batch 39)."""

from __future__ import annotations

import re
from pathlib import Path

from l2shock.analysis.shock_review import (
    SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION,
    SHOCK_REVIEW_ORDER_VERSION,
    SHOCK_REVIEW_ORDER_VERSION_V3,
)

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")

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
)


def test_readme_states_both_order_defaults_exactly() -> None:
    assert f"UI default        {SHOCK_REVIEW_DEFAULT_UI_ORDER_VERSION}" in README
    assert f"backend default   {SHOCK_REVIEW_ORDER_VERSION}" in README
    assert (
        f"`{SHOCK_REVIEW_ORDER_VERSION_V3}` (`SHOCK_REVIEW_ORDER_VERSION_V3`)" in README
    )
    assert "A weighted or percentile score is a future order version" not in README


def test_readme_annotation_switch_labels_match_the_ui() -> None:
    tab = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    for label in ("Show B/C lines and rank labels", "Show B-area bands"):
        assert f'"{label}"' in tab
        assert f'"{label}"' in README


def test_readme_describes_header_shutdown_and_shock_stop() -> None:
    assert "global header Shutdown button" in README
    assert "-> stop Shock-Start review" in README
    assert "-> stop Manual Analysis" not in README
    assert "l2shock/ui/shutdown.py" in README
    assert (ROOT / "l2shock/ui/shutdown_control.py").exists()


def test_readme_intro_and_runtime_no_longer_describe_lm() -> None:
    assert "multi-scale Liquidity Movements" not in README
    assert "Liquidity Movement detection, and population ranking" not in README
    assert "retrospectively locating Shock-Start B areas" in README


def test_readme_has_no_retired_lm_sections() -> None:
    for heading in _RETIRED_HEADINGS:
        pattern = rf"^## {re.escape(heading)}\s*$"
        assert not re.search(pattern, README, flags=re.MULTILINE), heading


def test_readme_keeps_the_shock_start_contract() -> None:
    assert "### Shock-Start semantic contract (humans and AI models)" in README
    assert "### Shock-Start review chart publication" in README


_EXCLUSION_SENTENCE = (
    "Not part of Shock-Start (do not reintroduce silently): LM retracement\n"
    "confirmation, context bars, terminal_offline, Top-N height/sharpness union,\n"
    "price-filter eligibility, Bollinger/CWT/EMD/EVT/ML detectors."
)

_BATCH41_RETIRED_SUBSECTIONS = (
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
    "Analysis identity and exports",
)


def test_readme_has_no_lm_wording_outside_the_exclusion_list() -> None:
    # The one intentional mention lists what must NOT be reintroduced.
    assert README.count(_EXCLUSION_SENTENCE) == 1
    rest = README.replace(_EXCLUSION_SENTENCE, "")

    assert re.search(r"\bLM\b", rest) is None
    assert re.search(r"Liquidity[ -]Movement", rest, re.IGNORECASE) is None
    assert re.search(r"price[ -]filter", rest, re.IGNORECASE) is None


def test_readme_retired_lm_subsections_are_gone() -> None:
    for heading in _BATCH41_RETIRED_SUBSECTIONS:
        pattern = rf"^#{{2,4}} {re.escape(heading)}\s*$"
        assert not re.search(pattern, README, flags=re.MULTILINE), heading


def test_readme_documents_shock_dataset_runtime_and_identity() -> None:
    assert "### Verified Shock-Start L2 dataset" in README
    assert "### Application-owned Shock-Start runtime" in README
    assert "### Shock-Start identity and exports" in README
    assert "manual_shock_review" in README
    assert "observations, detecting\nretrospectively" not in README


def test_readme_documents_implemented_warning_regions_and_local_time() -> None:
    from pathlib import Path

    readme = Path(__file__).resolve().parents[1] / "README.md"
    flat = " ".join(readme.read_text(encoding="utf-8").split())

    for marker in (
        "Persistent red data-outage warning regions are drawn",
        "analysis.l2_long_invalid_warning_seconds (default 60)",
        "analysis.price_long_invalid_warning_minutes (default 3)",
        '"Show data-quality warnings" switch',
        'legend entry "Data-outage warning"',
        "Only presentation is localized",
        "chart coordinates stay UTC",
    ):
        assert marker in flat, marker

    assert "A persistent red warning region is required when" not in flat
    assert flat.count("```") % 2 == 0
