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
