# tests/test_shock_legacy_path_removed.py
from __future__ import annotations

from pathlib import Path

import l2shock.ui.shock_inspection as shock_inspection

ROOT = Path(__file__).resolve().parents[1]


def test_one_second_legacy_publication_path_is_gone() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert "_select_row_one_second_legacy" not in source
    assert "publish_shock_selection" not in source
    assert not hasattr(shock_inspection, "publish_shock_selection")
    assert "publish_shock_selection" not in shock_inspection.__all__


def test_row_selection_still_uses_bounded_viewport() -> None:
    source = (ROOT / "l2shock/ui/tab_shock_review.py").read_text(encoding="utf-8")

    assert 'table.on("rowClick", _select_row)' in source
    assert "await _show_bounded_view()" in source