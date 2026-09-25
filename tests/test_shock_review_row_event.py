# tests/test_shock_review_row_event.py
from __future__ import annotations

from types import SimpleNamespace

from l2shock.ui.tab_shock_review import _event_row

_ROW = {
    "id": "review-example:3",
    "inspection_position": 3,
    "direction": "up",
}


def test_quasar_row_click_extracts_second_argument() -> None:
    event = SimpleNamespace(
        args=[
            {"type": "click", "button": 0},
            _ROW,
            2,
        ]
    )

    assert _event_row(event) is _ROW


def test_wrapped_row_event_still_works() -> None:
    event = SimpleNamespace(args={"row": _ROW})

    assert _event_row(event) is _ROW


def test_wrapped_row_in_argument_list_still_works() -> None:
    event = SimpleNamespace(args=[{"row": _ROW}])

    assert _event_row(event) is _ROW


def test_browser_event_is_not_mistaken_for_result_row() -> None:
    event = SimpleNamespace(
        args=[
            {"type": "click"},
            {"unrelated": "value"},
            2,
        ]
    )

    assert _event_row(event) is None
