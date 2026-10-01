# tests/test_finalize_analysis_transition.py
"""Documentation-preservation contracts for Analysis finalization."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools/finalize_analysis_transition.py"


def _load_tool():
    name = "_l2shock_finalize_analysis_transition_test"
    existing = sys.modules.get(name)

    if existing is not None:
        return existing

    spec = importlib.util.spec_from_file_location(name, TOOL_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module

    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise

    return module


def _current_cores() -> str:
    return (
        (ROOT / "project_cores.md")
        .read_text(
            encoding="utf-8",
        )
        .replace("\r\n", "\n")
    )


def _between(text: str, start: str, end: str) -> str:
    assert text.count(start) == 1, start
    assert text.count(end) == 1, end

    begin = text.index(start)
    finish = text.index(end, begin + len(start))
    return text[begin:finish]


def _core(text: str, number: int) -> str:
    starts = list(
        re.finditer(
            rf"^# Core {number} .*$",
            text,
            flags=re.MULTILINE,
        )
    )
    assert len(starts) == 1

    start = starts[0].start()

    if number < 7:
        endings = list(
            re.finditer(
                rf"^# Core {number + 1} .*$",
                text,
                flags=re.MULTILINE,
            )
        )
    else:
        endings = list(
            re.finditer(
                r"^# Ranked single-core bug-finding rounds[ \t]*$",
                text,
                flags=re.MULTILINE,
            )
        )

    assert len(endings) == 1
    assert endings[0].start() > start
    return text[start : endings[0].start()]


def _retired_paths(module) -> set[str]:
    return set(module._production_manifest()) | set(module.RETIRED_TEST_PATHS)


def _first_inventory(text: str, heading: str) -> list[str]:
    heading_match = re.search(
        rf"^{re.escape(heading)}[ \t]*$",
        text,
        flags=re.MULTILINE,
    )
    assert heading_match is not None

    block = re.search(
        r"```text\n(.*?)\n```",
        text[heading_match.end() :],
        flags=re.DOTALL,
    )
    assert block is not None

    return [line.strip() for line in block.group(1).splitlines() if line.strip()]


def test_context_reconciliation_is_idempotent() -> None:
    module = _load_tool()
    current = _current_cores()

    once = module._reconcile_cores_context(current)
    twice = module._reconcile_cores_context(once)

    assert twice == once


def test_unexpected_retrieval_group_fails_closed() -> None:
    module = _load_tool()

    with pytest.raises(RuntimeError, match="retrieval group"):
        module._reconcile_cores_context(
            "# Recommended per-round retrieval groupings\n"
            "\n"
            "Unexpected document shape.\n"
        )


def test_proposal_generation_does_not_write_documents() -> None:
    module = _load_tool()
    before_cores = (ROOT / "project_cores.md").read_bytes()
    before_readme = (ROOT / "README.md").read_bytes()

    proposed = module._proposed_cores(_retired_paths(module))

    assert isinstance(proposed, str)
    assert proposed
    assert (ROOT / "project_cores.md").read_bytes() == before_cores
    assert (ROOT / "README.md").read_bytes() == before_readme


def test_proposal_preserves_unrelated_core_definitions() -> None:
    module = _load_tool()
    current = _current_cores()
    proposed = module._proposed_cores(_retired_paths(module))

    for number in (1, 2, 3, 7):
        assert _core(proposed, number) == _core(current, number), number


def test_proposal_preserves_ranking_headers_and_order() -> None:
    module = _load_tool()
    current = _current_cores()
    proposed = module._proposed_cores(_retired_paths(module))

    start = "# Ranked single-core bug-finding rounds"
    end = "# Recommended bug-finding round format"

    before = _between(current, start, end)
    after = _between(proposed, start, end)

    # Preserve actual section headings and every ranked core combination.
    pattern = (
        r"(?m)^(?:#{1,3}[ \t]+.*|"
        r"[ \t]*\d+\.[ \t]+Core[ \t]+\d"
        r"(?:[ \t]*\+[ \t]*Core[ \t]+\d)*)"
    )

    before_entries = re.findall(pattern, before)
    after_entries = re.findall(pattern, after)

    # Detector-specific descriptive heading text may change, but the
    # headings in this range identify ranking groups/core combinations.
    assert after_entries == before_entries

    for marker in (
        "## Unified priority ranking",
        "- parent-commit conflicts;",
        "- workflow seed gates;",
        "- source release-delay admission;",
        "- resource cleanup after worker failure;",
    ):
        assert marker in before, marker
        assert marker in after, marker


def test_proposal_preserves_review_prompt_and_completion_criteria() -> None:
    module = _load_tool()
    current = _current_cores()
    proposed = module._proposed_cores(_retired_paths(module))

    start = "# Recommended bug-finding round format"
    end = "# Recommended per-round retrieval groupings"

    assert _between(proposed, start, end) == _between(
        current,
        start,
        end,
    )

    completion = "# Completion criteria for a bug-finding round"
    assert current[current.index(completion) :] == (
        proposed[proposed.index(completion) :]
    )


def test_proposal_reconciles_retrieval_groups() -> None:
    module = _load_tool()
    proposed = module._proposed_cores(_retired_paths(module))

    retrieval = _between(
        proposed,
        "# Recommended per-round retrieval groupings",
        "# Completion criteria for a bug-finding round",
    )

    assert "bounded-chunk Analysis loading" in retrieval
    assert "independent timezone presentation" in retrieval
    assert "Shift+wheel Y zoom" in retrieval

    for marker in (
        "shock dataset/window",
        "Shock-Start",
        "B-area review",
        "v2/v3 ordering",
        "Shock review tab",
        "shock runtime/diagnostic CLI",
    ):
        assert marker not in retrieval, marker


def test_proposal_inventories_match_planned_retained_files() -> None:
    module = _load_tool()
    retired = _retired_paths(module)
    proposed = module._proposed_cores(retired)

    for root_name, heading in (
        ("l2shock", "# Global production-module inventory"),
        ("tests", "# Global test inventory"),
    ):
        expected = [
            relative
            for relative in module._python_paths(root_name)
            if relative not in retired
        ]
        assert _first_inventory(proposed, heading) == expected


def test_proposal_has_no_retired_paths_or_detector_context() -> None:
    module = _load_tool()
    retired = _retired_paths(module)
    proposed = module._proposed_cores(retired)

    listed_paths = set(
        re.findall(
            r"(?m)^((?:l2shock|tests|tools)/[A-Za-z0-9_./-]+\.py)$",
            proposed,
        )
    )

    assert listed_paths.isdisjoint(retired)
    assert all((ROOT / relative).is_file() for relative in listed_paths)
    assert "Shock-Start" not in proposed
    assert re.search(r"\bLM\b", proposed) is None
    assert "B-area review" not in proposed
    assert "deterministic v2/v3 ordering" not in proposed
    assert proposed.count("```") % 2 == 0
