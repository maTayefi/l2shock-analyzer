"""Preview or apply the coordinated detector-free Analysis finalization.

Default execution is read-only.

Explicit --apply:
- backs up affected files;
- updates project_cores.md;
- removes the agreed retired source/test files;
- verifies architecture/document contracts and full test collection;
- restores affected files if a verification command fails.

This is a developer refactoring tool, not application data maintenance.
It never edits PostgreSQL, raw archives, checkpoints, or analytical rows.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import itertools
import re
import runpy
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RETIRED_TEST_PATHS = (
    "tests/test_batch2_contracts.py",
    "tests/test_robust_stats.py",
    "tests/test_shock_analysis_handoff.py",
    "tests/test_shock_annotation_visibility_and_header_shutdown.py",
    "tests/test_shock_chart_options.py",
    "tests/test_shock_dataset.py",
    "tests/test_shock_dataset_view.py",
    "tests/test_shock_diagnostic_cli.py",
    "tests/test_shock_evidence.py",
    "tests/test_shock_legacy_path_removed.py",
    "tests/test_shock_legend.py",
    "tests/test_shock_lm_independence.py",
    "tests/test_shock_order_v3.py",
    "tests/test_shock_performance_equivalence.py",
    "tests/test_shock_price_context_ui.py",
    "tests/test_shock_review.py",
    "tests/test_shock_review_row_event.py",
    "tests/test_shock_runtime.py",
    "tests/test_shock_scale_controls.py",
    "tests/test_shock_start.py",
    "tests/test_shock_top_n_and_quality.py",
    "tests/test_shock_ui_localization.py",
    "tests/test_shock_view_bars.py",
    "tests/test_shock_view_chart_options.py",
    "tests/test_shock_view_selection.py",
    "tests/test_shock_warning_regions.py",
    "tests/test_tab_shock_review.py",
)

REQUIRED_RETAINED_PATHS = (
    "l2shock/analysis/aggregation.py",
    "l2shock/analysis/l2_seconds.py",
    "l2shock/analysis/l2_view_metrics.py",
    "l2shock/analysis/l2_view_stream.py",
    "l2shock/analysis/multi_market.py",
    "l2shock/analysis/timeframes.py",
    "l2shock/ui/analysis_inputs.py",
    "l2shock/ui/chart_interactions.py",
    "l2shock/ui/chart_navigation.py",
    "l2shock/ui/display_timezone.py",
    "l2shock/ui/echarts.py",
    "l2shock/ui/l2_view_chart_options.py",
    "l2shock/ui/l2_view_presentation.py",
    "l2shock/ui/l2_view_runtime.py",
    "l2shock/ui/l2_view_warning_style.py",
    "l2shock/ui/shutdown.py",
    "l2shock/ui/shutdown_control.py",
    "l2shock/ui/tab_l2_view.py",
    "tests/test_analysis_inputs_independence.py",
    "tests/test_chart_publication_preflight.py",
    "tests/test_detector_backend_deleted.py",
    "tests/test_header_shutdown_and_timezone.py",
    "tests/test_l2_ohlc_aggregation.py",
    "tests/test_l2_view_batch2.py",
    "tests/test_l2_view_batch3a.py",
    "tests/test_l2_view_batch3b.py",
    "tests/test_l2_view_batch3e.py",
    "tests/test_l2_view_stream.py",
    "tests/test_lm_backend_deleted.py",
    "tests/test_migration_freeze.py",
    "tests/test_processing_l2_coordinator_postgresql.py",
    "tests/test_project_cores_contract.py",
    "tests/test_project_cores_inventory.py",
    "tests/test_readme_contract.py",
    "tests/test_readme_analysis_transition.py",
    "tests/test_readme_analysis_doc02.py",
    "tests/test_ui_localization.py",
    "tests/test_ui_shutdown.py",
)

CORE4 = """# Core 4 — Verified detector-free Analysis loading and viewing mathematics

## Primary modules

```text
l2shock/analysis/__init__.py
l2shock/analysis/aggregation.py
l2shock/analysis/l2_seconds.py
l2shock/analysis/l2_view_metrics.py
l2shock/analysis/l2_view_stream.py
l2shock/analysis/multi_market.py
l2shock/analysis/timeframes.py
```

## Important dependencies

```text
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/db/engine.py
l2shock/presets/identity.py
l2shock/liquidity/block_codec.py
l2shock/price/block_codec.py
l2shock/ingest/sampling.py
l2shock/timeutils.py
l2shock/config.py
```

## Primary tests

```text
tests/test_analysis_multi_market.py
tests/test_l2_ohlc_aggregation.py
tests/test_l2_view_stream.py
tests/test_l2_view_batch2.py
tests/test_l2_view_batch3a.py
tests/test_l2_view_batch3e.py
tests/test_lm_backend_deleted.py
tests/test_detector_backend_deleted.py
tests/test_multi_market_aggregation.py
tests/test_timeframe_aggregation.py
tests/test_processing_l2_coordinator_postgresql.py
```

## Focus areas

1. Verified row, codec, preset, and source-provenance ownership.
2. Bounded reads without retaining the full one-second range.
3. Exact expected-market composition at each UTC second.
4. Missing, invalid, and partial-market seconds remain explicit gaps.
5. Strict viewing bars versus the retained endpoint aggregation foundation.
6. Total and Delta candles use same-second values, not separate extrema.
7. Percentage candles and undefined denominator ownership.
8. Adjacent usable viewing-bar closes own change metrics.
9. Closed request endpoints, partial edge bars, and UTC alignment.
10. Explicit timeframe rejection and finest-fitting Auto selection.
11. Cached coarsening preserves gaps, percentages, identity, and counts.
12. Optional price failure does not block independently usable L2.
13. Outage runs cross hour/chunk boundaries and use strict thresholds.
14. Duration limits, memory-budget heuristics, and cancellation boundaries.
15. Deterministic loaded input identity and complete displayed-bar exports.

---

"""

CORE5 = """# Core 5 — NiceGUI, Analysis charts, exports, controls, and workflows

## Primary modules

```text
l2shock/diagnostics.py
l2shock/main.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/ui/__init__.py
l2shock/ui/analysis_inputs.py
l2shock/ui/app.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/chart_interactions.py
l2shock/ui/chart_navigation.py
l2shock/ui/components.py
l2shock/ui/display_timezone.py
l2shock/ui/echarts.py
l2shock/ui/fetch_runtime.py
l2shock/ui/l2_view_chart_options.py
l2shock/ui/l2_view_presentation.py
l2shock/ui/l2_view_runtime.py
l2shock/ui/l2_view_warning_style.py
l2shock/ui/processing_runtime.py
l2shock/ui/remote_import_runtime.py
l2shock/ui/shutdown.py
l2shock/ui/shutdown_control.py
l2shock/ui/state.py
l2shock/ui/tab_fetch.py
l2shock/ui/tab_l2_view.py
l2shock/ui/tab_settings.py
```

## Important dependencies

```text
l2shock/config.py
l2shock/timeutils.py
l2shock/logging_setup.py
l2shock/db/engine.py
l2shock/filesystem.py
l2shock/analysis/l2_view_stream.py
l2shock/analysis/l2_view_metrics.py
```

## Primary tests

```text
tests/test_analysis_inputs_independence.py
tests/test_availability_calendar.py
tests/test_chart_interactions.py
tests/test_chart_publication_preflight.py
tests/test_diagnostics.py
tests/test_echarts_live_state.py
tests/test_echarts_publication.py
tests/test_header_shutdown_and_timezone.py
tests/test_l2_view_batch2.py
tests/test_l2_view_batch3a.py
tests/test_l2_view_batch3b.py
tests/test_l2_view_batch3e.py
tests/test_maintenance_actions.py
tests/test_maintenance_diagnostics.py
tests/test_remote_import_runtime.py
tests/test_tab_settings_lock_contract.py
tests/test_ui_fetch_runtime.py
tests/test_ui_foundation.py
tests/test_ui_localization.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
```

## Focus areas

1. Exactly five panels and the current eleven-metric registry.
2. Presentation-only metric/warning changes use displayed bars.
3. Timeframe/bar-budget changes use cache or a cancellable reload.
4. Maximum viewing bars commits on blur or Enter.
5. Synchronized X zoom and independent hovered-panel Shift+wheel Y zoom.
6. Metric changes reset only the changed panel's Y zoom.
7. Timeframe changes restore the UTC left edge and visible duration.
8. Strict JSON preflight before withdrawing old publication ownership.
9. Complete options, render-token acknowledgement, and stale publication.
10. Per-chart crosshair generations and latest-pointer frame coalescing.
11. PNG/SVG export from acknowledged ownership.
12. JSON/CSV export of the full displayed-bar dataset.
13. Localized inputs, labels, pointers, and tooltips retain UTC data.
14. Optional price status and warning-region truncation are truthful.
15. Calendar refresh races and closed-endpoint Analysis handoff.
16. Remote/local workflow visibility and shared operation admission.
17. Global header Shutdown confirmation, failure reporting, and retry.
18. Settings maintenance remains explicitly authorized and fail-closed.
19. Client disconnects, secret safety, and publication error clarity.

---

"""

CORE6 = """# Core 6 — Cross-cutting contracts, concurrency, cancellation, integration

## Primary scope

Core 6 may inspect every file in the global inventories. All tests are
valid for Core 6. Its purpose is to establish failures crossing subsystem
boundaries.

## Primary modules

```text
l2shock/ui/state.py
l2shock/ui/shutdown.py
l2shock/ui/shutdown_control.py
l2shock/ui/components.py
l2shock/ui/fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/l2_view_runtime.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/remote_import_runtime.py
l2shock/processing/checkpoint_references.py
l2shock/db/checkpoint_reference_locks.py
l2shock/acquisition/locks.py
l2shock/db/engine.py
l2shock/filesystem.py
l2shock/main.py
```

## Important dependencies

```text
l2shock/config.py
l2shock/timeutils.py
l2shock/logging_setup.py
l2shock/acquisition/coordinator.py
l2shock/acquisition/downloader.py
l2shock/processing/l2_coordinator.py
l2shock/processing/price_coordinator.py
l2shock/remote/hf_repository.py
l2shock/remote/importer.py
l2shock/remote_worker.py
l2shock/db/models.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/presets/identity.py
```

## Workflow files

```text
.github/workflows/remote-hourly-processing.yml
.github/workflows/bybit-contract-diagnostics.yml
```

## Primary tests

```text
tests/test_checkpoint_chain_contract.py
tests/test_checkpoint_reference_lock_contract.py
tests/test_checkpoint_reference_locks_postgresql.py
tests/test_compile_all_python.py
tests/test_detector_backend_deleted.py
tests/test_header_shutdown_and_timezone.py
tests/test_l2_view_batch3b.py
tests/test_lm_backend_deleted.py
tests/test_migration_freeze.py
tests/test_processing_post_stream_integrity.py
tests/test_processing_worker_repeated_cancellation.py
tests/test_project_cores_contract.py
tests/test_project_cores_inventory.py
tests/test_readme_contract.py
tests/test_readme_analysis_transition.py
tests/test_readme_analysis_doc02.py
tests/test_remote_workflow_contract.py
tests/test_ui_shutdown.py
```

## Focus areas

1. UTC, immutable identity, provenance, and source ownership.
2. Local/remote processing and verified importer equivalence.
3. Admission reservations and process-lock ownership.
4. Cancellation before execution and during synchronous worker execution.
5. Repeated cancellation must not release ownership before worker exit.
6. Shutdown engine disposal requires all work owners to be resolved.
7. Registered database readers outliving cancelled UI callers.
8. Source/checkpoint advisory-lock ordering and cross-process races.
9. Filesystem publication versus database transaction commit.
10. Idempotency, conflict detection, and recovery after partial publication.
11. Failed loads retain the previous successful projection.
12. Browser publication failure is not a guaranteed rollback.
13. Availability, analytical usability, and handoff policy distinctions.
14. Singleton contamination and test-suite ordering.
15. Secret-safe errors and truthful operational diagnostics.

---

"""

CORE_LABELS = {
    1: "Database, persistence, identity, provenance, and maintenance",
    2: "Acquisition, availability, retention, and lifecycle",
    3: "Ingestion, replay, checkpoints, liquidity, and price",
    4: "Verified detector-free Analysis loading and viewing mathematics",
    5: "NiceGUI, chart publication, controls, and exports",
    6: "Cross-cutting contracts, concurrency, and cancellation",
    7: "Remote processing, artifacts, workers, and imports",
}

SINGLE_ORDER = (7, 3, 2, 1, 6, 4, 5)


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8").replace("\r\n", "\n")


def _production_manifest() -> tuple[str, ...]:
    tree = ast.parse(_read("tests/test_detector_backend_deleted.py"))

    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "RETIRED_PRODUCTION_PATHS"
            for target in node.targets
        ):
            values = ast.literal_eval(node.value)
            if (
                not isinstance(values, tuple)
                or len(values) != 21
                or any(not isinstance(value, str) for value in values)
            ):
                raise RuntimeError("Unexpected retired-production manifest")
            return values

    raise RuntimeError("Retired-production manifest was not found")


def _python_paths(root_name: str) -> list[str]:
    return sorted(
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / root_name).rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _replace_span(
    text: str,
    start_pattern: str,
    end_pattern: str,
    replacement: str,
) -> str:
    starts = list(re.finditer(start_pattern, text, flags=re.MULTILINE))
    ends = list(re.finditer(end_pattern, text, flags=re.MULTILINE))

    if len(starts) != 1 or len(ends) != 1:
        raise RuntimeError(
            f"Expected unique document boundaries: {start_pattern!r}, "
            f"{end_pattern!r}"
        )

    start, end = starts[0], ends[0]
    if end.start() <= start.start():
        raise RuntimeError("Document boundaries are reversed")

    return text[: start.start()] + replacement + text[end.start() :]


def _replace_inventory(text: str, heading: str, paths: list[str]) -> str:
    pattern = rf"(^{re.escape(heading)}\n)" r"(?P<gap>.*?)(?P<block>```text\n.*?\n```)"
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE | re.DOTALL))

    if len(matches) != 1:
        raise RuntimeError(f"Expected one inventory block after {heading!r}")

    match = matches[0]
    gap = match.group("gap")
    if re.search(r"^# ", gap, flags=re.MULTILINE):
        raise RuntimeError(f"Inventory block is not owned by {heading!r}")

    new_block = "```text\n" + "\n".join(paths) + "\n```"
    return text[: match.start("block")] + new_block + text[match.end("block") :]


def _rankings() -> str:
    lines = [
        "# Ranked single-core bug-finding rounds",
        "",
        "Retain remote-first operational review priorities:",
        "",
    ]
    for position, number in enumerate(SINGLE_ORDER, 1):
        lines.append(f"{position}. Core {number} - {CORE_LABELS[number]}")

    priority = {number: position for position, number in enumerate(SINGLE_ORDER)}
    for width, title in (
        (2, "# Ranked dual-core combinations"),
        (3, "# Ranked triple-core combinations"),
    ):
        combinations = list(itertools.combinations(range(1, 8), width))
        combinations.sort(
            key=lambda values: tuple(sorted(priority[value] for value in values))
        )
        lines.extend(
            [
                "",
                title,
                "",
                "This operational ordering favors the same remote-first core",
                "priorities; it is review guidance, not analytical semantics.",
                "",
            ]
        )
        for position, values in enumerate(combinations, 1):
            label = " + ".join(f"Core {value}" for value in values)
            scopes = "; ".join(CORE_LABELS[value] for value in values)
            lines.append(f"{position}. {label}: {scopes}.")

    lines.extend(
        [
            "",
            "# Top-priority bug-finding rounds - unified ranking",
            "",
            "1. Remote source processing, artifact verification, and publication.",
            "2. Pinned-revision imports and durable identity/provenance.",
            "3. Verified Analysis loading, gaps, viewing mathematics, and cancellation.",
            "4. Acknowledged chart publication, interaction ownership, and exports.",
            "5. General persistence, acquisition, maintenance, and UI concerns.",
            "",
            "Cross-cutting Core 6 belongs in any round involving admission,",
            "worker cancellation, shutdown, publication, or transaction recovery.",
            "",
            "---",
            "",
        ]
    )
    return "\n".join(lines)


def _reconcile_cores_context(text: str) -> str:
    """Reconcile retired context without replacing useful review guidance.

    Ranking headings, ranking order, unrelated concern lists, the base
    review prompt, and completion criteria remain intact.

    The two retrieval groups have an explicit old/new contract so an
    unexpected document shape fails before any apply operation.
    """
    replacements = (
        (
            "Shock-Start loading, detection, channel evidence, and B-area review",
            "Verified detector-free Analysis loading and viewing mathematics",
        ),
        (
            "One-second L2 semantics consumed by Shock-Start detection and review.",
            "One-second L2 semantics consumed by verified Analysis loading and viewing.",
        ),
        (
            "Scan/review identity, bounded-view rendering, annotation toggles, export,",
            "Analysis input identity, viewing-bar rendering, warning toggles, export,",
        ),
        (
            "Scan/review identity, cancellation, deterministic v2/v3 ordering, and stale data.",
            "Analysis input identity, cancellation, deterministic projections, and stale data.",
        ),
        (
            "Shock-Start runtime",
            "Analysis loading runtime",
        ),
        (
            "bounded-view/UI publication",
            "viewing-bar/UI publication",
        ),
        (
            "scan/review identity",
            "Analysis input identity",
        ),
        (
            "Shock-Start analysis",
            "detector-free Analysis",
        ),
        (
            "Shock-Start execution, identity, cancellation, and stale-review prevention",
            "Analysis loading, identity, cancellation, and stale-publication prevention",
        ),
        (
            "- endpoint-state aggregation;",
            "- retained endpoint-state aggregation versus strict active viewing bars;",
        ),
        (
            "- endpoint-state semantics;",
            "- retained endpoint-state semantics versus strict active viewing bars;",
        ),
        (
            "- price and trade counts never used as detection input;",
            "- optional price and trade counts never determine L2 bar eligibility;",
        ),
        (
            "- 24-hour scan limit and calendar-handoff clipping;",
            "- editable owned-second duration limit and calendar-handoff clipping;",
        ),
        (
            "- pivot-radius and forward-horizon boundaries;",
            "- UTC-aligned viewing bars and clipped request boundaries;",
        ),
        (
            "- adverse-move accounting;",
            "- adjacent usable-bar change metrics and predecessor ownership;",
        ),
        (
            "- deterministic extrema/tie handling;",
            "- same-second metric extrema and undefined-denominator handling;",
        ),
        (
            "- deterministic v2/v3 review ordering;",
            "- deterministic Analysis input identity and cached coarsening;",
        ),
        (
            "- whole-scan Total-range denominator leakage;",
            "- undefined denominators and strict percentage-gap propagation;",
        ),
    )

    for before, after in replacements:
        text = text.replace(before, after)

    retrieval_replacements = (
        (
            (
                "Core 4:\n"
                "    timeframe/aggregation\n"
                "    multi-market loading\n"
                "    shock dataset/window\n"
                "    Shock-Start detection/channel evidence\n"
                "    B-area review and v2/v3 ordering\n"
                "    shock runtime/diagnostic CLI"
            ),
            (
                "Core 4:\n"
                "    retained timeframe/aggregation foundation\n"
                "    verified exact-second decoding and multi-market composition\n"
                "    bounded-chunk Analysis loading and strict viewing bars\n"
                "    eleven derived metrics and undefined-denominator handling\n"
                "    cached coarsening, coverage counts, and outage boundaries\n"
                "    Analysis input identity and optional verified price"
            ),
        ),
        (
            (
                "Core 5:\n"
                "    NiceGUI state/components/app\n"
                "    Shock review tab/controls/table/export\n"
                "    bounded-view chart options/annotation visibility/legend\n"
                "    ECharts/chart interactions\n"
                "    fetch/settings tabs\n"
                "    header shutdown control/shutdown runtime"
            ),
            (
                "Core 5:\n"
                "    NiceGUI state/components/app\n"
                "    detector-free Analysis tab/controls/image and data exports\n"
                "    viewing-bar chart options/warning visibility/formula legend\n"
                "    independent timezone presentation\n"
                "    ECharts/publication acknowledgement/chart interactions\n"
                "    synchronized X viewport and independent Shift+wheel Y zoom\n"
                "    fetch/settings tabs and availability-calendar handoff\n"
                "    header shutdown control/shutdown runtime"
            ),
        ),
    )

    for before, after in retrieval_replacements:
        old_count = text.count(before)
        new_count = text.count(after)

        if old_count == 1 and new_count == 0:
            text = text.replace(before, after, 1)
        elif old_count == 0 and new_count == 1:
            # Already reconciled: repeated proposal generation is safe.
            continue
        else:
            raise RuntimeError(
                "Expected one old or one reconciled retrieval group: "
                f"{before.splitlines()[0]!r}; "
                f"old={old_count}, new={new_count}"
            )

    return text


def _proposed_cores(retired: set[str]) -> str:
    text = _read("project_cores.md")

    text = _replace_span(
        text,
        r"^# Core 4 .*$",
        r"^# Core 7 .*$",
        CORE4 + CORE5 + CORE6,
    )
    # Preserve the existing ranking order, detailed concern lists, review
    # prompt, and completion criteria. Reconcile only retired descriptions
    # and the two affected retrieval-group entries.
    text = _reconcile_cores_context(text)

    production = [
        relative for relative in _python_paths("l2shock") if relative not in retired
    ]
    tests = [relative for relative in _python_paths("tests") if relative not in retired]
    text = _replace_inventory(
        text,
        "# Global production-module inventory",
        production,
    )
    text = _replace_inventory(text, "# Global test inventory", tests)

    support_path = "tools/finalize_analysis_transition.py"
    if support_path not in text:
        support_heading = re.search(r"^# Workflow,.*$", text, flags=re.MULTILINE)
        if support_heading is None:
            raise RuntimeError("Support-file inventory heading was not found")

        block = re.search(
            r"```text\n.*?\n```",
            text[support_heading.end() :],
            flags=re.DOTALL,
        )
        if block is None:
            raise RuntimeError("Support-file inventory block was not found")

        end = support_heading.end() + block.end() - len("\n```")
        text = text[:end] + "\n" + support_path + text[end:]

    retired_context_patterns = (
        r"Shock-Start",
        r"\bLM\b",
        r"Liquidity[ -]Movement",
        r"B-area review",
        r"within-tier percentiles",
        r"deterministic v2/v3 ordering",
        r"SHOCK_REVIEW",
        r"tab_shock_review\.py",
    )
    residual_lines = [
        f"{line_number}: {line}"
        for line_number, line in enumerate(text.splitlines(), 1)
        if any(
            re.search(pattern, line, flags=re.IGNORECASE)
            for pattern in retired_context_patterns
        )
    ]

    if residual_lines:
        raise RuntimeError(
            "Unreconciled retired detector context remains in proposed "
            "project_cores.md:\n" + "\n".join(residual_lines)
        )

    if text.count("```") % 2:
        raise RuntimeError("Proposed core document has unbalanced fences")

    for relative in re.findall(
        r"(?m)^((?:l2shock|tests|tools)/[A-Za-z0-9_./-]+\.py)$",
        text,
    ):
        if relative in retired or not (ROOT / relative).is_file():
            raise RuntimeError(
                f"Proposed document lists an unavailable file: {relative}"
            )

    return text


def _imported_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parts = list(path.relative_to(ROOT).with_suffix("").parts)
    parts.pop()
    package = ".".join(parts)
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)

        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                module = importlib.util.resolve_name(
                    "." * node.level + module,
                    package,
                )
            if module:
                names.add(module)
                names.update(f"{module}.{alias.name}" for alias in node.names)

        elif isinstance(node, ast.Call) and node.args:
            function = node.func
            called = (
                function.id
                if isinstance(function, ast.Name)
                else function.attr if isinstance(function, ast.Attribute) else ""
            )
            first = node.args[0]
            if (
                called in {"__import__", "import_module"}
                and isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and not first.value.startswith(".")
            ):
                names.add(first.value)

    return names


def _preflight(retired: set[str]) -> None:
    for relative in REQUIRED_RETAINED_PATHS:
        if not (ROOT / relative).is_file():
            raise RuntimeError(f"Required retained file is missing: {relative}")

    modules = {relative.removesuffix(".py").replace("/", ".") for relative in retired}
    offenders: list[str] = []

    for root_name in ("l2shock", "tests", "tools"):
        for relative in _python_paths(root_name):
            if relative in retired:
                continue

            imported = _imported_names(ROOT / relative)
            hits = sorted(
                name
                for name in imported
                if any(
                    name == module or name.startswith(module + ".")
                    for module in modules
                )
            )
            if hits:
                offenders.append(f"{relative}: {hits}")

    if offenders:
        raise RuntimeError(
            "Retained imports still depend on planned deletions:\n"
            + "\n".join(offenders)
        )

    readme = _read("README.md")
    for heading in (
        "### Shock-Start identity and exports",
        "### Shock-Start semantic contract (humans and AI models)",
        "### Shock-Start review chart publication",
    ):
        if heading in readme:
            raise RuntimeError(f"Apply README reconciliation first: {heading}")


def _run_gate(*arguments: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *arguments],
        cwd=ROOT,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"Verification failed ({result.returncode}): pytest {' '.join(arguments)}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Explicitly back up, update documentation, and remove retired files",
    )
    arguments = parser.parse_args()

    production = _production_manifest()
    if len(RETIRED_TEST_PATHS) != 27:
        raise RuntimeError("Unexpected retired-test manifest")

    retired = set(production) | set(RETIRED_TEST_PATHS)
    _preflight(retired)
    proposed = _proposed_cores(retired)

    present = sorted(relative for relative in retired if (ROOT / relative).is_file())

    print(f"Retired production manifest: {len(production)}")
    print(f"Retired test manifest: {len(RETIRED_TEST_PATHS)}")
    print(f"Files currently present and planned for removal: {len(present)}")
    for relative in present:
        print(f"  REMOVE {relative}")
    print("  UPDATE project_cores.md")

    if not arguments.apply:
        import difflib

        print()
        print("Proposed project_cores.md diff:")
        print(
            "".join(
                difflib.unified_diff(
                    _read("project_cores.md").splitlines(keepends=True),
                    proposed.splitlines(keepends=True),
                    fromfile="project_cores.md (current)",
                    tofile="project_cores.md (proposed)",
                )
            ),
            end="",
        )
        print()
        print("Preview only. No files were changed.")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = ROOT / "data" / "refactoring-backups" / stamp
    backup.mkdir(parents=True, exist_ok=False)

    affected = ["project_cores.md", *present]
    for relative in affected:
        destination = backup / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)

    print(f"Backup: {backup}")

    try:
        (ROOT / "project_cores.md").write_text(
            proposed,
            encoding="utf-8",
            newline="\n",
        )

        for relative in present:
            (ROOT / relative).unlink()

        _run_gate(
            "-q",
            "tests/test_detector_backend_deleted.py",
            "tests/test_lm_backend_deleted.py",
            "tests/test_project_cores_contract.py",
            "tests/test_project_cores_inventory.py",
            "tests/test_readme_contract.py",
            "tests/test_readme_analysis_transition.py",
            "tests/test_readme_analysis_doc02.py",
            "tests/test_header_shutdown_and_timezone.py",
            "tests/test_ui_localization.py",
            "tests/test_analysis_inputs_independence.py",
            "tests/test_chart_publication_preflight.py",
        )
        _run_gate("--collect-only", "-q")

    except BaseException:
        print("Verification failed or execution was interrupted; restoring files.")
        for relative in affected:
            destination = ROOT / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup / relative, destination)
        print(f"Restored affected files. Backup retained at: {backup}")
        raise

    print("Coordinated finalization and collection verification completed.")
    print("Run the retained full test suite and browser checks separately.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
