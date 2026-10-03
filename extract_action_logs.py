#!/usr/bin/env python3
"""Extract timers, warnings, and errors from GitHub Actions logs.

Usage:
    python extract_action_logs.py <unzipped_logs_dir> [-o report.txt]

Scans all *.txt job logs recursively and extracts:
  1. Full timing blocks  (=== TIMING SUMMARY === ... ===...===)
  2. Phase timer one-liners
  3. Warnings / errors / tracebacks / GH annotations

Streams line-by-line -> safe for 80MB+ logs.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from collections import defaultdict

TIMING_START = "=== TIMING SUMMARY ==="
TIMING_JSON_SCHEMA = "l2shock.remote_worker_timing"

PHASE_KEYWORDS = [
    "process_remote_hour",
    "inspect_existing_state",
    "acquire_sources",
    "l2_processing",
    "replay_predecessors",
    "sampling_target",
    "encoding",
    "hf_publish_l2",
    "hf_publish_price",
    "select_target",
    "process_hour",
]
WARNING_MARKERS = ["WARNING", "##[warning]", "[warn]", "WARN "]
ERROR_MARKERS = [
    "ERROR",
    "CRITICAL",
    "##[error]",
    "[error]",
    "Error:",
    "FAILED",
    "Traceback",
]


def build_phase_regex() -> re.Pattern:
    names = "|".join(re.escape(p) for p in PHASE_KEYWORDS)
    return re.compile(rf"(?:{names})\S*\s+\d+\.\d+\s*s", re.IGNORECASE)


def matches_any(line: str, markers) -> bool:
    up = line.upper()
    return any(m.upper() in up for m in markers)


def is_timing_close(line: str) -> bool:
    s = line.strip()
    return len(s) >= 20 and set(s) == {"="}


def scan_file(path: Path, phase_re):
    in_block = False
    buf = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if TIMING_START in line:
                    in_block = True
                    buf = [line]
                    continue
                if in_block:
                    buf.append(line)
                    if is_timing_close(line):
                        in_block = False
                        yield ("TIMING_BLOCK", "\n".join(buf))
                        buf = []
                    continue
                if TIMING_JSON_SCHEMA in line:
                    yield ("TIMING_JSON", line)
                    continue
                if phase_re.search(line):
                    yield ("PHASE", line)
                    continue
                if matches_any(line, WARNING_MARKERS):
                    yield ("WARNING", line)
                    continue
                if matches_any(line, ERROR_MARKERS):
                    yield ("ERROR", line)
                    continue
    except (OSError, UnicodeDecodeError) as exc:
        yield ("READ_ERROR", f"Could not read {path}: {exc}")


def main():
    ap = argparse.ArgumentParser(
        description="Extract timers/warnings/errors from GH Actions logs."
    )
    ap.add_argument("logs_dir", help="Unzipped GitHub Actions logs folder.")
    ap.add_argument("-o", "--output", default="action_logs_report.txt")
    args = ap.parse_args()

    root = Path(args.logs_dir)
    if not root.is_dir():
        raise SystemExit(f"Logs directory not found: {root}")

    txt_files = sorted(root.rglob("*.txt"))
    if not txt_files:
        raise SystemExit(f"No .txt files found under {root}")

    phase_re = build_phase_regex()
    counts = defaultdict(int)
    body = []

    for f in txt_files:
        hits = list(scan_file(f, phase_re))
        if not hits:
            continue
        body.append("")
        body.append("#" * 72)
        body.append(f"FILE: {f}")
        body.append("#" * 72)
        for category, content in hits:
            counts[category] += 1
            body.append(f"[{category}]")
            body.append(content)
            body.append("-" * 40)

    header = [
        "=" * 72,
        "GITHUB ACTIONS LOG EXTRACTION REPORT",
        f"Scanned {len(txt_files)} .txt file(s) under: {root}",
        "",
        "CATEGORY COUNTS:",
    ]
    for cat in [
        "TIMING_BLOCK",
        "TIMING_JSON",
        "PHASE",
        "WARNING",
        "ERROR",
        "READ_ERROR",
    ]:
        header.append(f"  {cat:<14}: {counts.get(cat, 0)}")
    header.append("=" * 72)

    report = "\n".join(header + body) + "\n"
    Path(args.output).write_text(report, encoding="utf-8")

    # Console preview (first 2000 chars) + location
    print(report[:2000])
    print(f"\nFull report written to: {Path(args.output).resolve()}")


if __name__ == "__main__":
    main()
