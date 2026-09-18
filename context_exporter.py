#!/usr/bin/env python3
"""
Generic AI repository context exporter.

Default behavior:

    python tools/context_exporter.py --root .

This writes segmented whole-project context files into ./ai_chunks:

    project_01_of_NN_segment.txt
    project_02_of_NN_segment.txt
    ...

Useful modes:

Newest files under a logical cap:

    python tools/context_exporter.py --root . --recent-files-cap 3.4

Explicit custom file list:

    python tools/context_exporter.py --root . --custom custom.txt

Prioritize requested files at the beginning of project segments:

    python tools/context_exporter.py --root . --custom-priority custom.txt

Generate three reproducibly shuffled full-codebase variants:

    python tools/context_exporter.py --root . --full-versions 3 --seed 101

Small physical files for upload systems with limited file visibility:

    python tools/context_exporter.py --root . --small-chunks

Behavior:

- scans text-like repository files;
- excludes generated outputs, logs, caches, virtual environments, binary files,
  database files, and configured excluded directories;
- preserves source indentation exactly;
- splits oversized source files only at complete source-line boundaries;
- redacts likely credentials from exported copies;
- never modifies repository source files;
- automatically includes README.md first in recent/custom modes when present;
- writes start/end verification markers into every generated TXT file.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import io
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from collections.abc import Callable, Iterable

DELIMITER = "#####################"

# Randomized full-codebase variants are opt-in. Normal project segments remain
# deterministic unless --full-versions is requested.
DEFAULT_FULL_VERSIONS = 0

DEFAULT_SEGMENT_COUNT = 0
DEFAULT_SEGMENT_TARGET_BYTES = 900 * 1024

# Some upload platforms expose only a limited amount of each uploaded file.
DEFAULT_SMALL_SEGMENT_TARGET_BYTES = 400 * 1024

# Reserve space for generated headers, path labels, delimiters, and verification
# markers when calculating the source payload available in a physical segment.
DEFAULT_SEGMENT_OVERHEAD_RESERVE_BYTES = 48 * 1024


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number, e.g. 1, 1.2, 2.5") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")

    return parsed


def mib_to_bytes(value: float | None) -> int | None:
    """Convert a MiB float into bytes.

    Used for output-bundle caps. Accepts decimal MiB values such as 3.3 or 4.1.
    """
    if value is None:
        return None

    cap = int(float(value) * 1024 * 1024)
    if cap <= 0:
        raise ValueError("MiB cap must be greater than zero")

    return cap


@dataclass(frozen=True)
class FileEntry:
    path: Path
    rel_posix: str
    size_bytes: int
    est_bundle_bytes: int


@dataclass(frozen=True)
class BundlePart:
    """One complete-line portion of a source file.

    ``text`` has already passed through the export boundary, including secret
    redaction.

    A source line is never divided. Leading whitespace and indentation remain
    exactly as they appeared in the transformed exported source.
    """

    entry: FileEntry
    text: str
    first_line: int
    last_line: int
    part_index: int
    part_count: int

    @property
    def display_path(self) -> str:
        if self.part_count <= 1:
            return self.entry.rel_posix

        return (
            f"{self.entry.rel_posix} "
            f"[[source lines {self.first_line}-{self.last_line}; "
            f"file part {self.part_index}/{self.part_count}]]"
        )

    @property
    def est_output_bytes(self) -> int:
        text_bytes = len(self.text.encode("utf-8"))
        label_bytes = len(self.display_path.encode("utf-8"))
        delimiter_bytes = len(DELIMITER.encode("utf-8"))

        # Path label newline, optional source newline, delimiter, and its newline.
        trailing_newline_bytes = 0 if self.text.endswith("\n") else 1
        return (
            label_bytes + 1 + text_bytes + trailing_newline_bytes + delimiter_bytes + 1
        )


# Segment filenames are shown by some upload platforms using only their first
# 12 characters. Keep a short, stable family identifier before the segment
# number so a failed upload can be identified without opening the file.
#
# The complete leading token may exceed 12 characters once "_of_NN" is added,
# e.g. "fdcp_05_of_10". The enforced invariant is that:
#
#     family_identifier + "_" + segment_number + "_"
#
# fits within 12 characters. Thus the platform-visible prefix always identifies
# both the output family and the physical segment number.
_SEGMENT_FILENAME_FAMILY_ALIASES: dict[str, str] = {
    "project_segment": "project",
    "custom_subset": "custom",
}


def _fallback_segment_family_identifier(prefix: str) -> str:
    """Return a deterministic short identifier for an unknown segment family.

    Known output families use explicit aliases above. The fallback keeps custom
    logical prefixes usable without requiring an explicit alias.
    """
    cleaned = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        str(prefix or "").strip(),
    ).strip("_")

    if not cleaned:
        return "segment"

    parts = [part for part in cleaned.split("_") if part]
    lowered = [part.lower() for part in parts]

    # Multiple custom subset lists use prefixes such as
    # custom_subset_02_my_list. Keep the list number visible.
    if len(parts) >= 3 and lowered[0:2] == ["custom", "subset"]:
        number = re.sub(r"\D+", "", parts[2])
        return f"cs{number or 'x'}"

    # Build a readable acronym from path/name components. Ignore generic words
    # where possible so "feature_new_profile_workflow" becomes approximately
    # "fnpw" rather than an unhelpful prefix slice.
    generic_words = {
        "segment",
        "segments",
        "bundle",
        "bundles",
        "dependencies",
        "dependency",
    }
    significant = [part for part in parts if part.lower() not in generic_words]

    initials = "".join(part[0] for part in significant if part)
    if initials:
        return initials[:8]

    return cleaned[:8]


def _segment_family_identifier(
    prefix: str,
    *,
    segment_width: int,
) -> str:
    """Return a family identifier whose visible family+segment prefix fits.

    Enforced invariant:

        len(f"{family}_{segment_number}_") <= 12

    The `_of_total` portion follows immediately afterward, so platforms that
    show only the first 12 filename characters still reveal the output family
    and physical segment number.
    """
    raw_prefix = str(prefix or "").strip()
    family = _SEGMENT_FILENAME_FAMILY_ALIASES.get(raw_prefix)

    if family is None:
        family = _fallback_segment_family_identifier(raw_prefix)

    family = re.sub(r"[^A-Za-z0-9_-]+", "", family).strip("_-") or "seg"

    # family + "_" + zero-padded segment number + "_"
    max_family_len = max(1, 12 - int(segment_width) - 2)
    return family[:max_family_len]


def segmented_output_filename(
    prefix: str,
    *,
    segment_index: int,
    segment_count: int,
    include_descriptor: bool = True,
) -> str:
    """Return an upload-friendly segmented output filename.

    Examples:

        project_segment, 4/12
            -> project_04_of_12_segment.txt

        custom_subset, 6/9
            -> custom_06_of_09_segment.txt

        custom_subset_02_backend, 2/7
            -> cs02_02_of_07_custom_subset_02_backend_segment.txt

    `include_descriptor` retains the original logical family after an
    abbreviated leading identifier. It is automatically omitted when the short
    family already describes the complete logical prefix.
    """
    total = max(1, int(segment_count or 1))
    index = int(segment_index or 0)

    if index < 1 or index > total:
        raise ValueError(
            "segment_index must be inside [1, segment_count]; "
            f"got index={index}, count={total}"
        )

    width = max(2, len(str(total)))
    family = _segment_family_identifier(
        prefix,
        segment_width=width,
    )

    lead = f"{family}_" f"{index:0{width}d}_of_{total:0{width}d}"

    raw_prefix = str(prefix or "").strip()
    descriptor = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        raw_prefix,
    ).strip("._-")

    omit_descriptor_for = {
        "custom_subset",
        "outside_batches",
        "project_segment",
    }

    if (
        include_descriptor
        and descriptor
        and descriptor not in omit_descriptor_for
        and descriptor != family
    ):
        return f"{lead}_{descriptor}_segment.txt"

    return f"{lead}_segment.txt"


def looks_like_text(path: Path, max_file_size: int) -> bool:
    try:
        st = path.stat()
    except OSError:
        return False
    if not path.is_file():
        return False
    if st.st_size > max_file_size:
        return False
    try:
        with path.open("rb") as f:
            head = f.read(2048)
        if b"\x00" in head:
            return False
    except OSError:
        return False
    return True


def is_generated_output_name(name: str) -> bool:
    """Return True when a filename is produced by this exporter.

    The broader project-contents pattern also excludes old exports copied from
    other repositories, such as:

        l2_liquidity_shock_analyzer-project_contents.txt
    """
    normalized = str(name or "").lower()

    patterns = (
        "*project_contents*.txt",
        "project_segment_*.txt",
        "project_*_of_*_segment.txt",
        "*_project_segment_segment.txt",
        "custom_subset*.txt",
        "custom_*_of_*_segment.txt",
        "cs*_of_*_custom_subset*_segment.txt",
        "context_export_manifest.md",
        # Remove old topic-exporter outputs if they were copied with the script.
        "topic_chunks_manifest.md",
        "batch_*.txt",
        "dual_*.txt",
        "triple_*.txt",
        "feature_*.txt",
        "outside_*.txt",
        "*_batch_*.txt",
        "*_dual_*.txt",
        "*_triple_*.txt",
        "*_feature_*.txt",
    )

    return any(fnmatch.fnmatch(normalized, pattern) for pattern in patterns)


def cleanup_generated_outputs(
    out_dir: Path,
    *,
    name_patterns: Iterable[str] | None = None,
) -> int:
    """Remove stale generated files.

    When name_patterns is None, remove all known generated outputs as before.

    When name_patterns is provided, remove only generated files whose names
    match one of those patterns. This is used by --only so a custom-only run
    does not delete unrelated old topic/dual/triple/project outputs.
    """
    if not out_dir.exists():
        return 0

    patterns_l = [
        str(pat or "").strip().lower()
        for pat in (name_patterns or [])
        if str(pat or "").strip()
    ]

    removed = 0
    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        if not is_generated_output_name(p.name):
            continue

        if patterns_l and not any(
            fnmatch.fnmatch(p.name.lower(), pat) for pat in patterns_l
        ):
            continue

        try:
            p.unlink()
            removed += 1
        except OSError:
            pass

    return removed


def is_excluded(
    path: Path,
    *,
    root: Path,
    exclude_dirs: set[str],
    exclude_exts: set[str],
    exclude_names: set[str],
) -> bool:
    name_l = path.name.lower()

    if name_l in exclude_names:
        return True
    if is_generated_output_name(name_l):
        return True
    if path.suffix.lower() in exclude_exts:
        return True

    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = path

    # Case-insensitive dir matching, important on Windows and for .vscodecounter.
    for part in rel.parts[:-1]:
        if part.lower() in exclude_dirs:
            return True

    return False


def iter_valid_files(
    root: Path,
    *,
    max_file_size: int,
    exclude_dirs: set[str],
    exclude_exts: set[str],
    exclude_names: set[str],
) -> list[FileEntry]:
    root = root.resolve()
    entries: list[FileEntry] = []

    for p in root.rglob("*"):
        try:
            if p.is_symlink():
                continue
        except OSError:
            continue

        if not p.is_file():
            continue

        if is_excluded(
            p,
            root=root,
            exclude_dirs=exclude_dirs,
            exclude_exts=exclude_exts,
            exclude_names=exclude_names,
        ):
            continue

        if not looks_like_text(p, max_file_size=max_file_size):
            continue

        try:
            rel = p.relative_to(root).as_posix()
        except ValueError:
            rel = p.as_posix()

        size = p.stat().st_size
        overhead = len(rel.encode("utf-8")) + 1 + len(DELIMITER.encode("utf-8")) + 2
        entries.append(
            FileEntry(
                path=p,
                rel_posix=rel,
                size_bytes=size,
                est_bundle_bytes=size + overhead,
            )
        )

    entries.sort(key=lambda e: e.rel_posix.lower())
    return entries


def _redact_generic_secrets(
    text: str,
    *,
    code_like: bool = False,
) -> str:
    """Redact secrets that may appear outside config.yaml.

    This affects only exported AI bundles, not the source repository.

    ``code_like=True`` is used for Python source. In that mode, an unquoted
    identifier/expression assigned to a sensitive-looking local name must not
    be rewritten into a string literal. Quoted literal secrets remain
    redactable.
    """

    def _looks_placeholder(value: str) -> bool:
        v = str(value or "").strip().strip("'\"")
        if not v:
            return True

        low = v.lower()
        placeholders = {
            "none",
            "null",
            "false",
            "true",
            "changeme",
            "change_me",
            "your_api_key",
            "your-token",
            "your_token",
            "example",
            "test",
            "dummy",
            "***redacted***",
            "***redacted_uuid_secret***",
            "***redacted_url_password***",
        }
        if low in placeholders:
            return True

        if low.startswith(("example_", "dummy_", "test_", "your_")):
            return True

        if "${" in v or "%{" in v or "{{" in v:
            return True

        if v.startswith(
            (
                "os.environ",
                "os.getenv",
                "getenv(",
                "settings.",
                "cfg.",
            )
        ):
            return True

        # Avoid redacting harmless flags like token=false, password=0, etc.
        if len(v) < 8:
            return True

        return False

    def _looks_like_code_expression(value: str, *, quote: str) -> bool:
        """Avoid destroying Python source while still redacting literal secrets."""
        if quote:
            return False

        v = str(value or "").strip()
        if not v:
            return False

        if any(ch in v for ch in ("(", ")", "[", "]", "{", "}", ",")):
            return True

        if re.search(r"\s(?:=|==|!=|<=|>=|<|>)\s", v):
            return True

        if re.search(r"\b(?:for|if|else|in|lambda|return|or|and|not)\b", v):
            return True

        if v.startswith(("_", "self.", "cls.")):
            return True

        if code_like:
            # Exact sensitive-looking local names are common in Python:
            #
            #     token = current_token
            #     password = settings.database.password
            #
            # These are expressions, not literal secrets. Without this guard,
            # exported review bundles can silently change program semantics.
            if re.fullmatch(
                r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*",
                v,
            ):
                return True

        return False

    def _redact_labelled_assignment(match: re.Match[str]) -> str:
        prefix = match.group("prefix")
        quote = match.group("quote") or ""
        value = match.group("value")
        suffix = match.group("suffix") or ""

        if _looks_placeholder(value) or _looks_like_code_expression(
            value,
            quote=quote,
        ):
            return match.group(0)

        marker = "***REDACTED_LABELLED_SECRET***"

        if quote:
            return f"{prefix}{quote}{marker}{quote}{suffix}"

        # Use quotes for unquoted ENV/YAML/INI values so accidental redaction in
        # a code-like file remains syntactically safer than bare *** markers.
        return f'{prefix}"{marker}"{suffix}'

    out = text

    # URLs with embedded credentials, e.g.
    # postgresql://user:password@host:5432/db
    out = re.sub(
        r"\b([A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+):([^@\s/]+)@",
        r"\1:***REDACTED_URL_PASSWORD***@",
        out,
    )

    # Telegram bot tokens look like: 1234567890:AA...
    out = re.sub(
        r"\b\d{8,12}:[A-Za-z0-9_-]{30,}\b",
        "***REDACTED_TELEGRAM_BOT_TOKEN***",
        out,
    )

    # Common high-entropy token families.
    token_patterns = (
        (
            r"\bsk-[A-Za-z0-9_-]{20,}\b",
            "***REDACTED_OPENAI_STYLE_KEY***",
        ),
        (
            r"\bgh[pousr]_[A-Za-z0-9_]{30,}\b",
            "***REDACTED_GITHUB_TOKEN***",
        ),
        (
            r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b",
            "***REDACTED_SLACK_TOKEN***",
        ),
        (
            r"\bAKIA[0-9A-Z]{16}\b",
            "***REDACTED_AWS_ACCESS_KEY_ID***",
        ),
        (
            r"\bAIza[0-9A-Za-z_-]{30,}\b",
            "***REDACTED_GOOGLE_API_KEY***",
        ),
        (
            r"\b(?:sk|pk)_(?:live|test)_[0-9A-Za-z]{20,}\b",
            "***REDACTED_STRIPE_KEY***",
        ),
    )
    for pattern, replacement in token_patterns:
        out = re.sub(pattern, replacement, out)

    # Generic labelled YAML/INI/env/Python-style assignments.
    # This also handles UUID-shaped values when assigned to sensitive labels.
    secret_name = (
        r"(?:"
        r"api[_-]?keys?|apikeys?|access[_-]?keys?|secret[_-]?keys?|"
        r"token|auth[_-]?token|bearer[_-]?token|refresh[_-]?token|"
        r"secret|client[_-]?secret|password|passwd|pwd|credential|"
        r"bot[_-]?token|chat[_-]?token|private[_-]?key|vapid[_-]?private"
        r")"
    )

    # Match a real secret field name, optionally namespaced by segments such as
    # "coinalyze_api_key", "database.password", or "telegram-bot-token".
    #
    # Do not allow arbitrary suffix text after the recognized secret field.
    # Identifiers such as new_token_source, token_count, or api_key_parser are
    # ordinary source-code identifiers and must never be redacted.
    secret_label = rf"(?:[\w.\-]*[._-])?{secret_name}"

    labelled_re = re.compile(
        rf"(?im)^(?P<prefix>\s*(?:export\s+|set\s+|(?:const|let|var)\s+)?"
        rf"{secret_label}\s*[:=]\s*)"
        rf"(?P<quote>['\"]?)"
        rf"(?P<value>[^'\"\r\n#]+?)"
        rf"(?P=quote)"
        rf"(?P<suffix>\s*(?:#.*)?)$"
    )
    out = labelled_re.sub(_redact_labelled_assignment, out)

    # JSON object form: "api_key": "..."
    json_labelled_re = re.compile(
        rf"(?i)(?P<prefix>\"{secret_label}\"\s*:\s*)"
        rf"(?P<quote>\")"
        rf"(?P<value>[^\"\r\n]+?)"
        rf"(?P=quote)"
        rf"(?P<suffix>\s*)"
    )
    out = json_labelled_re.sub(_redact_labelled_assignment, out)

    # VAPID / service-worker / other PEM private keys.
    out = re.sub(
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
        "***REDACTED_PRIVATE_KEY_PEM***",
        out,
        flags=re.DOTALL,
    )

    return out


_PYTHON_SECRET_FIELD_NAMES = {
    "api_key",
    "api_keys",
    "apikey",
    "apikeys",
    "access_key",
    "access_keys",
    "secret_key",
    "secret_keys",
    "auth_token",
    "bearer_token",
    "refresh_token",
    "client_secret",
    "password",
    "passwd",
    "pwd",
    "bot_token",
    "chat_token",
    "private_key",
    "vapid_private",
    "vapid_private_key",
}


def _python_secret_field_name(value: Any) -> str:
    text_value = str(value or "").strip().lower()
    text_value = re.sub(r"[^a-z0-9_.-]+", "_", text_value)
    text_value = text_value.strip("._-")

    while text_value.startswith("default_"):
        text_value = text_value[len("default_") :]

    # Attribute assignments such as cfg.database.password use the final
    # component as their semantic field name.
    final_component = re.split(r"[.]", text_value)[-1]
    return final_component


def _python_literal_secret_is_placeholder(value: str) -> bool:
    normalized = str(value or "").strip().strip("'\"").lower()

    if not normalized:
        return True

    if "redacted" in normalized:
        return True

    placeholders = {
        "none",
        "null",
        "false",
        "true",
        "changeme",
        "change_me",
        "your_api_key",
        "your-token",
        "your_token",
        "example",
        "test",
        "dummy",
        "placeholder",
        "secret",
    }

    if normalized in placeholders:
        return True

    return normalized.startswith(
        (
            "example_",
            "example-",
            "dummy_",
            "dummy-",
            "test_",
            "test-",
            "your_",
            "your-",
            "${",
            "{{",
        )
    )


def _python_hardcoded_secret_findings(
    source: str,
) -> list[str]:
    """Return residual hardcoded-secret locations after generic redaction.

    This is a fail-closed export boundary. It does not rewrite Python AST nodes:
    source-preserving redaction of arbitrary collection/config expressions is
    easy to get wrong. If a labelled secret still contains a literal value,
    refuse the AI bundle and require the source/configuration to be cleaned.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        # The source exporter already preserves invalid source text for review.
        # Do not hide the original syntax error behind a secret-scanner parse
        # failure.
        return []

    findings: list[str] = []

    def _is_secret_label(value: Any) -> bool:
        return _python_secret_field_name(value) in _PYTHON_SECRET_FIELD_NAMES

    def _target_labels(node: ast.AST) -> list[str]:
        if isinstance(node, ast.Name):
            return [node.id]

        if isinstance(node, ast.Attribute):
            return [node.attr]

        if isinstance(node, (ast.Tuple, ast.List)):
            out: list[str] = []
            for item in node.elts:
                out.extend(_target_labels(item))
            return out

        return []

    def _literal_values(node: ast.AST | None) -> list[tuple[int, str]]:
        """Return literal secret values without inspecting executable lookups.

        Supported literal forms include:

            token = "actual-secret"
            api_keys = ["key-one", "key-two"]
            credentials = {"token": "actual-token"}
            passwords: tuple[str, ...] = ("one", "two")

        Do not descend into executable expressions such as:

            token = raw.get("bot_token", "")
            token = settings["token"]
            token = os.getenv("TOKEN", "")
            token = config.get("password")
            token = build_token("prefix")

        String constants inside those expressions are lookup keys, defaults, or
        ordinary function arguments rather than necessarily hardcoded secrets.
        """
        if node is None:
            return []

        if isinstance(node, ast.Constant):
            if not isinstance(node.value, str):
                return []

            value = str(node.value)
            if _python_literal_secret_is_placeholder(value):
                return []

            return [
                (
                    int(getattr(node, "lineno", 0) or 0),
                    value,
                )
            ]

        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            out: list[tuple[int, str]] = []
            for item in node.elts:
                out.extend(_literal_values(item))
            return out

        if isinstance(node, ast.Dict):
            out: list[tuple[int, str]] = []

            # Dictionary keys are field names, not credential values. Inspect
            # values only. A nested literal dictionary/list remains supported.
            for value_node in node.values:
                out.extend(_literal_values(value_node))

            return out

        # Calls, attributes, subscriptions, comprehensions, binary operations,
        # f-strings, and other executable expressions are intentionally ignored.
        return []

    def _looks_like_secret_literal(value: str) -> bool:
        """Return True only for literals that plausibly look like credentials."""
        value = str(value or "").strip()

        if _python_literal_secret_is_placeholder(value):
            return False

        # Very short human-readable constants are almost never secrets.
        if len(value) < 16:
            return False

        # Typical credential character set.
        return bool(re.fullmatch(r"[A-Za-z0-9_\-+/=:.]+", value))

    def _record(label: str, node: ast.AST | None) -> None:
        for line_number, literal_value in _literal_values(node):
            if not _looks_like_secret_literal(literal_value):
                continue

            findings.append(
                f"line {line_number}: {label} contains "
                f"{literal_value!r} (length={len(literal_value)})"
            )

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            labels = [
                label
                for target in node.targets
                for label in _target_labels(target)
                if _is_secret_label(label)
            ]
            for label in labels:
                _record(label, node.value)

        elif isinstance(node, ast.AnnAssign):
            labels = [
                label
                for label in _target_labels(node.target)
                if _is_secret_label(label)
            ]
            for label in labels:
                _record(label, node.value)

        elif isinstance(node, ast.keyword):
            if node.arg and _is_secret_label(node.arg):
                _record(node.arg, node.value)

        elif isinstance(node, ast.Dict):
            for key_node, value_node in zip(node.keys, node.values):
                if not (
                    isinstance(key_node, ast.Constant)
                    and isinstance(key_node.value, str)
                    and _is_secret_label(key_node.value)
                ):
                    continue

                _record(str(key_node.value), value_node)

    return findings


def redact_sensitive_text(rel_posix: str, text: str) -> str:
    """Redact local secrets before writing AI-review bundles.

    Config structure stays visible. Actual API keys/passwords/tokens are
    irrelevant for bug-finding and are easy to leak accidentally.
    """
    rel_l = rel_posix.lower().replace("\\", "/")
    code_like = rel_l.endswith((".py", ".pyi"))

    text = _redact_generic_secrets(
        text,
        code_like=code_like,
    )

    if code_like:
        residual_secret_findings = _python_hardcoded_secret_findings(text)
        if residual_secret_findings:
            raise RuntimeError(
                "Refusing to export Python source with residual hardcoded "
                f"secret literal(s): {rel_posix}. "
                + " | ".join(residual_secret_findings[:20])
                + (
                    f" | ... +{len(residual_secret_findings) - 20} more"
                    if len(residual_secret_findings) > 20
                    else ""
                )
            )

    if rel_l not in {"config.yaml", "config.yml"}:
        return text

    try:
        import yaml

        raw = yaml.safe_load(text) or {}
        redacted = raw

        sensitive_keys = {
            "api_key",
            "api_keys",
            "apikey",
            "apikeys",
            "access_key",
            "access_keys",
            "secret",
            "secret_key",
            "secret_keys",
            "client_secret",
            "token",
            "auth_token",
            "bearer_token",
            "refresh_token",
            "password",
            "passwd",
            "pwd",
            "credential",
            "credentials",
            "private_key",
            "bot_token",
            "chat_token",
            "vapid_private",
            "vapid_private_key",
        }

        def _normalized_yaml_key(value: Any) -> str:
            normalized = re.sub(
                r"[^a-z0-9]+",
                "_",
                str(value or "").strip().lower(),
            )
            return normalized.strip("_")

        def _redacted_value(value: Any) -> Any:
            if isinstance(value, list):
                return ["***REDACTED***" for _ in value]
            if isinstance(value, tuple):
                return ["***REDACTED***" for _ in value]
            if isinstance(value, dict):
                return "***REDACTED***"
            return "***REDACTED***"

        def _walk(value: Any) -> Any:
            if isinstance(value, dict):
                result: dict[Any, Any] = {}

                for key, child in value.items():
                    normalized_key = _normalized_yaml_key(key)

                    if normalized_key in sensitive_keys:
                        result[key] = _redacted_value(child)
                    else:
                        result[key] = _walk(child)

                return result

            if isinstance(value, list):
                return [_walk(item) for item in value]

            return value

        redacted = _walk(redacted)

        dumped = yaml.safe_dump(
            redacted,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )

        return (
            "# NOTE: sensitive values redacted by context_exporter.py "
            "before bundling.\n" + dumped
        )
    except Exception:
        out = re.sub(
            r"(?im)^(\s*api_key\s*:\s*).+$",
            r'\1"***REDACTED***"',
            text,
        )
        out = re.sub(
            r"(?im)^(\s*password\s*:\s*).+$",
            r'\1"***REDACTED***"',
            out,
        )
        out = re.sub(
            r"(?ims)^(\s*api_keys\s*:\s*\n)(?:\s*-\s*.*?\n)+",
            r'\1    - "***REDACTED***"\n',
            out,
        )
        return (
            "# NOTE: sensitive values redacted by context_exporter.py before bundling.\n"
            + out
        )


def _assert_redaction_regression_self_test() -> None:
    """Fail fast if generic secret redaction corrupts ordinary source code.

    The exporter exists to provide trustworthy source context to AI reviewers.
    If ordinary identifiers such as ``new_token_source`` are redacted, generated
    bundles no longer represent the repository and must not be written.
    """
    unchanged_samples = (
        "new_token_source = prefix + body",
        "token_count = 3",
        "api_key_parser = build_parser()",
        "refresh_token_handler = make_handler()",
        "token = current_token",
        "password = settings.database.password",
        "api_key = source_key",
    )

    # Build secret-looking test fixtures dynamically.
    #
    # topic_chunker.py can itself be included in an AI bundle. If these samples
    # appear as literal source assignments such as:
    #
    #     api_key = "real-secret-value-here"
    #
    # the export redactor correctly rewrites its own self-test fixture. An AI
    # reviewing that transformed bundle then reports that startup always fails,
    # even though the original source is valid.
    #
    # Dynamic construction keeps the runtime regression test equally strict
    # without presenting literal secret assignments to the source exporter.
    secret_value = "-".join(("real", "secret", "value", "here"))
    api_key_label = "api" + "_" + "key"
    api_keys_label = "service" + "_" + "api" + "_" + "keys"
    password_label = "database" + "." + "password"
    bot_token_label = "bot" + "_" + "token"

    placeholder_samples = (
        "example-private-key",
        "example_api_key",
        "dummy-private-key",
        "test-private-key",
        "your-api-key",
    )

    for placeholder in placeholder_samples:
        if not _python_literal_secret_is_placeholder(placeholder):
            raise RuntimeError(
                "context exporter secret-placeholder self-test failed: "
                f"{placeholder!r} was not recognized as a placeholder."
            )

    secret_samples = (
        f'{api_key_label} = "{secret_value}"',
        f'{api_keys_label} = "{secret_value}"',
        f'{password_label} = "{secret_value}"',
        f'"{bot_token_label}": "{secret_value}"',
    )

    for sample in unchanged_samples:
        redacted = _redact_generic_secrets(
            sample,
            code_like=True,
        )
        if redacted != sample:
            raise RuntimeError(
                "context exporter secret-redaction self-test failed: ordinary "
                f"source code was modified.\nInput: {sample!r}\n"
                f"Output: {redacted!r}"
            )

    for sample in secret_samples:
        redacted = _redact_generic_secrets(
            sample,
            code_like=True,
        )
        if (
            redacted == sample
            or "real-secret-value-here" in redacted
            or "REDACTED" not in redacted
        ):
            raise RuntimeError(
                "context exporter secret-redaction self-test failed: a labelled "
                f"secret was not redacted.\nInput: {sample!r}\n"
                f"Output: {redacted!r}"
            )


def _assert_segment_filename_regression_self_test() -> None:
    """Fail fast if visible segment filenames or markers become ambiguous."""
    filename_cases = (
        (
            "project_segment",
            4,
            12,
            "project_04_of_12_segment.txt",
            "project_04_",
        ),
        (
            "custom_subset",
            6,
            9,
            "custom_06_of_09_segment.txt",
            "custom_06_",
        ),
        (
            "custom_subset_02_backend_review",
            2,
            7,
            "cs02_02_of_07_custom_subset_02_backend_review_segment.txt",
            "cs02_02_",
        ),
    )

    for prefix, index, total, expected_name, visible_identity in filename_cases:
        actual = segmented_output_filename(
            prefix,
            segment_index=index,
            segment_count=total,
        )

        if actual != expected_name:
            raise RuntimeError(
                "context exporter segment-filename self-test failed.\n"
                f"Prefix: {prefix!r}\n"
                f"Expected: {expected_name!r}\n"
                f"Actual: {actual!r}"
            )

        if len(visible_identity) > 12:
            raise RuntimeError(
                "context exporter visible filename identity exceeds "
                f"12 characters: {visible_identity!r}"
            )

        if not actual.startswith(visible_identity):
            raise RuntimeError(
                "context exporter segment number is not visible near the "
                f"beginning of {actual!r}"
            )

    marker_cases = (
        (1, 1, "start_of_segment_01_of_01", "end_of_segment_01_of_01"),
        (1, 5, "start_of_segment_01_of_05", "end_of_segment_01_of_05"),
        (5, 5, "start_of_segment_05_of_05", "end_of_segment_05_of_05"),
        (7, 120, "start_of_segment_007_of_120", "end_of_segment_007_of_120"),
    )

    for index, total, expected_start, expected_end in marker_cases:
        actual_start, actual_end = segment_boundary_markers(index, total)

        if actual_start != expected_start or actual_end != expected_end:
            raise RuntimeError(
                "context exporter segment-marker self-test failed.\n"
                f"Index/total: {index}/{total}\n"
                f"Expected start: {expected_start!r}\n"
                f"Actual start: {actual_start!r}\n"
                f"Expected end: {expected_end!r}\n"
                f"Actual end: {actual_end!r}"
            )


def segment_boundary_markers(
    segment_index: int,
    segment_count: int,
) -> tuple[str, str]:
    """Return machine-checkable first/last lines for one physical TXT file."""
    total = max(1, int(segment_count or 1))
    index = int(segment_index or 0)

    if index < 1 or index > total:
        raise ValueError(
            "segment_index must be inside [1, segment_count]; "
            f"got index={index}, count={total}"
        )

    width = max(2, len(str(total)))
    identity = f"segment_{index:0{width}d}_of_{total:0{width}d}"

    return (
        f"start_of_{identity}",
        f"end_of_{identity}",
    )


def _read_exported_entry_text(entry: FileEntry) -> str:
    """Read and redact one source file for export.

    Repository files are never modified. Redaction affects only the exported
    text written into AI context files.
    """
    try:
        text = entry.path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "[[ERROR: could not read file]]\n"

    return redact_sensitive_text(entry.rel_posix, text)


def _split_exported_text_at_line_boundaries(
    entry: FileEntry,
    text: str,
    *,
    max_text_bytes: int,
) -> list[BundlePart]:
    """Split transformed source without dividing or modifying source lines.

    Every emitted source line retains its original leading whitespace. If one
    individual source line exceeds the available segment payload, generation
    fails because splitting that line would alter the source representation.
    """
    max_text_bytes = max(1, int(max_text_bytes))
    lines = text.splitlines(keepends=True)

    if not lines:
        return [
            BundlePart(
                entry=entry,
                text="",
                first_line=1,
                last_line=1,
                part_index=1,
                part_count=1,
            )
        ]

    raw_parts: list[tuple[str, int, int]] = []
    current_lines: list[str] = []
    current_bytes = 0
    current_first_line = 1

    for line_number, line in enumerate(lines, start=1):
        line_bytes = len(line.encode("utf-8"))

        if line_bytes > max_text_bytes:
            raise ValueError(
                f"Cannot safely split {entry.rel_posix}: source line "
                f"{line_number} is {line_bytes:,} UTF-8 bytes, exceeding the "
                f"available per-segment source payload of "
                f"{max_text_bytes:,} bytes. The exporter refuses to divide an "
                "individual source line because that could damage source text."
            )

        if current_lines and current_bytes + line_bytes > max_text_bytes:
            raw_parts.append(
                (
                    "".join(current_lines),
                    current_first_line,
                    line_number - 1,
                )
            )
            current_lines = []
            current_bytes = 0
            current_first_line = line_number

        current_lines.append(line)
        current_bytes += line_bytes

    if current_lines:
        raw_parts.append(
            (
                "".join(current_lines),
                current_first_line,
                current_first_line + len(current_lines) - 1,
            )
        )

    part_count = len(raw_parts)

    return [
        BundlePart(
            entry=entry,
            text=part_text,
            first_line=first_line,
            last_line=last_line,
            part_index=part_index,
            part_count=part_count,
        )
        for part_index, (
            part_text,
            first_line,
            last_line,
        ) in enumerate(raw_parts, start=1)
    ]


def _unique_entries_from_parts(
    parts: Iterable[BundlePart],
) -> list[FileEntry]:
    out: list[FileEntry] = []
    seen: set[str] = set()

    for part in parts:
        rel = part.entry.rel_posix
        if rel in seen:
            continue

        seen.add(rel)
        out.append(part.entry)

    return out


def _write_bundle_parts(
    parts: Iterable[BundlePart],
    out_path: Path,
    *,
    header: str | None,
    segment_index: int,
    segment_count: int,
) -> None:
    """Write one physical bundle segment with first/last verification lines."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    start_marker, end_marker = segment_boundary_markers(
        segment_index,
        segment_count,
    )

    with out_path.open("w", encoding="utf-8", newline="\n") as out:
        # This must remain the first physical line in the TXT file.
        out.write(start_marker + "\n")

        if header:
            out.write(header.rstrip() + "\n")
            out.write(DELIMITER + "\n")

        for part in parts:
            out.write(part.display_path + "\n")
            out.write(part.text)

            if not part.text.endswith("\n"):
                out.write("\n")

            out.write(DELIMITER + "\n")

        # This must remain the last physical line in the TXT file.
        out.write(end_marker + "\n")


def write_marked_text_file(
    out_path: Path,
    text: str,
) -> None:
    """Write a non-segmented TXT report with 01-of-01 boundary markers."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    start_marker, end_marker = segment_boundary_markers(1, 1)

    body = str(text or "")
    if body and not body.endswith("\n"):
        body += "\n"

    out_path.write_text(
        start_marker + "\n" + body + end_marker + "\n",
        encoding="utf-8",
        newline="\n",
    )


def write_segmented_ordered_bundle(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    target_bytes: int,
    prefix: str,
    header_factory: Callable[[list[FileEntry], int, int], str | None] | None = None,
    global_cap_bytes: int | None = None,
) -> list[tuple[str, int, int]]:
    """Write ordered physical TXT segments without dividing source lines.

    Source files larger than the available physical payload are divided into
    BundlePart objects at complete line boundaries. Leading whitespace and
    indentation are preserved exactly.

    Every physical TXT file starts and ends with a matching verification marker.

    Returns:
        (filename, unique_source_file_count, estimated_payload_bytes)
    """
    entries = list(entries or [])
    if not entries:
        return []

    target_bytes = max(1, int(target_bytes or DEFAULT_SEGMENT_TARGET_BYTES))

    # Keep enough room for generated headers, path labels, delimiters, and
    # start/end verification markers. For unusually small custom targets, use
    # at most one fifth of the physical target as the reserve.
    overhead_reserve = min(
        DEFAULT_SEGMENT_OVERHEAD_RESERVE_BYTES,
        max(4 * 1024, target_bytes // 5),
    )
    payload_target = max(1, target_bytes - overhead_reserve)

    # Leave room inside each BundlePart for its path/line-range label and
    # delimiter. The final on-disk size is still verified after writing.
    per_part_text_target = max(1, payload_target - 2 * 1024)

    prepared_parts: list[BundlePart] = []

    for entry in entries:
        text = _read_exported_entry_text(entry)
        prepared_parts.extend(
            _split_exported_text_at_line_boundaries(
                entry,
                text,
                max_text_bytes=per_part_text_target,
            )
        )

    groups: list[list[BundlePart]] = []
    current_group: list[BundlePart] = []
    current_bytes = 0

    for part in prepared_parts:
        part_bytes = part.est_output_bytes

        if current_group and current_bytes + part_bytes > payload_target:
            groups.append(current_group)
            current_group = []
            current_bytes = 0

        current_group.append(part)
        current_bytes += part_bytes

    if current_group:
        groups.append(current_group)

    total = len(groups)
    if total == 0:
        return []

    headers: list[str | None] = []

    for segment_index, group in enumerate(groups, start=1):
        header_entries = _unique_entries_from_parts(group)
        headers.append(
            header_factory(header_entries, segment_index, total)
            if header_factory
            else None
        )

    logical_est_bytes = 0

    for segment_index, (group, header) in enumerate(
        zip(groups, headers),
        start=1,
    ):
        start_marker, end_marker = segment_boundary_markers(
            segment_index,
            total,
        )
        logical_est_bytes += len((start_marker + "\n").encode("utf-8"))
        logical_est_bytes += len((end_marker + "\n").encode("utf-8"))
        logical_est_bytes += sum(part.est_output_bytes for part in group)

        if header:
            header_text = header.rstrip() + "\n" + DELIMITER + "\n"
            logical_est_bytes += len(header_text.encode("utf-8"))

    if global_cap_bytes is not None and logical_est_bytes > int(global_cap_bytes):
        print(
            f"Skipping {prefix}: estimated logical output size "
            f"{logical_est_bytes:,} bytes exceeds --global-files-cap "
            f"{int(global_cap_bytes):,} bytes."
        )
        return []

    outputs: list[tuple[str, int, int]] = []
    written_paths: list[Path] = []

    try:
        for segment_index, (group, header) in enumerate(
            zip(groups, headers),
            start=1,
        ):
            out_path = out_dir / segmented_output_filename(
                prefix,
                segment_index=segment_index,
                segment_count=total,
            )
            written_paths.append(out_path)

            _write_bundle_parts(
                group,
                out_path,
                header=header,
                segment_index=segment_index,
                segment_count=total,
            )

            try:
                actual_bytes = int(out_path.stat().st_size)
            except OSError as exc:
                raise RuntimeError(
                    f"Could not verify generated bundle size for " f"{out_path}: {exc}"
                ) from exc

            if actual_bytes > target_bytes:
                raise RuntimeError(
                    f"Generated segment {out_path.name} is "
                    f"{actual_bytes:,} bytes, exceeding its physical target of "
                    f"{target_bytes:,} bytes. The complete {prefix} output "
                    "family will be removed. Increase the overhead reserve or "
                    "reduce --segment-target-bytes."
                )

            unique_entries = _unique_entries_from_parts(group)
            outputs.append(
                (
                    out_path.name,
                    len(unique_entries),
                    sum(part.est_output_bytes for part in group),
                )
            )

        if global_cap_bytes is not None:
            actual_logical_bytes = 0

            for written_path in written_paths:
                try:
                    actual_logical_bytes += int(written_path.stat().st_size)
                except OSError as exc:
                    raise RuntimeError(
                        f"Could not verify generated bundle size for "
                        f"{written_path}: {exc}"
                    ) from exc

            if actual_logical_bytes > int(global_cap_bytes):
                raise RuntimeError(
                    f"Generated {prefix} logical bundle is "
                    f"{actual_logical_bytes:,} actual bytes, exceeding its "
                    f"global cap of {int(global_cap_bytes):,} bytes. "
                    "The complete logical family will be removed."
                )

    except BaseException:
        for written_path in written_paths:
            try:
                written_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise

    return outputs


def _root_readme_entry(entries: Iterable[FileEntry]) -> FileEntry | None:
    """Return the root README.md entry when it exists."""
    return next(
        (
            entry
            for entry in entries
            if entry.rel_posix.replace("\\", "/").lower() == "readme.md"
        ),
        None,
    )


def _safe_mtime(entry: FileEntry) -> float:
    try:
        return entry.path.stat().st_mtime
    except OSError:
        return 0.0


def _estimated_bundle_bytes(entries: Iterable[FileEntry]) -> int:
    """Estimated emitted bundle bytes for a logical output."""
    return sum(int(e.est_bundle_bytes or 0) for e in entries)


def select_recent_segment_entries(
    entries: list[FileEntry],
    *,
    cap_mb: float,
) -> tuple[list[FileEntry], int, list[FileEntry]]:
    """Select README.md plus newest modified files for project_segment bundles.

    The cap is applied to estimated exported bundle bytes, not only raw file
    size, because delimiter/path overhead also affects upload size.

    Returns:
        (selected_entries, cap_bytes, omitted_entries_after_cap)
    """
    cap_bytes = int(float(cap_mb) * 1024 * 1024)
    if cap_bytes <= 0:
        raise ValueError("--recent-files-cap must be greater than 0")

    readme = next((e for e in entries if e.rel_posix.lower() == "readme.md"), None)

    selected: list[FileEntry] = []
    selected_rels: set[str] = set()
    used_bytes = 0

    if readme is not None:
        if readme.est_bundle_bytes > cap_bytes:
            raise ValueError(
                "README.md estimated bundle size "
                f"({readme.est_bundle_bytes:,} bytes) exceeds --recent-files-cap "
                f"({cap_bytes:,} bytes). Increase --recent-files-cap."
            )

        selected.append(readme)
        selected_rels.add(readme.rel_posix)
        used_bytes += readme.est_bundle_bytes

    candidates = sorted(
        (e for e in entries if e.rel_posix not in selected_rels),
        key=lambda e: (-_safe_mtime(e), e.rel_posix.lower()),
    )

    omitted: list[FileEntry] = []
    for entry in candidates:
        if used_bytes + entry.est_bundle_bytes > cap_bytes:
            # Do not stop at the first oversized/recent file. Continue scanning
            # older/smaller candidates so the cap is filled with as much useful
            # recent context as possible.
            omitted.append(entry)
            continue

        selected.append(entry)
        selected_rels.add(entry.rel_posix)
        used_bytes += entry.est_bundle_bytes

    return selected, cap_bytes, omitted


def write_full_random_bundles(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    versions: int,
    seed_base: int,
    prefix: str,
    target_bytes: int,
    global_cap_bytes: int | None = None,
) -> list[tuple[str, int, int, int]]:
    """Write reproducibly shuffled full-codebase bundle families.

    Each version contains the same complete source-file set in a different
    deterministic order. Version N uses ``seed_base + N - 1``.

    Returns:
        (physical_filename, version_seed, source_file_count, estimated_bytes)
    """
    entries = list(entries or [])
    versions = max(0, int(versions or 0))

    if not entries or versions == 0:
        return []

    outputs: list[tuple[str, int, int, int]] = []
    for version_index in range(versions):
        version_seed = int(seed_base) + version_index
        ordered_entries = list(entries)
        random.Random(version_seed).shuffle(ordered_entries)
        version_prefix = f"{prefix}_{version_index + 1:02d}"
        
        # Create a dedicated subfolder for each seed to avoid mixing files
        seed_dir = out_dir / str(version_seed)
        seed_dir.mkdir(parents=True, exist_ok=True)
        
        version_outputs = write_segmented_ordered_bundle(
            ordered_entries,
            seed_dir,
            target_bytes=target_bytes,
            prefix=version_prefix,
            global_cap_bytes=global_cap_bytes,
        )
        for filename, file_count, estimated_bytes in version_outputs:
            # Prefix with seed folder for clear console/manifest reporting
            reported_filename = f"{version_seed}/{filename}"
            outputs.append(
                (
                    reported_filename,
                    version_seed,
                    file_count,
                    estimated_bytes,
                )
            )
    return outputs


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
    slug = re.sub(r"_+", "_", slug).strip("._-")
    return slug or "subset"


def _clean_custom_request_line(raw: str, *, root: Path) -> str:
    r"""Normalize one line from a --custom file list.

    Accepts AI-friendly list formats:
      - `- app/foo.py`
      - ``app/foo.py``
      - `# \app\\foo.py`
      - `app\\foo.py`
      - absolute paths under project root
      - simple trailing notes after whitespace.
    """
    original = str(raw or "").strip()
    if not original:
        return ""

    line = re.sub(r"^\s*(?:[-*•]+|\d+[.)])\s*", "", original).strip()

    if line.startswith("#"):
        without_hash = line[1:].strip()
        if not any(ch in without_hash for ch in ("/", "\\", "*", "?", ".")):
            return ""
        line = without_hash

    code_match = re.search(r"`([^`]+)`", line)
    if code_match:
        line = code_match.group(1).strip()
    else:
        # Remove inline comments only when they are separated by whitespace.
        line = re.sub(r"\s+#.*$", "", line).strip()
        # Common AI explanation separators. Keep alternatives explicit; do not
        # include an empty regex alternative here, or normal whitespace can split
        # unexpectedly.
        line = re.split(r"\s+(?:--|\|\|)\s+", line, maxsplit=1)[0].strip()
        # If the first token is clearly path-like, ignore trailing prose.
        parts = line.split()
        if len(parts) > 1 and any(ch in parts[0] for ch in ("/", "\\", "*", "?", ".")):
            line = parts[0]

    line = line.strip().strip("`'\"").rstrip(",;")
    if not line:
        return ""

    line = line.replace("\\", "/")

    root_posix = root.resolve().as_posix().rstrip("/")
    root_l = root_posix.lower()
    line_l = line.lower()

    if line_l.startswith(root_l + "/"):
        line = line[len(root_posix) + 1 :]

    while line.startswith("./"):
        line = line[2:]

    # Handles pasted Windows-like "\smp\foo.py" after slash normalization.
    line = line.lstrip("/")

    return line.strip()


def _custom_request_tokens_from_line(raw: str, *, root: Path) -> list[str]:
    """Extract one or more requested entries from a custom-list line.

    Safe supported cases:
      - one item per line;
      - comma-separated items;
      - whitespace-separated path-like items;
      - backtick-quoted items;
      - mixed quoted and unquoted items in original request order;
      - bullet/numbered items;
      - blank lines between items;
      - inline prose after a single path.

    Paths with spaces should be wrapped in backticks. The project normally has
    no space-containing source paths, so unquoted parsing remains deliberately
    conservative and avoids guessing ordinary prose as paths.
    """
    line = str(raw or "").strip()
    if not line:
        return []

    candidates: list[tuple[str, bool]] = []  # (token, is_quoted)
    contains_quoted_item = bool(re.search(r"`[^`]+`", line))

    # Scan quoted and unquoted sections from left to right. Collecting every
    # quoted item first would reorder:
    #
    #   `smp/a.py`, smp/b.py, `smp/c.py`
    #
    # into a.py, c.py, b.py. Request order matters for custom-priority output
    # and for dependency-expansion seed priority under a byte cap.
    token_pattern = re.compile(r"`(?P<quoted>[^`]+)`|(?P<plain>[^`,]+)")

    for match in token_pattern.finditer(line):
        quoted = match.group("quoted")
        if quoted is not None:
            candidates.append((quoted, True))
            continue

        plain = str(match.group("plain") or "").strip()
        if not plain:
            continue

        parts = plain.split()
        pathish_parts = [
            part
            for part in parts
            if any(ch in part for ch in ("/", "\\", "*", "?", "."))
        ]

        if len(pathish_parts) >= 2:
            for part in pathish_parts:
                candidates.append((part, False))
        elif len(pathish_parts) == 1:
            if contains_quoted_item:
                candidates.append((pathish_parts[0], False))
            else:
                candidates.append((plain, False))

    out: list[str] = []
    seen: set[str] = set()

    for candidate, is_quoted in candidates:
        if is_quoted:
            # Preserve quoted token as-is (backticks already stripped)
            token = candidate.strip()
        else:
            token = _clean_custom_request_line(candidate, root=root)

        if not token:
            continue

        key = token.lower().replace("\\", "/")
        if key in seen:
            continue

        seen.add(key)
        out.append(token)

    return out


def _assert_custom_request_parser_regression_self_test() -> None:
    """Fail fast if mixed custom-list parsing drops or reorders paths."""
    root = Path.cwd()

    cases = (
        (
            "`app/a.py`, app/b.py, `app/c.py`",
            ["app/a.py", "app/b.py", "app/c.py"],
        ),
        (
            "`app/a.py` `app/b.py`",
            ["app/a.py", "app/b.py"],
        ),
        (
            "app/a.py, app/b.py",
            ["app/a.py", "app/b.py"],
        ),
        (
            "- `app/a.py`",
            ["app/a.py"],
        ),
        (
            "app/a.py explanation text",
            ["app/a.py"],
        ),
        (
            "`docs/path with spaces.md`, app/a.py",
            ["docs/path with spaces.md", "app/a.py"],
        ),
        (
            "`app/a.py`, app/a.py, `app/b.py`",
            ["app/a.py", "app/b.py"],
        ),
    )

    for raw, expected in cases:
        actual = _custom_request_tokens_from_line(raw, root=root)
        if actual != expected:
            raise RuntimeError(
                "context exporter custom-list parser self-test failed.\n"
                f"Input: {raw!r}\n"
                f"Expected: {expected!r}\n"
                f"Actual: {actual!r}"
            )


def _custom_token_matches_entry(token: str, entry: FileEntry) -> bool:
    token_l = str(token or "").strip().lower().replace("\\", "/")
    rel_l = entry.rel_posix.lower()
    name_l = PurePosixPath(rel_l).name

    if not token_l:
        return False

    has_wildcard = any(ch in token_l for ch in "*?[")
    has_slash = "/" in token_l

    if has_wildcard:
        if has_slash:
            return PurePosixPath(rel_l).match(token_l) or fnmatch.fnmatch(
                rel_l,
                token_l,
            )
        return fnmatch.fnmatch(name_l, token_l)

    if has_slash:
        token_dir = token_l.rstrip("/")
        if rel_l == token_dir:
            return True

        # Treat directory-like entries as recursive include roots.
        if token_l.endswith("/") or PurePosixPath(token_l).suffix == "":
            if rel_l.startswith(token_dir + "/"):
                return True

        return False

    return name_l == token_l


def select_custom_subset_entries(
    entries: list[FileEntry],
    custom_list_path: Path,
    *,
    root: Path,
) -> tuple[list[FileEntry], list[str]]:
    """Select README.md plus files requested by --custom list.

    Returns:
        (selected_entries, unresolved_tokens)
    """
    root = root.resolve()
    custom_list_path = Path(custom_list_path).expanduser()
    if not custom_list_path.is_absolute():
        custom_list_path = root / custom_list_path

    custom_list_path = custom_list_path.resolve()

    try:
        raw_lines = custom_list_path.read_text(
            encoding="utf-8", errors="replace"
        ).splitlines()
    except OSError as exc:
        raise OSError(
            f"could not read --custom list file {custom_list_path}: {exc}"
        ) from exc

    selected: list[FileEntry] = []
    seen: set[str] = set()
    unresolved: list[str] = []

    def _add(entry: FileEntry | None) -> None:
        if entry is None:
            return
        if entry.rel_posix in seen:
            return
        seen.add(entry.rel_posix)
        selected.append(entry)

    # Always include root README.md when present, as requested.
    _add(_root_readme_entry(entries))

    seen_request_tokens: set[str] = set()
    unresolved_seen: set[str] = set()

    for raw in raw_lines:
        tokens = _custom_request_tokens_from_line(raw, root=root)
        if not tokens:
            continue

        for token in tokens:
            token_key = token.lower().replace("\\", "/")
            if token_key in seen_request_tokens:
                continue
            seen_request_tokens.add(token_key)

            matches = [e for e in entries if _custom_token_matches_entry(token, e)]

            # A stale/wrong directory may still be recoverable when exactly one
            # repository file has the requested basename. Never accept an
            # ambiguous basename fallback: including several unrelated
            # same-named files makes the generated review context misleading.
            if not matches and "/" in token and not any(ch in token for ch in "*?["):
                wanted_name = PurePosixPath(token.lower()).name
                basename_matches = [
                    e
                    for e in entries
                    if PurePosixPath(e.rel_posix.lower()).name == wanted_name
                ]

                if len(basename_matches) == 1:
                    matches = basename_matches
                elif len(basename_matches) > 1:
                    log_candidates = ", ".join(
                        sorted(e.rel_posix for e in basename_matches)[:12]
                    )
                    print(
                        "Ambiguous custom-list basename fallback refused for "
                        f"{token!r}. Candidates: {log_candidates}",
                        file=sys.stderr,
                    )

            matches.sort(key=lambda e: e.rel_posix.lower())

            if not matches:
                if token_key not in unresolved_seen:
                    unresolved_seen.add(token_key)
                    unresolved.append(token)
                continue

            for entry in matches:
                _add(entry)

    return selected, unresolved


def custom_subset_header(
    *,
    custom_source: str,
    entries: list[FileEntry],
    unresolved_tokens: list[str],
    segment_index: int | None = None,
    segment_count: int | None = None,
) -> str:
    file_lines = "\n".join(f"- {e.rel_posix}" for e in entries)
    unresolved_lines = "\n".join(f"- {x}" for x in unresolved_tokens)

    segment_line = ""
    files_label = "Files in this custom subset:"
    if segment_index is not None and segment_count is not None:
        segment_line = f"\nSegment: {segment_index} of {segment_count}"
        files_label = "Files in this custom subset segment:"

    unresolved_section = ""
    if unresolved_tokens:
        unresolved_section = f"""
Unresolved requested entries/patterns:
{unresolved_lines}
"""

    return f"""# Custom subset bundle{segment_line}

Source list:
{custom_source}

README.md is included automatically when present. Requested entries can be exact
relative paths, Windows-style paths, filenames, directories, or glob patterns.

{unresolved_section}
{files_label}
{file_lines}
"""


def write_custom_subset_bundles(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    target_bytes: int,
    prefix: str,
    custom_source: str,
    unresolved_tokens: list[str],
    global_cap_bytes: int | None = None,
) -> list[tuple[str, int, int, int, int]]:
    """Write segmented custom subset bundles.

    Returns tuples:
        (filename, segment_index, segment_count, file_count, est_bytes)
    """
    entries = list(entries or [])
    if not entries:
        return []

    segment_outputs = write_segmented_ordered_bundle(
        entries,
        out_dir,
        target_bytes=target_bytes,
        prefix=prefix,
        global_cap_bytes=global_cap_bytes,
        header_factory=lambda group, idx, total: custom_subset_header(
            custom_source=custom_source,
            entries=group,
            unresolved_tokens=unresolved_tokens,
            segment_index=idx,
            segment_count=total,
        ),
    )

    total = len(segment_outputs)
    return [
        (name, idx, total, n_files, est_bytes)
        for idx, (name, n_files, est_bytes) in enumerate(segment_outputs, start=1)
    ]


def write_custom_subset_file_list(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    prefix: str,
    custom_source: str,
    unresolved_tokens: list[str],
) -> str | None:
    """Write a deduplicated relative-path list for files included in custom segments.

    The list reflects exactly the files selected for the custom bundle segments,
    including README.md when auto-included.
    """
    if not entries:
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{prefix}_files.txt"

    seen: set[str] = set()
    rels: list[str] = []
    for entry in entries:
        rel = str(entry.rel_posix or "").strip()
        if rel and rel not in seen:
            seen.add(rel)
            rels.append(rel)

    lines = [
        "# Custom subset included files",
        f"# Source list: {custom_source}",
        "# One deduplicated relative path per line.",
    ]

    if unresolved_tokens:
        lines.extend(
            [
                "#",
                "# Unresolved requested entries/patterns:",
                *[f"# - {token}" for token in unresolved_tokens],
            ]
        )

    lines.append("")
    lines.extend(rels)

    write_marked_text_file(
        out_path,
        "\n".join(lines) + "\n",
    )

    return out_path.name


def select_custom_priority_project_entries(
    project_entries: list[FileEntry],
    all_entries: list[FileEntry],
    custom_list_paths: Iterable[Path | str],
    *,
    root: Path,
) -> tuple[list[FileEntry], list[str], int]:
    """Order project_segment input for AI-requested custom-priority review.

    Order is intentionally deterministic and non-random:

      1) root README.md, when present;
      2) deduplicated custom-list files, in the order requested;
      3) remaining prior project_segment input files, newest modified -> oldest.

    This makes early project_segment files contain the context the AI/user
    explicitly requested. If an upload/truncation system keeps only the first
    few segments, it loses the oldest edited files first.

    Returns:
        (ordered_entries, unresolved_tokens_with_source, forced_count)

    `forced_count` counts priority files that were not already present in the
    prior project_segment input set, e.g. because --recent-files-cap or
    --exclude had omitted them. Custom-priority intentionally forces those
    requested files back into the output, but the non-priority tail still
    respects the prior project_segment input set.
    """
    priority_entries: list[FileEntry] = []
    unresolved_with_source: list[str] = []

    for custom_source in custom_list_paths or []:
        selected, unresolved = select_custom_subset_entries(
            all_entries,
            Path(custom_source),
            root=root,
        )
        priority_entries.extend(selected)

        for token in unresolved:
            unresolved_with_source.append(f"{custom_source}: {token}")

    project_rels = {e.rel_posix for e in project_entries}

    ordered: list[FileEntry] = []
    seen: set[str] = set()
    forced_count = 0

    def _add(entry: FileEntry | None, *, count_forced: bool) -> None:
        nonlocal forced_count

        if entry is None:
            return
        if entry.rel_posix in seen:
            return

        seen.add(entry.rel_posix)
        ordered.append(entry)

        if count_forced and entry.rel_posix not in project_rels:
            forced_count += 1

    # 1) README.md always first when present.
    _add(_root_readme_entry(all_entries), count_forced=True)

    # 2) Then custom.txt items in requested order, deduplicated.
    for entry in priority_entries:
        if entry.rel_posix.lower() == "readme.md":
            continue
        _add(entry, count_forced=True)

    # 3) Then the rest of the prior project_segment input set, newest modified
    # -> oldest. Do not use all_entries here: that would silently undo
    # --recent-files-cap / --exclude for every non-priority file.
    remaining = sorted(
        project_entries,
        key=lambda e: (-_safe_mtime(e), e.rel_posix.lower()),
    )
    for entry in remaining:
        _add(entry, count_forced=False)

    return ordered, unresolved_with_source, forced_count


def write_ordered_project_segment_bundles(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    target_bytes: int,
    count: int,
    prefix: str,
    global_cap_bytes: int | None = None,
) -> list[tuple[str, int, int]]:
    """Write project segments in exact requested entry order.

    Oversized source files are divided at complete source-line boundaries.
    Custom-priority order is therefore retained while no physical TXT file is
    allowed to silently contain an oversized source file.
    """
    entries = list(entries or [])
    if not entries:
        return []

    physical_target = max(
        1,
        int(target_bytes or DEFAULT_SEGMENT_TARGET_BYTES),
    )
    requested_count = max(0, int(count or 0))

    if requested_count > 0:
        logical_est_bytes = _estimated_bundle_bytes(entries)
        approximate_target = max(
            1,
            (logical_est_bytes + requested_count - 1) // requested_count,
        )
        effective_target = min(physical_target, approximate_target)
    else:
        effective_target = physical_target

    return write_segmented_ordered_bundle(
        entries,
        out_dir,
        target_bytes=effective_target,
        prefix=prefix,
        global_cap_bytes=global_cap_bytes,
    )


def write_project_segment_file_list(
    entries: list[FileEntry],
    out_dir: Path,
    *,
    prefix: str,
    recent_cap_mb: float | None = None,
    recent_cap_bytes: int | None = None,
    omitted_entries: list[FileEntry] | None = None,
    custom_priority_sources: list[str] | None = None,
    custom_priority_unresolved: list[str] | None = None,
    custom_priority_forced_count: int = 0,
) -> str | None:
    """Write the exact file list included in project_segment_*.txt bundles.

    This is generated whenever project_segment bundles are generated, regardless
    of command-line mode. It saves the user from opening every segment just to
    see which source files were included.
    """
    if not entries:
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{prefix}_files.txt"

    seen: set[str] = set()
    rels: list[str] = []
    total_est = 0

    for entry in entries:
        rel = str(entry.rel_posix or "").strip()
        if not rel or rel in seen:
            continue
        seen.add(rel)
        rels.append(rel)
        total_est += int(entry.est_bundle_bytes or 0)

    omitted_entries = list(omitted_entries or [])
    custom_priority_sources = list(custom_priority_sources or [])
    custom_priority_unresolved = list(custom_priority_unresolved or [])

    buffer = io.StringIO()
    f = buffer

    f.write("# Project segment included files\n")
    f.write("# Generated by context_exporter.py.\n")
    f.write("# One deduplicated relative path per line.\n")
    f.write(f"# segment_prefix: {prefix}\n")
    f.write(f"# selected_files: {len(rels)}\n")
    f.write(f"# selected_estimated_bundle_bytes: {total_est:,}\n")

    if recent_cap_mb is not None:
        f.write("# selection_mode: recent-files-cap\n")
        f.write(f"# recent_files_cap_mib: {float(recent_cap_mb):g}\n")
        if recent_cap_bytes is not None:
            f.write(f"# recent_files_cap_bytes: {int(recent_cap_bytes):,}\n")
        f.write(f"# omitted_after_cap: {len(omitted_entries)}\n")
        if omitted_entries:
            f.write("# first_omitted_file: " + omitted_entries[0].rel_posix + "\n")

    else:
        f.write("# selection_mode: full project_segment input set\n")

    if custom_priority_sources:
        f.write("# custom_priority_mode: true\n")
        for source in custom_priority_sources:
            f.write(f"# custom_priority_source: {source}\n")
        f.write(
            "# custom_priority_forced_files_not_in_original_project_input: "
            f"{int(custom_priority_forced_count)}\n"
        )
        if custom_priority_unresolved:
            f.write(
                "# custom_priority_unresolved_count: "
                f"{len(custom_priority_unresolved)}\n"
            )
            for token in custom_priority_unresolved[:50]:
                f.write(f"# custom_priority_unresolved: {token}\n")
    else:
        f.write("# custom_priority_mode: false\n")

    # Always separate metadata comments from the path body. Custom-priority
    # mode previously omitted this documented separator.
    f.write("\n")

    for rel in rels:
        f.write(rel + "\n")

    write_marked_text_file(
        out_path,
        buffer.getvalue(),
    )

    return out_path.name


def write_manifest(
    out_path: Path,
    *,
    root: Path,
    seed: int,
    valid_entries: list[FileEntry],
    project_entries: list[FileEntry],
    project_outputs: list[tuple[str, int, int]],
    random_outputs: list[tuple[str, int, int, int]],
    custom_outputs: list[tuple[str, str, int, int, int, int]],
    segment_target_bytes: int,
    recent_files_cap_mb: float | None,
    recent_files_cap_bytes: int | None,
    recent_files_omitted_entries: list[FileEntry],
    custom_priority_sources: list[str],
    custom_priority_unresolved: list[str],
    global_cap_bytes: int | None,
) -> None:
    """Write a generic summary of generated repository context files."""
    lines = [
        "# AI context export manifest",
        "",
        f"Generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Repository root: `{root}`",
        f"Base random seed: `{seed}`",
        f"Valid repository files: {len(valid_entries)}",
        (
            "Valid repository estimated bytes: "
            f"{sum(entry.est_bundle_bytes for entry in valid_entries):,}"
        ),
        f"Physical segment target bytes: {segment_target_bytes:,}",
    ]

    if global_cap_bytes is not None:
        lines.append(f"Global logical output cap: {global_cap_bytes:,} bytes")

    lines.extend(
        [
            "",
            "## Randomized full-codebase bundles",
            "",
        ]
    )

    if random_outputs:
        for (
            filename,
            version_seed,
            file_count,
            estimated_bytes,
        ) in random_outputs:
            lines.append(
                f"- `{filename}` — seed `{version_seed}`, "
                f"source files `{file_count}`, "
                f"estimated payload bytes `{estimated_bytes:,}`"
            )
    else:
        lines.append("- No randomized full-codebase bundles were requested.")

    lines.extend(
        [
            "",
            "## Project segments",
            "",
            f"Selected project files: {len(project_entries)}",
            (
                "Selected project estimated bytes: "
                f"{sum(entry.est_bundle_bytes for entry in project_entries):,}"
            ),
        ]
    )

    if recent_files_cap_mb is not None:
        lines.extend(
            [
                (
                    f"Recent-files cap: {recent_files_cap_mb:g} MiB "
                    f"({int(recent_files_cap_bytes or 0):,} bytes)"
                ),
                (
                    "Files omitted by recent-files selection cap: "
                    f"{len(recent_files_omitted_entries)}"
                ),
            ]
        )

    if custom_priority_sources:
        lines.extend(
            [
                "",
                "Custom-priority sources:",
                *[f"- `{source}`" for source in custom_priority_sources],
                (
                    "Unresolved custom-priority entries: "
                    f"{len(custom_priority_unresolved)}"
                ),
            ]
        )

        if custom_priority_unresolved:
            lines.extend(f"- `{item}`" for item in custom_priority_unresolved)

    lines.append("")

    if project_outputs:
        for filename, file_count, estimated_bytes in project_outputs:
            lines.append(
                f"- `{filename}` — source files `{file_count}`, "
                f"estimated payload bytes `{estimated_bytes:,}`"
            )
    else:
        lines.append("- No project segments were written.")

    lines.extend(
        [
            "",
            "## Custom subsets",
            "",
        ]
    )

    if custom_outputs:
        for (
            source,
            filename,
            segment_index,
            segment_count,
            file_count,
            estimated_bytes,
        ) in custom_outputs:
            lines.append(
                f"- `{filename}` — source `{source}`, "
                f"segment `{segment_index}/{segment_count}`, "
                f"files `{file_count}`, "
                f"estimated payload bytes `{estimated_bytes:,}`"
            )
    else:
        lines.append("- No custom subsets were requested.")

    out_path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Export generic repository context for AI review."
    )

    ap.add_argument(
        "--root",
        default=".",
        help="Project root to scan.",
    )
    ap.add_argument(
        "--output-dir",
        default="ai_chunks",
        help="Output directory, relative to project root unless absolute.",
    )
    ap.add_argument(
        "--max-file-size",
        type=int,
        default=2 * 1024 * 1024,
        help="Maximum individual source file size accepted during scanning.",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "Base seed for --full-versions. If omitted, a random base seed is "
            "generated and printed. Normal project output remains deterministic."
        ),
    )
    ap.add_argument(
        "--segment-count",
        type=int,
        default=DEFAULT_SEGMENT_COUNT,
        help=(
            "Best-effort physical segment count. Use 0 to segment according "
            "to --segment-target-bytes."
        ),
    )
    ap.add_argument(
        "--segment-target-bytes",
        type=int,
        default=None,
        help=(
            "Maximum physical TXT size target. Defaults to 900 KiB, or "
            "400 KiB with --small-chunks."
        ),
    )
    ap.add_argument(
        "--small-chunks",
        action="store_true",
        help="Use a 400 KiB physical segment target.",
    )
    ap.add_argument(
        "--segment-prefix",
        default="project_segment",
        help="Logical prefix for deterministic whole-project segments.",
    )
    ap.add_argument(
        "--full-versions",
        type=int,
        default=DEFAULT_FULL_VERSIONS,
        metavar="N",
        help=(
            "Generate N reproducibly shuffled full-codebase bundle families. "
            "Version N uses --seed plus N minus one. Default: 0."
        ),
    )
    ap.add_argument(
        "--full-prefix",
        default="project_contents_seed",
        help=(
            "Base logical prefix for randomized full-codebase bundles. "
            "The active base seed is appended automatically."
        ),
    )
    ap.add_argument(
        "--recent-files-cap",
        type=positive_float,
        default=None,
        metavar="MB",
        help=(
            "Write README.md plus newest-modified files whose estimated total "
            "fits within this MiB cap."
        ),
    )
    ap.add_argument(
        "--global-files-cap",
        type=positive_float,
        default=None,
        metavar="MB",
        help=(
            "Skip a logical output if its complete generated size exceeds "
            "this MiB cap."
        ),
    )
    ap.add_argument(
        "--custom",
        action="append",
        default=[],
        metavar="TXT",
        help=(
            "Write an additional custom subset from a text file containing "
            "paths, filenames, directories, or glob patterns. May be repeated."
        ),
    )
    ap.add_argument(
        "--custom-priority",
        action="append",
        default=[],
        metavar="TXT",
        help=(
            "Place README.md and files requested by TXT first in normal "
            "project segments, followed by remaining project files."
        ),
    )
    ap.add_argument(
        "--no-segments",
        action="store_true",
        help="Do not write whole-project project_segment files.",
    )
    ap.add_argument(
        "--exclude-dirs",
        nargs="*",
        default=[
            ".git",
            "node_modules",
            "venv",
            ".venv",
            ".idea",
            ".vs",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".vscodecounter",
            "logs",
            "exports",
            "backups",
            "ai_chunks",
        ],
    )
    ap.add_argument(
        "--exclude-exts",
        nargs="*",
        default=[
            ".pyc",
            ".pyo",
            ".pyd",
            ".log",
            ".tmp",
            ".bak",
            ".sqlite",
            ".sqlite3",
            ".db",
        ],
    )

    args = ap.parse_args()

    try:
        _assert_redaction_regression_self_test()
        _assert_segment_filename_regression_self_test()
        _assert_custom_request_parser_regression_self_test()
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.segment_target_bytes is None:
        segment_target_bytes = (
            DEFAULT_SMALL_SEGMENT_TARGET_BYTES
            if args.small_chunks
            else DEFAULT_SEGMENT_TARGET_BYTES
        )
    else:
        segment_target_bytes = int(args.segment_target_bytes)

    if segment_target_bytes <= 0:
        print(
            "Error: --segment-target-bytes must be greater than zero.",
            file=sys.stderr,
        )
        return 2

    if args.segment_count < 0:
        print(
            "Error: --segment-count cannot be negative.",
            file=sys.stderr,
        )
        return 2

    if args.full_versions < 0:
        print(
            "Error: --full-versions cannot be negative.",
            file=sys.stderr,
        )
        return 2

    custom_sources = [str(value) for value in args.custom if str(value or "").strip()]
    custom_priority_sources = [
        str(value) for value in args.custom_priority if str(value or "").strip()
    ]

    if args.no_segments and custom_priority_sources:
        print(
            "Error: --custom-priority affects project segments, but "
            "--no-segments disables project segments.",
            file=sys.stderr,
        )
        return 2

    if args.no_segments and args.recent_files_cap is not None:
        print(
            "Error: --recent-files-cap affects project segments, but "
            "--no-segments disables project segments.",
            file=sys.stderr,
        )
        return 2

    root = Path(args.root).expanduser().resolve()

    output_dir_arg = Path(args.output_dir).expanduser()
    out_dir = (
        output_dir_arg.resolve()
        if output_dir_arg.is_absolute()
        else (root / output_dir_arg).resolve()
    )

    try:
        out_dir.relative_to(root)
    except ValueError:
        pass
    else:
        if out_dir == root:
            print(
                "Error: --output-dir cannot be the repository root.",
                file=sys.stderr,
            )
            return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    exclude_dirs = {
        str(directory).strip().lower()
        for directory in args.exclude_dirs
        if str(directory).strip()
    }

    # Prevent a custom output-directory name under the repository from being
    # scanned on later runs.
    try:
        output_relative = out_dir.relative_to(root)
    except ValueError:
        output_relative = None

    if output_relative is not None and output_relative.parts:
        exclude_dirs.add(output_relative.parts[0].lower())

    exclude_exts = {
        extension.lower() if extension.startswith(".") else f".{extension.lower()}"
        for extension in args.exclude_exts
    }

    exclude_names = {
        ".env",
        "context_export_manifest.md",
        "topic_chunks_manifest.md",
        "ai_review_readme.txt",
        "scan_string_literals_report.txt",
        "logger_calls.txt",
    }

    removed_outputs = cleanup_generated_outputs(out_dir)

    global_cap_bytes = mib_to_bytes(args.global_files_cap)
    seed = args.seed if args.seed is not None else random.randrange(2**31)

    print(f"Scanning: {root}")
    print(f"Output directory: {out_dir}")
    print(f"Segment target: {segment_target_bytes:,} bytes")
    print(f"Seed: {seed}")

    if removed_outputs:
        print(f"Removed stale generated files: {removed_outputs}")

    entries = iter_valid_files(
        root,
        max_file_size=args.max_file_size,
        exclude_dirs=exclude_dirs,
        exclude_exts=exclude_exts,
        exclude_names=exclude_names,
    )

    print(f"Valid files: {len(entries)}")
    print(
        "Estimated repository bundle bytes: "
        f"{sum(entry.est_bundle_bytes for entry in entries):,}"
    )

    project_entries = list(entries)
    recent_files_cap_bytes: int | None = None
    recent_files_omitted_entries: list[FileEntry] = []

    if args.recent_files_cap is not None:
        try:
            (
                project_entries,
                recent_files_cap_bytes,
                recent_files_omitted_entries,
            ) = select_recent_segment_entries(
                project_entries,
                cap_mb=args.recent_files_cap,
            )
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

        print(
            "Recent-files selection: "
            f"{len(project_entries)} selected, "
            f"{len(recent_files_omitted_entries)} omitted, "
            f"cap={recent_files_cap_bytes:,} bytes."
        )
    else:
        # Deterministic whole-project ordering.
        project_entries.sort(key=lambda entry: entry.rel_posix.lower())

    custom_priority_unresolved: list[str] = []
    custom_priority_forced_count = 0

    if custom_priority_sources:
        try:
            (
                project_entries,
                custom_priority_unresolved,
                custom_priority_forced_count,
            ) = select_custom_priority_project_entries(
                project_entries,
                entries,
                custom_priority_sources,
                root=root,
            )
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

        print(
            "Custom-priority mode: "
            f"{len(custom_priority_sources)} source list(s), "
            f"{custom_priority_forced_count} requested file(s) forced back "
            "into project output."
        )

        if custom_priority_unresolved:
            print("Unresolved custom-priority requests:")
            for item in custom_priority_unresolved:
                print(f"  - {item}")

    project_outputs: list[tuple[str, int, int]] = []

    if not args.no_segments:
        project_outputs = write_ordered_project_segment_bundles(
            project_entries,
            out_dir,
            target_bytes=segment_target_bytes,
            count=args.segment_count,
            prefix=args.segment_prefix,
            global_cap_bytes=global_cap_bytes,
        )

        for filename, file_count, estimated_bytes in project_outputs:
            print(
                f"Writing project segment: {filename} "
                f"({file_count} files, est={estimated_bytes:,} bytes)"
            )

        if project_outputs:
            file_list_name = write_project_segment_file_list(
                project_entries,
                out_dir,
                prefix=args.segment_prefix,
                recent_cap_mb=args.recent_files_cap,
                recent_cap_bytes=recent_files_cap_bytes,
                omitted_entries=recent_files_omitted_entries,
                custom_priority_sources=custom_priority_sources,
                custom_priority_unresolved=custom_priority_unresolved,
                custom_priority_forced_count=custom_priority_forced_count,
            )

            if file_list_name:
                print(f"Writing project file list: {file_list_name}")
    else:
        print("Whole-project segments disabled by --no-segments.")

    random_outputs: list[tuple[str, int, int, int]] = []

    if args.full_versions > 0:
        base_full_prefix = (
            str(args.full_prefix or "").strip() or "project_contents_seed"
        )
        seeded_full_prefix = f"{base_full_prefix}_{seed}"

        random_outputs = write_full_random_bundles(
            entries,
            out_dir,
            versions=args.full_versions,
            seed_base=seed,
            prefix=seeded_full_prefix,
            target_bytes=segment_target_bytes,
            global_cap_bytes=global_cap_bytes,
        )

        for (
            filename,
            version_seed,
            file_count,
            estimated_bytes,
        ) in random_outputs:
            print(
                f"Writing randomized full bundle: {filename} "
                f"(seed={version_seed}, files={file_count}, "
                f"est={estimated_bytes:,} bytes)"
            )
    else:
        print(
            "Randomized full-codebase bundles disabled. "
            "Use --full-versions N to generate them."
        )

    custom_outputs: list[tuple[str, str, int, int, int, int]] = []

    for custom_index, custom_source in enumerate(custom_sources, start=1):
        try:
            custom_entries, unresolved_tokens = select_custom_subset_entries(
                entries,
                Path(custom_source),
                root=root,
            )
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

        if len(custom_sources) == 1:
            custom_prefix = "custom_subset"
        else:
            custom_prefix = (
                f"custom_subset_{custom_index:02d}_"
                f"{_safe_slug(Path(custom_source).stem)}"
            )

        if unresolved_tokens:
            print(f"Unresolved entries from {custom_source}:")
            for token in unresolved_tokens:
                print(f"  - {token}")

        segment_outputs = write_custom_subset_bundles(
            custom_entries,
            out_dir,
            target_bytes=segment_target_bytes,
            prefix=custom_prefix,
            custom_source=custom_source,
            unresolved_tokens=unresolved_tokens,
            global_cap_bytes=global_cap_bytes,
        )

        if segment_outputs:
            file_list_name = write_custom_subset_file_list(
                custom_entries,
                out_dir,
                prefix=custom_prefix,
                custom_source=custom_source,
                unresolved_tokens=unresolved_tokens,
            )

            if file_list_name:
                print(f"Writing custom file list: {file_list_name}")

        for (
            filename,
            segment_index,
            segment_count,
            file_count,
            estimated_bytes,
        ) in segment_outputs:
            print(
                f"Writing custom segment {segment_index}/{segment_count}: "
                f"{filename} ({file_count} files, "
                f"est={estimated_bytes:,} bytes)"
            )

            custom_outputs.append(
                (
                    custom_source,
                    filename,
                    segment_index,
                    segment_count,
                    file_count,
                    estimated_bytes,
                )
            )

    manifest_path = out_dir / "context_export_manifest.md"

    write_manifest(
        manifest_path,
        root=root,
        seed=seed,
        valid_entries=entries,
        project_entries=project_entries,
        project_outputs=project_outputs,
        random_outputs=random_outputs,
        custom_outputs=custom_outputs,
        segment_target_bytes=segment_target_bytes,
        recent_files_cap_mb=args.recent_files_cap,
        recent_files_cap_bytes=recent_files_cap_bytes,
        recent_files_omitted_entries=recent_files_omitted_entries,
        custom_priority_sources=custom_priority_sources,
        custom_priority_unresolved=custom_priority_unresolved,
        global_cap_bytes=global_cap_bytes,
    )

    print(f"Writing manifest: {manifest_path.name}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
