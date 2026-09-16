#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import collections
import io
import os
import re
import shutil
import subprocess
import tokenize
import unicodedata
import warnings as py_warnings
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from collections.abc import Callable, Iterable

SKIP_DIRS = {
    ".git",
    "__pycache__",
    "backups",
    "data",
    ".venv",
    "venv",
    "env",
    "build",
    "dist",
    "node_modules",
}

AI_COPY_SKIP_DIRS = {
    *SKIP_DIRS,
    ".vscodecounter",
    ".vscode",
    ".idea",
    ".vs",
    ".mypy_cache",
    "pylint_reports",
    "logs",
    "data",
    "exports",
    "backups",
    "ai_chunks",
    "old",
    "old_version",
    "older_version",
}

AI_COPY_SKIP_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".log",
    ".tmp",
    ".bak",
    ".sqlite",
    ".sqlite3",
    ".db",
    ".dump",
    ".7z",
    ".zip",
}

AI_COPY_SKIP_FILENAMES = {
    ".env",
    "config.yaml",
    "config.yml",
    "scan_string_literals_report.txt",
    "ai_review_readme.txt",
}

DEFAULT_7Z_EXE_CANDIDATES = (
    Path(r"C:\Program Files\PeaZip\res\bin\7z\7z.exe"),
    Path(r"C:\Program Files\7-Zip\7z.exe"),
)

DEFAULT_7Z_BACKUP_DIR = Path.home() / "Desktop" / "l2_liquidity_shock_analyzer_backup"

DEFAULT_7Z_EXCLUDE_PATTERNS = (
    "logs",
    "old",
    "old_version",
    "older_version",
    ".mypy_cache",
    "pylint_reports",
    ".VSCodeCounter",
    "exports",
    "ai_chunks",
    "backups",
    "data",
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.log",
    "*.tmp",
    "*.bak",
)

FSTRING_START_TYPE = getattr(tokenize, "FSTRING_START", None)
FSTRING_MIDDLE_TYPE = getattr(tokenize, "FSTRING_MIDDLE", None)
FSTRING_END_TYPE = getattr(tokenize, "FSTRING_END", None)

STRING_PREFIX_RE = re.compile(
    r"(?is)^(?P<prefix>[rubf]*)" r'(?P<quote>"""|\'\'\'|"|\')'
)


@dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    col: int
    char: str
    codepoint: str
    context_start: int
    context_end: int


@dataclass(frozen=True)
class Edit:
    start: int
    end: int
    replacement: str


def read_source_with_encoding(path: Path) -> tuple[str, str]:
    with path.open("rb") as f:
        encoding, _ = tokenize.detect_encoding(f.readline)
        f.seek(0)
        data = f.read().decode(encoding)
    return data, encoding


def build_line_offsets(source: str) -> list[int]:
    offsets = [0]
    pos = 0
    for line in source.splitlines(keepends=True):
        pos += len(line)
        offsets.append(pos)
    return offsets


def pos_to_offset(line_offsets: list[int], line: int, col: int) -> int:
    if line < 1 or line > len(line_offsets):
        raise ValueError(f"invalid token line {line}")
    offset = line_offsets[line - 1] + col
    if offset < 0 or offset > line_offsets[-1]:
        raise ValueError(f"invalid token position line={line} col={col}")
    return offset


def offset_to_line_col(
    token_text: str, offset: int, start_line: int, start_col: int
) -> tuple[int, int]:
    before = token_text[:offset]
    line = start_line + before.count("\n")
    last_nl = before.rfind("\n")
    col = start_col + offset if last_nl == -1 else offset - last_nl - 1
    return line, col


def is_variation_selector_codepoint(code: int) -> bool:
    return (0xFE00 <= code <= 0xFE0F) or (0xE0100 <= code <= 0xE01EF)


EXTRA_SANITIZATION_CODEPOINTS = {
    0x00A0,  # no-break space
    0x200B,  # zero width space
    0x200C,  # zero width non-joiner
    0x200D,  # zero width joiner, common in emoji sequences
    0x2060,  # word joiner
    0x20E3,  # combining enclosing keycap
    0xFEFF,  # zero width no-break space / BOM if it appears inside a string
}


def is_extra_sanitization_char(ch: str) -> bool:
    code = ord(ch)
    return (
        is_variation_selector_codepoint(code) or code in EXTRA_SANITIZATION_CODEPOINTS
    )


def build_char_predicate(
    chars: str | None,
    *,
    include_extra_marks: bool = True,
    all_non_ascii: bool = False,
) -> Callable[[str], bool]:
    """
    Matching modes:

      1. If --chars is provided:
         exact-character matching is used.

      2. If --all-nonascii is provided:
         every non-ASCII character is matched. This is strongest for AI-review
         copies because it turns all Unicode inside safe string literals into
         ASCII-only Python escapes.

      3. Default:
         matches non-ASCII Unicode punctuation/symbols, plus selected
         invisible/emoji marks.

    The default catches arrows, bullets, checkmarks, em dashes, box drawing
    chars, emoji, math symbols, and emoji variation selectors while avoiding
    ordinary non-ASCII letters.
    """
    if chars:
        wanted = set(chars)
        return lambda ch: ch in wanted

    if all_non_ascii:
        return lambda ch: ord(ch) > 127

    def pred(ch: str) -> bool:
        if ord(ch) <= 127:
            return False
        cat = unicodedata.category(ch)
        if cat[0] in {"P", "S"}:
            return True
        if include_extra_marks and is_extra_sanitization_char(ch):
            return True
        return False

    return pred


def split_string_token(token_text: str) -> tuple[str, str, str] | None:
    """
    Return (prefix, quote, body) for a Python string token.

    This intentionally does not parse escape sequences. The body returned here
    is source text between the opening and closing quotes.
    """
    m = STRING_PREFIX_RE.match(token_text)
    if not m:
        return None

    prefix = m.group("prefix")
    quote = m.group("quote")

    if len(token_text) < len(prefix) + 2 * len(quote):
        return None
    if not token_text.endswith(quote):
        return None

    body = token_text[len(prefix) + len(quote) : -len(quote)]
    return prefix, quote, body


def is_safe_to_rewrite(prefix: str) -> bool:
    p = prefix.lower()
    return ("r" not in p) and ("b" not in p)


def is_legacy_fstring(prefix: str) -> bool:
    return "f" in prefix.lower()


def escape_char(ch: str) -> str:
    code = ord(ch)
    if code <= 0xFFFF:
        return f"\\u{code:04X}"
    return f"\\U{code:08X}"


def escape_text_for_ascii_report(text: str) -> str:
    """
    Render text using ASCII-safe notation.

    This is for reports only. It does not change source files. It prevents AI
    websites from silently dropping visible Unicode in the report/context lines.
    """
    parts: list[str] = []

    for ch in text:
        code = ord(ch)

        if ch == "\t":
            parts.append(ch)
        elif ch == "\n":
            parts.append("\\n")
        elif ch == "\r":
            parts.append("\\r")
        elif code < 32 or code == 127:
            parts.append(f"\\x{code:02X}")
        elif code > 127:
            parts.append(escape_char(ch))
        else:
            parts.append(ch)

    return "".join(parts)


def safe_for_report(value: object, ascii_safe: bool) -> str:
    text = str(value)
    return escape_text_for_ascii_report(text) if ascii_safe else text


def format_char(ch: str) -> str:
    name = unicodedata.name(ch, "UNNAMED")
    return f"{ascii(ch)} ({escape_char(ch)}, U+{ord(ch):04X}, {name})"


def count_matches(text: str, char_pred: Callable[[str], bool]) -> int:
    return sum(1 for ch in text if char_pred(ch))


def rewrite_text_fragment(
    text: str, char_pred: Callable[[str], bool]
) -> tuple[str, int]:
    """
    Replace matched literal source characters with Unicode escapes.

    Important safety detail:
    If the generated escape would immediately follow an odd-length run of
    backslashes, add one extra backslash first. Without that, source like "\\X"
    where X is replaced by "\\uXXXX" can accidentally turn the previous
    backslash into part of the escape sequence and change the runtime value.
    """
    parts: list[str] = []
    replacements = 0
    trailing_backslashes = 0

    for ch in text:
        if char_pred(ch):
            if trailing_backslashes % 2 == 1:
                parts.append("\\")
                trailing_backslashes += 1

            parts.append(escape_char(ch))
            replacements += 1
            trailing_backslashes = 0
            continue

        parts.append(ch)
        if ch == "\\":
            trailing_backslashes += 1
        else:
            trailing_backslashes = 0

    return "".join(parts), replacements


def literal_eval_token(token_text: str) -> object:
    with py_warnings.catch_warnings():
        py_warnings.simplefilter("ignore")
        return ast.literal_eval(token_text)


def literal_value_preserved(old_token: str, new_token: str) -> tuple[bool, str]:
    try:
        old_value = literal_eval_token(old_token)
        new_value = literal_eval_token(new_token)
    except Exception as e:
        return False, f"literal_eval failed: {e}"

    if old_value != new_value:
        return False, "literal runtime value changed"

    return True, ""


def ast_fingerprint(source: str, path: Path) -> str:
    with py_warnings.catch_warnings():
        py_warnings.simplefilter("ignore")
        tree = ast.parse(source, filename=str(path), mode="exec", type_comments=True)
    return ast.dump(tree, include_attributes=False)


def apply_edits(source: str, edits: list[Edit]) -> str:
    if not edits:
        return source

    edits_sorted = sorted(edits, key=lambda e: (e.start, e.end))
    last_end = -1

    for edit in edits_sorted:
        if edit.start < last_end:
            raise ValueError("overlapping edits detected")
        if not (0 <= edit.start <= edit.end <= len(source)):
            raise ValueError("edit outside source bounds")
        last_end = edit.end

    parts: list[str] = []
    cursor = 0
    for edit in edits_sorted:
        parts.append(source[cursor : edit.start])
        parts.append(edit.replacement)
        cursor = edit.end
    parts.append(source[cursor:])

    return "".join(parts)


def scan_file(
    path: Path,
    char_pred: Callable[[str], bool],
    context: int,
) -> tuple[list[Finding], collections.Counter[str], list[str]]:
    """
    Returns:
      findings, per-char counts, warnings
    """
    warnings: list[str] = []
    context = max(0, context)

    try:
        source, _encoding = read_source_with_encoding(path)
    except Exception as e:
        warnings.append(f"{path}: failed to read/decode file: {e}")
        return [], collections.Counter(), warnings

    lines = source.splitlines()
    findings: list[Finding] = []
    char_counts: collections.Counter[str] = collections.Counter()

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except Exception as e:
        warnings.append(f"{path}: tokenization failed: {e}")
        return findings, char_counts, warnings

    def add_findings(fragment: str, start_line: int, start_col: int) -> None:
        for idx, ch in enumerate(fragment):
            if not char_pred(ch):
                continue

            line, col = offset_to_line_col(fragment, idx, start_line, start_col)
            context_start = max(1, line - context)
            context_end = min(len(lines), line + context)

            findings.append(
                Finding(
                    path=path,
                    line=line,
                    col=col,
                    char=ch,
                    codepoint=f"U+{ord(ch):04X}",
                    context_start=context_start,
                    context_end=context_end,
                )
            )
            char_counts[ch] += 1

    for tok in tokens:
        if FSTRING_MIDDLE_TYPE is not None and tok.type == FSTRING_MIDDLE_TYPE:
            add_findings(tok.string, tok.start[0], tok.start[1])
            continue

        if tok.type != tokenize.STRING:
            continue

        split = split_string_token(tok.string)
        if not split:
            continue

        prefix, quote, body = split
        start_col = tok.start[1] + len(prefix) + len(quote)
        add_findings(body, tok.start[0], start_col)

    return findings, char_counts, warnings


def rewrite_file(
    path: Path,
    char_pred: Callable[[str], bool],
    *,
    backup: bool = True,
    ast_check: bool = True,
    literal_check: bool = True,
    rewrite_fstring_middle: bool = True,
    rewrite_fstring_expr_strings: bool = True,
    rewrite_legacy_fstrings: bool = False,
) -> tuple[bool, int, list[str]]:
    """
    Rewrite safe string literal source text by replacing matched characters with
    Unicode escapes.

    Returns:
      changed, replacements_count, warnings
    """
    warnings: list[str] = []

    try:
        source, encoding = read_source_with_encoding(path)
    except Exception as e:
        warnings.append(f"{path}: failed to read/decode file for rewrite: {e}")
        return False, 0, warnings

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except Exception as e:
        warnings.append(f"{path}: tokenization failed for rewrite: {e}")
        return False, 0, warnings

    line_offsets = build_line_offsets(source)
    edits: list[Edit] = []
    total_replacements = 0
    skip_counts: collections.Counter[str] = collections.Counter()
    fstring_depth = 0

    for tok in tokens:
        if FSTRING_START_TYPE is not None and tok.type == FSTRING_START_TYPE:
            fstring_depth += 1
            continue

        if FSTRING_END_TYPE is not None and tok.type == FSTRING_END_TYPE:
            fstring_depth = max(0, fstring_depth - 1)
            continue

        if FSTRING_MIDDLE_TYPE is not None and tok.type == FSTRING_MIDDLE_TYPE:
            try:
                start_abs = pos_to_offset(line_offsets, tok.start[0], tok.start[1])
                end_abs = pos_to_offset(line_offsets, tok.end[0], tok.end[1])
            except ValueError as e:
                warnings.append(
                    f"{path}:{tok.start[0]}:{tok.start[1]} skipped f-string text: {e}"
                )
                continue

            fragment = source[start_abs:end_abs]
            hits = count_matches(fragment, char_pred)
            if not hits:
                continue

            if not rewrite_fstring_middle:
                skip_counts["fstring_middle"] += hits
                continue

            new_fragment, replacements = rewrite_text_fragment(fragment, char_pred)
            if replacements:
                edits.append(Edit(start_abs, end_abs, new_fragment))
                total_replacements += replacements

            continue

        if tok.type != tokenize.STRING:
            continue

        try:
            token_start_abs = pos_to_offset(line_offsets, tok.start[0], tok.start[1])
            token_end_abs = pos_to_offset(line_offsets, tok.end[0], tok.end[1])
        except ValueError as e:
            warnings.append(
                f"{path}:{tok.start[0]}:{tok.start[1]} skipped string token: {e}"
            )
            continue

        token_source = source[token_start_abs:token_end_abs]
        split = split_string_token(token_source)
        if not split:
            continue

        prefix, quote, body = split
        hits = count_matches(body, char_pred)
        if not hits:
            continue

        lower_prefix = prefix.lower()

        if not is_safe_to_rewrite(prefix):
            skip_counts["raw_bytes"] += hits
            continue

        if is_legacy_fstring(prefix) and not rewrite_legacy_fstrings:
            skip_counts["legacy_fstring"] += hits
            continue

        if fstring_depth > 0 and not rewrite_fstring_expr_strings:
            skip_counts["fstring_expr_string"] += hits
            continue

        new_body, replacements = rewrite_text_fragment(body, char_pred)
        if not replacements:
            continue

        if literal_check and "f" not in lower_prefix:
            new_token_source = prefix + quote + new_body + quote
            ok, reason = literal_value_preserved(token_source, new_token_source)
            if not ok:
                warnings.append(
                    f"{path}:{tok.start[0]}:{tok.start[1]} skipped string token: {reason}"
                )
                continue

        body_start_abs = token_start_abs + len(prefix) + len(quote)
        body_end_abs = token_end_abs - len(quote)
        edits.append(Edit(body_start_abs, body_end_abs, new_body))
        total_replacements += replacements

    skip_labels = {
        "raw_bytes": "raw or bytes string literals",
        "legacy_fstring": "whole f-string tokens tokenized as STRING",
        "fstring_middle": "f-string literal text tokens",
        "fstring_expr_string": "string literals inside f-string expressions",
    }
    for key, label in skip_labels.items():
        n = skip_counts.get(key, 0)
        if n:
            warnings.append(f"{path}: skipped {n} matched char(s) in {label}.")

    if not edits:
        return False, 0, warnings

    try:
        new_source = apply_edits(source, edits)
    except Exception as e:
        warnings.append(
            f"{path}: internal edit application failed; no changes written: {e}"
        )
        return False, 0, warnings

    if ast_check:
        try:
            old_fp = ast_fingerprint(source, path)
            new_fp = ast_fingerprint(new_source, path)
        except Exception as e:
            warnings.append(f"{path}: AST safety check failed; no changes written: {e}")
            return False, 0, warnings

        if old_fp != new_fp:
            warnings.append(
                f"{path}: AST safety check detected a semantic change; no changes written."
            )
            return False, 0, warnings

    if backup:
        backup_path = path.with_suffix(path.suffix + ".bak")
        try:
            if not backup_path.exists():
                shutil.copy2(path, backup_path)
        except Exception as e:
            warnings.append(f"{path}: failed to create backup {backup_path}: {e}")
            return False, 0, warnings

    try:
        with path.open("w", encoding=encoding, newline="") as f:
            f.write(new_source)
    except Exception as e:
        warnings.append(f"{path}: failed to write rewritten file: {e}")
        return False, 0, warnings

    return True, total_replacements, warnings


def iter_python_files(root: Path) -> Iterable[Path]:
    if root.is_file():
        if root.name.endswith(".py"):
            yield root
        return

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield Path(dirpath) / name


def path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _sanitize_archive_part(value: object, *, default: str = "backup") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    text = text.strip("._-")
    return text or default


def find_7z_exe(override: str | Path | None = None) -> Path:
    """Locate a 7z executable.

    Defaults cover your PeaZip path and a normal 7-Zip install. PATH is also
    checked. Raises FileNotFoundError if nothing is found.
    """
    exe_name = "7z.exe" if os.name == "nt" else "7z"

    if override:
        cand = Path(override).expanduser()
        if cand.is_dir():
            cand = cand / exe_name
        if cand.exists():
            return cand
        raise FileNotFoundError(f"7z executable not found at: {cand}")

    for cand in DEFAULT_7Z_EXE_CANDIDATES:
        if cand.exists():
            return cand

    found = shutil.which("7z") or shutil.which("7z.exe")
    if found:
        return Path(found)

    raise FileNotFoundError(
        "7z executable not found. Install 7-Zip/PeaZip, add 7z to PATH, "
        "or pass --seven-zip <path-to-7z.exe>."
    )


def create_7z_backup(
    source: Path,
    backup_dir: Path,
    *,
    label: str,
    timestamp: str,
    description: str = "",
    seven_zip_override: str | Path | None = None,
) -> Path:
    """Create a .7z archive for `source` outside the source tree."""
    source = source.resolve()
    backup_dir = Path(backup_dir).expanduser().resolve()

    if not source.exists():
        raise FileNotFoundError(f"Backup source does not exist: {source}")

    source_root_for_safety = source if source.is_dir() else source.parent
    if path_is_relative_to(backup_dir, source_root_for_safety):
        raise ValueError(
            f"Backup directory must be outside the source tree. "
            f"backup_dir={backup_dir}, source={source}"
        )

    backup_dir.mkdir(parents=True, exist_ok=True)

    seven_zip = find_7z_exe(seven_zip_override)

    base_name = _sanitize_archive_part(source.name or "source")
    label_part = _sanitize_archive_part(label, default="backup")

    desc_raw = str(description or "").strip()
    desc_part = ""
    if desc_raw:
        desc_part = "_" + _sanitize_archive_part(desc_raw, default="")

    dest = backup_dir / f"{base_name}_{label_part}{desc_part}_{timestamp}.7z"

    cmd = [
        str(seven_zip),
        "a",
        "-t7z",
        "-m0=PPMd",
        "-mx=9",
        "-mmem=256m",
        "-mo=32",
        "-ms=4g",
        "-mqs=on",
        "-sccUTF-8",
        "-bb0",
        "-mmt=off",
        "-mtc=on",
        "-mta=on",
    ]

    cmd.extend(f"-xr!{pattern}" for pattern in DEFAULT_7Z_EXCLUDE_PATTERNS)
    cmd.extend([str(dest), str(source)])

    print(f"Creating 7z backup [{label_part}]: {dest}")
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise RuntimeError(
            f"7z backup failed for {source} with exit code {proc.returncode}"
        )

    return dest


def remove_existing_ai_copy_target(target: Path) -> None:
    if target.parent == target:
        raise ValueError("refusing to overwrite a filesystem root")

    try:
        home = Path.home().resolve()
    except Exception:
        home = None

    if home is not None and target == home:
        raise ValueError("refusing to overwrite the home directory")

    if target.is_symlink() or target.is_file():
        target.unlink()
    elif target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


def create_ai_review_copy(
    source_root: Path,
    target: Path,
    *,
    overwrite: bool,
) -> Path:
    """
    Create a disposable AI-review copy.

    Safety choices:
      - target may not equal, be inside, or contain the source path;
      - existing target is refused unless --overwrite-ai-copy is supplied;
      - symlinks are skipped, so rewriting the copy cannot follow a symlink and
        accidentally modify files outside the copy.
    """
    source_root = source_root.resolve()
    target = target.resolve()

    source_container = source_root if source_root.is_dir() else source_root.parent

    if target == source_root:
        raise ValueError("--ai-copy target must not be the original source path.")

    if path_is_relative_to(target, source_container):
        raise ValueError(
            "--ai-copy target must be outside the source tree. "
            "Do not create the AI copy inside the project being copied."
        )

    if path_is_relative_to(source_root, target):
        raise ValueError(
            "--ai-copy target must not contain the original source path. "
            "With --overwrite-ai-copy, deleting such a target would also delete "
            f"the source repository: source={source_root}, target={target}"
        )

    if source_root.is_dir():
        if target.exists() or target.is_symlink():
            if not overwrite:
                raise FileExistsError(
                    f"AI copy target already exists: {target}. "
                    "Use --overwrite-ai-copy only if you want to delete and recreate it."
                )
            remove_existing_ai_copy_target(target)

        target.parent.mkdir(parents=True, exist_ok=True)

        def ignore(dirpath: str, names: list[str]) -> set[str]:
            ignored: set[str] = set()

            for name in names:
                p = Path(dirpath) / name
                name_l = name.lower()

                if p.is_symlink():
                    ignored.add(name)
                    continue

                if p.is_dir() and name_l in {d.lower() for d in AI_COPY_SKIP_DIRS}:
                    ignored.add(name)
                    continue

                if p.is_file():
                    if name_l in AI_COPY_SKIP_FILENAMES:
                        ignored.add(name)
                        continue
                    if name_l.endswith(".bak"):
                        ignored.add(name)
                        continue
                    if p.suffix.lower() in AI_COPY_SKIP_SUFFIXES:
                        ignored.add(name)
                        continue

            return ignored

        shutil.copytree(source_root, target, ignore=ignore)
        return target

    if source_root.is_file():
        if target == source_root:
            raise ValueError("--ai-copy target must not be the original source file")

        if target.exists() or target.is_symlink():
            if not overwrite:
                raise FileExistsError(
                    f"AI copy target already exists: {target}. "
                    "Use --overwrite-ai-copy only if you want to delete and recreate it."
                )
            remove_existing_ai_copy_target(target)

        if target.suffix:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_root, target)
            return target

        target.mkdir(parents=True, exist_ok=False)
        dest = target / source_root.name
        shutil.copy2(source_root, dest)
        return dest

    raise ValueError(f"source root is neither a file nor a directory: {source_root}")


def write_ai_review_readme(
    working_root: Path,
    source_root: Path,
    report_path: Path,
) -> None:
    base_dir = working_root if working_root.is_dir() else working_root.parent
    readme_path = base_dir / "AI_REVIEW_README.txt"

    source_text = escape_text_for_ascii_report(str(source_root))
    working_text = escape_text_for_ascii_report(str(working_root))
    report_text = escape_text_for_ascii_report(str(report_path))

    content = (
        "AI REVIEW COPY\n"
        "==============\n\n"
        "This is a disposable copy for AI code review.\n"
        "The original source was not modified by --ai-copy.\n\n"
        "Matched Unicode characters inside safe Python string literals are converted\n"
        "to ASCII Python source escapes such as \\u2192 or \\U0001F600.\n"
        "That is intentional. It helps prevent AI websites from dropping, hiding,\n"
        "or replacing visible Unicode characters in before/after snippets.\n\n"
        f"Original source: {source_text}\n"
        f"AI review root: {working_text}\n"
        f"Report: {report_text}\n\n"
        "Recommended workflow:\n"
        "1. Upload/send this AI-review copy to AI tools.\n"
        "2. Treat the original repository as the source of truth.\n"
        "3. Review any AI patch carefully before applying it to the real project.\n"
    )

    readme_path.write_text(content, encoding="utf-8", newline="\n")


def write_report(
    report_path: Path,
    root: Path,
    args: argparse.Namespace,
    total_files: int,
    files_with_hits: int,
    total_hits: int,
    char_counts: collections.Counter[str],
    findings_by_file: dict[Path, list[Finding]],
    warnings: list[str],
    *,
    rewritten: int = 0,
    total_replacements: int = 0,
    pre_files_with_hits: int | None = None,
    pre_total_hits: int | None = None,
    pre_unique_chars: int | None = None,
    pre_char_counts: collections.Counter[str] | None = None,
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)

    escape_mode = bool(getattr(args, "escape", False))
    effective_backup = bool(getattr(args, "effective_backup", False))
    ascii_report = bool(getattr(args, "ascii_report", True))

    def safe(value: object) -> str:
        return safe_for_report(value, ascii_report)

    if args.chars:
        match_mode = "exact characters from --chars"
    elif getattr(args, "all_nonascii", False):
        match_mode = "all non-ASCII characters"
    else:
        match_mode = "non-ASCII punctuation/symbols plus selected marks"

    with report_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("SCAN STRING LITERALS REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
        f.write(f"Root: {safe(root)}\n")

        ai_copy_source = getattr(args, "ai_copy_source", None)
        if ai_copy_source is not None:
            f.write("AI copy mode: True\n")
            f.write(f"AI copy source: {safe(ai_copy_source)}\n")
            f.write(f"AI copy working root: {safe(root)}\n")
            f.write("Original source modified by this run: False\n")
        else:
            f.write("AI copy mode: False\n")

        f.write(f"Chars filter: {ascii(args.chars)}\n")
        f.write(f"Effective match mode: {match_mode}\n")
        f.write(f"All non-ASCII mode: {getattr(args, 'all_nonascii', False)}\n")
        f.write(f"Context lines: {args.context}\n")
        f.write(f"ASCII-safe report: {ascii_report}\n")
        f.write(f"Escape mode: {escape_mode}\n")
        f.write(f"Backup mode: {effective_backup}\n")
        f.write(f"AST safety check: {getattr(args, 'ast_check', True)}\n")
        f.write(f"Literal value check: {getattr(args, 'literal_check', True)}\n")
        f.write(
            f"Include extra invisible/emoji marks: {getattr(args, 'include_extra_marks', True)}\n"
        )
        f.write(
            f"Rewrite f-string text tokens: {getattr(args, 'rewrite_fstring_middle', True)}\n"
        )
        f.write(
            "Rewrite strings inside f-string expressions: "
            f"{getattr(args, 'rewrite_fstring_expr_strings', True)}\n"
        )
        f.write(
            f"Rewrite legacy whole f-string tokens: {getattr(args, 'rewrite_legacy_fstrings', False)}\n"
        )

        if ascii_report:
            f.write("\nREPORT FORMAT NOTE\n")
            f.write("-" * 80 + "\n")
            f.write(
                "Context lines are AI-safe. Raw non-ASCII characters are rendered as "
                "\\uXXXX or \\UXXXXXXXX escapes. This prevents AI websites from "
                "displaying those characters as empty strings or replacing them.\n"
            )

        f.write("\nSUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Files scanned: {total_files}\n")

        if escape_mode and pre_total_hits is not None:
            f.write(f"Initial files with hits: {pre_files_with_hits}\n")
            f.write(f"Initial total hits: {pre_total_hits}\n")
            f.write(f"Initial unique special characters: {pre_unique_chars}\n")
            f.write(f"Files rewritten: {rewritten}\n")
            f.write(f"Total replacements: {total_replacements}\n")
            f.write(f"Remaining files with hits: {files_with_hits}\n")
            f.write(f"Remaining total hits: {total_hits}\n")
            f.write(f"Remaining unique special characters: {len(char_counts)}\n")
        else:
            f.write(f"Files with hits: {files_with_hits}\n")
            f.write(f"Total hits: {total_hits}\n")
            f.write(f"Unique special characters: {len(char_counts)}\n")

        if warnings:
            f.write(f"Warnings: {len(warnings)}\n")

        if escape_mode and pre_char_counts is not None:
            f.write("\nINITIAL MATCHED CHARACTERS BEFORE REWRITE\n")
            f.write("-" * 80 + "\n")
            if pre_char_counts:
                for ch, count in pre_char_counts.most_common():
                    f.write(f"{format_char(ch)} | hits={count}\n")
            else:
                f.write("None\n")

        title = (
            "REMAINING SPECIAL CHARACTERS FOUND"
            if escape_mode
            else "SPECIAL CHARACTERS FOUND"
        )
        f.write(f"\n{title}\n")
        f.write("-" * 80 + "\n")
        if char_counts:
            for ch, count in char_counts.most_common():
                files_seen = sum(
                    1
                    for _path, items in findings_by_file.items()
                    if any(item.char == ch for item in items)
                )
                f.write(f"{format_char(ch)} | hits={count} | files={files_seen}\n")
        else:
            f.write("None\n")

        if warnings:
            f.write("\nWARNINGS\n")
            f.write("-" * 80 + "\n")
            for w in warnings:
                f.write(safe(w.rstrip()) + "\n")

        f.write("\nFINDINGS BY FILE\n")
        f.write("-" * 80 + "\n")
        if not findings_by_file:
            f.write("No matches found.\n")
        else:
            for path in sorted(findings_by_file, key=lambda p: str(p).lower()):
                items = findings_by_file[path]
                f.write(f"\n[{safe(path)}]  hits={len(items)}\n")
                f.write("-" * 80 + "\n")

                try:
                    source, _ = read_source_with_encoding(path)
                    lines = source.splitlines()
                except Exception as e:
                    f.write(f"Could not reload file for context: {safe(e)}\n")
                    continue

                for item in items:
                    f.write(
                        f"{item.line}:{item.col}  {item.codepoint}  {ascii(item.char)}\n"
                    )
                    for n in range(item.context_start, item.context_end + 1):
                        marker = ">" if n == item.line else " "
                        text = lines[n - 1] if 0 <= n - 1 < len(lines) else ""
                        if ascii_report:
                            text = escape_text_for_ascii_report(text)
                        f.write(f" {marker} {n:5}: {text}\n")
                    f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scan Python string literals for non-ASCII Unicode symbols/punctuation, "
            "and optionally create an AI-safe escaped copy."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        type=Path,
        default=Path("."),
        help="Root directory or .py file to scan. Default: current directory.",
    )
    parser.add_argument(
        "--chars",
        default=None,
        help=(
            "Exact characters to flag. If omitted, matching uses either the default "
            "non-ASCII symbol/punctuation rule or --all-nonascii."
        ),
    )
    parser.add_argument(
        "--all-nonascii",
        "--all-non-ascii",
        dest="all_nonascii",
        action="store_true",
        help=(
            "Flag every non-ASCII character inside supported string literal text. "
            "This is strongest for AI-review copies."
        ),
    )
    parser.add_argument(
        "--context",
        type=int,
        default=2,
        help="Number of lines above/below to include in the report.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Path to the output .txt report file. Default: <root>/scan_string_literals_report.txt",
    )

    report_group = parser.add_mutually_exclusive_group()
    report_group.add_argument(
        "--ascii-report",
        dest="ascii_report",
        action="store_true",
        default=True,
        help=(
            "Write report context lines with raw non-ASCII rendered as "
            "\\uXXXX/\\UXXXXXXXX escapes. Default."
        ),
    )
    report_group.add_argument(
        "--raw-report",
        dest="ascii_report",
        action="store_false",
        help="Write raw Unicode in report context lines. Not recommended for AI websites.",
    )

    parser.add_argument(
        "--escape",
        action="store_true",
        help=(
            "Rewrite safe string literals by replacing matched characters with "
            "Unicode escapes. Modifies the selected root unless --ai-copy is used."
        ),
    )
    parser.add_argument(
        "--ai-copy",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Create a disposable copy at PATH, then run escape mode on that copy only. "
            "The original project is not modified. Use with --all-nonascii for maximum "
            "AI-website safety."
        ),
    )
    parser.add_argument(
        "--overwrite-ai-copy",
        action="store_true",
        help=(
            "Allow deleting and recreating an existing --ai-copy target. "
            "Use carefully."
        ),
    )

    archive_group = parser.add_mutually_exclusive_group()
    archive_group.add_argument(
        "--archive-backups",
        dest="archive_backups",
        action="store_true",
        default=None,
        help=(
            "Create .7z backups. Default: enabled automatically when --ai-copy "
            "is used."
        ),
    )
    archive_group.add_argument(
        "--no-archive-backups",
        dest="archive_backups",
        action="store_false",
        help="Disable automatic .7z backups.",
    )
    parser.add_argument(
        "--archive-backup-dir",
        type=Path,
        default=DEFAULT_7Z_BACKUP_DIR,
        help=(
            "Directory for automatic .7z backups. Default: "
            "%%USERPROFILE%%\\Desktop\\l2_liquidity_shock_analyzer_backup"
        ),
    )
    parser.add_argument(
        "--seven-zip",
        default=None,
        help=(
            "Path to 7z.exe or its containing folder. Default checks PeaZip, "
            "7-Zip, then PATH."
        ),
    )
    parser.add_argument(
        "--backup-description",
        default="",
        help="Optional short text inserted into automatic .7z archive filenames.",
    )

    backup_group = parser.add_mutually_exclusive_group()
    backup_group.add_argument(
        "--backup",
        dest="backup",
        action="store_true",
        default=None,
        help=(
            "Create .bak backups before rewriting. Default: on when --escape modifies "
            "the original root; off for --ai-copy."
        ),
    )
    backup_group.add_argument(
        "--no-backup",
        dest="backup",
        action="store_false",
        help="Do not create .bak backups before rewriting.",
    )

    parser.add_argument(
        "--no-ast-check",
        dest="ast_check",
        action="store_false",
        default=True,
        help="Disable whole-file AST equivalence safety check before writing.",
    )
    parser.add_argument(
        "--no-literal-check",
        dest="literal_check",
        action="store_false",
        default=True,
        help="Disable per-literal runtime value checks for normal strings.",
    )
    parser.add_argument(
        "--no-extra-marks",
        dest="include_extra_marks",
        action="store_false",
        default=True,
        help="Do not flag selected invisible/emoji marks such as variation selectors.",
    )
    parser.add_argument(
        "--skip-fstring-middle",
        dest="rewrite_fstring_middle",
        action="store_false",
        default=True,
        help="Do not rewrite Python 3.12+ f-string literal text tokens.",
    )
    parser.add_argument(
        "--skip-fstring-expr-strings",
        dest="rewrite_fstring_expr_strings",
        action="store_false",
        default=True,
        help=(
            "Do not rewrite normal string literals inside Python 3.12+ f-string "
            "expressions. Use this if rewritten source must remain Python <= 3.11 compatible."
        ),
    )
    parser.add_argument(
        "--rewrite-legacy-fstrings",
        action="store_true",
        help=(
            "Opt in to rewriting whole f-string tokens on Python versions/tokenizers "
            "that expose f-strings as STRING. Default is off because this can be risky."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print limited progress to the console.",
    )

    args = parser.parse_args()
    args.context = max(0, args.context)

    source_root = args.root.resolve()
    if not source_root.exists():
        print(f"Root path does not exist: {source_root}")
        return 2

    args.ai_copy_source = None

    archive_outputs: list[tuple[str, Path]] = []
    do_archive_backups = (
        bool(args.archive_backups)
        if args.archive_backups is not None
        else args.ai_copy is not None
    )
    archive_timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    if do_archive_backups:
        try:
            original_label = "original" if args.ai_copy is not None else "before_scan"
            original_archive = create_7z_backup(
                source_root,
                args.archive_backup_dir,
                label=original_label,
                timestamp=archive_timestamp,
                description=args.backup_description,
                seven_zip_override=args.seven_zip,
            )
            archive_outputs.append((original_label, original_archive))
        except Exception as e:
            print(f"Failed to create original/source .7z backup: {e}")
            return 2

    if args.ai_copy is not None:
        try:
            copied_root = create_ai_review_copy(
                source_root,
                args.ai_copy,
                overwrite=args.overwrite_ai_copy,
            )
        except Exception as e:
            print(f"Failed to create AI review copy: {e}")
            return 2

        args.ai_copy_source = source_root
        args.escape = True
        root = copied_root.resolve()
        print(f"AI review copy created at: {root}")
    else:
        root = source_root

    args.effective_backup = (
        bool(args.backup)
        if args.backup is not None
        else bool(args.escape and args.ai_copy_source is None)
    )

    report_base = root if root.is_dir() else root.parent
    report_path = (
        args.report.resolve()
        if args.report
        else (report_base / "scan_string_literals_report.txt")
    )

    char_pred = build_char_predicate(
        args.chars,
        include_extra_marks=args.include_extra_marks,
        all_non_ascii=args.all_nonascii,
    )

    warnings: list[str] = []

    if args.ai_copy_source is not None:
        try:
            write_ai_review_readme(root, args.ai_copy_source, report_path)
        except Exception as e:
            warnings.append(f"{root}: failed to write AI_REVIEW_README.txt: {e}")

    files = list(iter_python_files(root))
    total_files = len(files)

    rewritten = 0
    total_replacements = 0

    initial_files_with_hits = 0
    initial_total_hits = 0
    initial_char_counts: collections.Counter[str] = collections.Counter()
    initial_findings_by_file: dict[Path, list[Finding]] = {}

    for idx, path in enumerate(files, start=1):
        findings, counts, scan_warnings = scan_file(path, char_pred, args.context)
        warnings.extend(scan_warnings)

        if findings:
            initial_files_with_hits += 1
            initial_total_hits += len(findings)
            initial_char_counts.update(counts)
            if not args.escape:
                initial_findings_by_file[path] = findings

        if args.escape:
            changed, replacements, rewrite_warnings = rewrite_file(
                path,
                char_pred,
                backup=args.effective_backup,
                ast_check=args.ast_check,
                literal_check=args.literal_check,
                rewrite_fstring_middle=args.rewrite_fstring_middle,
                rewrite_fstring_expr_strings=args.rewrite_fstring_expr_strings,
                rewrite_legacy_fstrings=args.rewrite_legacy_fstrings,
            )
            warnings.extend(rewrite_warnings)
            if changed:
                rewritten += 1
                total_replacements += replacements
                if args.verbose:
                    print(f"[rewrite] {path} ({replacements} replacements)")

        if args.verbose:
            print(f"[scan] {idx}/{total_files}: {path}")

    if args.escape:
        findings_by_file: dict[Path, list[Finding]] = {}
        char_counts: collections.Counter[str] = collections.Counter()
        files_with_hits = 0
        total_hits = 0

        for path in files:
            findings, counts, scan_warnings = scan_file(path, char_pred, args.context)
            warnings.extend(scan_warnings)

            if findings:
                findings_by_file[path] = findings
                files_with_hits += 1
                total_hits += len(findings)
                char_counts.update(counts)
    else:
        findings_by_file = initial_findings_by_file
        char_counts = initial_char_counts
        files_with_hits = initial_files_with_hits
        total_hits = initial_total_hits

    write_report(
        report_path=report_path,
        root=root,
        args=args,
        total_files=total_files,
        files_with_hits=files_with_hits,
        total_hits=total_hits,
        char_counts=char_counts,
        findings_by_file=findings_by_file,
        warnings=warnings,
        rewritten=rewritten,
        total_replacements=total_replacements,
        pre_files_with_hits=initial_files_with_hits if args.escape else None,
        pre_total_hits=initial_total_hits if args.escape else None,
        pre_unique_chars=len(initial_char_counts) if args.escape else None,
        pre_char_counts=initial_char_counts if args.escape else None,
    )

    if do_archive_backups:
        try:
            result_label = (
                "literal_fixed" if args.ai_copy_source is not None else "after_scan"
            )
            result_archive = create_7z_backup(
                root,
                args.archive_backup_dir,
                label=result_label,
                timestamp=archive_timestamp,
                description=args.backup_description,
                seven_zip_override=args.seven_zip,
            )
            archive_outputs.append((result_label, result_archive))
        except Exception as e:
            print(f"Failed to create result .7z backup: {e}")
            return 3

    print(f"Done. Report saved to: {report_path}")

    if archive_outputs:
        print("7z backups created:")
        for label, path in archive_outputs:
            print(f"  {label}: {path}")

    if args.ai_copy_source is not None:
        print("Original source was not modified.")
        print(f"Send this AI-review copy to AI tools: {root}")

    if args.escape:
        print(
            f"Files scanned: {total_files} | initial hits: {initial_total_hits} | "
            f"rewritten files: {rewritten} | replacements: {total_replacements} | "
            f"remaining hits: {total_hits} | remaining unique chars: {len(char_counts)}"
        )
    else:
        print(
            f"Files scanned: {total_files} | files with hits: {files_with_hits} | "
            f"total hits: {total_hits} | unique chars: {len(char_counts)}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
