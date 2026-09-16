#!/usr/bin/env python3
"""
Split a text-based file into AI-friendly segments without dividing source lines.

Supported input includes any decodable text file, such as:

    .txt, .md, .json, .py, .yaml, .yml, .ini, .toml, .csv, .xml, .html

The splitter:

- uses an estimated token limit per output segment;
- defaults to 10,000 estimated tokens;
- never divides an input line;
- preserves tabs, spaces, indentation, and original line endings;
- adds explicit start/end markers to every output file;
- uses byte-aware decoding and rejects likely binary files;
- avoids overwriting existing outputs unless --overwrite is passed.

Token counts are estimates unless the optional `tiktoken` package is installed
and --tokenizer tiktoken is selected.

Examples:

    python tools/text_segmenter.py request.txt

    python tools/text_segmenter.py request.txt --tokens 20000

    python tools/text_segmenter.py request.txt --tokens 10000 --output-dir ai_segments

    python tools/text_segmenter.py request.txt --tokenizer tiktoken

    python tools/text_segmenter.py request.txt --encoding utf-8 --overwrite
"""

from __future__ import annotations

import argparse
import codecs
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable

DEFAULT_TOKEN_LIMIT = 10_000
DEFAULT_CHARS_PER_TOKEN = 4.0
DEFAULT_ENCODING = "utf-8"

# A segment may slightly exceed the requested token limit only when one
# individual input line exceeds that limit. Lines are never divided.
BINARY_SAMPLE_BYTES = 8192


@dataclass(frozen=True)
class Segment:
    index: int
    first_line: int
    last_line: int
    text: str
    estimated_content_tokens: int


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")

    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")

    return parsed


def is_probably_binary(path: Path) -> bool:
    """Return True when the input appears to be binary rather than text."""
    with path.open("rb") as handle:
        sample = handle.read(BINARY_SAMPLE_BYTES)

    return b"\x00" in sample


def detect_bom_encoding(raw: bytes, fallback: str) -> str:
    """Choose an encoding from a byte-order mark when one is present."""
    if raw.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    if raw.startswith(codecs.BOM_UTF32_LE):
        return "utf-32-le"
    if raw.startswith(codecs.BOM_UTF32_BE):
        return "utf-32-be"
    if raw.startswith(codecs.BOM_UTF16_LE):
        return "utf-16-le"
    if raw.startswith(codecs.BOM_UTF16_BE):
        return "utf-16-be"
    return fallback


def read_text_preserving_line_endings(
    path: Path,
    requested_encoding: str,
) -> tuple[str, str]:
    """
    Decode a text file while preserving its original newline characters.

    Returns:
        (decoded_text, encoding_used)
    """
    raw = path.read_bytes()
    encoding = detect_bom_encoding(raw, requested_encoding)

    try:
        text = raw.decode(encoding)
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            f"Could not decode {path} using {encoding!r}. "
            "Specify another encoding with --encoding."
        ) from exc

    return text, encoding


def make_estimated_counter(chars_per_token: float) -> Callable[[str], int]:
    """
    Return a conservative dependency-free token estimator.

    This estimate accounts for words, punctuation, whitespace, and Unicode
    text. It is still only an approximation because actual tokenization depends
    on the AI model.
    """
    token_piece_re = re.compile(
        r"""
        [A-Za-z]+(?:'[A-Za-z]+)? |
        \d+(?:\.\d+)?           |
        [^\w\s]                 |
        [^\x00-\x7F]            |
        \s+
        """,
        re.VERBOSE,
    )

    def count(text: str) -> int:
        if not text:
            return 0

        character_estimate = max(
            1,
            int((len(text) + chars_per_token - 1) // chars_per_token),
        )

        pieces = token_piece_re.findall(text)
        structural_estimate = 0

        for piece in pieces:
            if piece.isspace():
                # Newlines and indentation consume context too.
                structural_estimate += max(
                    1,
                    int((len(piece) + chars_per_token - 1) // chars_per_token),
                )
            elif piece.isascii() and piece.isalpha():
                structural_estimate += max(
                    1,
                    int((len(piece) + 7) // 8),
                )
            else:
                structural_estimate += 1

        # Taking the larger estimate is safer than relying only on character
        # count for source code, JSON, punctuation-heavy text, or indentation.
        return max(character_estimate, structural_estimate)

    return count


def make_tiktoken_counter(encoding_name: str) -> Callable[[str], int]:
    """Return an exact counter for a tiktoken encoding."""
    try:
        import tiktoken
    except ImportError as exc:
        raise RuntimeError(
            "--tokenizer tiktoken requires the optional tiktoken package.\n"
            "Install it with: python -m pip install tiktoken"
        ) from exc

    try:
        encoding = tiktoken.get_encoding(encoding_name)
    except Exception as exc:
        raise RuntimeError(f"Unknown tiktoken encoding {encoding_name!r}.") from exc

    def count(text: str) -> int:
        return len(encoding.encode(text, disallowed_special=()))

    return count


def choose_token_counter(args: argparse.Namespace) -> Callable[[str], int]:
    if args.tokenizer == "tiktoken":
        return make_tiktoken_counter(args.tiktoken_encoding)

    return make_estimated_counter(args.chars_per_token)


def marker_filename(
    input_path: Path,
    segment_index: int,
    segment_count: int,
) -> str:
    width = max(2, len(str(segment_count)))
    return (
        f"{input_path.stem}_"
        f"{segment_index:0{width}d}_of_{segment_count:0{width}d}"
        f"{input_path.suffix or '.txt'}"
    )


def start_marker(
    source_name: str,
    output_name: str,
    segment_index: int,
    segment_count: int,
    first_line: int,
    last_line: int,
) -> str:
    return (
        f"[start of {source_name}; "
        f"segment {segment_index} of {segment_count}; "
        f"source lines {first_line}-{last_line}; "
        f"output {output_name}]"
    )


def end_marker(
    source_name: str,
    output_name: str,
    segment_index: int,
    segment_count: int,
    first_line: int,
    last_line: int,
) -> str:
    return (
        f"[end of {source_name}; "
        f"segment {segment_index} of {segment_count}; "
        f"source lines {first_line}-{last_line}; "
        f"output {output_name}]"
    )


def split_lines_by_token_limit(
    lines: list[str],
    token_limit: int,
    count_tokens: Callable[[str], int],
) -> list[Segment]:
    """
    Split complete input lines into segments.

    Empty files produce one empty segment. A single oversized line remains
    intact and is placed in a segment by itself.
    """
    if not lines:
        return [
            Segment(
                index=1,
                first_line=0,
                last_line=0,
                text="",
                estimated_content_tokens=0,
            )
        ]

    raw_segments: list[tuple[int, int, str, int]] = []

    current_lines: list[str] = []
    current_tokens = 0
    first_line = 1

    for line_number, line in enumerate(lines, start=1):
        line_tokens = count_tokens(line)

        if current_lines and current_tokens + line_tokens > token_limit:
            raw_segments.append(
                (
                    first_line,
                    line_number - 1,
                    "".join(current_lines),
                    current_tokens,
                )
            )
            current_lines = []
            current_tokens = 0
            first_line = line_number

        current_lines.append(line)
        current_tokens += line_tokens

    if current_lines:
        raw_segments.append(
            (
                first_line,
                len(lines),
                "".join(current_lines),
                current_tokens,
            )
        )

    return [
        Segment(
            index=index,
            first_line=first,
            last_line=last,
            text=text,
            estimated_content_tokens=tokens,
        )
        for index, (first, last, text, tokens) in enumerate(
            raw_segments,
            start=1,
        )
    ]


def ensure_marker_separation(text: str) -> str:
    """
    Ensure the end marker starts on a new line without altering source content.

    If the source segment has no final newline, a separator newline is added
    after it. The source text itself is otherwise unchanged.
    """
    if not text:
        return ""

    if text.endswith(("\n", "\r")):
        return text

    return text + "\n"


def write_segments(
    *,
    input_path: Path,
    output_dir: Path,
    segments: list[Segment],
    count_tokens: Callable[[str], int],
    encoding: str,
    overwrite: bool,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    output_paths = [
        output_dir
        / marker_filename(
            input_path,
            segment.index,
            len(segments),
        )
        for segment in segments
    ]

    if not overwrite:
        existing = [path for path in output_paths if path.exists()]
        if existing:
            listed = "\n".join(f"  - {path}" for path in existing[:20])
            extra = (
                f"\n  ... and {len(existing) - 20} more" if len(existing) > 20 else ""
            )
            raise FileExistsError(
                "Refusing to overwrite existing output files:\n"
                f"{listed}{extra}\n"
                "Use --overwrite to replace them."
            )

    written: list[Path] = []

    for segment, output_path in zip(segments, output_paths):
        start = start_marker(
            input_path.name,
            output_path.name,
            segment.index,
            len(segments),
            segment.first_line,
            segment.last_line,
        )
        end = end_marker(
            input_path.name,
            output_path.name,
            segment.index,
            len(segments),
            segment.first_line,
            segment.last_line,
        )

        output_text = start + "\n" + ensure_marker_separation(segment.text) + end + "\n"

        output_path.write_text(
            output_text,
            encoding=encoding,
            newline="",
        )
        written.append(output_path)

        total_tokens = count_tokens(output_text)
        warning = ""
        if segment.estimated_content_tokens > 0:
            # The only expected over-limit case is an indivisible long line.
            warning = ""

        print(
            f"Wrote {output_path} "
            f"(source lines {segment.first_line}-{segment.last_line}, "
            f"content tokens ~{segment.estimated_content_tokens:,}, "
            f"including markers ~{total_tokens:,})"
            f"{warning}"
        )

    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Split one text file into line-safe, AI-friendly segments with "
            "explicit start/end markers."
        )
    )

    parser.add_argument(
        "input_file",
        type=Path,
        help="Text-based input file to split.",
    )
    parser.add_argument(
        "--tokens",
        type=positive_int,
        default=DEFAULT_TOKEN_LIMIT,
        help=(
            "Maximum estimated content tokens per segment "
            f"(default: {DEFAULT_TOKEN_LIMIT})."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory for output files. Default: a '<stem>_segments' "
            "directory beside the input file."
        ),
    )
    parser.add_argument(
        "--encoding",
        default=DEFAULT_ENCODING,
        help=(
            "Input/output text encoding when no BOM identifies it "
            f"(default: {DEFAULT_ENCODING})."
        ),
    )
    parser.add_argument(
        "--tokenizer",
        choices=("estimate", "tiktoken"),
        default="estimate",
        help=(
            "Token-counting strategy. 'estimate' has no dependencies; "
            "'tiktoken' is more accurate but requires the tiktoken package."
        ),
    )
    parser.add_argument(
        "--chars-per-token",
        type=positive_float,
        default=DEFAULT_CHARS_PER_TOKEN,
        help=(
            "Character/token ratio used by the dependency-free estimator "
            f"(default: {DEFAULT_CHARS_PER_TOKEN}). Lower is more conservative."
        ),
    )
    parser.add_argument(
        "--tiktoken-encoding",
        default="o200k_base",
        help=(
            "tiktoken encoding used with --tokenizer tiktoken " "(default: o200k_base)."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing segment files.",
    )

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    input_path = args.input_file.expanduser().resolve()

    if not input_path.exists():
        parser.error(f"input file does not exist: {input_path}")

    if not input_path.is_file():
        parser.error(f"input path is not a file: {input_path}")

    try:
        if is_probably_binary(input_path):
            parser.error(
                f"input appears to be binary because it contains NUL bytes: "
                f"{input_path}"
            )

        text, encoding_used = read_text_preserving_line_endings(
            input_path,
            args.encoding,
        )
        count_tokens = choose_token_counter(args)

        # keepends=True preserves LF, CRLF, CR, tabs, spaces, and indentation.
        lines = text.splitlines(keepends=True)

        segments = split_lines_by_token_limit(
            lines,
            args.tokens,
            count_tokens,
        )

        output_dir = (
            args.output_dir.expanduser().resolve()
            if args.output_dir is not None
            else input_path.parent / f"{input_path.stem}_segments"
        )

        oversized = [
            segment
            for segment in segments
            if segment.estimated_content_tokens > args.tokens
        ]

        print(f"Input: {input_path}")
        print(f"Encoding: {encoding_used}")
        print(f"Tokenizer: {args.tokenizer}")
        print(f"Content token target: {args.tokens:,}")
        print(f"Segments: {len(segments):,}")
        print(f"Output directory: {output_dir}")

        if oversized:
            print(
                "Warning: "
                f"{len(oversized)} segment(s) exceed the token target because "
                "an individual source line is larger than the target. "
                "Those lines were preserved intact.",
                file=sys.stderr,
            )

        write_segments(
            input_path=input_path,
            output_dir=output_dir,
            segments=segments,
            count_tokens=count_tokens,
            encoding=encoding_used,
            overwrite=args.overwrite,
        )

    except (OSError, RuntimeError, FileExistsError, LookupError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
