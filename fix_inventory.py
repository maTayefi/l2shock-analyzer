from pathlib import Path
import re

root = Path.cwd()
path = root / "project_cores.md"
text = path.read_text(encoding="utf-8")

for directory, heading in (
    ("l2shock", "# Global production-module inventory"),
    ("tests", "# Global test inventory"),
):
    heading_pattern = re.compile(
        r"(?m)^" + re.escape(heading) + r"[ \t]*\r?$"
    )
    matches = list(heading_pattern.finditer(text))
    if len(matches) != 1:
        raise SystemExit(f"Expected one heading: {heading}")

    heading_match = matches[0]
    section_end = re.search(
        r"(?m)^# ",
        text[heading_match.end():],
    )
    stop = (
        len(text)
        if section_end is None
        else heading_match.end() + section_end.start()
    )
    section = text[heading_match.end():stop]
    block = re.search(
        r"```text[ \t]*\r?\n(?P<body>.*?)\r?\n```",
        section,
        flags=re.DOTALL,
    )
    if block is None:
        raise SystemExit(f"Inventory fence not found: {heading}")

    files = sorted(
        item.relative_to(root).as_posix()
        for item in (root / directory).rglob("*.py")
        if "__pycache__" not in item.parts
    )
    replacement = "\n".join(files)
    begin = heading_match.end() + block.start("body")
    end = heading_match.end() + block.end("body")
    text = text[:begin] + replacement + text[end:]

path.write_text(text, encoding="utf-8")
print("Successfully updated project_cores.md inventories.")