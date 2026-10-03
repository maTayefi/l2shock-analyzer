import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CORES = ROOT / "project_cores.md"


def get_files(directory: Path) -> list[str]:
    """Return a sorted list of relative POSIX paths for all Python files."""
    return sorted(
        path.relative_to(ROOT).as_posix()
        for path in directory.rglob("*.py")
        if "__pycache__" not in path.parts
    )


tests = get_files(ROOT / "tests")
production = get_files(ROOT / "l2shock")

text = CORES.read_text(encoding="utf-8")


def replace_block(text: str, heading: str, paths: list[str]) -> str:
    """Replace the contents of a ```text block under a specific heading."""
    pattern = rf"({re.escape(heading)}\s*\n\s*```text\n)(.*?)(\n\s*```)"
    replacement = r"\1" + "\n".join(paths) + r"\3"
    return re.sub(pattern, replacement, text, flags=re.DOTALL)


# Update both global inventories to match the actual repository
text = replace_block(text, "# Global test inventory", tests)
text = replace_block(text, "# Global production-module inventory", production)

CORES.write_text(text, encoding="utf-8", newline="\n")
print(
    "Successfully synced project_cores.md inventories with the actual repository files."
)
