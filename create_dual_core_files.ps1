````powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Keep the PowerShell window open so errors/results can be read.
try {

# ============================================================
# L2 Liquidity Shock Analyzer
# Generate all 21 non-duplicate dual-core file inventories.
#
# Run this .ps1 from the project root.
# Input:
#   .\project_cores.md
#
# Output:
#   D:\cores_1_2.txt
#   D:\cores_1_3.txt
#   ...
#   D:\cores_6_7.txt
#
# Existing output files are overwritten.
# ============================================================

$ProjectRoot = (Get-Location).Path
$CoresFile   = Join-Path $ProjectRoot "project_cores.md"
$OutputRoot  = "D:\"

if (-not (Test-Path -LiteralPath $CoresFile -PathType Leaf)) {
    Write-Error "project_cores.md not found at: $CoresFile"
    exit 1
}

if (-not (Test-Path -LiteralPath $OutputRoot -PathType Container)) {
    Write-Error "Output drive/root does not exist: $OutputRoot"
    exit 1
}

# ------------------------------------------------------------
# Parse project_cores.md using embedded Python.
#
# Python is used only for parsing because the markdown structure
# is easier and safer to process there.
# ------------------------------------------------------------

$PythonCode = @'
from pathlib import Path
import re
import sys
import itertools

cores_file = Path(sys.argv[1])
output_root = Path(sys.argv[2])

text = cores_file.read_text(encoding="utf-8")

# ------------------------------------------------------------
# Locate the seven actual core definition sections.
#
# We deliberately match:
#   "# Core 1 — ..."
# through
#   "# Core 7 — ..."
#
# This avoids accidentally treating later headings such as
# "### 17. Core 1" in the ranking sections as core definitions.
# ------------------------------------------------------------

core_sections = {}

for core_number in range(1, 8):
    pattern = rf"^# Core {core_number} —.*?$"
    match = re.search(pattern, text, re.MULTILINE)

    if not match:
        raise RuntimeError(
            f"Could not find definition section for Core {core_number}"
        )

    start = match.start()

    next_starts = []
    for next_core in range(core_number + 1, 8):
        next_pattern = rf"^# Core {next_core} —.*?$"
        next_match = re.search(next_pattern, text, re.MULTILINE)

        if next_match:
            next_starts.append(next_match.start())

    end = min(next_starts) if next_starts else len(text)

    core_sections[core_number] = text[start:end]


# ------------------------------------------------------------
# Extract the global test inventory.
# ------------------------------------------------------------

global_test_match = re.search(
    r"^# Global test inventory\s*$",
    text,
    re.MULTILINE,
)

if not global_test_match:
    raise RuntimeError("Could not find '# Global test inventory'")

global_test_tail = text[global_test_match.end():]

global_test_block = re.search(
    r"```(?:text)?\s*\n(.*?)\n```",
    global_test_tail,
    re.DOTALL,
)

if not global_test_block:
    raise RuntimeError("Could not find global test inventory code block")

global_tests = [
    line.strip()
    for line in global_test_block.group(1).splitlines()
    if line.strip()
]


# ------------------------------------------------------------
# Sections that define files belonging to a core.
#
# We intentionally do NOT parse:
#   Focus areas
#   Ranked combinations
#   Unified rankings
#   Recommended retrieval groupings
#
# Those sections describe review strategy, not authoritative
# file inventories.
# ------------------------------------------------------------

inventory_headers = [
    "## Primary modules",
    "## Important dependencies",
    "## Workflow files",
    "## Primary tests",
]


def extract_inventory_files(section_text):
    files = []

    for header in inventory_headers:
        header_match = re.search(
            rf"^{re.escape(header)}\s*$",
            section_text,
            re.MULTILINE,
        )

        if not header_match:
            continue

        remainder = section_text[header_match.end():]

        code_block_match = re.search(
            r"```(?:text)?\s*\n(.*?)\n```",
            remainder,
            re.DOTALL,
        )

        if not code_block_match:
            continue

        for line in code_block_match.group(1).splitlines():
            line = line.strip()

            if not line:
                continue

            files.append(line)

    # Deduplicate while preserving the first occurrence.
    return list(dict.fromkeys(files))


core_files = {}

for core_number in range(1, 8):
    files = extract_inventory_files(core_sections[core_number])

    # Core 6 explicitly says:
    #
    # "All tests are valid for Core 6"
    #
    # Therefore include the complete global test inventory rather
    # than only the smaller illustrative "Primary tests" list.
    if core_number == 6:
        files.extend(global_tests)

    # Final deduplication while preserving first-seen order.
    core_files[core_number] = list(dict.fromkeys(files))


# ------------------------------------------------------------
# Basic validation.
# ------------------------------------------------------------

for core_number in range(1, 8):
    if not core_files[core_number]:
        raise RuntimeError(
            f"Core {core_number} produced an empty file inventory"
        )


# ------------------------------------------------------------
# Generate all 21 unordered dual-core combinations.
#
# itertools.combinations([1..7], 2) produces:
#
# 1_2
# 1_3
# ...
# 1_7
# 2_3
# ...
# 6_7
# ------------------------------------------------------------

created = []

for core_a, core_b in itertools.combinations(range(1, 8), 2):

    # Union of both core inventories.
    #
    # dict.fromkeys() provides stable deduplication while retaining
    # the order in which files occur in Core A and then Core B.
    combined_files = list(
        dict.fromkeys(
            core_files[core_a] +
            core_files[core_b]
        )
    )

    output_file = output_root / f"cores_{core_a}_{core_b}.txt"

    # Explicit overwrite.
    output_file.write_text(
        "\n".join(combined_files) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    created.append(
        (
            core_a,
            core_b,
            output_file,
            len(combined_files),
        )
    )


# ------------------------------------------------------------
# Print a concise result for PowerShell.
# ------------------------------------------------------------

print("")
print("Dual-core inventories created:")
print("")

for core_a, core_b, output_file, count in created:
    print(
        f"  Core {core_a}+{core_b}: "
        f"{count} unique files -> {output_file}"
    )

print("")
print(f"Created {len(created)} files.")
'@

# ------------------------------------------------------------
# Execute the embedded Python.
# ------------------------------------------------------------

$PythonCode | py -3.14 - "$CoresFile" "$OutputRoot"

if ($LASTEXITCODE -ne 0) {
    Write-Error "Embedded Python parser failed with exit code $LASTEXITCODE."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Done."
Write-Host "Dual-core files are in D:\"

}
catch {
    Write-Host ""
    Write-Host "============================================================"
    Write-Host "SCRIPT FAILED"
    Write-Host "============================================================"
    Write-Host ""
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host ""
    Write-Host "Full error:"
    Write-Host $_
    Write-Host ""
}
finally {
    Write-Host ""
    Read-Host "Press ENTER to close this window"
}
````