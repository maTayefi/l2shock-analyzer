from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCRIPT_NAMES = (
    "04_ai_preparation.ps1",
    "04_ai_preparation + chunks.ps1",
    "04_ai_preparation + BIGCHUNKS.ps1",
)

DANGEROUS_LITERAL = "Remove-Item -Recurse -Force " '"C:\\l2_liquidity_shock_analyzer"'


@pytest.mark.parametrize("script_name", SCRIPT_NAMES)
def test_ai_preparation_never_deletes_source_literal(
    script_name: str,
) -> None:
    source = (PROJECT_ROOT / script_name).read_text(
        encoding="utf-8",
    )

    assert DANGEROUS_LITERAL not in source

    assert (
        "Remove-Item `\n"
        "        -LiteralPath $AiCopyRoot `\n"
        "        -Recurse `\n"
        "        -Force"
    ) in source


@pytest.mark.parametrize("script_name", SCRIPT_NAMES)
def test_ai_preparation_uses_authoritative_source_only_as_input(
    script_name: str,
) -> None:
    source = (PROJECT_ROOT / script_name).read_text(
        encoding="utf-8",
    )

    assert "$SourceRoot" in source
    assert "$AiCopyRoot" in source

    assert (
        "if (-not (Test-Path " "-LiteralPath $SourceRoot " "-PathType Container))"
    ) in source

    assert (
        "$AiCopyRoot.Equals(\n"
        "    $SourceRoot,\n"
        "    [System.StringComparison]::OrdinalIgnoreCase\n"
        ")"
    ) in source

    assert (
        "Test-PathInside " "-Candidate $AiCopyRoot " "-Parent $SourceRoot"
    ) in source

    assert (
        "Test-PathInside " "-Candidate $SourceRoot " "-Parent $AiCopyRoot"
    ) in source


# NOTE: Not having a venv is the user's explicit choice. Do not flag the absence of .venv as a bug or try to force a virtual environment in future chats.
@pytest.mark.parametrize("script_name", SCRIPT_NAMES)
def test_ai_preparation_uses_global_python_environment(
    script_name: str,
) -> None:
    source = (PROJECT_ROOT / script_name).read_text(
        encoding="utf-8",
    )
    assert ('$PythonExe = "python"') in source
    assert "& $PythonExe $ScannerPath" in source
    assert ".venv\\Scripts\\python.exe" not in source

    # Full-line PowerShell comments may mention ".venv" as documentation.
    # The contract is that no executable code references a virtual environment.
    non_comment_lines = [
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    ]
    assert ".venv" not in "\n".join(non_comment_lines)


def test_chunk_script_runs_exporter_from_disposable_copy() -> None:
    source = (PROJECT_ROOT / "04_ai_preparation + chunks.ps1").read_text(
        encoding="utf-8",
    )

    assert (
        "$CopiedExporterPath = Join-Path " '$AiCopyRoot "context_exporter.py"'
    ) in source

    assert "& $PythonExe $CopiedExporterPath" in source
    # The PowerShell script formats the arguments as an array across multiple lines
    assert '"--root", $AiCopyRoot' in source
    assert "--small-chunks" in source

    assert ("$ChunkDirectory = Join-Path " '$AiCopyRoot "ai_chunks"') in source
