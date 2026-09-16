$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# NOTE: Not having a venv is the user's explicit choice. Do not flag the absence of .venv as a bug or try to force a virtual environment in future chats.
$SourceRoot = [System.IO.Path]::GetFullPath(
    "C:\l2_liquidity_shock_analyzer"
)
$AiCopyRoot = [System.IO.Path]::GetFullPath(
    "D:\project_AI_REVIEW2"
)
$PythonExe = "python"
$ScannerPath = Join-Path $SourceRoot "scan_string_literals.py"
$CopiedExporterPath = Join-Path $AiCopyRoot "context_exporter.py"

function Test-PathInside {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Candidate,

        [Parameter(Mandatory = $true)]
        [string] $Parent
    )

    $candidateFull = [System.IO.Path]::GetFullPath($Candidate)
    $parentFull = [System.IO.Path]::GetFullPath($Parent).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )

    if ($candidateFull.Equals(
        $parentFull,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        return $true
    }

    $prefix = $parentFull + [System.IO.Path]::DirectorySeparatorChar

    return $candidateFull.StartsWith(
        $prefix,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

if (-not (Test-Path -LiteralPath $SourceRoot -PathType Container)) {
    throw "Project source directory does not exist: $SourceRoot"
}

# Virtual environment check removed: Running Python globally without .venv is intentional.

if (-not (Test-Path -LiteralPath $ScannerPath -PathType Leaf)) {
    throw "Scanner script does not exist: $ScannerPath"
}

if ($AiCopyRoot.Equals(
    $SourceRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "AI-review destination must not equal the source repository."
}

if (Test-PathInside -Candidate $AiCopyRoot -Parent $SourceRoot) {
    throw "AI-review destination must be outside the source repository."
}

if (Test-PathInside -Candidate $SourceRoot -Parent $AiCopyRoot) {
    throw (
        "AI-review destination must not contain the source repository. " +
        "Deleting that destination could delete the authoritative source."
    )
}

& $PythonExe -c @"
import sys
raise SystemExit(
    0 if sys.version_info[:2] == (3, 14) else 1
)
"@

if ($LASTEXITCODE -ne 0) {
    throw "The globally installed Python does not use Python 3.14."
}

if (Test-Path -LiteralPath $AiCopyRoot) {
    Write-Host "Removing old disposable AI-review copy: $AiCopyRoot"
    Remove-Item `
        -LiteralPath $AiCopyRoot `
        -Recurse `
        -Force
}

Set-Location -LiteralPath $SourceRoot

& $PythonExe $ScannerPath `
    $SourceRoot `
    --ai-copy $AiCopyRoot `
    --all-nonascii `
    --verbose `
    --backup-description before_ai_review

if ($LASTEXITCODE -ne 0) {
    throw (
        "AI-review copy generation failed with exit code " +
        "$LASTEXITCODE."
    )
}

if (-not (Test-Path -LiteralPath $AiCopyRoot -PathType Container)) {
    throw "AI-review copy was not created: $AiCopyRoot"
}

if (-not (Test-Path -LiteralPath $CopiedExporterPath -PathType Leaf)) {
    throw (
        "The copied context exporter was not found: " +
        "$CopiedExporterPath"
    )
}

Write-Host ""
Write-Host "Generating upload-sized project context segments..."

& $PythonExe $CopiedExporterPath `
    --root $AiCopyRoot `

if ($LASTEXITCODE -ne 0) {
    throw (
        "Context export failed with exit code " +
        "$LASTEXITCODE."
    )
}

$ChunkDirectory = Join-Path $AiCopyRoot "ai_chunks"

if (-not (Test-Path -LiteralPath $ChunkDirectory -PathType Container)) {
    throw "Context-export output directory was not created: $ChunkDirectory"
}

$Segments = @(
    Get-ChildItem `
        -LiteralPath $ChunkDirectory `
        -File `
        -Filter "project_*_of_*_segment.txt"
)

if ($Segments.Count -eq 0) {
    throw "Context exporter produced no project segment files."
}

Write-Host ""
Write-Host "AI-review copy and context segments created successfully."
Write-Host "Authoritative source was not removed by cleanup."
Write-Host "Source:   $SourceRoot"
Write-Host "Copy:     $AiCopyRoot"
Write-Host "Segments: $ChunkDirectory"
Write-Host "Count:    $($Segments.Count)"