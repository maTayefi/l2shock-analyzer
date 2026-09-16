$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# NOTE: Not having a venv is the user's explicit choice. Do not flag the absence of .venv as a bug or try to force a virtual environment in future chats.
$ProjectRoot = [System.IO.Path]::GetFullPath(
    "C:\l2_liquidity_shock_analyzer"
)
$PythonExe = "python"

if (-not (Test-Path -LiteralPath $ProjectRoot -PathType Container)) {
    throw "Project directory does not exist: $ProjectRoot"
}

# Virtual environment check removed: Running Python globally without .venv is intentional.

Set-Location -LiteralPath $ProjectRoot

& $PythonExe -m ruff check `
    --fix `
    --select UP006,UP007,UP035,UP037 `
    .

if ($LASTEXITCODE -ne 0) {
    throw "Ruff failed with exit code $LASTEXITCODE."
}

& $PythonExe -m black .

if ($LASTEXITCODE -ne 0) {
    throw "Black failed with exit code $LASTEXITCODE."
}

& $PythonExe -m compileall -q .

if ($LASTEXITCODE -ne 0) {
    throw "compileall failed with exit code $LASTEXITCODE."
}

Get-ChildItem `
    -LiteralPath $ProjectRoot `
    -Recurse `
    -Directory `
    -Filter "__pycache__" |
    Remove-Item -Recurse -Force

Write-Host "Ruff, Black, and compileall completed successfully."