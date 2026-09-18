param(
    [Parameter(Mandatory=$true)]
    [string]$TargetDate, # e.g., "2026-09-17"
    [Parameter(Mandatory=$true)]
    [string]$TargetHour  # e.g., "12" or "13"
)

$ErrorActionPreference = "Stop"
$Symbol = "BTCUSDT"

# 1. Calculate Predecessor Hour
$target_dt = [datetime]::ParseExact("$TargetDate $TargetHour", "yyyy-MM-dd HH", $null)
$pred_dt = $target_dt.AddHours(-1)
$pred_date = $pred_dt.ToString("yyyy-MM-dd")
$pred_hour = $pred_dt.ToString("HH")

# Extract integer parts explicitly to prevent PowerShell interpolation bugs
$pred_year = $pred_dt.Year
$pred_month = $pred_dt.Month
$pred_day = $pred_dt.Day
$pred_hour_val = $pred_dt.Hour

Write-Host "Target Hour to Process: $TargetDate $TargetHour"
Write-Host "Predecessor Hour (for synthetic checkpoint): $pred_date $pred_hour"

# 2. Generate Synthetic Checkpoint for Predecessor Hour
Write-Host "`n--- Generating Synthetic Checkpoint ---"
py -3.14 -c @"
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from httpx import Client
from l2shock.ingest.checkpoint_codec import encode_checkpoint, checkpoint_encoding_info
from l2shock.ingest.replay import CheckpointLevel, OrderBookCheckpoint
from l2shock.ingest import BookSide

symbol = '$Symbol'
through_hour = datetime($pred_year, $pred_month, $pred_day, $pred_hour_val, 0, 0, tzinfo=timezone.utc)

client = Client(timeout=30.0)
resp = client.get('https://fapi.binance.com/fapi/v1/depth', params={'symbol': symbol, 'limit': 1000})
resp.raise_for_status()
book = resp.json()

levels = []
for p, q in book['bids']: levels.append(CheckpointLevel(side=BookSide.BID, price=Decimal(p), quantity=Decimal(q), order_count=None))
for p, q in book['asks']: levels.append(CheckpointLevel(side=BookSide.ASK, price=Decimal(p), quantity=Decimal(q), order_count=None))

cp = OrderBookCheckpoint(provider='cryptohftdata', venue='binance_futures', symbol=symbol, through_hour_utc=through_hour, last_update_id=int(book.get('lastUpdateId', 0)), levels=tuple(levels), source_content_sha256=None)
encoded = encode_checkpoint(cp)
info = checkpoint_encoding_info(encoded)

out_dir = Path('data/cache/checkpoints/binance_futures') / symbol / "$pred_date" / "$pred_hour"
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / f'checkpoint-v1-{info.content_sha256}.l2checkpoint'
out_path.write_bytes(encoded)
print(f'Synthetic checkpoint written: {out_path}')
"@

# Find the generated checkpoint
$checkpoint_file = Get-ChildItem -Path "data\cache\checkpoints\binance_futures\$Symbol\$pred_date\$pred_hour" -Filter "*.l2checkpoint" | Select-Object -First 1
if (-not $checkpoint_file) { throw "Failed to find generated synthetic checkpoint." }
Write-Host "Using checkpoint: $($checkpoint_file.FullName)"

# 3. Verify Raw File Exists (from NiceGUI fetch)
$raw_file = "data\raw\cryptohftdata\binance_futures\$TargetDate\$TargetHour\${Symbol}_orderbook.parquet"
if (-not (Test-Path $raw_file)) {
    $raw_file_alt = "data\raw\binance_futures\$TargetDate\$TargetHour\${Symbol}_orderbook.parquet"
    if (Test-Path $raw_file_alt) { $raw_file = $raw_file_alt }
    else { throw "Raw file not found at $raw_file. Please ensure you fetched this exact hour via NiceGUI!" }
}
Write-Host "Raw file found: $raw_file"

# 4. Process via Headless CLI
Write-Host "`n--- Processing Hour via CLI ---"
$input_dir = "data\raw"
$output_dir = "data\remote-worker-output"

py -3.14 -m l2shock.remote_cli l2 `
    --venue binance_futures `
    --instrument $Symbol `
    --hour "${TargetDate}T${TargetHour}:00:00Z" `
    --depth-lower 0 `
    --depth-upper 0.01 `
    --checkpoint-in "$($checkpoint_file.FullName)" `
    --input-dir $input_dir `
    --output-dir $output_dir

if ($LASTEXITCODE -ne 0) { throw "CLI processing failed." }

# 5. Publish to Hugging Face
Write-Host "`n--- Publishing to Hugging Face ---"
# Filter strictly by the TargetHour and Symbol to avoid grabbing leftover files from previous runs
$artifact_file = Get-ChildItem -Path $output_dir -Recurse -Filter "$TargetHour.parquet" | Where-Object { $_.FullName -match "\\$Symbol\\" } | Select-Object -First 1
$manifest_file = Get-ChildItem -Path $output_dir -Recurse -Filter "$TargetHour.manifest.json" | Where-Object { $_.FullName -match "\\$Symbol\\" } | Select-Object -First 1

if (-not $artifact_file -or -not $manifest_file) { throw "Could not find output artifact or manifest." }

Write-Host "Artifact: $($artifact_file.FullName)"
Write-Host "Manifest: $($manifest_file.FullName)"

py -3.14 -c @"
import os
from pathlib import Path
from pydantic import SecretStr
from l2shock.remote.hf_repository import HuggingFaceDatasetRepository
from l2shock.remote.artifact_codec import read_remote_artifact_file

repo_id = os.environ.get('L2SHOCK_HF_REPO_ID', 'maTayefi/l2shock-processed')
token = os.environ.get('L2SHOCK__REMOTE__HF_TOKEN', '')
if not token:
    print('ERROR: Set L2SHOCK__REMOTE__HF_TOKEN environment variable'); exit(1)

artifact_path = Path(r'$($artifact_file.FullName)')
manifest_path = Path(r'$($manifest_file.FullName)')

artifact = read_remote_artifact_file(artifact_path, external_manifest_bytes=manifest_path.read_bytes())
print(f'Verified locally: {artifact.manifest.key.relative_path}')

repo = HuggingFaceDatasetRepository(repo_id=repo_id, revision='main', token=SecretStr(token))
result = repo.publish_artifact(artifact)
print(f'Published to HF! revision={result.revision}')
"@

Write-Host "`nSUCCESS! Binance BTC seed is now in Hugging Face."