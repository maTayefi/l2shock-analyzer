"""Publish a locally produced L2 artifact to your private HF dataset."""

import os, sys
from pathlib import Path
from pydantic import SecretStr
from l2shock.remote.hf_repository import HuggingFaceDatasetRepository
from l2shock.remote.artifact_codec import read_remote_artifact_file

# --- Configuration ---
REPO_ID = os.environ.get("L2SHOCK_HF_REPO_ID", "maTayefi/l2shock-processed")
HF_TOKEN = os.environ.get("L2SHOCK__REMOTE__HF_TOKEN", "")
OUTPUT_DIR = Path("data/remote-output")

if not HF_TOKEN:
    print("ERROR: Set L2SHOCK__REMOTE__HF_TOKEN environment variable")
    sys.exit(1)

# Find the artifact and manifest
artifact_path = None
manifest_path = None
for p in OUTPUT_DIR.rglob("*.parquet"):
    if "l2" in str(p):
        artifact_path = p
        manifest_path = p.with_suffix(".manifest.json")
        break

if not artifact_path or not manifest_path.exists():
    print("ERROR: No L2 artifact found in output directory")
    sys.exit(1)

print(f"Artifact: {artifact_path}")
print(f"Manifest: {manifest_path}")

# Read and verify locally
artifact = read_remote_artifact_file(
    artifact_path,
    external_manifest_bytes=manifest_path.read_bytes(),
)
print(f"Verified: {artifact.manifest.key.relative_path}")
print(f"Content SHA: {artifact.manifest.content_sha256}")

# Publish to HF
repo = HuggingFaceDatasetRepository(
    repo_id=REPO_ID,
    revision="main",
    token=SecretStr(HF_TOKEN),
)

result = repo.publish_artifact(artifact)
print(f"\nPublished! revision={result.revision}, created={result.created}")
print("Binance seed is now in your HF dataset.")
