"""Publish one explicitly selected, locally produced L2 seed artifact to HF.

Usage (PowerShell):
    python publish_seed_to_hf.py <path-to-l2-artifact.parquet>

The artifact must be an L2 artifact that owns a usable output checkpoint.
The matching external manifest is located through the artifact key, never
by guessing among multiple files.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path, PurePosixPath

from pydantic import SecretStr

from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    read_remote_artifact_file,
)
from l2shock.remote.contracts import RemoteArtifactKind
from l2shock.remote.hf_repository import HuggingFaceDatasetRepository

REPO_ID = os.environ.get("L2SHOCK_HF_REPO_ID", "maTayefi/l2shock-processed")
HF_TOKEN = os.environ.get("L2SHOCK__REMOTE__HF_TOKEN", "")
OUTPUT_DIR = Path("data/remote-output")


def _print_candidates() -> None:
    candidates = sorted(OUTPUT_DIR.rglob("*.parquet")) if OUTPUT_DIR.is_dir() else []
    print("Candidate artifacts under data/remote-output:")
    for candidate in candidates:
        print(f"  {candidate}")


def main(argv: list[str]) -> int:
    if not HF_TOKEN:
        print("ERROR: Set L2SHOCK__REMOTE__HF_TOKEN environment variable")
        return 1

    if len(argv) != 2:
        print("ERROR: pass exactly one artifact path")
        _print_candidates()
        return 2

    artifact_path = Path(argv[1]).expanduser().resolve()

    if artifact_path.is_symlink() or not artifact_path.is_file():
        print(f"ERROR: artifact is not a regular file: {artifact_path}")
        return 2

    embedded = read_remote_artifact_file(artifact_path)
    key = embedded.manifest.key

    if key.kind is not RemoteArtifactKind.L2 or not isinstance(
        embedded, RemoteL2ProcessedArtifact
    ):
        print("ERROR: a seed must be an L2 artifact")
        return 2

    if embedded.output_checkpoint is None:
        print(
            "ERROR: this L2 artifact has no output checkpoint; it cannot seed a chain"
        )
        return 2

    relative_parts = PurePosixPath(key.relative_path).parts

    if tuple(artifact_path.parts[-len(relative_parts) :]) != relative_parts:
        print("ERROR: artifact is not stored at its canonical relative path")
        return 2

    output_root = Path(*artifact_path.parts[: -len(relative_parts)])
    manifest_path = output_root.joinpath(
        *PurePosixPath(key.manifest_relative_path).parts
    )

    if not manifest_path.is_file():
        print(f"ERROR: matching manifest not found: {manifest_path}")
        return 2

    artifact = read_remote_artifact_file(
        artifact_path,
        expected_key=key,
        external_manifest_bytes=manifest_path.read_bytes(),
    )

    print(f"Artifact: {artifact_path}")
    print(f"Manifest: {manifest_path}")
    print(f"Key:      {key.relative_path}")
    print(f"Venue:    {key.venue} / {key.instrument} @ {key.hour_utc.isoformat()}")
    print(f"Content SHA: {artifact.manifest.content_sha256}")

    repo = HuggingFaceDatasetRepository(
        repo_id=REPO_ID,
        revision="main",
        token=SecretStr(HF_TOKEN),
    )
    result = repo.publish_artifact(artifact)
    print(f"\nPublished: revision={result.revision}, created={result.created}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
