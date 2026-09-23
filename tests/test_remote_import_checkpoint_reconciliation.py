from __future__ import annotations

import pytest

from l2shock.remote.importer import (
    RemoteArtifactImportError,
    _reconciled_output_checkpoint_references,
)


def test_checkpointless_import_retains_local_output_ownership() -> None:
    assert _reconciled_output_checkpoint_references(
        {"output_checkpoint_content_sha256": "a" * 64},
        None,
    ) == ("a" * 64, None)


def test_identical_remote_output_agrees_with_existing_local_output() -> None:
    assert _reconciled_output_checkpoint_references(
        {"output_checkpoint_content_sha256": "a" * 64},
        "a" * 64,
    ) == ("a" * 64, "a" * 64)


def test_checkpointless_import_retains_existing_remote_output() -> None:
    assert _reconciled_output_checkpoint_references(
        {"remote_output_checkpoint_content_sha256": "b" * 64},
        None,
    ) == ("b" * 64, "b" * 64)


def test_new_remote_output_cannot_replace_another_durable_output() -> None:
    with pytest.raises(RemoteArtifactImportError, match="conflicts"):
        _reconciled_output_checkpoint_references(
            {"output_checkpoint_content_sha256": "a" * 64},
            "b" * 64,
        )


def test_existing_conflicting_output_references_fail_closed() -> None:
    with pytest.raises(RemoteArtifactImportError, match="conflicting"):
        _reconciled_output_checkpoint_references(
            {
                "output_checkpoint_content_sha256": "a" * 64,
                "remote_output_checkpoint_content_sha256": "b" * 64,
            },
            None,
        )


def test_malformed_existing_output_reference_fails_closed() -> None:
    with pytest.raises(RemoteArtifactImportError, match="canonical"):
        _reconciled_output_checkpoint_references(
            {"output_checkpoint_content_sha256": "not-a-digest"},
            None,
        )
