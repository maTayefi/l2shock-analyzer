from __future__ import annotations

from l2shock.processing.l2_coordinator import (
    _updated_analytical_output_metadata,
)


def test_l2_output_metadata_preserves_legacy_and_new_hashes() -> None:
    hashes, outputs = _updated_analytical_output_metadata(
        {
            "analytical_content_sha256": "a" * 64,
        },
        preset_hash="b" * 64,
        content_sha256="c" * 64,
    )

    assert hashes == [
        "a" * 64,
        "c" * 64,
    ]
    assert outputs == {
        "b" * 64: "c" * 64,
    }


def test_l2_output_metadata_preserves_prior_preset_outputs() -> None:
    hashes, outputs = _updated_analytical_output_metadata(
        {
            "analytical_content_sha256": "b" * 64,
            "analytical_content_sha256s": [
                "a" * 64,
                "b" * 64,
            ],
            "analytical_outputs_by_preset": {
                "1" * 64: "a" * 64,
                "2" * 64: "b" * 64,
            },
        },
        preset_hash="3" * 64,
        content_sha256="c" * 64,
    )

    assert hashes == [
        "a" * 64,
        "b" * 64,
        "c" * 64,
    ]
    assert outputs == {
        "1" * 64: "a" * 64,
        "2" * 64: "b" * 64,
        "3" * 64: "c" * 64,
    }


def test_repeated_materialization_is_idempotent_in_metadata() -> None:
    hashes, outputs = _updated_analytical_output_metadata(
        {
            "analytical_content_sha256": "c" * 64,
            "analytical_content_sha256s": [
                "c" * 64,
            ],
            "analytical_outputs_by_preset": {
                "3" * 64: "c" * 64,
            },
        },
        preset_hash="3" * 64,
        content_sha256="c" * 64,
    )

    assert hashes == ["c" * 64]
    assert outputs == {
        "3" * 64: "c" * 64,
    }
