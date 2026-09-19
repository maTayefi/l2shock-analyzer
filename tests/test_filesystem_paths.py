from __future__ import annotations

from pathlib import Path

import pytest

from l2shock.filesystem import (
    OwnedPathError,
    prepare_owned_file_path,
    require_owned_regular_file,
)


def _directory_symlink_or_skip(
    link: Path,
    target: Path,
) -> None:
    target.mkdir(
        parents=True,
        exist_ok=True,
    )
    link.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        link.symlink_to(
            target,
            target_is_directory=True,
        )
    except OSError:
        pytest.skip("Directory symbolic links are unavailable on this platform")


def test_prepare_owned_file_path_accepts_real_parent_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    candidate = root / "provider" / "venue" / "source.parquet"

    observed = prepare_owned_file_path(
        root,
        candidate,
        create_parents=True,
    )

    assert observed == candidate.absolute()
    assert candidate.parent.is_dir()
    assert not candidate.exists()


def test_prepare_owned_file_path_rejects_intermediate_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    external = tmp_path / "external"
    redirected = root / "provider"

    root.mkdir()
    _directory_symlink_or_skip(
        redirected,
        external,
    )

    candidate = redirected / "venue" / "source.parquet"

    with pytest.raises(
        OwnedPathError,
        match="symbolic link or junction",
    ):
        prepare_owned_file_path(
            root,
            candidate,
            create_parents=True,
        )

    assert not (external / "venue" / "source.parquet").exists()


def test_require_owned_regular_file_rejects_intermediate_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw"
    external = tmp_path / "external"
    redirected = root / "provider"

    root.mkdir()
    _directory_symlink_or_skip(
        redirected,
        external,
    )

    external_file = external / "source.parquet"
    external_file.write_bytes(b"external-content")

    with pytest.raises(
        OwnedPathError,
        match="symbolic link or junction",
    ):
        require_owned_regular_file(
            root,
            redirected / "source.parquet",
        )

    assert external_file.read_bytes() == b"external-content"
