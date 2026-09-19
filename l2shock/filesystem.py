# l2shock/filesystem.py
"""Filesystem ownership checks for application-managed storage paths.

These helpers validate lexical containment and reject symbolic links and
Windows junctions in every existing component at or below the configured
storage root.

They intentionally do not call Path.resolve() on the candidate path before
validation. Resolving first would erase evidence that an intermediate
component redirects outside application-owned storage.

This substantially hardens path handling on Windows and POSIX. It does not
claim to eliminate every adversarial filesystem race: Python's portable
high-level APIs cannot make a complete multi-component check-and-use sequence
atomic on every supported platform.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

_PATH_ERROR_PREFIX: Final[str] = "Application-owned path validation failed"


class OwnedPathError(ValueError):
    """A path is outside its root or contains an unowned link-like entry."""


def absolute_path_without_resolution(
    path: Path | str,
) -> Path:
    """Return an absolute lexical path without following symbolic links."""
    return Path(
        os.path.abspath(
            os.path.expanduser(
                os.fspath(path),
            )
        )
    )


def path_entry_exists(path: Path | str) -> bool:
    """Return whether the directory entry exists, including broken symlinks."""
    return os.path.lexists(
        os.fspath(path),
    )


def path_entry_is_link_like(path: Path | str) -> bool:
    """Return whether one entry is a symbolic link or Windows junction."""
    entry = Path(path)

    try:
        if entry.is_symlink():
            return True

        is_junction = getattr(
            entry,
            "is_junction",
            None,
        )

        if callable(is_junction) and is_junction():
            return True

        return False
    except OSError as exc:
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: could not inspect a path component"
        ) from exc


def prepare_owned_file_path(
    root: Path | str,
    path: Path | str,
    *,
    create_parents: bool = False,
) -> Path:
    """Validate one file path beneath an application-owned storage root.

    Every existing component at or below ``root`` is inspected without first
    resolving the candidate. Symbolic links and Windows junctions are rejected.

    When ``create_parents`` is true, missing parent directories are created
    one component at a time and immediately revalidated.
    """
    owned_root = absolute_path_without_resolution(root)
    candidate = absolute_path_without_resolution(path)

    try:
        relative = candidate.relative_to(owned_root)
    except ValueError as exc:
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: candidate lies outside its storage root"
        ) from exc

    if not relative.parts:
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: a file path cannot equal its storage root"
        )

    if not path_entry_exists(owned_root):
        if not create_parents:
            raise OwnedPathError(f"{_PATH_ERROR_PREFIX}: storage root does not exist")

        owned_root.mkdir(
            parents=True,
            exist_ok=True,
        )

    if path_entry_is_link_like(owned_root):
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: storage root is a symbolic link or junction"
        )

    if not owned_root.is_dir():
        raise OwnedPathError(f"{_PATH_ERROR_PREFIX}: storage root is not a directory")

    current = owned_root

    for index, part in enumerate(relative.parts):
        current = current / part
        is_leaf = index == len(relative.parts) - 1

        if path_entry_exists(current):
            if path_entry_is_link_like(current):
                raise OwnedPathError(
                    f"{_PATH_ERROR_PREFIX}: path contains a symbolic "
                    "link or junction"
                )

            if not is_leaf and not current.is_dir():
                raise OwnedPathError(
                    f"{_PATH_ERROR_PREFIX}: a parent component is not " "a directory"
                )

            continue

        if is_leaf:
            continue

        if not create_parents:
            raise OwnedPathError(
                f"{_PATH_ERROR_PREFIX}: a parent directory does not exist"
            )

        try:
            current.mkdir()
        except FileExistsError:
            # Another actor created the entry after the existence check.
            # Revalidation below determines whether it is acceptable.
            pass
        except OSError as exc:
            raise OwnedPathError(
                f"{_PATH_ERROR_PREFIX}: could not create a parent directory"
            ) from exc

        if (
            not path_entry_exists(current)
            or path_entry_is_link_like(current)
            or not current.is_dir()
        ):
            raise OwnedPathError(
                f"{_PATH_ERROR_PREFIX}: a newly created parent directory "
                "is not application-owned"
            )

    return candidate


def require_owned_regular_file(
    root: Path | str,
    path: Path | str,
) -> Path:
    """Return a validated owned regular file or raise ``OwnedPathError``."""
    candidate = prepare_owned_file_path(
        root,
        path,
        create_parents=False,
    )

    if not path_entry_exists(candidate):
        raise OwnedPathError(f"{_PATH_ERROR_PREFIX}: file does not exist")

    if path_entry_is_link_like(candidate):
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: file is a symbolic link or junction"
        )

    try:
        if not candidate.is_file():
            raise OwnedPathError(f"{_PATH_ERROR_PREFIX}: path is not a regular file")
    except OSError as exc:
        raise OwnedPathError(
            f"{_PATH_ERROR_PREFIX}: could not inspect the file"
        ) from exc

    return candidate


__all__ = [
    "OwnedPathError",
    "absolute_path_without_resolution",
    "path_entry_exists",
    "path_entry_is_link_like",
    "prepare_owned_file_path",
    "require_owned_regular_file",
]
