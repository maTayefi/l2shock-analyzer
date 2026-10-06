# tests/test_remote_importer.py
from __future__ import annotations

import ast
from pathlib import Path

from l2shock.remote import REMOTE_IMPORT_ORIGIN


def test_remote_import_origin_is_stable() -> None:
    assert REMOTE_IMPORT_ORIGIN == "hugging_face_remote_import_v1"


def test_remote_importer_does_not_reimplement_processing() -> None:
    import l2shock.remote.importer as module

    path = Path(module.__file__)
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
        ),
        filename=str(path),
    )

    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)

        if isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert "l2shock.ingest.replay" not in imported_modules
    assert "l2shock.liquidity.depth" not in imported_modules
    assert "l2shock.liquidity.hourly" not in imported_modules
    assert "l2shock.price.hourly" not in imported_modules

    function_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef,
            ),
        )
    }

    assert "replay_orderbook_archives" not in function_names
    assert "sample_liquidity_archives" not in function_names
    assert "calculate_depth_liquidity" not in function_names
    assert "stream_trade_ohlc_hour" not in function_names


def _audit_import_attachment_case(tmp_path, monkeypatch):
    import hashlib
    from datetime import datetime, timezone
    from types import SimpleNamespace

    import l2shock.remote.importer as module
    from l2shock.acquisition import SourceFileSpec

    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    spec = SourceFileSpec(
        provider="cryptohftdata",
        venue="binance_futures",
        symbol="BTCUSDT",
        data_kind="trades",
        hour_utc=datetime(2095, 1, 1, 12, tzinfo=timezone.utc),
    )
    canonical = spec.local_path(raw_root)
    content = b"audit-import-raw-content"
    digest = hashlib.sha256(content).hexdigest()

    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(
            storage=SimpleNamespace(raw_path=raw_root),
        ),
    )

    row = SimpleNamespace(
        local_path=str(canonical),
        file_size_bytes=len(content),
    )
    return module, spec, canonical, content, digest, row


def test_audit_remote_import_accepts_owned_canonical_attachment(
    tmp_path,
    monkeypatch,
):
    module, spec, canonical, content, digest, row = _audit_import_attachment_case(
        tmp_path, monkeypatch
    )
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(content)

    module._verify_existing_local_source_attachment(
        row,
        spec,
        expected_content_sha256=digest,
    )
    assert canonical.read_bytes() == content


def test_audit_remote_import_rejects_redirected_parent(
    tmp_path,
    monkeypatch,
):
    import pytest

    module, spec, canonical, content, digest, row = _audit_import_attachment_case(
        tmp_path, monkeypatch
    )
    external = tmp_path / "external"
    external.mkdir()
    (external / canonical.name).write_bytes(content)
    canonical.parent.parent.mkdir(parents=True)

    try:
        canonical.parent.symlink_to(external, target_is_directory=True)
    except OSError, NotImplementedError:
        pytest.skip("Directory symlinks are unavailable on this platform")

    with pytest.raises(module.RemoteArtifactImportError):
        module._verify_existing_local_source_attachment(
            row,
            spec,
            expected_content_sha256=digest,
        )

    assert (external / canonical.name).read_bytes() == content


def test_audit_remote_import_rejects_alias_to_canonical_attachment(
    tmp_path,
    monkeypatch,
):
    import pytest

    module, spec, canonical, content, digest, row = _audit_import_attachment_case(
        tmp_path, monkeypatch
    )
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(content)
    alias = tmp_path / "alias.parquet"

    try:
        alias.symlink_to(canonical)
    except OSError, NotImplementedError:
        pytest.skip("File symlinks are unavailable on this platform")

    row.local_path = str(alias)

    with pytest.raises(module.RemoteArtifactImportError):
        module._verify_existing_local_source_attachment(
            row,
            spec,
            expected_content_sha256=digest,
        )

    assert canonical.read_bytes() == content
