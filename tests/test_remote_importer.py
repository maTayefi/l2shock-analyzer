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
