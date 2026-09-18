# tests/test_remote_workflow_contract.py
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "remote-hourly-processing.yml"


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(
        encoding="utf-8",
    )


def test_scheduled_workflow_has_independent_seed_gates() -> None:
    source = _workflow_text()

    assert "L2SHOCK_REMOTE_PROCESSING_ENABLED" in source
    assert "L2SHOCK_BINANCE_SEEDS_READY" in source
    assert "L2SHOCK_BYBIT_SEEDS_READY" in source

    assert 'BINANCE_SEEDS_READY}" != "true"' in source
    assert 'BYBIT_SEEDS_READY}" != "true"' in source
    assert 'EVENT_NAME}" == "schedule"' in source


def test_fully_unseeded_scheduled_matrix_retains_both_okx_chains() -> None:
    source = _workflow_text()

    assert '"chain":"okx_btc"' in source
    assert '"chain":"okx_eth"' in source
    assert '"venue":"okx_futures"' in source
    assert '"instrument":"BTC-USDT-SWAP"' in source
    assert '"instrument":"ETH-USDT-SWAP"' in source


def test_manual_binance_chain_choices_remain_available() -> None:
    source = _workflow_text()

    assert "- binance_btc" in source
    assert "- binance_eth" in source
    assert '"chain":"binance_btc"' in source
    assert '"chain":"binance_eth"' in source
    assert '"venue":"binance_futures"' in source
    assert '"instrument":"BTCUSDT"' in source
    assert '"instrument":"ETHUSDT"' in source


def test_checkpoint_dependent_jobs_are_never_cancelled_in_progress() -> None:
    source = _workflow_text()

    assert "group: l2shock-remote-${{ matrix.chain }}" in source
    assert "cancel-in-progress: false" in source
    assert "queue: single" in source
    assert "cancel-in-progress: true" not in source


def test_workflow_pins_supported_runner_and_node24_checkout() -> None:
    source = _workflow_text()

    assert "actions/checkout@v5" in source
    assert "actions/checkout@v4" not in source

    assert "runs-on: ubuntu-24.04" in source
    assert "runs-on: ubuntu-latest" not in source

    assert "actions/setup-python@v6" in source


def test_manual_bybit_chain_choices_are_available() -> None:
    source = _workflow_text()

    assert "- bybit_btc" in source
    assert "- bybit_eth" in source
    assert '"chain":"bybit_btc"' in source
    assert '"chain":"bybit_eth"' in source
    assert '"venue":"bybit"' in source
    assert '"instrument":"BTCUSDT"' in source
    assert '"instrument":"ETHUSDT"' in source


def test_all_chain_matrix_contains_six_independent_chains() -> None:
    source = _workflow_text()

    for chain in (
        "binance_btc",
        "binance_eth",
        "bybit_btc",
        "bybit_eth",
        "okx_btc",
        "okx_eth",
    ):
        assert f'"chain":"{chain}"' in source


def test_remote_workflow_uses_extended_bootstrap_search_bound() -> None:
    source = _workflow_text()

    assert 'default: "720"' in source
    assert "L2SHOCK_CATCH_UP_HOURS" in source
    assert "|| '720'" in source


def test_remote_workflow_staggers_shared_hf_writers() -> None:
    source = _workflow_text()

    assert '"publication_delay_seconds":0' in source
    assert '"publication_delay_seconds":45' in source
    assert '"publication_delay_seconds":90' in source
    assert '"publication_delay_seconds":135' in source
    assert '"publication_delay_seconds":180' in source
    assert '"publication_delay_seconds":225' in source

    assert "Stagger shared Hugging Face publication writers" in source
    assert 'sleep "${{ matrix.publication_delay_seconds }}"' in source
