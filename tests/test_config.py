from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from l2shock.config import (
    PROJECT_ROOT,
    DatabaseConfig,
    Settings,
    load_settings,
)


def _minimal_config() -> dict:
    return {
        "app": {
            "timezone": "Asia/Tehran",
        },
        "database": {
            "database": "l2shock_local",
        },
    }


def test_unknown_top_level_configuration_field_is_rejected() -> None:
    raw = _minimal_config()
    raw["databse"] = {
        "database": "misspelled-section",
    }

    with pytest.raises(
        ValidationError,
        match="Extra inputs are not permitted",
    ):
        Settings(**raw)


def test_unknown_nested_configuration_field_is_rejected() -> None:
    raw = _minimal_config()
    raw["database"]["databse"] = "misspelled-field"

    with pytest.raises(
        ValidationError,
        match="Extra inputs are not permitted",
    ):
        Settings(**raw)


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        1.5,
        -0.5,
        "1.5",
        object(),
    ],
)
def test_database_max_overflow_rejects_non_integer_values(
    value: object,
) -> None:
    raw = _minimal_config()
    raw["database"]["max_overflow"] = value

    with pytest.raises(ValidationError, match="max_overflow"):
        Settings(**raw)


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        1.5,
        -0.5,
        "1.5",
        object(),
    ],
)
def test_analysis_context_rejects_non_integer_values(
    value: object,
) -> None:
    raw = _minimal_config()
    raw["analysis"] = {
        "price_context_bars_before": value,
    }

    with pytest.raises(
        ValidationError,
        match="price_context_bars_before",
    ):
        Settings(**raw)


def test_integer_configuration_strings_remain_supported() -> None:
    raw = _minimal_config()
    raw["database"]["max_overflow"] = "7"
    raw["analysis"] = {
        "price_context_bars_before": "0",
        "price_context_bars_after": "3",
    }

    settings = Settings(**raw)

    assert settings.database.max_overflow == 7
    assert settings.analysis.price_context_bars_before == 0
    assert settings.analysis.price_context_bars_after == 3


def test_default_locked_lm_priorities_sum_to_one() -> None:
    settings = Settings(**_minimal_config())
    lm = settings.analysis.lm

    total = (
        lm.priority_height
        + lm.priority_sharpness
        + lm.priority_endpoint_extremeness
        + lm.priority_retracement_magnitude
        + lm.priority_retracement_count
    )

    assert total == pytest.approx(1.0)
    assert lm.priority_retracement_count == pytest.approx(0.05)


def test_invalid_lm_priority_total_is_rejected() -> None:
    raw = _minimal_config()
    raw["analysis"] = {
        "lm": {
            "priority_height": 0.35,
            "priority_sharpness": 0.30,
            "priority_endpoint_extremeness": 0.20,
            "priority_retracement_magnitude": 0.10,
            "priority_retracement_count": 0.50,
        }
    }

    with pytest.raises(ValidationError, match=r"must sum exactly to 1\.0"):
        Settings(**raw)


def test_only_btc_and_eth_are_currently_supported() -> None:
    raw = _minimal_config()
    raw["analysis"] = {"supported_bases": ["BTC", "SOL"]}

    with pytest.raises(ValidationError, match="only BTC and ETH"):
        Settings(**raw)


def test_config_file_loads_from_explicit_path(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(_minimal_config()),
        encoding="utf-8",
    )

    settings = load_settings(path)

    assert settings.database.database == "l2shock_local"
    assert settings.app.timezone == "Asia/Tehran"


def test_database_password_is_not_exposed_in_repr() -> None:
    database = DatabaseConfig(password="sensitive-value")

    assert "sensitive-value" not in repr(database)
    assert database.password.get_secret_value() == "sensitive-value"


def test_non_loopback_app_host_is_rejected() -> None:
    raw = _minimal_config()
    raw["app"]["host"] = "0.0.0.0"

    with pytest.raises(ValidationError, match="loopback"):
        Settings(**raw)


def test_default_cryptohft_rest_base_url_is_api_v1() -> None:
    settings = Settings(**_minimal_config())

    assert settings.cryptohft.base_url == "https://api.cryptohftdata.com/v1"


def test_default_automatic_fetch_catch_up_window_is_72_hours() -> None:
    settings = Settings(**_minimal_config())

    assert settings.cryptohft.automatic_fetch_catch_up_hours == 72


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        0,
        -1,
        8_761,
    ],
)
def test_automatic_fetch_catch_up_window_is_bounded(
    value: object,
) -> None:
    raw = _minimal_config()
    raw["cryptohft"] = {
        "automatic_fetch_catch_up_hours": value,
    }

    with pytest.raises(ValidationError):
        Settings(**raw)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("app", "port", True),
        ("database", "pool_size", 5.0),
        ("storage", "raw_retention_hours", 72.0),
        ("analysis", "default_chart_max_bars", 400.0),
    ],
)
def test_positive_integer_configuration_rejects_precoercion_values(
    section: str,
    field: str,
    value: object,
) -> None:
    raw = _minimal_config()
    raw.setdefault(section, {})
    raw[section][field] = value

    with pytest.raises(ValidationError, match=field):
        Settings(**raw)


def test_lm_priority_tolerance_cannot_cross_exact_decimal_boundary() -> None:
    raw = _minimal_config()
    raw["analysis"] = {
        "lm": {
            "priority_height": 0.35,
            "priority_sharpness": 0.30,
            "priority_endpoint_extremeness": 0.20,
            "priority_retracement_magnitude": 0.10,
            "priority_retracement_count": 0.0500000005,
        }
    }

    with pytest.raises(
        ValidationError,
        match="sum exactly to 1.0",
    ):
        Settings(**raw)


@pytest.mark.parametrize(
    "field_name",
    (
        "raw_dir",
        "cache_dir",
        "quarantine_dir",
        "export_dir",
        "log_dir",
        "backup_dir",
    ),
)
def test_storage_paths_cannot_be_blank(
    field_name: str,
) -> None:
    raw = _minimal_config()
    raw["storage"] = {
        field_name: "   ",
    }

    with pytest.raises(
        ValidationError,
        match="cannot be blank",
    ):
        Settings(**raw)


def test_storage_path_cannot_resolve_to_project_root() -> None:
    raw = _minimal_config()
    raw["storage"] = {
        "raw_dir": str(PROJECT_ROOT),
    }

    with pytest.raises(
        ValidationError,
        match="PROJECT_ROOT",
    ):
        Settings(**raw)


def test_storage_roles_cannot_share_one_directory(
    tmp_path: Path,
) -> None:
    shared = tmp_path / "shared"

    raw = _minimal_config()
    raw["storage"] = {
        "raw_dir": str(shared),
        "cache_dir": str(shared),
    }

    with pytest.raises(
        ValidationError,
        match="same directory",
    ):
        Settings(**raw)


def test_storage_roles_cannot_be_nested(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "storage"

    raw = _minimal_config()
    raw["storage"] = {
        "raw_dir": str(parent),
        "cache_dir": str(parent / "cache"),
    }

    with pytest.raises(
        ValidationError,
        match="cannot contain",
    ):
        Settings(**raw)
