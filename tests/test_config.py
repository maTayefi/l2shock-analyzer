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
    ("key", "value"),
    [
        ("activity_timeframes", ["1s"]),
        ("default_activity_timeframe", "1s"),
        ("default_chart_max_bars", 2000),
        ("price_context_bars_before", 3),
        ("price_context_bars_after", 3),
    ],
)
def test_retired_lm_era_analysis_keys_have_actionable_error(
    key: str,
    value: object,
) -> None:
    raw = _minimal_config()
    raw["analysis"] = {key: value}

    with pytest.raises(ValidationError, match=rf"analysis\.{key}"):
        Settings(**raw)


def test_retired_lm_era_analysis_fields_are_gone() -> None:
    fields = type(Settings(**_minimal_config()).analysis).model_fields

    for key in (
        "activity_timeframes",
        "default_activity_timeframe",
        "default_chart_max_bars",
        "price_context_bars_before",
        "price_context_bars_after",
    ):
        assert key not in fields, key


def test_integer_configuration_strings_remain_supported() -> None:
    raw = _minimal_config()
    raw["database"]["max_overflow"] = "7"
    raw["analysis"] = {
        "l2_long_invalid_warning_seconds": "90",
    }

    settings = Settings(**raw)

    assert settings.database.max_overflow == 7
    assert settings.analysis.l2_long_invalid_warning_seconds == 90


def test_lm_configuration_section_is_retired() -> None:
    settings = Settings(**_minimal_config())

    assert not hasattr(settings.analysis, "lm")
    assert "lm" not in type(settings.analysis).model_fields


def test_leftover_lm_block_has_actionable_error() -> None:
    raw = _minimal_config()
    raw["analysis"] = {
        "lm": {
            "top_n_height": 10,
        }
    }

    with pytest.raises(ValidationError, match="analysis.lm was removed"):
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
        ("analysis", "l2_long_invalid_warning_seconds", 60.0),
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


def test_remote_hf_defaults_to_remote_import_profile() -> None:
    settings = Settings(**_minimal_config())

    assert settings.remote.default_workflow == "remote_hf_import"
    assert settings.remote.hf_revision == "main"
    assert settings.remote.configured is False


def test_remote_hf_private_token_is_not_exposed_in_repr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Prevent the real .env file from overriding the test token.
    # env_settings has higher priority than init_settings (constructor args)
    # in Settings.settings_customise_sources, so we must set the env vars
    # explicitly to guarantee deterministic test isolation.
    monkeypatch.setenv("L2SHOCK__REMOTE__HF_REPO_ID", "maTayefi/l2shock-processed")
    monkeypatch.setenv("L2SHOCK__REMOTE__HF_REVISION", "main")
    monkeypatch.setenv("L2SHOCK__REMOTE__HF_TOKEN", "local-private-token-value")

    raw = _minimal_config()
    raw["remote"] = {
        "hf_repo_id": "maTayefi/l2shock-processed",
        "hf_revision": "main",
        "hf_token": "local-private-token-value",
    }

    settings = Settings(**raw)

    assert settings.remote.configured is True
    assert "local-private-token-value" not in repr(settings.remote)
    assert settings.remote.hf_token.get_secret_value() == ("local-private-token-value")


@pytest.mark.parametrize(
    "repo_id",
    (
        "missing-slash",
        "/dataset",
        "namespace/",
        "namespace/data set",
        "namespace/../dataset",
    ),
)
def test_remote_hf_repository_id_is_strict(
    repo_id: str,
) -> None:
    raw = _minimal_config()
    raw["remote"] = {
        "hf_repo_id": repo_id,
    }

    with pytest.raises(
        ValidationError,
        match="hf_repo_id",
    ):
        Settings(**raw)


def test_remote_hf_default_workflow_is_strict() -> None:
    raw = _minimal_config()
    raw["remote"] = {
        "default_workflow": "cloud_magic",
    }

    with pytest.raises(
        ValidationError,
        match="default_workflow",
    ):
        Settings(**raw)


def test_environment_example_contains_only_supported_settings(
    monkeypatch,
) -> None:
    import os
    from pathlib import Path

    from l2shock.config import Settings

    # Real process overrides have higher precedence than the template.
    # Remove them only inside this isolated test; monkeypatch restores them.
    for name in tuple(os.environ):
        if name.upper().startswith("L2SHOCK__"):
            monkeypatch.delenv(name, raising=False)

    template = Path(__file__).resolve().parents[1] / ".env.example"
    settings = Settings(_env_file=template)

    assert settings.processing.checkpoint_search_max_hours == 168
