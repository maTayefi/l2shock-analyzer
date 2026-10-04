from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from l2shock.config import B2Config, RemoteConfig, Settings


def _valid_b2_values() -> dict:
    return {
        "endpoint_url": "https://s3.us-west-004.backblazeb2.com",
        "bucket": "l2shock-test-storage",
        "key_id": "test-key-id",
        "application_key": "test-application-key",
    }


def test_b2_defaults_are_unconfigured() -> None:
    settings = B2Config()

    assert settings.configured is False
    assert settings.region == ""
    assert settings.total_max_attempts == 6


def test_b2_configuration_normalizes_endpoint_and_derives_region() -> None:
    values = _valid_b2_values()
    values["endpoint_url"] += "/"

    settings = B2Config(**values)

    assert settings.configured is True
    assert settings.endpoint_url == "https://s3.us-west-004.backblazeb2.com"
    assert settings.region == "us-west-004"


@pytest.mark.parametrize(
    "endpoint",
    (
        "http://s3.us-west-004.backblazeb2.com",
        "https://s3.us-west-004.backblazeb2.com/private-bucket",
        "https://s3.us-west-004.backblazeb2.com?token=secret",
        "https://user:secret@s3.us-west-004.backblazeb2.com",
        "https://s3.us-west-004.backblazeb2.com.evil.example",
        "https://127.0.0.1",
        "https://s3.amazonaws.com",
    ),
)
def test_b2_endpoint_rejects_unsafe_or_non_b2_locations(endpoint: str) -> None:
    values = _valid_b2_values()
    values["endpoint_url"] = endpoint

    with pytest.raises(ValidationError, match="endpoint_url"):
        B2Config(**values)


@pytest.mark.parametrize(
    "bucket",
    (
        "UPPERCASE",
        "ab",
        "contains/slash",
        "contains.dot",
        "-leading",
        "trailing-",
        "contains space",
    ),
)
def test_b2_bucket_uses_project_dns_safe_policy(bucket: str) -> None:
    values = _valid_b2_values()
    values["bucket"] = bucket

    with pytest.raises(ValidationError, match="bucket"):
        B2Config(**values)


@pytest.mark.parametrize(
    "field",
    (
        "total_max_attempts",
        "connect_timeout_seconds",
        "read_timeout_seconds",
        "maximum_retry_after_seconds",
    ),
)
@pytest.mark.parametrize("value", (True, False, 0, -1, 1.5, "1.5"))
def test_b2_integer_settings_reject_invalid_values(field: str, value) -> None:
    values = _valid_b2_values()
    values[field] = value

    with pytest.raises(ValidationError):
        B2Config(**values)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("total_max_attempts", 13),
        ("connect_timeout_seconds", 301),
        ("read_timeout_seconds", 601),
        ("maximum_retry_after_seconds", 3601),
    ),
)
def test_b2_integer_settings_are_bounded(field: str, value: int) -> None:
    values = _valid_b2_values()
    values[field] = value

    with pytest.raises(ValidationError):
        B2Config(**values)


def test_b2_secrets_are_excluded_from_repr_and_dump() -> None:
    settings = B2Config(**_valid_b2_values())

    assert "test-key-id" not in repr(settings)
    assert "test-application-key" not in repr(settings)

    dumped = settings.model_dump()

    assert "key_id" not in dumped
    assert "application_key" not in dumped


def test_b2_credential_validation_does_not_echo_secret() -> None:
    values = _valid_b2_values()
    values["application_key"] = "private-value\n"

    with pytest.raises(ValidationError) as captured:
        B2Config(**values)

    assert "private-value" not in str(captured.value)


def test_b2_addition_does_not_change_hf_configuration_semantics() -> None:
    remote = RemoteConfig(
        b2=B2Config(**_valid_b2_values()),
    )

    assert remote.b2.configured is True
    assert remote.configured is False
    assert remote.default_workflow == "remote_hf_import"


def test_b2_nested_environment_values_are_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in tuple(os.environ):
        if name.upper().startswith("L2SHOCK__"):
            monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv(
        "L2SHOCK__REMOTE__B2__ENDPOINT_URL",
        "https://s3.us-west-004.backblazeb2.com",
    )
    monkeypatch.setenv(
        "L2SHOCK__REMOTE__B2__BUCKET",
        "l2shock-test-storage",
    )
    monkeypatch.setenv(
        "L2SHOCK__REMOTE__B2__KEY_ID",
        "environment-key-id",
    )
    monkeypatch.setenv(
        "L2SHOCK__REMOTE__B2__APPLICATION_KEY",
        "environment-application-key",
    )
    monkeypatch.setenv(
        "L2SHOCK__REMOTE__B2__TOTAL_MAX_ATTEMPTS",
        "4",
    )

    settings = Settings(
        _env_file=None,
        app={"timezone": "Asia/Tehran"},
        database={"database": "l2shock_local"},
    )

    assert settings.remote.b2.configured is True
    assert settings.remote.b2.total_max_attempts == 4
    assert settings.remote.b2.key_id.get_secret_value() == "environment-key-id"
    assert (
        settings.remote.b2.application_key.get_secret_value()
        == "environment-application-key"
    )
    assert settings.remote.configured is False
