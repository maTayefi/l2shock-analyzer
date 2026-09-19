# l2shock/config.py
"""Validated project configuration.

Configuration precedence:

1. real ``L2SHOCK__...`` environment variables;
2. project-root ``.env``;
3. ``config.yaml``;
4. model defaults.

Passwords and API keys should remain in environment variables or ``.env``.
"""

from __future__ import annotations

import ipaddress
import math
import os
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.yaml"
DEFAULT_DOTENV_PATH = PROJECT_ROOT / ".env"


def _strict_positive_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, not boolean")

    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise ValueError(f"{field_name} must be a positive integer")

    if result <= 0:
        raise ValueError(f"{field_name} must be > 0")

    return result


def _finite_float(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric, not boolean")

    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc

    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")

    return result


def _strict_nonnegative_int(
    value: Any,
    *,
    field_name: str,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, not boolean")

    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise ValueError(f"{field_name} must be a non-negative integer")

    if result < 0:
        raise ValueError(f"{field_name} must be >= 0")

    return result


class StrictConfigModel(BaseModel):
    """Base for nested configuration objects.

    Unknown fields are rejected so misspelled configuration cannot silently
    fall back to a default value.
    """

    model_config = ConfigDict(extra="forbid")


class AppConfig(StrictConfigModel):
    title: str = "Liquidity Shock Analyzer"
    host: str = "127.0.0.1"
    port: int = 8080
    timezone: str = "Asia/Tehran"
    log_level: str = "INFO"

    @field_validator("title")
    @classmethod
    def _nonblank_title(cls, value: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError("app.title must be non-empty")
        return result

    @field_validator("host")
    @classmethod
    def _loopback_host(cls, value: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError("app.host must be non-empty")

        if result.lower() == "localhost":
            return result

        try:
            address = ipaddress.ip_address(result)
        except ValueError as exc:
            raise ValueError(
                "app.host must be a loopback address such as "
                "127.0.0.1, ::1, or localhost"
            ) from exc

        if not address.is_loopback:
            raise ValueError(
                "app.host must remain loopback-only until authentication "
                "and the network security model are implemented"
            )

        return result

    @field_validator("port", mode="before")
    @classmethod
    def _port(cls, value: Any) -> int:
        result = _strict_positive_int(value, field_name="app.port")
        if result > 65535:
            raise ValueError("app.port must be <= 65535")
        return result

    @field_validator("timezone")
    @classmethod
    def _timezone(cls, value: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError("app.timezone must be non-empty")

        try:
            ZoneInfo(result)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Invalid IANA timezone: {result!r}") from exc

        return result

    @field_validator("log_level")
    @classmethod
    def _log_level(cls, value: str) -> str:
        result = str(value or "").strip().upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if result not in allowed:
            raise ValueError(
                "app.log_level must be one of " + ", ".join(sorted(allowed))
            )
        return result


class DatabaseConfig(StrictConfigModel):
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "postgres"
    password: SecretStr = Field(
        default_factory=lambda: SecretStr(""),
        repr=False,
    )
    database: str = "l2shock_local"
    admin_database: str = "postgres"
    pool_size: int = 5
    max_overflow: int = 5
    pool_timeout_seconds: int = 30

    @field_validator("host", "user", "database", "admin_database")
    @classmethod
    def _nonblank_database_text(cls, value: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError("database identity fields must be non-empty")
        return result

    @field_validator("port", mode="before")
    @classmethod
    def _port(cls, value: Any) -> int:
        result = _strict_positive_int(value, field_name="database.port")
        if result > 65535:
            raise ValueError("database.port must be <= 65535")
        return result

    @field_validator(
        "pool_size",
        "pool_timeout_seconds",
        mode="before",
    )
    @classmethod
    def _positive_database_integer(cls, value: Any, info) -> int:
        return _strict_positive_int(
            value,
            field_name=f"database.{info.field_name}",
        )

    @field_validator("max_overflow", mode="before")
    @classmethod
    def _nonnegative_overflow(cls, value: Any) -> int:
        return _strict_nonnegative_int(
            value,
            field_name="database.max_overflow",
        )

    def _url_for_database(self, database_name: str) -> str:
        url = URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=database_name,
        )
        return url.render_as_string(hide_password=False)

    @property
    def sqlalchemy_url(self) -> str:
        return self._url_for_database(self.database)

    @property
    def admin_sqlalchemy_url(self) -> str:
        return self._url_for_database(self.admin_database)


class StorageConfig(StrictConfigModel):
    raw_dir: str = "data/raw"
    cache_dir: str = "data/cache"
    quarantine_dir: str = "data/quarantine"
    export_dir: str = "data/exports"
    log_dir: str = "logs"
    backup_dir: str = "backups"
    raw_retention_hours: int = 72
    minimum_free_disk_gib: float = 5.0

    @field_validator(
        "raw_dir",
        "cache_dir",
        "quarantine_dir",
        "export_dir",
        "log_dir",
        "backup_dir",
        mode="before",
    )
    @classmethod
    def _nonblank_storage_path(cls, value: Any, info) -> str:
        if not isinstance(value, str):
            raise ValueError(f"storage.{info.field_name} must be a path string")

        result = value.strip()

        if not result:
            raise ValueError(f"storage.{info.field_name} cannot be blank")

        return result

    @field_validator("raw_retention_hours", mode="before")
    @classmethod
    def _retention(cls, value: Any) -> int:
        return _strict_positive_int(
            value,
            field_name="storage.raw_retention_hours",
        )

    @field_validator("minimum_free_disk_gib")
    @classmethod
    def _minimum_free_disk(cls, value: Any) -> float:
        result = _finite_float(
            value,
            field_name="storage.minimum_free_disk_gib",
        )
        if result < 0.0:
            raise ValueError("storage.minimum_free_disk_gib must be >= 0")
        return result

    @model_validator(mode="after")
    def _separate_storage_roles(self) -> StorageConfig:
        paths = {
            "raw_dir": self.raw_path,
            "cache_dir": self.cache_path,
            "quarantine_dir": self.quarantine_path,
            "export_dir": self.export_path,
            "log_dir": self.log_path,
            "backup_dir": self.backup_path,
        }
        project_root = PROJECT_ROOT.resolve()

        for role, path in paths.items():
            if path == project_root:
                raise ValueError(f"storage.{role} cannot resolve to PROJECT_ROOT")

            anchor_text = path.anchor

            if anchor_text and path == Path(anchor_text).resolve():
                raise ValueError(f"storage.{role} cannot resolve to a filesystem root")

        ordered = tuple(paths.items())

        for index, (first_role, first_path) in enumerate(ordered):
            for second_role, second_path in ordered[index + 1 :]:
                if first_path == second_path:
                    raise ValueError(
                        f"storage.{first_role} and storage.{second_role} "
                        "cannot resolve to the same directory"
                    )

                if first_path in second_path.parents:
                    raise ValueError(
                        f"storage.{first_role} cannot contain " f"storage.{second_role}"
                    )

                if second_path in first_path.parents:
                    raise ValueError(
                        f"storage.{second_role} cannot contain " f"storage.{first_role}"
                    )

        return self

    def resolve_path(self, value: str) -> Path:
        result = Path(os.path.expandvars(str(value or "").strip())).expanduser()
        if not result.is_absolute():
            result = PROJECT_ROOT / result
        return result.resolve()

    @property
    def raw_path(self) -> Path:
        return self.resolve_path(self.raw_dir)

    @property
    def cache_path(self) -> Path:
        return self.resolve_path(self.cache_dir)

    @property
    def quarantine_path(self) -> Path:
        return self.resolve_path(self.quarantine_dir)

    @property
    def export_path(self) -> Path:
        return self.resolve_path(self.export_dir)

    @property
    def log_path(self) -> Path:
        return self.resolve_path(self.log_dir)

    @property
    def backup_path(self) -> Path:
        return self.resolve_path(self.backup_dir)


class CryptoHFTConfig(StrictConfigModel):
    base_url: str = "https://api.cryptohftdata.com/v1"
    api_key: SecretStr = Field(
        default_factory=lambda: SecretStr(""),
        repr=False,
    )
    request_timeout_seconds: int = 60
    download_rate_limit_per_minute: int = 55
    retry_max_attempts: int = 5
    retry_initial_backoff_seconds: int = 2
    expected_release_delay_minutes: int = 15
    release_poll_interval_minutes: int = 5

    # Automatic Fetch always protects the newest release-eligible hour first,
    # then searches backward through this bounded window for the nearest
    # incomplete hour. This permits catch-up after laptop shutdown without
    # creating an unbounded historical backfill.
    automatic_fetch_catch_up_hours: int = 72

    @field_validator("base_url")
    @classmethod
    def _base_url(cls, value: str) -> str:
        result = str(value or "").strip().rstrip("/")
        if not result.startswith(("http://", "https://")):
            raise ValueError("cryptohft.base_url must be an HTTP(S) URL")
        return result

    @field_validator(
        "request_timeout_seconds",
        "download_rate_limit_per_minute",
        "retry_max_attempts",
        "retry_initial_backoff_seconds",
        "expected_release_delay_minutes",
        "release_poll_interval_minutes",
        "automatic_fetch_catch_up_hours",
        mode="before",
    )
    @classmethod
    def _positive_ints(cls, value: Any, info) -> int:
        return _strict_positive_int(
            value,
            field_name=f"cryptohft.{info.field_name}",
        )

    @model_validator(mode="after")
    def _reasonable_operational_limits(self) -> CryptoHFTConfig:
        if self.download_rate_limit_per_minute > 60:
            raise ValueError(
                "cryptohft.download_rate_limit_per_minute must be <= 60 "
                "until an authenticated higher limit is explicitly verified"
            )

        if self.automatic_fetch_catch_up_hours > 8_760:
            raise ValueError("cryptohft.automatic_fetch_catch_up_hours must be <= 8760")

        return self


class ProcessingConfig(StrictConfigModel):
    checkpoint_search_max_hours: int = 168

    @field_validator("checkpoint_search_max_hours", mode="before")
    @classmethod
    def _checkpoint_search_max_hours(cls, value: Any) -> int:
        result = _strict_positive_int(
            value,
            field_name="processing.checkpoint_search_max_hours",
        )
        if result > 8760:
            raise ValueError("processing.checkpoint_search_max_hours must be <= 8760")
        return result


class RemoteConfig(StrictConfigModel):
    """Local private-Hugging-Face import configuration.

    The token belongs in ``L2SHOCK__REMOTE__HF_TOKEN`` or the project ``.env``.
    It must not be stored in ``config.yaml``.
    """

    hf_repo_id: str = ""
    hf_revision: str = "main"
    hf_token: SecretStr = Field(
        default_factory=lambda: SecretStr(""),
        repr=False,
    )
    default_workflow: str = "remote_hf_import"

    @field_validator("hf_repo_id")
    @classmethod
    def _hf_repo_id(cls, value: str) -> str:
        result = str(value or "").strip()

        if not result:
            return ""

        parts = result.split("/")

        if len(parts) != 2:
            raise ValueError(
                "remote.hf_repo_id must use canonical " "'namespace/dataset-name' form"
            )

        for part in parts:
            if (
                not part
                or not part[0].isalnum()
                or any(
                    not (character.isalnum() or character in {".", "_", "-"})
                    for character in part
                )
            ):
                raise ValueError(
                    "remote.hf_repo_id must use canonical "
                    "'namespace/dataset-name' form"
                )

        return result

    @field_validator("hf_revision")
    @classmethod
    def _hf_revision(cls, value: str) -> str:
        result = str(value or "").strip()

        if (
            not result
            or result in {".", ".."}
            or result.startswith("/")
            or result.endswith("/")
            or any(character.isspace() for character in result)
        ):
            raise ValueError(
                "remote.hf_revision contains an unsupported revision identity"
            )

        return result

    @field_validator("default_workflow")
    @classmethod
    def _default_workflow(cls, value: str) -> str:
        result = str(value or "").strip().lower()

        allowed = {
            "remote_hf_import",
            "local_fetch_processing",
        }

        if result not in allowed:
            raise ValueError(
                "remote.default_workflow must be remote_hf_import "
                "or local_fetch_processing"
            )

        return result

    @property
    def configured(self) -> bool:
        return bool(self.hf_repo_id and self.hf_token.get_secret_value().strip())


class LMConfig(StrictConfigModel):
    confirmation_retracement_fraction: float = 0.20
    decimal_precision: int = 34
    top_n_height: int = 10
    top_n_sharpness: int = 10

    priority_height: float = 0.35
    priority_sharpness: float = 0.30
    priority_endpoint_extremeness: float = 0.20
    priority_retracement_magnitude: float = 0.10
    priority_retracement_count: float = 0.05

    highlight_min_alpha: float = 0.05
    highlight_max_alpha: float = 0.18
    highlight_accumulated_alpha_cap: float = 0.40

    @field_validator(
        "top_n_height",
        "top_n_sharpness",
        mode="before",
    )
    @classmethod
    def _top_n(cls, value: Any, info) -> int:
        result = _strict_positive_int(
            value,
            field_name=f"analysis.lm.{info.field_name}",
        )
        if result > 1000:
            raise ValueError(f"analysis.lm.{info.field_name} must be <= 1000")
        return result

    @field_validator(
        "decimal_precision",
        mode="before",
    )
    @classmethod
    def _decimal_precision(cls, value: Any) -> int:
        result = _strict_positive_int(
            value,
            field_name="analysis.lm.decimal_precision",
        )

        if result < 16:
            raise ValueError("analysis.lm.decimal_precision must be at least 16")

        if result > 1000:
            raise ValueError("analysis.lm.decimal_precision must be <= 1000")

        return result

    @model_validator(mode="after")
    def _validate_lm_semantics(self) -> LMConfig:
        fraction = _finite_float(
            self.confirmation_retracement_fraction,
            field_name="analysis.lm.confirmation_retracement_fraction",
        )
        if not 0.0 < fraction < 1.0:
            raise ValueError(
                "analysis.lm.confirmation_retracement_fraction " "must be inside (0, 1)"
            )

        priority_fields = (
            "priority_height",
            "priority_sharpness",
            "priority_endpoint_extremeness",
            "priority_retracement_magnitude",
            "priority_retracement_count",
        )
        priorities: list[float] = []

        for field_name in priority_fields:
            value = _finite_float(
                getattr(self, field_name),
                field_name=f"analysis.lm.{field_name}",
            )
            if value < 0.0:
                raise ValueError(f"analysis.lm.{field_name} must be >= 0")
            setattr(self, field_name, value)
            priorities.append(value)

        exact_priority_total = sum(
            (Decimal(str(value)) for value in priorities),
            Decimal(0),
        )

        if exact_priority_total != Decimal(1):
            raise ValueError(
                "LM priority values must sum exactly to 1.0; "
                f"observed {exact_priority_total}"
            )

        alpha_fields = (
            "highlight_min_alpha",
            "highlight_max_alpha",
            "highlight_accumulated_alpha_cap",
        )
        for field_name in alpha_fields:
            value = _finite_float(
                getattr(self, field_name),
                field_name=f"analysis.lm.{field_name}",
            )
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"analysis.lm.{field_name} must be in [0, 1]")
            setattr(self, field_name, value)

        if self.highlight_max_alpha < self.highlight_min_alpha:
            raise ValueError(
                "analysis.lm.highlight_max_alpha must be >= " "highlight_min_alpha"
            )

        if self.highlight_accumulated_alpha_cap < self.highlight_max_alpha:
            raise ValueError(
                "analysis.lm.highlight_accumulated_alpha_cap must be >= "
                "highlight_max_alpha"
            )

        return self


class AnalysisConfig(StrictConfigModel):
    supported_bases: list[str] = Field(default_factory=lambda: ["BTC", "ETH"])
    base_sampling_interval_ms: int = 1000
    activity_timeframes: list[str] = Field(
        default_factory=lambda: ["1s", "5s", "10s", "15s", "30s"]
    )
    default_activity_timeframe: str = "1s"
    default_chart_max_bars: int = 400
    price_context_bars_before: int = 3
    price_context_bars_after: int = 3
    l2_long_invalid_warning_seconds: int = 60
    price_long_invalid_warning_minutes: int = 3
    lm: LMConfig = Field(default_factory=LMConfig)

    @field_validator("supported_bases")
    @classmethod
    def _bases(cls, value: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()

        for raw in value or []:
            base = str(raw or "").strip().upper()
            if not base:
                continue
            if base not in {"BTC", "ETH"}:
                raise ValueError(
                    "analysis.supported_bases currently permits only BTC and ETH"
                )
            if base not in seen:
                seen.add(base)
                result.append(base)

        if not result:
            raise ValueError("analysis.supported_bases cannot be empty")

        return result

    @field_validator("base_sampling_interval_ms", mode="before")
    @classmethod
    def _base_sampling_interval(cls, value: Any) -> int:
        result = _strict_positive_int(
            value,
            field_name="analysis.base_sampling_interval_ms",
        )
        if result != 1000:
            raise ValueError(
                "Version 1 requires analysis.base_sampling_interval_ms=1000"
            )
        return result

    @field_validator("activity_timeframes")
    @classmethod
    def _activity_timeframes(cls, value: list[str]) -> list[str]:
        allowed = ("1s", "5s", "10s", "15s", "30s")
        result: list[str] = []
        seen: set[str] = set()

        for raw in value or []:
            item = str(raw or "").strip()
            if item not in allowed:
                raise ValueError(
                    "analysis.activity_timeframes contains unsupported "
                    f"value {item!r}"
                )
            if item not in seen:
                seen.add(item)
                result.append(item)

        if not result:
            raise ValueError("analysis.activity_timeframes cannot be empty")

        return result

    @field_validator(
        "default_chart_max_bars",
        "l2_long_invalid_warning_seconds",
        "price_long_invalid_warning_minutes",
        mode="before",
    )
    @classmethod
    def _positive_analysis_ints(cls, value: Any, info) -> int:
        return _strict_positive_int(
            value,
            field_name=f"analysis.{info.field_name}",
        )

    @field_validator(
        "price_context_bars_before",
        "price_context_bars_after",
        mode="before",
    )
    @classmethod
    def _nonnegative_context(cls, value: Any, info) -> int:
        return _strict_nonnegative_int(
            value,
            field_name=f"analysis.{info.field_name}",
        )

    @model_validator(mode="after")
    def _default_activity_is_supported(self) -> AnalysisConfig:
        if self.default_activity_timeframe not in self.activity_timeframes:
            raise ValueError(
                "analysis.default_activity_timeframe must be present in "
                "analysis.activity_timeframes"
            )
        return self


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="L2SHOCK__",
        env_nested_delimiter="__",
        case_sensitive=False,
        env_file=str(DEFAULT_DOTENV_PATH),
        extra="forbid",
    )

    app: AppConfig = Field(default_factory=AppConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    cryptohft: CryptoHFTConfig = Field(default_factory=CryptoHFTConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    remote: RemoteConfig = Field(default_factory=RemoteConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            env_settings,
            dotenv_settings,
            init_settings,
            file_secret_settings,
        )


def load_settings(config_path: Path | str | None = None) -> Settings:
    raw_path = config_path or os.environ.get("L2SHOCK_CONFIG_PATH")
    path = Path(raw_path) if raw_path else DEFAULT_CONFIG_PATH

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {path}. "
            "Copy config.yaml.example to config.yaml."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise ValueError(f"Top-level YAML configuration must be an object: {path}")

    return Settings(**raw)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()


__all__ = [
    "AnalysisConfig",
    "AppConfig",
    "CryptoHFTConfig",
    "DatabaseConfig",
    "DEFAULT_CONFIG_PATH",
    "DEFAULT_DOTENV_PATH",
    "LMConfig",
    "PROJECT_ROOT",
    "ProcessingConfig",
    "RemoteConfig",
    "Settings",
    "StorageConfig",
    "clear_settings_cache",
    "get_settings",
    "load_settings",
]
