# l2shock/db/__init__.py
"""PostgreSQL engine, model, and analytical persistence foundation."""

from l2shock.db.engine import (
    get_engine,
    get_engine_application_name,
    get_session_factory,
    reset_engine,
    session_scope,
)
from l2shock.db.models import Base

_LAZY_IMPORTS = {
    "L2_PROVENANCE_SCHEMA": "l2shock.db.analytical_repository",
    "L2_PROVENANCE_SCHEMA_VERSION": "l2shock.db.analytical_repository",
    "AnalyticalRepository": "l2shock.db.analytical_repository",
    "AnalyticalRepositoryError": "l2shock.db.analytical_repository",
    "AnalyticalRowCorruptionError": "l2shock.db.analytical_repository",
    "AnalyticalRowNotFoundError": "l2shock.db.analytical_repository",
    "PresetDeletionBlockedError": "l2shock.db.analytical_repository",
    "HourlySeriesConflictError": "l2shock.db.analytical_repository",
    "HourlySeriesWriteResult": "l2shock.db.analytical_repository",
    "L2HourlyProvenance": "l2shock.db.analytical_repository",
    "PersistedDataPreset": "l2shock.db.analytical_repository",
    "PersistedL2HourlySeries": "l2shock.db.analytical_repository",
    "PresetIdentityConflictError": "l2shock.db.analytical_repository",
    "PresetWriteResult": "l2shock.db.analytical_repository",
    "SourceHourReference": "l2shock.db.analytical_repository",
    "PRICE_PROVENANCE_SCHEMA": "l2shock.db.price_repository",
    "PRICE_PROVENANCE_SCHEMA_VERSION": "l2shock.db.price_repository",
    "PersistedPriceHourlySeries": "l2shock.db.price_repository",
    "PriceAnalyticalRepository": "l2shock.db.price_repository",
    "PriceHourlyProvenance": "l2shock.db.price_repository",
    "PriceHourlySeriesConflictError": "l2shock.db.price_repository",
    "PriceHourlyWriteResult": "l2shock.db.price_repository",
    "PriceSourceHourReference": "l2shock.db.price_repository",
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        import importlib

        module = importlib.import_module(_LAZY_IMPORTS[name])
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


__all__ = [
    "L2_PROVENANCE_SCHEMA",
    "L2_PROVENANCE_SCHEMA_VERSION",
    "PRICE_PROVENANCE_SCHEMA",
    "PRICE_PROVENANCE_SCHEMA_VERSION",
    "AnalyticalRepository",
    "AnalyticalRepositoryError",
    "AnalyticalRowCorruptionError",
    "AnalyticalRowNotFoundError",
    "PresetDeletionBlockedError",
    "Base",
    "HourlySeriesConflictError",
    "HourlySeriesWriteResult",
    "L2HourlyProvenance",
    "PersistedDataPreset",
    "PersistedL2HourlySeries",
    "PersistedPriceHourlySeries",
    "PresetIdentityConflictError",
    "PresetWriteResult",
    "PriceAnalyticalRepository",
    "PriceHourlyProvenance",
    "PriceHourlySeriesConflictError",
    "PriceHourlyWriteResult",
    "PriceSourceHourReference",
    "SourceHourReference",
    "get_engine",
    "get_engine_application_name",
    "get_session_factory",
    "reset_engine",
    "session_scope",
]
