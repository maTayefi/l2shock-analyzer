from __future__ import annotations

from datetime import datetime, timezone

import pytest

from l2shock.diagnostics import (
    production_diagnostics_report,
    report_json_bytes,
)
from l2shock.db import get_engine
from l2shock.db.schema import verify_schema

pytestmark = pytest.mark.postgresql


def test_production_diagnostics_report_is_json_safe() -> None:
    verify_schema(get_engine())

    report = production_diagnostics_report(
        generated_at_utc=datetime(
            2089,
            1,
            1,
            12,
            tzinfo=timezone.utc,
        )
    )

    assert report["schema"] == "l2shock.production_diagnostics"
    assert report["schema_version"] == 1
    assert report["generated_at_utc"] == ("2089-01-01T12:00:00+00:00")

    assert isinstance(report["installation"], dict)
    assert isinstance(report["storage"], dict)
    assert isinstance(report["database_inventory"], dict)
    assert isinstance(report["raw_retention_dry_run"], dict)
    assert isinstance(report["maintenance_diagnostics"], dict)

    maintenance = report["maintenance_diagnostics"]

    assert maintenance["schema"] == ("l2shock.maintenance_diagnostics")
    assert maintenance["schema_version"] == 1
    assert isinstance(maintenance["checkpoint_storage"], dict)
    assert isinstance(maintenance["stale_sources"], dict)
    assert isinstance(maintenance["stale_fetch_runs"], dict)
    assert isinstance(
        maintenance["analytical_consistency"],
        dict,
    )
    assert isinstance(maintenance["performance"], dict)

    performance = maintenance["performance"]

    assert performance["processing_duration_history"]["available"] is False
    assert performance["peak_memory_history"]["available"] is False

    actions = maintenance["maintenance_actions"]

    assert actions["checkpoint_deletion_performed"] is False
    assert actions["source_status_recovery_performed"] is False
    assert actions["analytical_row_deletion_performed"] is False
    assert actions["analytical_row_repair_performed"] is False
    assert actions["raw_file_deletion_performed"] is False

    policy = report["maintenance_policy"]

    assert policy["raw_deletion_enabled"] is True
    assert policy["source_metadata_mutation_enabled"] is True
    assert policy["checkpoint_cleanup_enabled"] is True
    assert policy["orphan_checkpoint_deletion_enabled"] is True
    assert policy["stale_source_recovery_enabled"] is True
    assert policy["analytical_repair_enabled"] is False
    assert policy["analytical_deletion_enabled"] is False
    assert policy["automatic_maintenance_enabled"] is False
    assert policy["explicit_preview_required"] is True
    assert policy["explicit_confirmation_required"] is True

    security = report["security"]

    assert security["exception_arguments_exported"] is False
    assert security["database_password_exported"] is False
    assert security["cryptohft_api_key_exported"] is False

    encoded = report_json_bytes(report)

    assert encoded
    assert b"l2shock.production_diagnostics" in encoded
