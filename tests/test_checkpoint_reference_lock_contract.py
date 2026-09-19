from __future__ import annotations

import inspect

from l2shock.db.analytical_repository import AnalyticalRepository
from l2shock.maintenance_actions import (
    _execute_orphan_checkpoint_deletion,
)
from l2shock.processing.l2_coordinator import (
    SingleMarketL2ProcessingCoordinator,
)
from l2shock.remote.importer import _import_l2


def test_analytical_l2_write_locks_checkpoint_provenance() -> None:
    source = inspect.getsource(
        AnalyticalRepository.write_l2_hour,
    )

    lock_position = source.index("acquire_checkpoint_reference_transaction_locks")
    insert_position = source.index("postgresql_insert(L2HourlySeries)")

    assert lock_position < insert_position


def test_orphan_deletion_locks_digest_before_reference_query_and_unlink() -> None:
    source = inspect.getsource(
        _execute_orphan_checkpoint_deletion,
    )

    source_lock_position = source.index("acquire_source_hour_transaction_lock")
    checkpoint_lock_position = source.index(
        "acquire_checkpoint_reference_transaction_lock"
    )
    reference_query_position = source.index("_checkpoint_reference_set(session)")
    unlink_position = source.index("path.unlink()")

    assert (
        source_lock_position
        < checkpoint_lock_position
        < reference_query_position
        < unlink_position
    )


def test_local_l2_processing_locks_output_before_reference_persistence() -> None:
    source = inspect.getsource(
        SingleMarketL2ProcessingCoordinator._run_transaction,
    )

    checkpoint_lock_position = source.index(
        "acquire_checkpoint_reference_transaction_locks"
    )
    analytical_write_position = source.index("analytical_repository.write_l2_hour")
    quality_write_position = source.index("target_row.quality_json = updated_quality")

    assert checkpoint_lock_position < analytical_write_position < quality_write_position


def test_remote_l2_import_uses_source_then_checkpoint_lock_order() -> None:
    source = inspect.getsource(_import_l2)

    source_lock_position = source.index("acquire_source_hour_transaction_lock")
    checkpoint_lock_position = source.index(
        "acquire_checkpoint_reference_transaction_locks"
    )
    analytical_write_position = source.index("analytical_repository.write_l2_hour")
    source_metadata_position = source.index("_update_remote_source_metadata")

    assert (
        source_lock_position
        < checkpoint_lock_position
        < analytical_write_position
        < source_metadata_position
    )
