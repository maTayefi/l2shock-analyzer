from __future__ import annotations

import inspect

from l2shock.processing.checkpoint_references import (
    collect_referenced_checkpoint_sha256s,
)
from l2shock.processing.checkpoint_store import CheckpointStore
from l2shock.processing.l2_coordinator import (
    SingleMarketL2ProcessingCoordinator,
)
from l2shock.processing.source_repository import (
    SQLAlchemyProcessingSourceRepository,
)
from l2shock.remote import importer as remote_importer
from l2shock.ui.processing_runtime import ManualProcessingRuntime


def test_l2_coordinator_has_no_precommit_terminal_state_cache() -> None:
    source = inspect.getsource(
        SingleMarketL2ProcessingCoordinator,
    )

    assert "_terminal_state_cache" not in source
    assert "_cached_search_plan" not in source
    assert "_terminal_state_key" not in source


def test_l2_coordinator_locks_input_checkpoint_before_replay() -> None:
    source = inspect.getsource(
        SingleMarketL2ProcessingCoordinator._run_transaction,
    )

    locked_plan_position = source.index("plan = locked_plan")
    checkpoint_lock_position = source.index(
        "acquire_checkpoint_reference_transaction_locks",
        locked_plan_position,
    )
    replay_position = source.index(
        "target_initial_checkpoint = self._target_initial_checkpoint",
    )

    assert locked_plan_position < checkpoint_lock_position < replay_position


def test_l2_coordinator_does_not_inherit_old_output_checkpoint() -> None:
    source = inspect.getsource(
        SingleMarketL2ProcessingCoordinator._run_transaction,
    )

    assert "previous_output_checkpoint" not in source
    assert "output_checkpoint.encoding_info.content_sha256" in source


def test_sql_source_repository_exposes_durable_checkpoint_ownership() -> None:
    source = inspect.getsource(
        SQLAlchemyProcessingSourceRepository.find_durable_output_checkpoint_content_sha256,
    )

    assert '"output_checkpoint_content_sha256"' in source
    assert '"remote_output_checkpoint_content_sha256"' in source
    assert "conflicting output checkpoints" in source


def test_checkpoint_discovery_checks_durable_output_ownership() -> None:
    source = inspect.getsource(
        CheckpointStore.build_search_plan,
    )

    ownership_position = source.index("find_durable_output_checkpoint_content_sha256")
    found_return_position = source.index(
        "stop_reason=CheckpointSearchStopReason.CHECKPOINT_FOUND"
    )

    assert ownership_position < found_return_position
    assert "checkpoint = None" in source


def test_remote_l2_import_publishes_verified_checkpoint_locally() -> None:
    source = inspect.getsource(
        remote_importer._import_l2,
    )

    assert "decode_checkpoint" in source
    assert "checkpoint_store.publish" in source
    assert "published_checkpoint.encoding_info.content_sha256" in source


def test_remote_import_writes_standard_and_legacy_checkpoint_keys() -> None:
    source = inspect.getsource(
        remote_importer._update_remote_source_metadata,
    )

    assert '"input_checkpoint_content_sha256"' in source
    assert '"output_checkpoint_content_sha256"' in source
    assert '"remote_input_checkpoint_content_sha256"' in source
    assert '"remote_output_checkpoint_content_sha256"' in source


def test_checkpoint_reference_graph_includes_legacy_remote_keys() -> None:
    source = inspect.getsource(
        collect_referenced_checkpoint_sha256s,
    )

    assert '"remote_input_checkpoint_content_sha256"' in source
    assert '"remote_output_checkpoint_content_sha256"' in source


def test_processing_runtime_blocks_later_failed_l2_chain_hours() -> None:
    source = inspect.getsource(
        ManualProcessingRuntime._run,
    )

    assert "failed_l2_chain_hours" in source
    assert "target.hour_utc > failed_hour" in source
    assert "checkpoint-dependent hours are blocked" in source
