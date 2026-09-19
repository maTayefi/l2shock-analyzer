# L2 Liquidity Shock Analyzer — Bug-Finding Cores

This document defines the current bug-finding decomposition of the repository.

The repository has seven primary cores:

1. Database, persistence, identity, provenance, diagnostics, and maintenance.
2. Acquisition, availability, retention, and operational lifecycle.
3. Ingestion, venue adapters, replay, checkpoints, liquidity, and price.
4. Analysis loading, aggregation, filtering, LM detection, and ranking.
5. NiceGUI, charts, exports, controls, and user workflows.
6. Cross-cutting contracts, concurrency, cancellation, and integration.
7. Remote preprocessing, artifact transport, Hugging Face, workers, and imports.

The cores are intentionally overlapping. A file may be primary to one core and
still be valid dependency/context for another core.

During bug-finding rounds:

- production code is the primary review target;
- tests are evidence, not proof that production behavior is correct;
- documentation and workflows are contracts that must be compared with code;
- do not report intentional absence of a virtual environment as a bug;
- require concrete executable failure paths before reporting a bug;
- distinguish implementation bugs from intentionally strict validation;
- treat repository history and comments as weaker evidence than current code;
- prefer bugs that can corrupt data, violate identity/provenance, deadlock work,
  publish stale UI state, or make recovery impossible;
- include exact file paths and concrete triggering conditions in every finding.

---

# Global production-module inventory

The following is the current authoritative Python-module inventory for review.

```text
l2shock/__init__.py
l2shock/acquisition/__init__.py
l2shock/acquisition/availability.py
l2shock/acquisition/coordinator.py
l2shock/acquisition/downloader.py
l2shock/acquisition/errors.py
l2shock/acquisition/fetch_models.py
l2shock/acquisition/fetch_persistence.py
l2shock/acquisition/http.py
l2shock/acquisition/locks.py
l2shock/acquisition/models.py
l2shock/acquisition/persistence.py
l2shock/acquisition/planning.py
l2shock/acquisition/pruning_recovery.py
l2shock/acquisition/rate_limit.py
l2shock/acquisition/release_schedule.py
l2shock/acquisition/repository.py
l2shock/acquisition/results.py
l2shock/acquisition/retention.py
l2shock/acquisition/validation.py
l2shock/analysis/__init__.py
l2shock/analysis/aggregation.py
l2shock/analysis/dataset.py
l2shock/analysis/execution.py
l2shock/analysis/liquidity_movement.py
l2shock/analysis/multi_market.py
l2shock/analysis/price_filter.py
l2shock/analysis/ranking.py
l2shock/analysis/timeframes.py
l2shock/config.py
l2shock/db/__init__.py
l2shock/db/analytical_repository.py
l2shock/db/bootstrap.py
l2shock/db/engine.py
l2shock/db/models.py
l2shock/db/price_repository.py
l2shock/db/schema.py
l2shock/diagnostics.py
l2shock/ingest/__init__.py
l2shock/ingest/checkpoint_codec.py
l2shock/ingest/parquet_reader.py
l2shock/ingest/replay.py
l2shock/ingest/replay_validation.py
l2shock/ingest/reporting.py
l2shock/ingest/sampling.py
l2shock/ingest/venue_adapter.py
l2shock/liquidity/__init__.py
l2shock/liquidity/block_codec.py
l2shock/liquidity/depth.py
l2shock/liquidity/hourly.py
l2shock/logging_setup.py
l2shock/main.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/presets/__init__.py
l2shock/presets/identity.py
l2shock/presets/management.py
l2shock/price/__init__.py
l2shock/price/block_codec.py
l2shock/price/hourly.py
l2shock/processing/__init__.py
l2shock/processing/checkpoint_references.py
l2shock/processing/checkpoint_store.py
l2shock/processing/errors.py
l2shock/processing/integrity.py
l2shock/processing/l2_coordinator.py
l2shock/processing/models.py
l2shock/processing/price_coordinator.py
l2shock/processing/source_repository.py
l2shock/processing/target_planning.py
l2shock/remote/__init__.py
l2shock/remote/artifact_codec.py
l2shock/remote/contracts.py
l2shock/remote/headless_processing.py
l2shock/remote/hf_repository.py
l2shock/remote/importer.py
l2shock/remote/source_acquisition.py
l2shock/remote_worker.py
l2shock/remote_cli.py
l2shock/timeutils.py
l2shock/ui/__init__.py
l2shock/ui/analysis_chart.py
l2shock/ui/analysis_controls.py
l2shock/ui/analysis_export.py
l2shock/ui/analysis_legend.py
l2shock/ui/analysis_runtime.py
l2shock/ui/app.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/chart_interactions.py
l2shock/ui/components.py
l2shock/ui/echarts.py
l2shock/ui/fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/remote_import_runtime.py
l2shock/ui/shutdown.py
l2shock/ui/state.py
l2shock/ui/tab_analysis.py
l2shock/ui/tab_fetch.py
l2shock/ui/tab_settings.py
```

If a future export contains a production Python module absent from this
inventory, update this section before starting the next bug-finding round.

---

# Global test inventory

```text
tests/test_acquisition_coordinator.py
tests/test_acquisition_downloader.py
tests/test_acquisition_http.py
tests/test_acquisition_persistence.py
tests/test_acquisition_planning.py
tests/test_acquisition_rate_limit.py
tests/test_acquisition_repository_postgresql.py
tests/test_acquisition_validation.py
tests/test_ai_preparation_scripts.py
tests/test_analysis_chart.py
tests/test_analysis_dataset.py
tests/test_analysis_dataset_aggregate_regression.py
tests/test_analysis_execution.py
tests/test_analysis_multi_market.py
tests/test_analytical_provenance.py
tests/test_analytical_repository_postgresql.py
tests/test_automatic_fetch_runtime.py
tests/test_availability.py
tests/test_availability_calendar.py
tests/test_availability_filesystem.py
tests/test_bybit_orderbook_replay.py
tests/test_chart_interactions.py
tests/test_checkpoint_codec.py
tests/test_config.py
tests/test_data_preset_identity.py
tests/test_db_models.py
tests/test_depth_liquidity.py
tests/test_diagnostics.py
tests/test_diagnostics_postgresql.py
tests/test_echarts_publication.py
tests/test_hourly_block_codec.py
tests/test_hourly_liquidity.py
tests/test_liquidity_movement.py
tests/test_liquidity_movement_ranking.py
tests/test_maintenance_actions.py
tests/test_maintenance_actions_postgresql.py
tests/test_maintenance_diagnostics.py
tests/test_migration_freeze.py
tests/test_multi_market_aggregation.py
tests/test_multi_market_preset_identity.py
tests/test_okx_orderbook_replay.py
tests/test_orderbook_replay.py
tests/test_orderbook_sampling.py
tests/test_parquet_reader.py
tests/test_preset_management.py
tests/test_price_filter.py
tests/test_price_provenance.py
tests/test_price_repository_postgresql.py
tests/test_processing_checkpoint_store.py
tests/test_processing_contracts.py
tests/test_processing_integrity.py
tests/test_processing_l2_coordinator_postgresql.py
tests/test_processing_l2_metadata.py
tests/test_processing_price_coordinator_postgresql.py
tests/test_processing_target_planning.py
tests/test_pruning_recovery.py
tests/test_raw_retention.py
tests/test_remote_artifact_codec.py
tests/test_remote_bybit_processing.py
tests/test_remote_cli.py
tests/test_remote_contracts.py
tests/test_remote_headless_processing.py
tests/test_remote_hf_repository.py
tests/test_remote_import_runtime.py
tests/test_remote_importer.py
tests/test_remote_importer_postgresql.py
tests/test_remote_source_acquisition.py
tests/test_remote_worker.py
tests/test_remote_workflow_contract.py
tests/test_replay_diagnostics.py
tests/test_replay_validation_cli.py
tests/test_timeframe_aggregation.py
tests/test_timeutils.py
tests/test_trade_ohlc.py
tests/test_trade_ohlc_block_codec.py
tests/test_ui_analysis_controls.py
tests/test_ui_analysis_runtime.py
tests/test_ui_fetch_runtime.py
tests/test_ui_foundation.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
tests/test_venue_sequence_adapter.py
```

`tests/test_ui_analysis_controls.py` exists.
`tests/test_analysis_controls.py` does not exist and must not be invented.

---

# Workflow, configuration, and support-file inventory

These are not normal production Python modules, but they may encode executable
contracts and must be included when relevant.

```text
.env.example
.github/workflows/bybit-contract-diagnostics.yml
.github/workflows/remote-hourly-processing.yml
README.md
alembic.ini
alembic/env.py
alembic/script.py.mako
alembic/versions/0001_initial.py
alembic/versions/__init__.py
config.yaml.example
pyproject.toml
requirements.txt
tools/diagnose_bybit_contract.py
tools/inspect_venue_sequence_contract.py
tools/validate_okx_replay.py
```

Root-level diagnostic, publishing, setup, and maintenance scripts may be used as
context. Do not treat experimental inspection scripts as production behavior
unless a production workflow actually invokes them.

---

# Core 1 — Database, persistence, identity, provenance, diagnostics, maintenance

## Primary modules

```text
l2shock/config.py
l2shock/db/__init__.py
l2shock/db/analytical_repository.py
l2shock/db/bootstrap.py
l2shock/db/engine.py
l2shock/db/models.py
l2shock/db/price_repository.py
l2shock/db/schema.py
l2shock/diagnostics.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/presets/__init__.py
l2shock/presets/identity.py
l2shock/presets/management.py
l2shock/processing/checkpoint_references.py
l2shock/processing/checkpoint_store.py
l2shock/processing/integrity.py
l2shock/timeutils.py
```

## Primary tests

```text
tests/test_analytical_provenance.py
tests/test_analytical_repository_postgresql.py
tests/test_config.py
tests/test_data_preset_identity.py
tests/test_db_models.py
tests/test_diagnostics.py
tests/test_diagnostics_postgresql.py
tests/test_maintenance_actions.py
tests/test_maintenance_actions_postgresql.py
tests/test_maintenance_diagnostics.py
tests/test_migration_freeze.py
tests/test_multi_market_preset_identity.py
tests/test_preset_management.py
tests/test_price_provenance.py
tests/test_price_repository_postgresql.py
tests/test_processing_checkpoint_store.py
tests/test_processing_integrity.py
tests/test_processing_l2_metadata.py
tests/test_timeutils.py
```

## Focus areas

1. Alembic migration graph, frozen migration independence, and ORM drift.
2. PostgreSQL timezone ownership and naive-datetime rejection.
3. Transaction ownership, savepoints, rollback, and accidental commits.
4. Advisory-lock identity, scope, contention, and release.
5. Idempotent inserts versus immutable-content conflicts.
6. Preset canonicalization, hash reproducibility, and semantic immutability.
7. L2 and price provenance completeness and canonical round trips.
8. Duplicate logical source identities with conflicting content hashes.
9. Compact block verification before persistence and after retrieval.
10. Quality-summary consistency with decoded channels.
11. Checkpoint reference discovery and orphan classification.
12. Raw-retention and pruning-recovery DB/filesystem consistency.
13. Maintenance preview expiration, token identity, and execution revalidation.
14. Stale source and stale fetch-run recovery.
15. Multiple analytical outputs owned by one source hour.
16. Remote-import metadata merged with existing local source metadata.
17. Preset deletion rules and prevention of analytical cascades.
18. Diagnostics remaining read-only where promised.
19. Secret redaction in diagnostics, bootstrap, repository errors, and logs.
20. Configuration/example/default/environment precedence drift.
21. Deterministic row ordering in all repository list operations.
22. PostgreSQL JSONB normalization versus canonical identity payloads.
23. Cross-table base, venue, instrument, hour, preset, and content invariants.
24. Partial failure after filesystem publication but before DB commit.
25. Corrupt durable metadata that incorrectly appears usable.

---

# Core 2 — Acquisition, availability, retention, and operational lifecycle

## Primary modules

```text
l2shock/acquisition/__init__.py
l2shock/acquisition/availability.py
l2shock/acquisition/coordinator.py
l2shock/acquisition/downloader.py
l2shock/acquisition/errors.py
l2shock/acquisition/fetch_models.py
l2shock/acquisition/fetch_persistence.py
l2shock/acquisition/http.py
l2shock/acquisition/locks.py
l2shock/acquisition/models.py
l2shock/acquisition/persistence.py
l2shock/acquisition/planning.py
l2shock/acquisition/pruning_recovery.py
l2shock/acquisition/rate_limit.py
l2shock/acquisition/release_schedule.py
l2shock/acquisition/repository.py
l2shock/acquisition/results.py
l2shock/acquisition/retention.py
l2shock/acquisition/validation.py
l2shock/processing/source_repository.py
l2shock/processing/target_planning.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/shutdown.py
l2shock/ui/state.py
l2shock/ui/tab_fetch.py
```

## Primary tests

```text
tests/test_acquisition_coordinator.py
tests/test_acquisition_downloader.py
tests/test_acquisition_http.py
tests/test_acquisition_persistence.py
tests/test_acquisition_planning.py
tests/test_acquisition_rate_limit.py
tests/test_acquisition_repository_postgresql.py
tests/test_acquisition_validation.py
tests/test_automatic_fetch_runtime.py
tests/test_availability.py
tests/test_availability_calendar.py
tests/test_availability_filesystem.py
tests/test_processing_target_planning.py
tests/test_pruning_recovery.py
tests/test_raw_retention.py
tests/test_ui_fetch_runtime.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
```

## Focus areas

1. Manual, automatic, processing, analysis, and remote-import admission races.
2. Process-local operation lock versus cross-process advisory locks.
3. Complete source-hour status-transition coverage.
4. Fetch-run creation, terminalization, interruption, and restart recovery.
5. Download retry classification and `Retry-After` integration.
6. Rate-limiter finite configuration, fairness, cancellation, and starvation.
7. Atomic `.part` publication and concurrent destination creation.
8. Content-Length, SHA-256, Parquet validation, and quarantine behavior.
9. Reuse of existing archives without accepting symlinks or wrong paths.
10. Source-plan completeness for Binance, Bybit, and OKX.
11. Release-delay and catch-up cursor boundary behavior.
12. Automatic-fetch retry rotation and durable cursor publication.
13. Materialization-aware processing-target selection.
14. Processed-but-pruned and remotely imported source semantics.
15. Availability derived from actual filesystem and database evidence.
16. Multi-market availability and complete expected-market accounting.
17. Local-calendar conversion without changing UTC source ownership.
18. Fetch/processing completion triggering availability refresh exactly once.
19. Retention exclusion of active, unprocessed, invalid, or unverified sources.
20. Pruning execution revalidation under advisory lock.
21. Stranded `.pruning` recovery and canonical-path enforcement.
22. Cooperative cancellation during network waits and worker execution.
23. Native task cancellation versus durable operation finalization.
24. Shutdown grace periods, force-cancellation truthfulness, and task cleanup.
25. Runtime state diverging from durable backend state after crashes.

---

# Core 3 — Ingestion, venue adapters, replay, checkpoints, liquidity, price

## Primary modules

```text
l2shock/acquisition/models.py
l2shock/ingest/__init__.py
l2shock/ingest/checkpoint_codec.py
l2shock/ingest/parquet_reader.py
l2shock/ingest/replay.py
l2shock/ingest/replay_validation.py
l2shock/ingest/reporting.py
l2shock/ingest/sampling.py
l2shock/ingest/venue_adapter.py
l2shock/liquidity/__init__.py
l2shock/liquidity/block_codec.py
l2shock/liquidity/depth.py
l2shock/liquidity/hourly.py
l2shock/price/__init__.py
l2shock/price/block_codec.py
l2shock/price/hourly.py
l2shock/processing/checkpoint_store.py
l2shock/processing/integrity.py
l2shock/processing/l2_coordinator.py
l2shock/processing/price_coordinator.py
```

## Primary tests

```text
tests/test_bybit_orderbook_replay.py
tests/test_checkpoint_codec.py
tests/test_depth_liquidity.py
tests/test_hourly_block_codec.py
tests/test_hourly_liquidity.py
tests/test_okx_orderbook_replay.py
tests/test_orderbook_replay.py
tests/test_orderbook_sampling.py
tests/test_parquet_reader.py
tests/test_processing_integrity.py
tests/test_processing_l2_coordinator_postgresql.py
tests/test_processing_price_coordinator_postgresql.py
tests/test_replay_diagnostics.py
tests/test_replay_validation_cli.py
tests/test_trade_ohlc.py
tests/test_trade_ohlc_block_codec.py
tests/test_venue_sequence_adapter.py
```

## Focus areas

1. Row grouping across Arrow batches and row groups.
2. Event identity completeness and accidental event merging or splitting.
3. Received-time, event-time, transaction-time, and trade-time ownership.
4. Nullable fields and venue-specific interpretation.
5. Binance snapshot/update sequence semantics.
6. OKX reset snapshots and predecessor-frontier semantics.
7. Bybit native snapshots, boundary snapshots, and carried frontiers.
8. Refusal to initialize update-only archives without trusted prior state.
9. Snapshot completeness before replacing reconstructed state.
10. Equal-timestamp event ordering and final-state sampling.
11. Exact bucket-endpoint inclusion for L2.
12. Half-open second/hour ownership for price trades.
13. Cross-hour replay adjacency and checkpoint validation.
14. Duplicate updates versus conflicting duplicates.
15. Sequence gaps, regressions, and false-positive continuity failures.
16. Locked, crossed, and empty-side book classification.
17. Zero-quantity removal and resulting structural quality.
18. Exact depth-band boundary inclusion.
19. Decimal notional arithmetic and ambient-context independence.
20. Cancellation checks during row streaming and depth scanning.
21. Checkpoint canonical encoding, bounds, content hash, and atomic publication.
22. Hourly block canonical encoding and corruption detection.
23. Trade-ID deduplication and conflicting normalized content.
24. Sentinel-row handling without contaminating production prices.
25. Replay/report counters agreeing with events actually applied.
26. Deterministic output for identical archives and checkpoints.
27. Production replay matching diagnostic and headless replay behavior.
28. All-invalid hours remaining explicit rather than forward-filled.
29. Source content changing between verification and streaming.
30. Resource limits for malformed or adversarial encoded inputs.

---

# Core 4 — Analysis loading, aggregation, filtering, LM detection, ranking

## Primary modules

```text
l2shock/analysis/__init__.py
l2shock/analysis/aggregation.py
l2shock/analysis/dataset.py
l2shock/analysis/execution.py
l2shock/analysis/liquidity_movement.py
l2shock/analysis/multi_market.py
l2shock/analysis/price_filter.py
l2shock/analysis/ranking.py
l2shock/analysis/timeframes.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/presets/identity.py
l2shock/ui/analysis_controls.py
l2shock/ui/analysis_runtime.py
```

## Primary tests

```text
tests/test_analysis_dataset.py
tests/test_analysis_dataset_aggregate_regression.py
tests/test_analysis_execution.py
tests/test_analysis_multi_market.py
tests/test_liquidity_movement.py
tests/test_liquidity_movement_ranking.py
tests/test_multi_market_aggregation.py
tests/test_price_filter.py
tests/test_timeframe_aggregation.py
tests/test_ui_analysis_controls.py
tests/test_ui_analysis_runtime.py
```

## Focus areas

1. Closed endpoint snapping and exact-boundary off-by-one errors.
2. Requested, snapped, loaded, activity, and chart range ownership.
3. Missing-hour and missing-second generation.
4. Exact timestamp alignment of L2 and Binance price seconds.
5. Activity timeframe versus independent chart timeframe.
6. Partial aggregation buckets and expected/observed counts.
7. Endpoint-state L2 aggregation.
8. Real-trade OHLC aggregation.
9. Invalid/degraded quality propagation and hard discontinuities.
10. Numerically usable degraded market coverage.
11. Strict versus degraded multi-market coverage policy.
12. Component preset derivation and component-row lookup.
13. Aggregate content/provenance hash reproducibility.
14. Price-bound intersection and display clipping.
15. Context bars versus in-range candidate ownership.
16. LM pivot, endpoint, confirmation, and terminal-offline semantics.
17. Equal extrema and deterministic tie behavior.
18. Adverse-move accounting.
19. Population range isolation across segments and metrics.
20. Relative height, sharpness, percentile, MAD, and modified-Z behavior.
21. Height/sharpness recall union and deterministic final ordering.
22. Analysis ID and candidate ID completeness.
23. Cache-key completeness and stale cache prevention.
24. Cancellation before cache insertion and result publication.
25. Empty, constant, single-bar, all-excluded, and all-invalid datasets.
26. Ambient Decimal context independence.
27. Very large ranges and chart-timeframe fallback.
28. Changed source content invalidating aggregate and analysis identity.
29. Price filtering never mutating raw analytical values.
30. Same inputs producing byte-equivalent exports and equal results.

---

# Core 5 — NiceGUI, charts, exports, controls, and user workflows

## Primary modules

```text
l2shock/diagnostics.py
l2shock/main.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/ui/__init__.py
l2shock/ui/analysis_chart.py
l2shock/ui/analysis_controls.py
l2shock/ui/analysis_export.py
l2shock/ui/analysis_legend.py
l2shock/ui/analysis_runtime.py
l2shock/ui/app.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/chart_interactions.py
l2shock/ui/components.py
l2shock/ui/echarts.py
l2shock/ui/fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/remote_import_runtime.py
l2shock/ui/shutdown.py
l2shock/ui/state.py
l2shock/ui/tab_analysis.py
l2shock/ui/tab_fetch.py
l2shock/ui/tab_settings.py
```

## Primary tests

```text
tests/test_analysis_chart.py
tests/test_availability_calendar.py
tests/test_chart_interactions.py
tests/test_diagnostics.py
tests/test_echarts_publication.py
tests/test_maintenance_actions.py
tests/test_maintenance_diagnostics.py
tests/test_remote_import_runtime.py
tests/test_ui_analysis_controls.py
tests/test_ui_analysis_runtime.py
tests/test_ui_fetch_runtime.py
tests/test_ui_foundation.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
```

## Focus areas

1. Undefined elements, stale closures, and wrong NiceGUI client context.
2. Process runtime state versus browser-visible state.
3. Start/stop button enablement under every active operation.
4. Completion sequence handling and stale result publication.
5. Rapid stop/start and operation ownership changes.
6. Client disconnect during progress and publication.
7. ECharts option replacement and stale merged series.
8. Browser render-token acknowledgement.
9. Chart publication superseded while awaiting acknowledgement.
10. DataZoom capture and percentage restoration.
11. Temporal viewport restoration across chart timeframes.
12. Compressed category mapping across discontinuities.
13. Table-row identity and selected-ranking lookup.
14. Selected-LM emphasis ownership.
15. Crosshair installation, rebinding, removal, and render generations.
16. Tooltip safety and discontinuity metadata.
17. PNG/SVG export from the currently acknowledged generation.
18. Deterministic exact JSON export.
19. Export filename safety and extension preservation.
20. Empty-chart, zero-visible-bar, and zero-candidate behavior.
21. Availability calendar refresh races and obsolete snapshots.
22. Calendar-to-analysis handoff and preset identity.
23. Remote/local workflow profile visibility and state.
24. Persistent-notification duplication.
25. Maintenance preview/confirmation usability and stale previews.
26. Shutdown sequencing and new-work prevention.
27. Tracked-task cleanup.
28. Secret and internal-path exposure through UI diagnostics.
29. Large-table and large-chart responsiveness.
30. Accessibility, keyboard operation, labels, and error clarity.

---

# Core 6 — Cross-cutting contracts, concurrency, cancellation, integration

## Primary scope

Core 6 may inspect every file in the global inventories. Its purpose is to find
failures that cannot be established from one subsystem alone.

## Primary tests

All tests are valid for Core 6, especially:

```text
tests/test_acquisition_coordinator.py
tests/test_acquisition_repository_postgresql.py
tests/test_analysis_dataset_aggregate_regression.py
tests/test_analysis_execution.py
tests/test_automatic_fetch_runtime.py
tests/test_echarts_publication.py
tests/test_processing_contracts.py
tests/test_processing_integrity.py
tests/test_remote_headless_processing.py
tests/test_remote_importer_postgresql.py
tests/test_remote_worker.py
tests/test_ui_analysis_runtime.py
tests/test_ui_fetch_runtime.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
```

## Focus areas

1. Global UTC and identity invariants.
2. Source → replay → materialization → aggregate → analysis identity.
3. Local and remote processing equivalence.
4. Operation admission across all runtimes.
5. Async lock ownership while synchronous worker threads execute.
6. Cross-process advisory locking versus process-local locks.
7. Cooperative cancellation propagation across nested layers.
8. Native task cancellation while threads continue running.
9. Shutdown truthfulness about worker termination.
10. Database engine disposal while work is still active.
11. Filesystem publication versus transaction commit ordering.
12. Exactly-once versus at-least-once behavior.
13. Retry idempotency after partial publication.
14. Restart recovery of source, checkpoint, pruning, and remote states.
15. Durable backend truth versus optimistic runtime/UI truth.
16. Error translation across acquisition, replay, processing, remote, and UI.
17. Secret safety across all exception and logging boundaries.
18. Deterministic hashes and canonical JSON across modules.
19. Cache invalidation after source or semantic changes.
20. Missing/invalid/degraded quality propagation.
21. Availability agreeing with actual analytical usability.
22. Test-suite ordering and shared singleton contamination.
23. Production behavior versus workflow and diagnostic-script assumptions.
24. Configuration defaults matching workflow and packaging requirements.
25. Deadlocks caused by inconsistent lock acquisition order.
26. TOCTOU between validation, hashing, streaming, pruning, and import.
27. Cancellation after commit but before runtime result publication.
28. Duplicate concurrent workers publishing the same immutable identity.
29. Failure to release resources after exceptions.
30. Cross-module assumptions that are only enforced in tests or comments.

---

# Core 7 — Remote preprocessing, artifact transport, Hugging Face, workers, imports

## Primary modules

```text
l2shock/remote/__init__.py
l2shock/remote/artifact_codec.py
l2shock/remote/contracts.py
l2shock/remote/headless_processing.py
l2shock/remote/hf_repository.py
l2shock/remote/importer.py
l2shock/remote/source_acquisition.py
l2shock/remote_worker.py
l2shock/remote_cli.py
l2shock/ui/remote_import_runtime.py
```

## Important dependencies

```text
l2shock/acquisition/downloader.py
l2shock/acquisition/models.py
l2shock/acquisition/release_schedule.py
l2shock/ingest/checkpoint_codec.py
l2shock/ingest/parquet_reader.py
l2shock/ingest/replay.py
l2shock/liquidity/block_codec.py
l2shock/liquidity/hourly.py
l2shock/presets/identity.py
l2shock/price/block_codec.py
l2shock/price/hourly.py
l2shock/processing/integrity.py
l2shock/processing/models.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/acquisition/repository.py
```

## Primary tests

```text
tests/test_remote_artifact_codec.py
tests/test_remote_bybit_processing.py
tests/test_remote_cli.py
tests/test_remote_contracts.py
tests/test_remote_headless_processing.py
tests/test_remote_hf_repository.py
tests/test_remote_import_runtime.py
tests/test_remote_importer.py
tests/test_remote_importer_postgresql.py
tests/test_remote_source_acquisition.py
tests/test_remote_worker.py
tests/test_remote_workflow_contract.py
```

## Workflow files

```text
.github/workflows/bybit-contract-diagnostics.yml
.github/workflows/remote-hourly-processing.yml
```

## Focus areas

1. Remote artifact-key canonical identity.
2. Artifact path and manifest path traversal prevention.
3. Embedded manifest versus external manifest equivalence.
4. Analytical content hash versus transport file hash.
5. L2 preset content and preset hash agreement.
6. Source archive references and current-hour ownership.
7. Input/output checkpoint hashes and embedded checkpoint verification.
8. Headless behavior matching local processing behavior.
9. Headless cancellation normalization.
10. Target-only L2 processing and immediately preceding checkpoints.
11. Price source-offset policy and explicit adjacent-source selection.
12. Temporary remote-worker workspace isolation and cleanup.
13. Release-boundary admission before HTTP.
14. Shared downloader behavior in local and remote execution.
15. Hugging Face revision pinning to a full immutable commit.
16. Artifact and manifest downloaded from the same revision.
17. Optimistic concurrent publication and parent-commit handling.
18. Concurrent identical publication versus conflicting publication.
19. Partial artifact/manifest publication and retry recovery.
20. Sequential checkpoint-dependent chain progression.
21. Independent venue/instrument chain concurrency.
22. Catch-up frontier selection and bounded search.
23. Runtime and maximum-hours budgets.
24. Missing-source retry behavior.
25. Workflow seed gates for Binance and Bybit.
26. Scheduled versus manually dispatched workflow behavior.
27. Importer transactional rollback after analytical insertion.
28. Importing without falsely claiming local raw bytes.
29. Existing local attachment verification during remote import.
30. Remote metadata merging without erasing prior analytical references.
31. Remote import idempotency.
32. Imported source status and availability semantics.
33. Token, repository ID, revision, and path leakage.
34. Workflow permissions, artifact retention, and secret boundaries.
35. Remote worker type validation and bool-as-int rejection.
36. Production support differences among Binance, Bybit, and OKX.
37. Failed/invalid hours never publishing authoritative checkpoints.
38. Branch mutation during download or publication.
39. Artifact codec resource limits and corruption handling.
40. CLI exit statuses matching actual failure classification.

---

# Ranked single-core bug-finding rounds

Run focused single-core reviews in this order:

```text
1. Core 7 — Remote processing, artifacts, Hugging Face, workers, imports
2. Core 3 — Ingestion, replay, checkpoints, liquidity, and price
3. Core 2 — Acquisition, availability, retention, and lifecycle
4. Core 1 — Database, persistence, provenance, and maintenance
5. Core 6 — Cross-cutting contracts, concurrency, and cancellation
6. Core 4 — Analysis, aggregation, filtering, LM, and ranking
7. Core 5 — NiceGUI, charts, exports, and workflows
```

Core 7 ranks first because it is the newest large subsystem and crosses
filesystem, networking, immutable identity, remote concurrency, processing,
PostgreSQL import, and UI runtime boundaries.

Core 6 is exceptionally valuable but is ranked after concrete subsystem reviews
because its best results depend on already understanding those subsystems.

---

# Ranked dual-core combinations

```text
1. Core 1 + Core 7
   Persistence/provenance/importer/Hugging Face identity and rollback.

2. Core 3 + Core 7
   Local versus headless replay, checkpoints, artifacts, and venue behavior.

3. Core 2 + Core 6
   Operation admission, cancellation, retention, restart, and shutdown.

4. Core 2 + Core 7
   Remote source acquisition, release scheduling, catch-up, and local import.

5. Core 1 + Core 2
   Source state machine, advisory locks, retention, and recovery.

6. Core 3 + Core 4
   Sampling/price semantics consumed by aggregation and LM detection.

7. Core 4 + Core 5
   Analysis identity, rendering, navigation, export, and stale publication.

8. Core 5 + Core 7
   Remote import UX, progress, cancellation, availability, and workflow mode.

9. Core 1 + Core 6
   Foundational identity, transaction, determinism, and recovery invariants.

10. Core 3 + Core 6
    Replay worker cancellation, deterministic output, and cross-hour state.

11. Core 2 + Core 5
    Fetch/processing controls, availability, lifecycle, and shutdown.

12. Core 4 + Core 6
    Cache identity, cancellation, deterministic ranking, and stale data.
```

---

# Ranked triple-core combinations

```text
1. Core 1 + Core 3 + Core 7
   Raw source → headless replay → remote artifact → verified PostgreSQL import.

2. Core 2 + Core 6 + Core 7
   Scheduled acquisition, catch-up, concurrency, cancellation, and publication.

3. Core 1 + Core 2 + Core 3
   Complete local download → durable source → replay → materialization pipeline.

4. Core 3 + Core 4 + Core 6
   Raw semantics → analysis semantics → cross-cutting determinism and quality.

5. Core 1 + Core 2 + Core 7
   Source lifecycle, remote imports, availability, and durable metadata.

6. Core 4 + Core 5 + Core 6
   Analysis runtime → chart/UI publication → cancellation and stale ownership.

7. Core 1 + Core 6 + Core 7
   Immutable identity, transaction boundaries, remote concurrency, and recovery.

8. Core 2 + Core 5 + Core 7
   Local/remote workflow UX, progress, availability, and operation admission.

9. Core 1 + Core 4 + Core 7
   Imported analytical rows → aggregate loading → cache/analysis identity.

10. Core 2 + Core 3 + Core 6
    Active-file ownership, replay workers, retention, cancellation, and shutdown.
```
---
# Top-priority bug-finding rounds — unified ranking

The rankings below intentionally combine single-core, dual-core, and triple-core
reviews into one priority order.

Priority is determined by the project's current operational objectives, not by
the general architectural importance of a subsystem.

Current priority order:

1. GitHub Actions remote preprocessing must not fail.
2. Fetching and importing published remote artifacts from GitHub/Hugging Face
   must not fail or silently corrupt identity/provenance.
3. LM analysis must not fail or consume incorrect, stale, incomplete, or
   non-equivalent analytical data.
4. Only after those paths are covered should general acquisition, persistence,
   maintenance, NiceGUI, export, and other secondary concerns receive priority.

A connected triple-core review should outrank an unrelated dual-core or
single-core review when it covers a more important end-to-end failure path.

the importance of cores for user functionality is in this order:

1. Core 2
2. Core 6
3. Core 7
4. Core 3
5. Core 1
6. Core 4
7. Core 5

but to find the bugs related to their interactions we sort them in this below rankings:

## Unified priority ranking

### 1. Core 2 + Core 6 + Core 7
**Scheduled acquisition → workflow execution → concurrency/cancellation →
remote worker → artifact publication**

Highest priority because this covers the operational path most directly
responsible for GitHub Actions success.

Primary concerns:
- scheduled versus manual workflow behavior;
- source release-delay admission;
- catch-up selection and bounded search;
- duplicate worker admission;
- concurrent venue/instrument execution;
- cancellation and timeout behavior;
- worker failure classification;
- retry/idempotency after partial publication;
- workflow seed gates;
- failed/invalid hours accidentally becoming authoritative;
- resource cleanup after worker failure;
- workflow completion status disagreeing with actual publication state.

### 2. Core 3 + Core 6 + Core 7
**Replay/checkpoints → headless processing → remote artifacts → worker
concurrency**

Highest-value correctness review after workflow orchestration itself.

Primary concerns:
- replay behavior in headless versus local processing;
- cross-hour checkpoint continuity;
- checkpoint frontier selection;
- checkpoint publication after invalid or failed processing;
- cancellation while replay is active;
- synchronous worker threads surviving cancelled async wrappers;
- deterministic artifact generation;
- duplicate/conflicting publication;
- processing-chain recovery after one failed hour;
- local/remote processing divergence.

### 3. Core 2 + Core 3 + Core 7
**Remote source acquisition → replay → processing → artifact publication**

This is the complete remote preprocessing data path and should be treated as one
failure domain.

Primary concerns:
- missing or late source archives;
- release-boundary mistakes;
- catch-up gaps;
- venue-specific source handling;
- update-only archives with no valid predecessor;
- snapshot/frontier initialization;
- cross-hour source ownership;
- replay correctness feeding artifact creation;
- missing output checkpoints;
- invalid output being published as usable;
- source changes between validation and streaming.

### 4. Core 1 + Core 6 + Core 7
**Remote identity/provenance → concurrent publication → transactional import**

This is the most important remote-integrity review.

Primary concerns:
- immutable artifact identity;
- manifest/artifact consistency;
- revision pinning;
- parent-commit conflicts;
- concurrent publication;
- transactional import failure;
- metadata merging;
- provenance preservation;
- duplicate logical identities with different content;
- stale remote revisions;
- partial publication recovery;
- filesystem/DB ordering during import.

### 5. Core 1 + Core 3 + Core 7
**Source identity → replay/checkpoint identity → remote artifact identity**

This targets the risk that the pipeline technically succeeds but publishes or imports
the wrong data under a valid-looking identity.

Primary concerns:
- source-hour ownership;
- input/output checkpoint references;
- checkpoint content hashes;
- analytical content hashes;
- preset identity;
- artifact-key canonicalization;
- raw-source references;
- replay output determinism;
- provenance round trips;
- incorrect artifacts being accepted as the expected hour.

### 6. Core 1 + Core 7
**Remote artifact/import identity, provenance, revision, and transaction rules**

Highest-priority dual-core review for the local side of the remote workflow.

Primary concerns:
- importing the wrong revision;
- artifact and manifest coming from different revisions;
- immutable-content conflicts;
- import idempotency;
- source metadata accidentally overwritten;
- remote-imported rows appearing local/raw-backed when they are not;
- partial DB rollback;
- duplicate analytical ownership;
- stale remote data being accepted as current.

### 7. Core 3 + Core 7
**Local/headless processing equivalence**

Primary concerns:
- local replay versus headless replay differences;
- Binance/Bybit/OKX semantic divergence;
- checkpoint handling;
- artifact codec correctness;
- quality metadata;
- invalid/degraded hour handling;
- price-source adjacency policy;
- deterministic output;
- remote processing producing a different result from local processing.

### 8. Core 2 + Core 7
**Remote source acquisition, release scheduling, catch-up, and workflow admission**

Primary concerns:
- release-delay boundaries;
- available-hour discovery;
- catch-up cursor progression;
- source-path construction;
- missing-source classification;
- bounded historical search;
- venue-specific availability;
- workflow seed requirements;
- retry behavior;
- accidental permanent skipping of recoverable hours.

### 9. Core 1 + Core 4 + Core 7
**Remote import → analytical identity → aggregate/analysis loading**

This is the first major LM-analysis protection round.

Primary concerns:
- imported rows carrying incorrect identity;
- aggregate content hash mismatch;
- stale aggregate cache after remote replacement/conflict;
- preset identity mismatch;
- source/provenance lookup failures;
- imported analytical content being rejected as incomplete;
- imported content appearing complete when it is not;
- remote metadata causing analysis selection to load the wrong dataset.

### 10. Core 3 + Core 4 + Core 6
**L2/price semantics → aggregation → LM-analysis semantics**

Primary concerns:
- timestamp alignment;
- second/hour ownership;
- missing-hour generation;
- endpoint-state aggregation;
- degraded/invalid quality propagation;
- price filtering;
- activity-range ownership;
- aggregation boundary errors;
- cancellation and stale analysis publication;
- deterministic analysis inputs.

### 11. Core 4 + Core 6 + Core 7
**Remote-imported data → analysis cache/identity → analysis execution**

Primary concerns:
- stale imported data entering analysis;
- cache keys omitting source/content identity;
- analysis cancellation publishing stale results;
- remote-import completion racing analysis execution;
- changed source content not invalidating cached aggregates;
- deterministic analysis identity;
- runtime state disagreeing with persisted analytical truth.

### 12. Core 3 + Core 4
**Data semantics consumed by LM analysis**

Primary concerns:
- exact L2 sampling boundaries;
- price/L2 timestamp alignment;
- missing versus invalid periods;
- degraded coverage;
- aggregation bucket ownership;
- endpoint-state semantics;
- LM pivot and confirmation boundaries;
- adverse-move accounting;
- deterministic extrema/tie handling.

### 13. Core 4 + Core 6
**LM analysis execution, caching, cancellation, and stale-result prevention**

Primary concerns:
- cache-key incompleteness;
- cancellation after computation but before publication;
- stale analysis ownership;
- concurrent analysis requests;
- deterministic ranking;
- population-range leakage;
- empty/all-invalid datasets;
- large-range behavior;
- analysis identity drift.

### 14. Core 3
**Replay, checkpoints, liquidity, and price correctness**

Primary concerns:
- event grouping;
- venue sequence semantics;
- checkpoint continuity;
- sampling boundaries;
- depth-band boundaries;
- price ownership;
- invalid-hour behavior;
- deterministic replay;
- corruption detection.

This remains high priority because defects here can make both remote processing
and LM analysis wrong even when all surrounding infrastructure succeeds.

### 15. Core 2
**Acquisition, availability, retention, and lifecycle correctness**

Primary concerns:
- download validation;
- source state transitions;
- retry behavior;
- availability calculation;
- retention/pruning;
- recovery;
- cancellation;
- automatic-fetch state;
- lifecycle consistency.

### 16. Core 7
**Remote subsystem as a complete single-core review**

Use this when a full cross-file inspection is needed specifically for:
- artifact codec;
- contracts;
- headless processing;
- HF repository;
- importer;
- source acquisition;
- worker;
- remote CLI;
- remote UI integration.

### 17. Core 1
**Persistence, provenance, identity, and maintenance foundations**

Primary concerns:
- transaction correctness;
- immutable identity;
- provenance;
- checkpoint references;
- maintenance recovery;
- configuration precedence;
- diagnostics;
- migration/ORM consistency.

This is lower than the remote/analysis pipeline because failures here are only
more urgent when they directly affect one of those higher-priority paths.

### 18. Core 6
**Cross-cutting concurrency, cancellation, and integration**

Use as a broad independent review after the major end-to-end paths have already
been inspected.

Primary concerns:
- lock ordering;
- task cancellation;
- worker survival;
- shutdown;
- runtime/backend divergence;
- publication ordering;
- retry idempotency;
- resource cleanup.

### 19. Core 5
**NiceGUI, charts, exports, controls, and user workflows**

This is intentionally last in the current ranking.

Primary concerns:
- stale UI publication;
- chart generation ownership;
- control races;
- export correctness;
- availability-calendar refresh;
- remote/local workflow presentation;
- shutdown UI behavior.

UI defects should be prioritized earlier only when they prevent the three
primary operational objectives above from succeeding.

---

# Recommended bug-finding round format

Use this base prompt for every selected core or core combination:

```text
Focus on the selected core or core combination. Treat the rest of the
repository as available dependency/context.

Inspect production code first and use tests, workflows, configuration, and
documentation as supporting evidence.

Find concrete bugs only. For every proposed bug:

1. Name the exact production file and function/class.
2. Describe the exact triggering state or input.
3. Trace the executable path to the incorrect behavior.
4. Explain the user-visible, data-integrity, concurrency, recovery, security,
   or determinism impact.
5. Explain why existing validation or tests do not prevent it.
6. Distinguish the bug from intentional strict validation.
7. Give a minimal correction direction, but do not produce a patch unless
   explicitly requested.
8. Assign confidence: certain, high, medium, or speculative.
9. Exclude speculative items from the final confirmed-bug list.
10. Prefer a small number of high-confidence bugs over a long weak list.

Pay particular attention to:
- bool accepted as int;
- NaN or infinity passing numeric validation;
- stale async task ownership;
- synchronous workers surviving cancelled asyncio wrappers;
- transaction/filesystem publication ordering;
- immutable identity conflicts;
- revision drift;
- missing lock coverage;
- incomplete provenance;
- cross-hour boundary errors;
- invalid/degraded data silently becoming usable;
- local and remote processing divergence;
- runtime/UI truth diverging from durable backend truth.
```

---

# Recommended per-round retrieval groupings

For large exports, retrieve files in coherent groups rather than one arbitrary
segment at a time.

```text
Core 1:
    database models/repositories
    presets/provenance
    migrations/schema/bootstrap
    maintenance/diagnostics

Core 2:
    acquisition models/state
    downloader/HTTP/rate limit
    coordinator/automatic fetch
    availability/retention/recovery
    UI operational runtimes

Core 3:
    Parquet reader/venue adapter
    replay/checkpoint
    sampling/depth/hourly liquidity
    price construction/codecs
    processing coordinators

Core 4:
    timeframe/aggregation
    multi-market loading
    dataset/price filter
    LM detection/ranking/execution
    analysis runtime

Core 5:
    NiceGUI state/components/app
    analysis controls/table/export
    ECharts/chart interactions
    fetch/settings tabs
    shutdown

Core 6:
    operation admission paths
    cancellation paths
    publication/transaction paths
    restart/recovery paths
    cache/identity paths

Core 7:
    remote contracts/artifact codec
    headless processing/CLI
    source acquisition/worker
    Hugging Face repository
    importer/remote UI runtime
    GitHub workflows
```

---

# Completion criteria for a bug-finding round

A round is complete only when:

1. Every primary production module in the selected scope was inspected.
2. Relevant callers and callees outside the core were used as context.
3. Relevant tests were compared with production behavior.
4. Workflow and configuration assumptions were checked where applicable.
5. Findings were deduplicated by root cause.
6. Every confirmed finding has an executable failure path.
7. Uncertain observations are separated from confirmed bugs.
8. The final answer states which files were reviewed.
9. The final answer states important areas where no concrete bug was found.
10. Additional searching is unlikely to change the confirmed conclusions.
```

## Important inventory note

The available package manifest appeared slightly behind the newest source export: newer files such as these were visible in the actual project contents and are intentionally included above:

```text
l2shock/remote/importer.py
l2shock/remote_worker.py
l2shock/ui/remote_import_runtime.py
tests/test_bybit_orderbook_replay.py
tests/test_remote_bybit_processing.py
tests/test_remote_import_runtime.py
tests/test_remote_importer.py
tests/test_remote_importer_postgresql.py
tests/test_remote_worker.py
tests/test_remote_workflow_contract.py
```

If any of those files have since been renamed, the validation command below will identify the mismatch.

---

## Batch 1 validation

Run from the project root in PowerShell 7:

```powershell
@'
from pathlib import Path

path = Path("project_cores.md")
text = path.read_text(encoding="utf-8")

required = (
    "# Core 1 —",
    "# Core 2 —",
    "# Core 3 —",
    "# Core 4 —",
    "# Core 5 —",
    "# Core 6 —",
    "# Core 7 —",
    "# Ranked single-core bug-finding rounds",
    "# Ranked dual-core combinations",
    "# Ranked triple-core combinations",
    "l2shock/remote/importer.py",
    "l2shock/remote_worker.py",
    "l2shock/ui/remote_import_runtime.py",
    "tests/test_remote_import_runtime.py",
    "tests/test_remote_workflow_contract.py",
)

missing = tuple(value for value in required if value not in text)

if missing:
    raise AssertionError(f"project_cores.md is missing: {missing}")

print("project_cores.md structure OK")
'@ | python
```

Use this second check to find inventory entries that do not exist locally:

```powershell
@'
from pathlib import Path
import re

text = Path("project_cores.md").read_text(encoding="utf-8")

paths = sorted(
    set(
        re.findall(
            r"(?m)^(l2shock/[A-Za-z0-9_./-]+\.py|tests/test_[A-Za-z0-9_./-]+\.py)$",
            text,
        )
    )
)

missing = [value for value in paths if not Path(value).is_file()]

print(f"Documented Python paths: {len(paths)}")

if missing:
    print("Documented paths not found locally:")
    for value in missing:
        print(f"  {value}")
    raise SystemExit(1)

print("All documented Python paths exist")
'@ | python
```
