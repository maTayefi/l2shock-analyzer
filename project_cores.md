# Final merged bug-finding cores

## Core 1 — Database, persistence, identity, provenance, diagnostics, maintenance

### Focus areas

1. Alembic/ORM schema drift and migration/runtime mismatch.
2. Frozen migration independence.
3. Transaction ownership, accidental commits, rollback behavior, and session leakage.
4. Advisory-lock coverage, ownership, timeout, and release.
5. Idempotent writes versus conflict behavior.
6. Typed provenance canonical encoding/decoding.
7. Duplicate logical archive identities.
8. Preset immutability and deterministic hash reproducibility.
9. Raw-retention filesystem/transaction recovery.
10. Orphan-checkpoint reference accuracy.
11. Multiple materialization hashes for one logical source.
12. OKX analytical orphan classification.
13. Secret leakage through diagnostics, logs, and health output.
14. UTC timestamp ownership and timezone consistency.
15. Maintenance-preview freshness and candidate revalidation.
16. Processing metadata/provenance consistency.
17. Repository-level analytical/price consistency.
18. Persistence behavior under partial failure and retry.
19. Cross-table identity invariants.
20. Database state versus filesystem state divergence.
21. Configuration/default/example drift and environment-variable precedence.
22. Invalid configuration acceptance, normalization, and validation boundaries.
23. Configuration-derived identity/hash reproducibility.

### Permissive file list

```text
README.md
alembic.ini
alembic/env.py
alembic/versions/0001_initial.py
l2shock/config.py 
l2shock/timeutils.py 
config.yaml.example
l2shock/db/__init__.py
l2shock/db/models.py
l2shock/db/engine.py
l2shock/db/schema.py
l2shock/db/bootstrap.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/acquisition/models.py
l2shock/acquisition/persistence.py
l2shock/acquisition/repository.py
l2shock/acquisition/locks.py
l2shock/acquisition/retention.py
l2shock/presets/identity.py
l2shock/presets/management.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/diagnostics.py
l2shock/processing/source_repository.py
l2shock/processing/models.py
l2shock/processing/errors.py
l2shock/processing/integrity.py
l2shock/processing/checkpoint_store.py
l2shock/processing/l2_coordinator.py
l2shock/processing/price_coordinator.py
tests/test_db_models.py
tests/test_migration_freeze.py
tests/test_schema_postgresql.py
tests/test_analytical_provenance.py
tests/test_price_provenance.py
tests/test_analytical_repository_postgresql.py
tests/test_price_repository_postgresql.py
tests/test_acquisition_repository_postgresql.py
tests/test_data_preset_identity.py
tests/test_multi_market_preset_identity.py
tests/test_preset_management.py
tests/test_raw_retention.py
tests/test_maintenance_actions.py
tests/test_maintenance_actions_postgresql.py
tests/test_maintenance_diagnostics.py
tests/test_diagnostics.py
tests/test_diagnostics_postgresql.py
tests/test_processing_l2_metadata.py
l2shock/__init__.py
alembic/script.py.mako
alembic/versions/__init__.py
l2shock/presets/__init__.py
l2shock/processing/checkpoint_references.py
tests/test_config.py
tests/test_timeutils.py
pyproject.toml
requirements.txt
```

---

## Core 2 — Acquisition, automatic fetch, availability, processing handoff, retention, lifecycle

### Focus areas

1. Manual versus automatic fetch admission races.
2. Fetch/Processing/Analysis mutual exclusion.
3. Operation-lock ownership and lock lifetime.
4. Native cancellation while workers are running.
5. Interrupted source statuses and resumability.
6. Atomic `.part` publication.
7. Retry behavior and `Retry-After`.
8. Rate-limiter fairness and starvation.
9. Catch-up retry-cursor correctness.
10. Six-file source completeness.
11. Checkpoint publication versus database commit ordering.
12. Materialization-aware target selection.
13. Processed/pruned source handling.
14. Shutdown sequencing and cancellation propagation.
15. Availability refresh after operations.
16. Downloaded-target → processing handoff.
17. Prevent processing from starting while automatic fetch owns acquisition.
18. Automatic-fetch completion publication.
19. Fetch persistence/recovery after process restart.
20. Duplicate fetch admission and duplicate work suppression.
21. HTTP error classification and retryability.
22. Downloader/result-state consistency.
23. Availability-calendar consistency with actual source state.
24. Retention interaction with active/in-flight operations.
25. Fetch state publication versus UI runtime state.
26. Duplicate worker execution and race-induced duplicate work.
27. Completion-versus-cancellation ordering.
28. Restart recovery versus new-work admission races.
29. Retention versus processing/fetch race windows.
30. Worker failure versus persisted state reconciliation.

### Permissive file list

```text
README.md
config.yaml.example
l2shock/config.py
l2shock/timeutils.py
l2shock/acquisition/__init__.py
l2shock/acquisition/models.py
l2shock/acquisition/planning.py
l2shock/acquisition/http.py
l2shock/acquisition/downloader.py
l2shock/acquisition/validation.py
l2shock/acquisition/results.py
l2shock/acquisition/rate_limit.py
l2shock/acquisition/fetch_models.py
l2shock/acquisition/fetch_persistence.py
l2shock/acquisition/persistence.py
l2shock/acquisition/repository.py
l2shock/acquisition/coordinator.py
l2shock/acquisition/automatic.py
l2shock/acquisition/availability.py
l2shock/acquisition/retention.py
l2shock/acquisition/locks.py
l2shock/processing/__init__.py
l2shock/processing/models.py
l2shock/processing/errors.py
l2shock/processing/integrity.py
l2shock/processing/source_repository.py
l2shock/processing/target_planning.py
l2shock/processing/checkpoint_store.py
l2shock/processing/l2_coordinator.py
l2shock/processing/price_coordinator.py
l2shock/ui/state.py
l2shock/ui/fetch_runtime.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/tab_fetch.py
l2shock/ui/shutdown.py
tests/test_acquisition_planning.py
tests/test_acquisition_http.py
tests/test_acquisition_rate_limit.py
tests/test_acquisition_validation.py
tests/test_acquisition_downloader.py
tests/test_acquisition_coordinator.py
tests/test_acquisition_persistence.py
tests/test_acquisition_repository_postgresql.py
tests/test_automatic_fetch_runtime.py
tests/test_raw_retention.py
tests/test_availability.py
tests/test_availability_calendar.py
tests/test_ui_fetch_runtime.py
tests/test_ui_processing_runtime.py
tests/test_ui_shutdown.py
tests/test_processing_contracts.py
tests/test_processing_integrity.py
tests/test_processing_checkpoint_store.py
tests/test_processing_target_planning.py
tests/test_processing_l2_coordinator_postgresql.py
tests/test_processing_price_coordinator_postgresql.py
l2shock/acquisition/errors.py
```

---

## Core 3 — Ingestion, venue adapters, replay, checkpoints, liquidity, price

### Focus areas

1. Event grouping across reader batches.
2. Nullable venue-specific fields.
3. Binance/OKX frontier ownership.
4. Snapshot completeness.
5. Refusal to initialize from update-only archives.
6. Cross-hour checkpoint ownership.
7. Duplicate versus conflicting replay events.
8. Same-timestamp event sampling.
9. Boundary-event inclusion.
10. Locked/crossed/empty-book invalidity.
11. Exact depth-boundary inclusion.
12. Exact Decimal accumulation and rounding.
13. Checkpoint corruption detection.
14. Checkpoint bounds validation.
15. Checkpoint content-address/hash integrity.
16. Immediate-next-hour checkpoint ownership.
17. Cross-hour continuity validation.
18. Trade-time half-open bucket semantics.
19. Duplicate trade handling.
20. Price-series memory growth.
21. Price-block encoding/decoding.
22. Venue-specific sequence semantics.
23. Replay ordering under equal timestamps.
24. Missing/partial source behavior.
25. Replay diagnostics consistency.
26. Liquidity output reproducibility from identical input.
27. Price/OHLC consistency with source trades.
28. Production replay behavior versus validation-tool behavior.
29. Reader/adapter contract drift across Binance and OKX.
30. Malformed/unknown source-field handling without silent reinterpretation.

### Permissive file list

```text
README.md
l2shock/acquisition/models.py
l2shock/ingest/__init__.py
l2shock/ingest/venue_adapter.py
l2shock/ingest/parquet_reader.py
l2shock/ingest/replay.py
l2shock/ingest/sampling.py
l2shock/ingest/checkpoint_codec.py
l2shock/ingest/reporting.py
l2shock/ingest/replay_validation.py
l2shock/liquidity/__init__.py
l2shock/liquidity/depth.py
l2shock/liquidity/hourly.py
l2shock/liquidity/block_codec.py
l2shock/price/__init__.py
l2shock/price/hourly.py
l2shock/price/block_codec.py
l2shock/analysis/multi_market.py
l2shock/presets/identity.py
l2shock/processing/checkpoint_store.py
l2shock/processing/integrity.py
tools/inspect_venue_sequence_contract.py
tools/validate_okx_replay.py
tests/test_parquet_reader.py
tests/test_venue_sequence_adapter.py
tests/test_okx_orderbook_replay.py
tests/test_orderbook_replay.py
tests/test_orderbook_sampling.py
tests/test_checkpoint_codec.py
tests/test_replay_diagnostics.py
tests/test_replay_validation_cli.py
tests/test_depth_liquidity.py
tests/test_hourly_liquidity.py
tests/test_hourly_block_codec.py
tests/test_trade_ohlc.py
tests/test_trade_ohlc_block_codec.py
tests/test_analysis_multi_market.py
```

---

## Core 4 — Analysis loading, aggregation, filtering, liquidity movement, LM detection, ranking

### Focus areas

1. Closed-endpoint snapping.
2. Exact-boundary off-by-one handling.
3. Missing-hour generation.
4. L2/price timestamp alignment.
5. OHLC-intersection eligibility.
6. Display clipping versus raw OHLC.
7. Context-window ownership.
8. Hard-discontinuity construction.
9. Partial-market degraded coverage.
10. Aggregate Decimal exactness.
11. LM pivot ownership.
12. LM confirmation ownership.
13. Offline terminal candidates.
14. Adverse-move diagnostics.
15. Population isolation.
16. Percentile/MAD/modified-Z behavior.
17. Top-N recall union.
18. Primary-before-secondary ordering.
19. Deterministic analysis identity.
20. Cache cancellation safety.
21. Aggregate provenance reproducibility.
22. Multi-market alignment.
23. Independent activity versus chart-series semantics.
24. Explicit invalid-series representation.
25. Price-excluded segmentation.
26. Hard L2-boundary handling.
27. Empty aggregate behavior.
28. Deterministic filtering/ranking under identical data.
29. Analysis request identity and cache keys.
30. Regression behavior for degraded/partial datasets.
31. Cache key completeness versus all analysis-affecting inputs.
32. Cache invalidation when source, preset, timeframe, or semantic version changes.

### Permissive file list

```text
README.md
l2shock/analysis/__init__.py
l2shock/analysis/timeframes.py
l2shock/analysis/aggregation.py
l2shock/analysis/price_filter.py
l2shock/analysis/multi_market.py
l2shock/analysis/dataset.py
l2shock/analysis/liquidity_movement.py
l2shock/analysis/ranking.py
l2shock/analysis/execution.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/presets/identity.py
l2shock/acquisition/availability.py
l2shock/ui/analysis_runtime.py
l2shock/ui/analysis_controls.py
tests/test_timeframe_aggregation.py
tests/test_price_filter.py
tests/test_analysis_multi_market.py
tests/test_analysis_dataset.py
tests/test_analysis_dataset_aggregate_regression.py
tests/test_liquidity_movement.py
tests/test_liquidity_movement_ranking.py
tests/test_analysis_execution.py
tests/test_ui_analysis_runtime.py
```

---

## Core 5 — NiceGUI, charts, exports, UI state, lifecycle, user workflows

### Focus areas

1. Undefined widget names.
2. Stale closures and stale captured state.
3. NiceGUI client-context ownership.
4. Operation control enable/disable correctness.
5. Stale runtime-completion publication.
6. Browser render-token ownership.
7. ECharts merge ghosts.
8. Crosshair rebinding.
9. DataZoom synchronization.
10. Compressed category mapping.
11. Table-row/ranking identity.
12. Selected-LM emphasis.
13. Timeframe viewport restoration.
14. PNG/SVG generation ownership.
15. Exact JSON export.
16. Persistent warning duplication.
17. Cross-tab calendar handoff.
18. Preset mutation locks.
19. Maintenance confirmation flow.
20. Safe application shutdown.
21. Secret/path leakage through UI and health.
22. UI publication after operation cancellation.
23. UI publication after client disappearance.
24. Runtime state versus backend truth.
25. Chart acknowledgement/render synchronization.
26. ECharts option-layout validation.
27. Stale chart data after series changes.
28. Selection/highlight identity across refreshes.
29. Availability/fetch/processing tab synchronization.
30. Lifecycle races between background workers and browser events.
31. UI test isolation, stale fixtures, and shared-state leakage.
32. Backend-authoritative state versus optimistic UI state.

### Permissive file list

```text
README.md
l2shock/main.py
l2shock/config.py
l2shock/diagnostics.py
l2shock/maintenance_actions.py
l2shock/maintenance_diagnostics.py
l2shock/ui/__init__.py
l2shock/ui/app.py
l2shock/ui/state.py
l2shock/ui/components.py
l2shock/ui/echarts.py
l2shock/ui/chart_interactions.py
l2shock/ui/analysis_chart.py
l2shock/ui/analysis_controls.py
l2shock/ui/analysis_export.py
l2shock/ui/analysis_legend.py
l2shock/ui/analysis_runtime.py
l2shock/ui/availability_calendar.py
l2shock/ui/fetch_runtime.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/tab_fetch.py
l2shock/ui/tab_analysis.py
l2shock/ui/tab_settings.py
l2shock/ui/shutdown.py
tests/test_ui_foundation.py
tests/test_ui_fetch_runtime.py
tests/test_ui_processing_runtime.py
tests/test_ui_analysis_runtime.py
tests/test_ui_shutdown.py
tests/test_analysis_chart.py
tests/test_chart_interactions.py
tests/test_analysis_export.py
tests/test_analysis_legend.py
tests/test_availability_calendar.py
tests/test_echarts_publication.py
tests/test_ui_analysis_controls.py
tests/test_diagnostics.py
tests/test_maintenance_diagnostics.py
tests/test_maintenance_actions.py
l2shock/logging_setup.py
```

`tests/test_ui_analysis_controls.py` exists in the current repository.
`tests/test_analysis_controls.py` does not exist and is intentionally omitted.

---

# Core 6 — Cross-cutting contracts, invariants, and integration

This is the addition I would make to the two original lists. It is important enough to deserve its own export rather than being scattered among the five cores.

### Focus areas

1. Global UTC/timezone invariants.
2. Shared operation-lock semantics.
3. Fetch → processing → analysis state transitions.
4. Source identity → materialization identity → checkpoint identity.
5. Filesystem publication → DB commit ordering.
6. Cancellation propagation across acquisition, processing, analysis, and UI.
7. Restart/recovery semantics.
8. Idempotency across every pipeline stage.
9. Provenance preservation across transformations.
10. Deterministic hashes/identities.
11. Availability state versus actual filesystem/database state.
12. Checkpoint continuity versus source continuity.
13. Manual versus automatic workflow equivalence.
14. Binance versus OKX semantic normalization.
15. L2 versus price dataset alignment.
16. Partial/missing/degraded data propagation.
17. Runtime/UI state versus authoritative backend state.
18. Cache key correctness and invalidation.
19. Error classification and propagation across layers.
20. Shutdown safety and resource ownership.
21. Cross-module invariant violations that individual bundle reviews can miss.
22. Restart/recovery idempotency across partially completed operations.
23. Test-suite order dependence and shared-state contamination.
24. Production-code behavior versus test/validation-tool assumptions.
25. State-machine completeness: every success, failure, cancellation, retry, and restart transition.
26. Exactly-once versus at-least-once behavior where duplicate execution is possible.
27. Cross-core deadlocks, race conditions, and inconsistent ownership boundaries.

### Permissive file list

```text
README.md
config.yaml.example
l2shock/config.py
l2shock/timeutils.py
l2shock/acquisition/models.py
l2shock/acquisition/locks.py
l2shock/acquisition/persistence.py
l2shock/acquisition/repository.py
l2shock/acquisition/coordinator.py
l2shock/acquisition/automatic.py
l2shock/acquisition/availability.py
l2shock/acquisition/retention.py
l2shock/processing/models.py
l2shock/processing/errors.py
l2shock/processing/integrity.py
l2shock/processing/source_repository.py
l2shock/processing/target_planning.py
l2shock/processing/checkpoint_store.py
l2shock/processing/l2_coordinator.py
l2shock/processing/price_coordinator.py
l2shock/ingest/venue_adapter.py
l2shock/ingest/replay.py
l2shock/ingest/sampling.py
l2shock/ingest/checkpoint_codec.py
l2shock/liquidity/depth.py
l2shock/liquidity/hourly.py
l2shock/price/hourly.py
l2shock/analysis/dataset.py
l2shock/analysis/aggregation.py
l2shock/analysis/multi_market.py
l2shock/analysis/execution.py
l2shock/analysis/liquidity_movement.py
l2shock/analysis/ranking.py
l2shock/presets/identity.py
l2shock/db/analytical_repository.py
l2shock/db/price_repository.py
l2shock/ui/state.py
l2shock/ui/fetch_runtime.py
l2shock/ui/automatic_fetch_runtime.py
l2shock/ui/processing_runtime.py
l2shock/ui/analysis_runtime.py
l2shock/ui/shutdown.py
tests/test_processing_contracts.py
tests/test_processing_integrity.py
tests/test_analysis_execution.py
tests/test_analysis_dataset.py
tests/test_analysis_dataset_aggregate_regression.py
tests/test_automatic_fetch_runtime.py
tests/test_ui_fetch_runtime.py
tests/test_ui_processing_runtime.py
tests/test_ui_analysis_runtime.py
tests/test_ui_shutdown.py
tests/test_echarts_publication.py
l2shock/acquisition/errors.py
l2shock/logging_setup.py
tests/test_ai_preparation_scripts.py
```

---

```text
Core 1  Database / persistence / identity / provenance
Core 2  Acquisition / automatic fetch / lifecycle / handoff
Core 3  Ingestion / replay / checkpoints / liquidity / price
Core 4  Analysis / filtering / LM / ranking
Core 5  UI / charts / exports / lifecycle
Core 6  Cross-cutting contracts / invariants / integration
```

# ranking of triple and dual combination of cores to find bugs in them

our ranking would be:
Core 1 + Core 2 + Core 3 
Core 3 + Core 4 + Core 6 
Core 3 + Core 4          
Core 1 + Core 2          
Core 4 + Core 5          
Core 2 + Core 5          
Core 1 + Core 6

# Bug-Finding Prompts for Each Core Combination

---

## 1. Core 1 + Core 2 + Core 3

**Focus on Core 1 + Core 2 + Core 3. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **database/persistence/identity/provenance** (Core 1), **acquisition/automatic-fetch/lifecycle/retention** (Core 2), and **ingestion/replay/checkpoints/liquidity/price-construction** (Core 3). These three cores form the complete data pipeline from remote download through durable storage to replay-ready state. Look for bugs that only appear when all three interact.

### A. Acquisition → Persistence → Replay Handoff

1. **Source-hour status transition legality during replay consumption.** The `source_hours.status` column transitions through `discovered → downloading → downloaded → processing → processed`. Verify that replay (Core 3) can never consume a source whose status is `downloading`, `missing`, `invalid`, `quarantined`, or `error`. Check `SQLAlchemyProcessingSourceRepository.find_replayable()` — it filters by `_REPLAYABLE_STATUSES`. Confirm no code path allows a race where a source transitions out of `downloaded` between the `find_replayable` check and the actual `read_orderbook_file` / `read_trade_file` call.

2. **Advisory lock coverage during replay.** `acquire_source_hour_transaction_lock` is called in `AcquisitionRepository` methods and in `l2_coordinator._run_transaction`. Verify that the lock is held for the entire duration that replay reads the source file. If the lock is released (by transaction commit/rollback) before replay finishes reading, another operation could prune or re-download the file mid-read.

3. **Checkpoint publication vs. DB commit ordering.** In `SingleMarketL2ProcessingCoordinator._run_transaction`, the checkpoint is published to the filesystem *before* the DB transaction commits. If the DB commit fails, an orphan checkpoint exists. Verify that:
   - The orphan-checkpoint diagnostics in `maintenance_diagnostics.py` correctly identify this state.
   - The `collect_referenced_checkpoint_sha256s` function in `checkpoint_references.py` does NOT reference the orphan (since the DB row was never committed).
   - Retry of the same processing operation can reuse the orphan checkpoint via `CheckpointStore.publish` idempotency.

4. **Retention vs. active replay.** `plan_processed_raw_retention` selects processed sources older than the cutoff. If a replay operation is currently reading that source (status = `processing`), the retention planner must not select it. Verify the status filter excludes `processing`. Also verify that `execute_maintenance_action` for `PRUNE_RAW_FILES` re-checks status under advisory lock before mutation.

5. **`content_sha256` consistency across layers.** The acquisition layer computes `sha256_file` and stores it in `source_hours.content_sha256`. The processing layer calls `verify_processing_source_archive` which re-hashes the file. The replay layer reads the file via PyArrow. Verify that no layer silently re-downloads, truncates, or replaces the file between hash verification and PyArrow read. Check for TOCTOU (time-of-check-time-of-use) windows.

### B. Checkpoint Store ↔ DB Models ↔ Source Repository

6. **Checkpoint identity vs. source identity.** `CheckpointIdentity` uses `(provider, venue, instrument, through_hour_utc)`. `SourceFileSpec` uses `(provider, venue, data_kind, symbol, hour_utc)`. Verify that `instrument` in checkpoint identity always equals `symbol` in source spec. A mismatch would cause `validate_for_source` to silently fail or accept the wrong checkpoint.

7. **`source_content_sha256` in checkpoint vs. `content_sha256` in source_hours.** When a checkpoint is created, `OrderBookCheckpoint.source_content_sha256` records the hash of the source archive it was derived from. In `CheckpointStore.build_search_plan`, the code compares `checkpoint.checkpoint.source_content_sha256` against `source_repository.find_durable_content_sha256(predecessor_spec)`. Verify this comparison is exact string equality, not case-insensitive or truncated. Also verify that a pruned source (where `local_path` is NULL but `content_sha256` remains) still provides the correct durable digest.

8. **Checkpoint format version vs. DB schema version.** `CHECKPOINT_FORMAT_VERSION` is 1. `L2HourlySeries.schema_version` is also 1 but represents a different concept. Verify no code conflates these two version numbers. Check `encode_checkpoint` / `decode_checkpoint` against `L2HourlySeries.schema_version` usage.

### C. Provenance Chain Integrity

9. **L2 provenance `source_hours` list completeness.** `L2HourlyProvenance.source_hours` must include the current analytical hour's orderbook source. In `l2_coordinator._run_transaction`, the provenance is built from `plan.replay_sources`. Verify that the target hour's source is always included, even when the target was initialized from a checkpoint (i.e., `plan.replay_sources` has length 1).

10. **Price provenance adjacent-source policy.** `PriceHourlyProvenance.validate_for_hour` allows sources at offset -1, 0, +1. In `price_coordinator._select_sources`, when `include_adjacent_sources=True`, the code queries `find_replayable` for offsets -1 and +1. Verify that if an adjacent source is `processed` but pruned (`local_path` is NULL), `find_replayable` returns `None` and the adjacent source is silently omitted — not treated as an error that blocks the current-hour processing.

11. **Duplicate logical archive identity in provenance.** `L2HourlyProvenance.__post_init__` rejects two `SourceHourReference` objects with the same `archive_identity_tuple` but different `content_sha256`. Verify this check fires correctly when a source is re-downloaded with different content (e.g., after a corrupted download is quarantined and re-fetched).

### D. Configuration → Identity → Hash Reproducibility

12. **`config.yaml` → `Settings` → processing behavior.** `processing.checkpoint_search_max_hours` defaults to 168. If a user changes this to a smaller value after checkpoints already exist beyond the new bound, `build_search_plan` will stop at `SEARCH_BOUND_REACHED` and replay from scratch. Verify this does not produce a different analytical result than the original run (it should not, because replay is deterministic given the same source archives). But verify that the *provenance* recorded in the DB correctly reflects the shorter replay chain.

13. **Preset hash stability across DB reads.** `LiquidityDataPreset.preset_hash` is computed from `canonical_json_bytes`. The DB stores `config_json` as JSONB. PostgreSQL JSONB normalizes key order. Verify that reading `config_json` back from PostgreSQL and re-encoding it produces the same bytes as the original `canonical_json_bytes`. If PostgreSQL reorders keys, the hash will not match. Check `AnalyticalRepository.ensure_preset` and `_assert_preset_identity_matches`.

### E. Concurrency and Locking Across All Three Cores

14. **Fetch → Processing → Replay serialization.** The process-wide `operation_lock` (asyncio.Lock) prevents concurrent fetch and processing. But replay within processing is synchronous and runs in a worker thread. Verify that the asyncio lock is held for the entire duration of the worker thread's replay, not just the `await asyncio.to_thread(...)` call. If the lock is released before the thread finishes, another fetch could start downloading the same source.

15. **Automatic fetch catch-up vs. manual processing.** `AutomaticFetchRuntime._run` calls `create_production_manual_fetch_coordinator` which acquires `state.operation_lock`. `ManualProcessingRuntime._run` also acquires `state.operation_lock`. Verify that if automatic fetch is mid-iteration and the user clicks "Process Downloaded Data", the processing start is correctly rejected with `ProcessingRuntimeBusyError`, and vice versa.

16. **Shutdown ordering across fetch/processing/replay.** `shutdown_runtime` stops fetch → processing → analysis → tasks → engine. If processing is mid-replay (inside `replay_orderbook_archives`), the cooperative cancellation event is set. Verify that `ReplayCancelledError` propagates correctly through `sample_liquidity_archives` → `OneSecondLiquiditySampler` → `calculate_depth_liquidity` → `_sum_side_liquidity` without leaving the `OrderBookReplayState` in a half-mutated state.

### F. Data Integrity at Boundaries

17. **Hour boundary in replay vs. source file naming.** Source files are named by UTC hour (`YYYY-MM-DD/HH/`). Replay chains require adjacent hours. Verify that `_validated_replay_sources` correctly rejects non-adjacent hours when DST transitions cause a 23-hour or 25-hour day. (CryptoHFTData uses UTC, so this should not happen, but verify the code does not assume 24-hour days.)

18. **`row_count` vs. `event_count` in source_hours.** After replay, `l2_coordinator` writes `target_row.row_count` and `target_row.event_count` from `ReplayArchiveReport`. Verify that `row_count` comes from `reader_report.rows_read` (Parquet rows) and `event_count` comes from `reader_report.events_seen` (grouped events). These are different numbers. A bug that swaps them would corrupt diagnostics.

19. **Quality state propagation from replay to DB.** `l2_coordinator` computes `ProcessingQualityState` from `valid_count`, `degraded_count`, `invalid_count`. Verify the mapping: all 3600 valid → VALID; any valid or degraded → DEGRADED; all invalid → INVALID. Check that a hour with 3599 valid and 1 invalid is DEGRADED, not VALID. Check that a hour with 0 valid and 3600 invalid is INVALID, not DEGRADED.

20. **`quality_json` schema drift.** `l2_coordinator` writes a `quality_json` dict with keys like `analytical_content_sha256`, `analytical_content_sha256s`, `analytical_outputs_by_preset`, `output_checkpoint_content_sha256`. `maintenance_diagnostics._quality_content_hashes` reads these keys. Verify that the key names match exactly between writer and reader. A typo or rename in one place would cause diagnostics to miss references.

### G. Edge Cases and Failure Modes

21. **Empty Parquet file (0 rows).** `read_orderbook_file` raises `StreamedParquetReadError` if `rows_read != metadata_rows`. But what if `metadata.num_rows` is 0? The reader would produce 0 events. Replay would produce no checkpoint. Verify that the processing coordinator handles this gracefully (the source should be marked `error` or `invalid`, not `processed`).

22. **Checkpoint with zero levels.** `OrderBookCheckpoint.__post_init__` raises if `levels` is empty. But `OrderBookReplayState.checkpoint()` also checks `self._bids` and `self._asks` are non-empty. Verify there is no code path where a checkpoint with levels is created but then all levels are removed before encoding (e.g., by a concurrent mutation of the replay state).

23. **`write_checkpoint_file` atomicity on Windows.** The function uses `os.link` for atomic publish. On Windows, `os.link` may fail across volumes. Verify that `destination.parent` and `temporary.parent` are always on the same volume. Since `temporary` is created in `destination.parent`, this should be safe, but verify no symlink or junction can cause a cross-volume scenario.

24. **Retention dry-run vs. actual pruning divergence.** `plan_processed_raw_retention` checks `path.stat().st_size` and `sha256_file(path)`. Between the dry-run and the actual `execute_maintenance_action`, the file could be modified. Verify that `_execute_raw_pruning` re-hashes the file under advisory lock before renaming. Check that the re-hash uses the same `_HASH_CHUNK_SIZE` and produces the same digest as the dry-run.

25. **`_updated_analytical_output_metadata` idempotency.** When the same preset is materialized twice for the same source hour, `_updated_analytical_output_metadata` should produce the same `analytical_content_sha256s` list. Verify that the list is sorted and deduplicated. If the same content hash appears under two different preset hashes, both entries in `analytical_outputs_by_preset` should exist, but the `analytical_content_sha256s` list should contain the hash only once.

---

## 2. Core 3 + Core 4 + Core 6

**Focus on Core 3 + Core 4 + Core 6. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **ingestion/replay/checkpoints/liquidity/price** (Core 3), **analysis/aggregation/filtering/LM-detection/ranking** (Core 4), and **cross-cutting contracts/invariants/integration** (Core 6). These three cores span the entire analytical pipeline from raw replay through derived metrics to Liquidity Movement detection, bound together by cross-cutting invariants. Look for bugs that only appear when all three interact.

### A. Replay → Sampling → Aggregation Timestamp Ownership

1. **`received_time_ns` as the sole L2 sampling clock.** The replay layer records `last_event_received_time_ns` on every `apply()` call. The sampling layer (`OneSecondBookSampler`) uses this to assign events to 1-second buckets. The analysis layer (`aggregate_l2_seconds`) receives `L2Second` objects with `timestamp_utc`. Verify that the conversion from `received_time_ns` (nanoseconds since epoch) to `timestamp_utc` (Python datetime) is exact and does not lose sub-millisecond precision. Check `_epoch_ns_to_utc` in `parquet_reader.py` and `_datetime_to_epoch_ns` in `sampling.py` for round-trip fidelity.

2. **`trade_time_ms` as the sole price candle clock.** The price layer (`OneSecondTradeOHLCAccumulator`) uses `trade_time_ms` for bucket assignment. The analysis layer receives `PriceSecond` objects. Verify that no code path uses `received_time_ns` or `event_time_ms` for candle assignment. Check `stream_trade_ohlc_hour` and `build_trade_ohlc_hour` for clock confusion.

3. **Bucket boundary semantics: L2 vs. Price.** L2 uses closed-right buckets: an event at exactly `HH:00:01.000` belongs to the bucket ending at `HH:00:01`. Price uses half-open buckets: a trade at exactly `HH:00:01.000` belongs to the bucket starting at `HH:00:01`. Verify that the analysis layer (`build_aligned_analysis_dataset`) correctly aligns these two different boundary conventions when joining L2 and Price by UTC second. A one-second off-by-one would silently shift all price candles relative to L2 observations.

4. **Same-timestamp event collapsing.** When multiple events share the same `received_time_ns`, the sampler applies all of them before emitting the bucket. Verify that the analysis layer does not re-split or re-order these events. Check that `L2Second.source_count` is always 1 for single-market data (not the number of events at that timestamp).

### B. Invalid/Degraded Data Propagation Across All Three Cores

5. **Replay invalidation → sampling INVALID → aggregation hard discontinuity.** When replay invalidates state (continuity mismatch, conflicting update), the sampler emits `BookSampleQuality.INVALID` with reason `REPLAY_INVALIDATED`. The aggregation layer marks the bar as `AnalysisBarQuality.INVALID` and sets `hard_discontinuity=True`. The LM detector must not bridge this discontinuity. Trace the full chain: `OrderBookReplayState._invalidate` → `OneSecondBookSampler._append_current_sample` → `aggregate_l2_seconds` → `detect_liquidity_movements`. Verify no layer silently converts INVALID to DEGRADED or forward-fills.

6. **Locked/crossed/empty book → INVALID sample → no liquidity values.** When the book structure is LOCKED, CROSSED, or EMPTY_*, the sampler emits INVALID. The liquidity layer (`calculate_depth_liquidity`) raises `DepthLiquidityUnavailableError`. Verify that `OneSecondLiquiditySampler._append_from_state` catches this and emits an INVALID `LiquidityObservation` with null liquidity values, rather than propagating the exception.

7. **DEGRADED aggregate from partial market coverage → LM detection.** In multi-market aggregation (`aggregate_market_l2_seconds`), if one market is missing, the aggregate is `AggregateL2QualityState.DEGRADED`. This converts to `L2Second(coverage_degraded=True)`. In `aggregate_l2_seconds`, a bar where the endpoint is valid but `coverage_degraded=True` is `AnalysisBarQuality.DEGRADED`. Verify that the LM detector treats DEGRADED bars as valid for candidate detection (they have numerical liquidity values) but records the degraded count. Check that `LiquidityMovementCandidate.degraded_bar_count` is correctly incremented.

8. **Price INVALID bar → hard discontinuity → LM population split.** If a price bar is INVALID (no real trades), the analysis layer marks it as a hard discontinuity. The LM detector must not produce a candidate that spans this bar. Verify that `_build_segments` in `dataset.py` splits the core at INVALID price bars, and that `detect_liquidity_movements` only operates within a single segment.

### C. Determinism and Reproducibility (Core 6 Cross-Cutting)

9. **Replay determinism given identical source archives.** Given the same Parquet file, `replay_orderbook_archives` must produce the same `ReplayChainReport` and the same final checkpoint. Verify that no code path introduces non-determinism (e.g., `dict` iteration order, `set` iteration, `id()` usage, `time.time()`, `random`). Check `_event_signature` in `replay.py` — it uses a tuple, which is deterministic. Check `_canonical_levels` in `checkpoint_codec.py` — it sorts bids descending and asks ascending.

10. **Analysis ID determinism.** `liquidity_movement_analysis_id` is a SHA-256 over a canonical JSON payload. Verify that the payload includes all semantically relevant inputs (dataset analysis ID, config, metrics, timeframes) and excludes all presentation-only inputs (colors, visibility, zoom). Check that changing a display-only setting does not change the analysis ID.

11. **LM ranking determinism.** `rank_liquidity_movements` uses `percentile_ranks` and `positive_tail_modified_z_scores`. Verify that ties are broken deterministically. The percentile function uses midpoint ties. The ranking uses lexicographic ordering: primary evidence → secondary evidence → component evidence → candidate identity. Verify that `candidate identity` is a total order (no two candidates can have the same identity tuple).

12. **Aggregate L2 content SHA-256 determinism.** `AggregateL2Series.aggregate_content_sha256` is computed from a canonical JSON payload that includes expected markets, component preset hashes, and hourly content hashes. Verify that the market order is sorted, the hour order is sorted, and the JSON encoding uses `sort_keys=True` and `separators=(",", ":")`.

### D. Cross-Core Invariants (Core 6)

13. **UTC invariant across all layers.** Every persisted timestamp must be timezone-aware UTC. Every user-visible timestamp must be converted to the configured IANA timezone. Verify that no layer stores or compares naive datetimes. Check `require_aware_utc` and `require_utc_hour` usage in all three cores. Check that `_epoch_ns_to_utc` and `_epoch_ms_to_utc` always produce timezone-aware results.

14. **Decimal exactness across the pipeline.** Liquidity values are `Decimal` from replay through liquidity calculation through aggregation. Verify that no layer converts to `float` for computation. Check `_exact_multiply`, `_exact_add`, `_exact_subtract` in `depth.py`. Check `_exact_nonnegative_decimal_sum` in `aggregation.py`. Check that `bid_ask_imbalance` uses `localcontext` with explicit precision. The only float conversion allowed is for display (chart rendering).

15. **Source identity consistency.** `SourceFileSpec.identity_tuple` is `(provider, venue, data_kind, symbol, hour_utc)`. `ProcessingSourceArchive.spec` must match. `L2HourlyProvenance.source_hours[].instrument` must equal `spec.symbol`. `PriceHourlyProvenance.source_hours[].instrument` must equal the price symbol. Verify no layer silently normalizes or transforms the symbol (e.g., `BTCUSDT` vs `btcusdt`).

16. **Operation lock semantics.** The process-wide `operation_lock` is an `asyncio.Lock`. It must be held for the entire duration of fetch, processing, and analysis operations. Verify that the lock is not released prematurely (e.g., by an `await` inside a `with` block that yields control). Check that `ManualAnalysisRuntime._run` acquires the lock before calling `asyncio.to_thread` and releases it only after the thread completes.

17. **Cancellation propagation.** Cancellation is cooperative via `threading.Event` (for sync workers) or `asyncio.Event` (for async code). Verify that every long-running loop checks the cancellation probe. In replay: `read_orderbook_file` checks every `cancellation_check_interval_rows`. In liquidity: `_sum_side_liquidity` checks every `cancellation_check_interval_levels`. In analysis: `execute_liquidity_movement_analysis` checks between slices. Verify that cancellation does not leave partial state in the cache.

### E. Multi-Market Aggregation Integrity

18. **Component preset hash derivation.** `component_data_presets` derives single-market preset hashes from the aggregate preset. Verify that the derived hash matches the hash produced by `build_binance_futures_data_preset` / `build_okx_futures_data_preset` with the same depth band. If the derivation logic diverges from the builder, the analysis loader will look up the wrong `l2_hourly_series` rows.

19. **Aggregate second with zero contributing markets.** If no market contributes valid liquidity for a given second, the aggregate is `INVALID` with reason `NO_VALID_MARKETS`. Verify that this converts to `L2Second(quality=INVALID, invalid_reason=UNINITIALIZED)` in `_aggregate_series_to_l2_seconds`. The choice of `UNINITIALIZED` as the reason for "no markets contributed" is semantically questionable — verify it does not confuse downstream code that distinguishes `UNINITIALIZED` (replay not started) from `REPLAY_INVALIDATED` (replay failed).

20. **Aggregate imbalance with zero total liquidity.** If all contributing markets have zero bid and ask liquidity (e.g., empty depth band), the aggregate total is 0. `AggregatedL2Second.bid_ask_imbalance()` returns `None`. Verify that the analysis layer treats this as a valid observation with null imbalance, not as an invalid observation. Check `LiquidityObservation.__post_init__` — it requires `total_liquidity == bid + ask` and handles zero total by requiring null imbalance.

### F. Cache and State Management (Core 6)

21. **Analysis cache key completeness.** `LiquidityMovementAnalysisCache` is keyed by `analysis_id`. Verify that `analysis_id` changes when any semantically relevant input changes: dataset content, preset hash, timeframe, price bounds, context counts, confirmation fraction, Top-N, priorities. Check that changing `maximum_chart_bars` (which affects automatic chart timeframe selection) changes the analysis ID when it causes a different chart timeframe to be selected.

22. **Cache eviction and stale results.** The cache uses LRU eviction with `maximum_entries`. If a result is evicted and the same request is re-run, the full pipeline executes again. Verify that the re-execution produces the same result (determinism). If the underlying data has changed (e.g., a new L2 hour was processed), the dataset analysis ID will differ, and the cache will not return the stale result.

23. **Partial failure and cache pollution.** If `execute_liquidity_movement_analysis` raises after processing some slices but before completion, no result should be cached. Verify that the cache `put` is called only after the full result is constructed. Check that `LiquidityMovementAnalysisCancelledError` is raised before the cache `put`.

### G. Edge Cases at Core Boundaries

24. **Replay produces valid checkpoint but sampling produces all-INVALID hour.** This can happen if the checkpoint initializes the book but the first event in the hour immediately invalidates it. The `SampledLiquidityChain` would have `replay_report.finally_valid == True` but `hours[0].valid_count == 0`. Verify that the processing coordinator handles this correctly: the source should be `processed` with quality `INVALID`, and the L2 hourly row should still be persisted (with all-invalid validity block).

25. **Price hour with exactly one trade.** A single trade produces one valid candle and 3599 invalid candles. The quality state is `DEGRADED`. Verify that the analysis layer can build a valid dataset from this hour (the price filter should find the one valid bar). Verify that LM detection can produce candidates from a series with only one valid price bar (it should not, because there is no price movement, but verify it does not crash).

26. **Timeframe aggregation with fewer bars than the timeframe.** If the analysis range is 3 seconds and the timeframe is 5s, there is one partial bucket. Verify that `aggregate_l2_seconds` and `aggregate_price_seconds` handle this correctly. The bucket should contain only the 3 available seconds, not 5. Check that `iter_timeframe_buckets` does not produce a bucket that extends beyond the requested range.

27. **`select_chart_timeframe` with a range that fits no timeframe.** If the range is very large and `maximum_chart_bars` is very small, no timeframe fits. The function returns the coarsest timeframe. Verify that the resulting bar count is reported accurately and that the UI warns the user. Check that the analysis ID reflects the actual chart timeframe used, not the requested one.

---

## 3. Core 3 + Core 4

**Focus on Core 3 + Core 4. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **ingestion/replay/checkpoints/liquidity/price** (Core 3) and **analysis/aggregation/filtering/LM-detection/ranking** (Core 4). These two cores form the complete analytical pipeline from raw order-book replay through derived liquidity metrics to Liquidity Movement detection and ranking. Look for semantic mismatches between replay-produced data and analysis-consumed data, boundary errors, timestamp ownership, invalid/degraded data propagation, determinism, and bugs that only appear when the two cores interact.

### A. Timestamp Ownership and Boundary Semantics

1. **L2 sampling clock vs. analysis bucket alignment.** Replay uses `received_time_ns` as the authoritative sampling clock. The sampler converts this to UTC datetime via `_epoch_ns_to_utc`. The analysis layer receives `L2Second.timestamp_utc`. Verify that the conversion preserves nanosecond precision through the datetime representation. Python `datetime` has microsecond resolution. Check that `_epoch_ns_to_utc` correctly truncates nanoseconds to microseconds without rounding errors that could shift a bucket boundary.

2. **Price candle clock vs. L2 sampling clock.** Price uses `trade_time_ms` (milliseconds). L2 uses `received_time_ns` (nanoseconds). These are different clocks from different source fields. The analysis layer joins them by UTC second identity. Verify that a trade at `trade_time_ms = X` and an L2 event at `received_time_ns = X * 1_000_000` are assigned to the same UTC second. Check for off-by-one errors in the millisecond-to-second and nanosecond-to-second conversions.

3. **Bucket boundary: L2 closed-right vs. Price half-open.** L2 bucket `[HH:00:00, HH:00:01]` includes events at exactly `HH:00:01`. Price bucket `[HH:00:00, HH:00:01)` excludes trades at exactly `HH:00:01`. Verify that the analysis layer's `build_aligned_analysis_dataset` correctly handles this asymmetry. A trade at exactly the second boundary should appear in the *next* price bar but the *current* L2 bar. Check that the joined `AlignedAnalysisBar` at that second has the correct L2 state and the correct price OHLC.

4. **Hour boundary in multi-hour replay chains.** When replay spans two hours, the checkpoint from hour N initializes hour N+1. The sampler produces 3600 observations per hour. The analysis layer receives `L2Second` objects from both hours. Verify that the analysis range snapping (`snap_closed_analysis_range`) does not create a bucket that straddles the hour boundary. Check that `iter_timeframe_buckets` aligns to the snapped range, not to arbitrary hour boundaries.

### B. Invalid/Degraded Data Propagation

5. **Replay invalidation reason → sampling invalid reason → aggregation discontinuity reason.** Replay produces `ReplayIssue.kind` (e.g., `update_continuity_mismatch`). The sampler maps this to `BookSampleInvalidReason.REPLAY_INVALIDATED`. The aggregation layer maps this to `AnalysisDiscontinuityReason.L2_INVALID`. Verify that the reason chain is complete and that no invalidation reason is silently dropped or mapped to the wrong category. Check that `BookSampleInvalidReason.UNINITIALIZED` (before first snapshot) is distinct from `REPLAY_INVALIDATED` (after a continuity failure).

6. **Zero-quantity level removal → empty side → INVALID sample.** When an update removes the last ask level (quantity=0), the book becomes `EMPTY_ASK`. The sampler emits INVALID with reason `EMPTY_ASK`. The liquidity layer raises `DepthLiquidityUnavailableError`. Verify that the hourly liquidity sampler catches this and emits an INVALID `LiquidityObservation` with null liquidity, not a zero-liquidity VALID observation.

7. **DEGRADED L2 bar with valid endpoint.** If an earlier second in a 5-second bar is INVALID but the endpoint (last second) is VALID, the aggregated bar is `DEGRADED`. The LM detector should still use this bar's endpoint liquidity values for candidate detection. Verify that `detect_liquidity_movements` does not skip DEGRADED bars. Check that `LiquidityMovementCandidate.degraded_bar_count` correctly counts them.

8. **Price-excluded bar → context extension → LM candidate extent.** When a price bar is excluded by the price filter, it creates a discontinuity. Context bars (configured `context_before` / `context_after`) may extend into excluded territory. Verify that an LM candidate's `start_index` / `end_index` can include context bars but its `in_range_start_index` / `in_range_end_index` cannot. Check that the candidate's `absolute_height` is computed from the full extent (including context), not just the in-range extent.

### C. Liquidity Calculation → Analysis Consumption

9. **Depth band fraction → liquidity values → LM detection.** The depth band (`lower_fraction`, `upper_fraction`) determines which price levels contribute to liquidity. A change in depth band changes all liquidity values, which changes all LM candidates. Verify that the analysis dataset's provenance includes the preset hash (which encodes the depth band). If two analyses use different depth bands, they must have different analysis IDs.

10. **Imbalance precision and LM detection.** `bid_ask_imbalance` is computed with `decimal_precision=34` by default. The LM detector uses imbalance as one of four metrics. Verify that the precision is sufficient to distinguish small imbalance differences. Check that the ranking layer's `percentile_ranks` and `positive_tail_modified_z_scores` handle `Decimal` inputs correctly (they should, since `Decimal` supports comparison and arithmetic).

11. **Zero total liquidity → null imbalance → LM metric availability.** When total liquidity is zero, imbalance is null. The analysis layer's `liquidity_metric_value` returns `None` for `BID_ASK_IMBALANCE` when the bar's `bid_ask_imbalance()` returns `None`. Verify that the LM detector skips bars where the selected metric is `None`. Check that a candidate is not produced from a run of bars where some have null metric values.

### D. Multi-Market Aggregation → Analysis

12. **Component hour alignment in aggregate loading.** `aggregate_market_l2_seconds` requires all component series to cover the same `[start, end)` range. If one component has a missing hour (e.g., OKX data not yet processed), the aggregate for that hour has fewer contributing markets. Verify that the analysis loader (`load_verified_analysis_dataset`) handles this correctly: it should produce `L2Second` with `coverage_degraded=True` for hours where not all components are available, and `L2Second` with `quality=INVALID` for hours where no component is available.

13. **Aggregate content SHA-256 as analysis identity input.** The aggregate series has a deterministic `aggregate_content_sha256`. This is included in the dataset provenance. Verify that changing a component's content (e.g., re-processing with a different preset) changes the aggregate hash, which changes the dataset analysis ID, which changes the LM execution ID. This ensures cache invalidation works correctly.

### E. Determinism and Reproducibility

14. **Replay → sampling → liquidity → aggregation → LM: full pipeline determinism.** Given identical source archives and identical configuration, the entire pipeline must produce identical results. Verify that no layer introduces non-determinism. Check for: `dict` iteration order (Python 3.7+ is insertion-ordered, but verify no code relies on `set` iteration), `id()` usage, `hash()` usage, `time.time()`, `random`, `os.urandom`, thread scheduling effects.

15. **LM candidate ordering.** `detect_liquidity_movements` returns candidates in segment order, then by `start_index`. Verify that this ordering is deterministic. Check that the ranking layer's `rank_liquidity_movements` preserves this ordering for ties. The final ranking uses lexicographic ordering with candidate identity as the last tiebreaker. Verify that candidate identity is a total order.

### F. Edge Cases

16. **All-invalid hour → empty analysis segment.** If every second in an hour is INVALID (e.g., update-only archive with no snapshot), the analysis layer produces no valid bars. `build_aligned_analysis_dataset` should produce a dataset with zero segments. Verify that `detect_liquidity_movements` handles an empty series without crashing. Check that `rank_liquidity_movements` handles zero candidates.

17. **Single-bar series → no LM candidates.** A series with only one valid bar cannot produce an LM (an LM requires at least two bars: a start pivot and an endpoint). Verify that the detector returns an empty tuple, not a crash.

18. **Constant liquidity series → no LM candidates.** If liquidity is constant across all bars, there is no movement. The detector should return empty. Verify that the relative height calculation does not divide by zero when `scan_max == scan_min`. Check that `_scan_bounds` in `execution.py` returns `None` when the range is zero, and that the slice is empty.

19. **Price filter excludes all bars.** If the price bounds exclude every bar, there are no eligible segments. The analysis dataset has zero segments. Verify that the LM detector and ranking handle this gracefully. Check that the analysis result has zero candidates and zero selected, not an error.

20. **Timeframe larger than the analysis range.** If the activity timeframe is 30s but the analysis range is only 10s, there is one partial bucket. Verify that `aggregate_l2_seconds` produces one bar with `expected_seconds=30` but `observed_seconds=10`. Check that the bar's quality is correctly computed (it should be DEGRADED if any of the 10 observed seconds is invalid, VALID if all 10 are valid).

---

## 4. Core 1 + Core 2

**Focus on Core 1 + Core 2. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **database/persistence/identity/provenance/diagnostics/maintenance** (Core 1) and **acquisition/automatic-fetch/availability/processing-handoff/retention/lifecycle** (Core 2). These two cores manage the complete lifecycle of source data from remote discovery through download, validation, persistence, processing handoff, retention, and diagnostics. Look for bugs in state transitions, locking, idempotency, concurrency, and lifecycle management.

### A. Source-Hour Status State Machine

1. **Complete transition matrix verification.** The allowed transitions are defined in `_ALLOWED_SOURCE_TRANSITIONS`. Verify every cell:
   - `discovered → downloading, downloaded, missing, error` (allowed)
   - `downloading → downloaded, missing, invalid, quarantined, error` (allowed)
   - `downloaded → downloaded, downloading, processing, missing, invalid, quarantined, error` (allowed)
   - `processing → processing, downloaded, processed, invalid, quarantined, error` (allowed)
   - `processed → processed, error` (allowed)
   - All other transitions must raise `InvalidSourceHourTransitionError`.
   Check that `PROCESSING → DOWNLOADED` requires `allow_processing_cancellation_reset=True` and that this flag is only passed in the cancellation path of `l2_coordinator._reset_cancelled_source` and `price_coordinator._reset_cancelled_source`.

2. **`downloaded → downloading` re-download.** A downloaded source can transition back to downloading (re-fetch). Verify that this does not clear `content_sha256` or `file_size_bytes` prematurely. If the re-download produces different content, the old hash must be overwritten. If the re-download fails, the source should transition to `error` or `missing`, not remain in `downloading`.

3. **`processed → error` transition.** A processed source can transition to error. Verify that this does not delete the analytical rows (`l2_hourly_series`, `price_hourly_series`). The analytical rows are owned by preset hash and hour, not by source status. Check that `maintenance_actions` never cascade-deletes analytical data.

4. **Stale-source recovery correctness.** `_execute_stale_source_recovery` transitions `downloading → error` and `processing → downloaded` (if raw intact) or `processing → error` (if raw missing). Verify that the `downloaded` recovery path correctly clears `processed_at` and that the `error` path sets `error_text`. Check that the recovery does not touch sources in `downloaded`, `processed`, `missing`, `invalid`, or `quarantined` status.

### B. Fetch Run Lifecycle

5. **Fetch run creation → completion atomicity.** `create_fetch_run` inserts a row with `status='running'`. `complete_fetch_run` updates it to a terminal status. If the process crashes between creation and completion, the row remains `running` forever. Verify that the diagnostics layer (`stale_source_diagnostics` or `performance_diagnostics`) detects and reports stale running fetch runs. Check that there is no automatic cleanup that could corrupt a genuinely in-progress fetch.

6. **`files_downloaded` semantics in `fetch_runs`.** The DB column `files_downloaded` in `fetch_runs` means "successfully available local files" (downloaded + reused). The `details_json` field records the split. Verify that the UI and diagnostics read the correct field. Check that `ManualFetchResult.files_available` matches `files_downloaded + files_reused`, not just `files_downloaded`.

7. **Concurrent fetch run prevention.** The process-wide `operation_lock` prevents concurrent fetches. But the DB has no unique constraint on "only one running fetch run". If two processes run simultaneously (e.g., two terminals), both could create fetch runs. Verify that the advisory lock (`acquire_source_hour_transaction_lock`) prevents concurrent source-level operations, even if the fetch-run-level lock is process-local.

### C. Advisory Locking

8. **Lock scope and lifetime.** `acquire_source_hour_transaction_lock` uses `pg_try_advisory_xact_lock`, which is released at transaction end. Verify that the transaction is not committed prematurely (e.g., by an implicit `session.commit()` inside a repository method). Check that `session_scope()` in `engine.py` commits only at the end of the `with` block.

9. **Lock key collision.** `source_hour_lock_keys` uses BLAKE2b with a domain separator. Verify that two different source identities cannot produce the same key pair. The 64-bit output space (two 32-bit integers) gives 2^64 possible keys. With ~10^6 source hours, collision probability is negligible, but verify the hash is computed correctly.

10. **Lock contention during retention.** `_execute_raw_pruning` acquires advisory locks for each candidate. If a concurrent processing operation holds the lock, the pruning should fail for that candidate (not block indefinitely). Verify that `pg_try_advisory_xact_lock` returns `False` on contention and that the code handles this by adding the candidate to the `failed` list.

### D. Retention and Pruning

11. **Retention cutoff vs. processing lag.** The retention cutoff is `now - retention_hours`. If processing is slow (e.g., a large backlog), sources may be older than the cutoff but not yet processed. Verify that the retention planner only selects `processed` sources. Check that `plan_processed_raw_retention` filters by `SourceHour.status == 'processed'`.

12. **Pruning and re-download interaction.** After pruning, `source_hours.local_path` is NULL but `content_sha256` remains. If the user re-downloads the same hour, the acquisition layer should create a new file and update `local_path`, `file_size_bytes`, and `content_sha256`. Verify that the re-download does not fail because of a stale `content_sha256` mismatch. Check that `CryptoHFTDownloader._existing_artifact` handles the case where `local_path` is NULL.

13. **Retention dry-run vs. execution divergence.** Between `build_maintenance_preview` and `execute_maintenance_action`, the filesystem or DB state could change. Verify that `_execute_raw_pruning` re-validates every candidate: status, path, size, hash, retention cutoff. Check that if any re-validation fails, the candidate is added to the `failed` list and the remaining candidates are still processed.

### E. Availability Calendar and Handoff

14. **Availability state derivation from DB.** `load_hourly_availability` queries `source_hours`, `l2_hourly_series`, and `price_hourly_series`. Verify that the state derivation is correct:
   - `UNKNOWN`: no source or analytical record.
   - `MISSING`: source status is `missing` and no stronger evidence.
   - `PARTIAL`: some but not all required sources/analytical rows exist.
   - `DOWNLOADED`: both orderbook and trades sources are locally available.
   - `MATERIALIZED`: L2 and price rows exist but no valid seconds.
   - `ANALYZABLE`: L2 and price rows exist with at least one valid second.

15. **Multi-market availability.** For a multi-market preset, availability checks each component market independently. Verify that `expected_l2_market_count` matches the number of eligible markets in the preset. Check that `materialized_l2_market_count` counts only markets with an existing `l2_hourly_series` row for the correct component preset hash.

16. **Calendar handoff → Analysis controls.** `AnalysisRangeHandoff` carries `start_utc`, `end_utc` (exclusive), and `hour_count`. The Analysis tab converts this to closed user endpoints: `start_utc` and `end_utc - 1 second`. Verify that the conversion is correct and that the Analysis request's `requested_start_utc` and `requested_end_utc` are set correctly.

### F. Diagnostics and Maintenance

17. **Checkpoint reference completeness.** `collect_referenced_checkpoint_sha256s` reads from `source_hours.quality_json` (keys `input_checkpoint_content_sha256`, `output_checkpoint_content_sha256`) and `l2_hourly_series.provenance_json` (key `checkpoint_content_sha256`). Verify that all three keys are checked. If a new reference location is added in the future, this function must be updated. Check that the function raises `CheckpointReferenceError` on malformed hashes rather than silently skipping them.

18. **Orphan checkpoint detection vs. in-flight processing.** A checkpoint published by an in-flight processing operation is temporarily unreferenced (the DB transaction has not committed yet). Verify that the orphan-checkpoint diagnostics do not flag this as an orphan. Check that the diagnostics run in a separate transaction and that the in-flight processing holds an advisory lock that prevents concurrent diagnostics from seeing a partially committed state.

19. **Maintenance preview token security.** `_preview_token` is a SHA-256 over the canonical preview payload. Verify that the payload includes the action kind, generation timestamp, policy parameters, and all candidate items. If any of these change between preview and execution, the token will differ and execution will be refused. Check that the token is not predictable (it includes a timestamp, but an attacker who knows the timestamp and candidates could compute it — this is acceptable for a local application).

20. **Analytical consistency diagnostics: L2/price mismatch.** `analytical_consistency_diagnostics` compares processed source metadata with analytical rows. It checks for L2 hours without price hours and vice versa. Verify that this comparison uses `base` and `hour_utc` only, not `preset_hash`. A multi-market preset produces multiple L2 rows per hour (one per component), but only one price row. The comparison should not flag this as a mismatch.

### G. Configuration and Environment

21. **`L2SHOCK__DATABASE__PASSWORD` environment variable precedence.** The `Settings` class uses `pydantic-settings` with `env_prefix="L2SHOCK__"` and `env_nested_delimiter="__"`. Verify that the environment variable overrides `config.yaml`. Check that the password is never logged, printed, or included in error messages. Check that `DatabaseConfig.password` uses `SecretStr` and that `repr(DatabaseConfig)` does not expose the value.

22. **`config.yaml.example` vs. `config.yaml` drift.** The example file documents all configuration keys. Verify that every key in the example is consumed by the `Settings` model. Check that no key in the `Settings` model is missing from the example. If a new setting is added to the model but not the example, users will not know about it.

23. **`raw_retention_hours` validation.** The default is 72. Verify that the value is a positive integer. Check that `StorageConfig.raw_retention_hours` rejects zero, negative, and non-integer values. Verify that `raw_retention_cutoff_hour` uses this value correctly.

---

## 5. Core 4 + Core 5

**Focus on Core 4 + Core 5. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **analysis/aggregation/filtering/LM-detection/ranking** (Core 4) and **UI/charts/exports/lifecycle/user-workflows** (Core 5). These two cores connect the analytical engine to the user-facing application. Look for bugs in result rendering, chart publication, export determinism, runtime state management, and the boundary between worker-thread analysis and event-loop UI.

### A. Analysis Runtime → UI State

1. **Worker-thread result → event-loop publication.** `ManualAnalysisRuntime._run` executes analysis in a worker thread via `asyncio.to_thread`. The result is an immutable `LiquidityMovementAnalysisResult`. Verify that the result is not mutated after being returned to the event loop. Check that `ManualAnalysisResult.__post_init__` validates the result's integrity (request match, analysis ID consistency).

2. **Stale completion publication.** `_refresh_runtime_view` in `tab_analysis.py` checks `snapshot.completion_sequence != observed_completion_sequence`. If the sequence changes, it publishes the new result. Verify that a rapid stop→start cycle does not cause the old result to be published after the new one. Check that `completion_sequence` is monotonically increasing and that the UI never reads a result from a previous operation.

3. **Analysis cancellation → no partial result.** If the user clicks "Stop Analysis", the cancellation event is set. The worker thread checks this between slices. Verify that if cancellation occurs after some slices are processed but before the result is complete, no partial result is cached or published. Check that `LiquidityMovementAnalysisCancelledError` is caught in `_run` and the result is `ManualAnalysisStatus.STOPPED` with `analysis=None`.

4. **Operation lock and UI button states.** When analysis is running, the "Run Analysis" button should be disabled. When another operation (fetch, processing) is active, it should also be disabled. Verify that `_refresh_runtime_view` correctly computes `new_work_allowed` and that all controlled inputs are disabled/enabled consistently. Check that the preset selector is disabled during analysis but re-enabled after completion.

### B. Chart Publication and Render Acknowledgement

5. **ECharts `setOption(notMerge=true)` vs. NiceGUI property update.** `set_echart_options` updates both `chart._props["options"]` and calls `run_chart_method(":setOption", ...)`. Verify that both updates use the same option object. If they diverge, the browser will render one version and NiceGUI will store another. Check that the hidden render-token series is included in both.

6. **Render acknowledgement and stale publication rejection.** `confirm_echart_render_identity` polls the browser for the expected render token and category count. If a newer publication supersedes the current one before acknowledgement, the confirmation should fail. Verify that `_l2shock_render_token` is updated atomically and that the acknowledgement check compares against the current token, not a stale one.

7. **Chart publication after operation cancellation.** If analysis is cancelled after the chart option is built but before publication, the chart should not be updated. Verify that `_render_latest_chart` checks the current result's validity before publishing. Check that `chart_controller.publish` is not called with a result from a cancelled operation.

8. **Selected-LM focus series and `notMerge`.** The chart includes 5 hidden "selected focus" series (one per panel). When `notMerge=true` is used, these series are recreated on every publication. Verify that the focus series are empty (`data: []`) on initial publication and that `emphasize_analysis_chart_window` correctly updates them via `setOption` without `notMerge` (to avoid clearing the main chart data).

### C. Table → Chart Navigation

9. **Row identity → ranking identity mapping.** The table row contains `metric`, `timeframe`, `direction`, and `population_rank`. `_navigate_to_table_row` uses these to find the corresponding `RankedLiquidityMovement` in `analysis.selected`. Verify that the lookup is unique: no two selected rankings should have the same `(metric, timeframe, direction, population_rank)` tuple. If the lookup fails, the UI should display an error, not silently do nothing.

10. **Navigation window → dataZoom mapping.** `chart_navigation_window` maps a candidate's source indices to compressed chart-category indices. Verify that the mapping accounts for excluded/invalid bars that are removed from the visible category sequence. Check that `source_to_display` in `_visible_chart_bars` is correct and that the navigation window's `start_index` / `end_index` are valid category indices.

11. **Crosshair and navigation interaction.** After navigation, the custom gapped crosshair should still work. Verify that `install_analysis_gapped_crosshair` is called after navigation and that the crosshair's render-token check passes. If the navigation triggers a `setOption` call, the crosshair may need to be reinstalled.

### D. Export Determinism

12. **JSON export canonical encoding.** `analysis_export_json_bytes` uses `json.dumps` with `sort_keys=True`, `separators=(",", ":")`, and `ensure_ascii=True`. Verify that the output is byte-identical across runs. Check that no field contains a float that could have platform-dependent representation. All analytical values should be strings (canonical decimal text).

13. **PNG/SVG export and render-token verification.** `export_analysis_chart_image` verifies that the current Analysis result still owns the chart before exporting. If the user runs a new analysis between clicking "Export" and the export completing, the render token will have changed. Verify that the export fails gracefully with a clear error message, not a corrupted image.

14. **Export filename determinism.** `analysis_export_filename` uses `result.dataset.request.base` and `result.analysis_id[:16]`. Verify that the filename is filesystem-safe (no `/`, `\`, `:`, etc.). Check that the `.json` extension is always present.

### E. Analysis Controls → Request Construction

15. **Local datetime → UTC conversion.** `parse_local_analysis_datetime` converts user-input date/time to UTC using the configured timezone. Verify that ambiguous DST times are rejected (not silently resolved). Check that the conversion uses `local_to_utc` from `timeutils.py` and that the result is timezone-aware UTC.

16. **Price bounds → `PriceBounds` construction.** `build_analysis_request_and_config` constructs `PriceBounds` from the UI inputs. Verify that if both min and max are enabled and min > max, the `PriceBounds` constructor raises an error. Check that the error message is user-friendly and displayed via `persistent_notify`.

17. **Chart timeframe override → analysis identity.** If the user selects an explicit chart timeframe, the `AnalysisDatasetRequest.chart_timeframe_override` is set. This changes the analysis ID. Verify that the UI correctly passes the override value and that the analysis result reflects the overridden timeframe. Check that switching back to "Automatic" clears the override.

### F. UI State Management

18. **`RuntimeState.tracked_tasks` cleanup.** Every task created by `create_tracked_task` is added to `state.tracked_tasks`. When the task completes, it should be removed. Verify that the `_task_done` callback in `components.py` correctly removes the task. Check that cancelled tasks are also removed. If a task is not removed, `cancel_and_wait_for_tracked_tasks` during shutdown will try to cancel an already-done task.

19. **`persistent_notify` duplication.** `persistent_notify` creates a non-dismissable notification. If the same error occurs repeatedly (e.g., a polling timer), the UI could accumulate many identical notifications. Verify that the analysis tab does not call `persistent_notify` inside a timer callback. Check that `_refresh_runtime_view` uses `persistent_notify` only on state transitions (completion sequence change), not on every poll.

20. **Dark mode and chart colors.** The chart uses `AnalysisChartColors` with hardcoded hex values. Verify that these colors are visible in both light and dark mode. The application enables `ui.dark_mode()` in `index_page`. Check that the chart background (`#0f172a`) is consistent with the page background.

### G. Edge Cases

21. **Empty analysis result → chart display.** If the analysis produces zero candidates (e.g., all data is invalid), the chart should display the empty-state option. Verify that `build_analysis_chart_option` handles a result with no candidates gracefully. Check that the table displays zero rows and the summary label shows "0 candidate(s), 0 selected".

22. **Very large candidate count → table performance.** If the analysis produces thousands of candidates, the table could become slow. Verify that the table uses virtual scrolling (`virtual-scroll` prop). Check that the pagination is set to a reasonable value (25 rows per page). Verify that sorting a large table does not block the event loop.

23. **Chart with zero visible bars.** If the price filter excludes all bars, the chart has zero visible categories. Verify that `build_analysis_chart_option` returns the empty-state option. Check that `chart_navigation_window` returns `None` for any candidate. Verify that the export functions handle this gracefully.

---

## 6. Core 2 + Core 5

**Focus on Core 2 + Core 5. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **acquisition/automatic-fetch/availability/processing-handoff/retention/lifecycle** (Core 2) and **UI/charts/exports/lifecycle/user-workflows** (Core 5). These two cores manage the operational lifecycle of the application: fetching data, processing it, displaying status, and shutting down. Look for bugs in runtime state management, progress reporting, operation coordination, availability display, and shutdown sequencing.

### A. Fetch Runtime → UI Progress

1. **Progress event → UI update.** `ManualFetchCoordinator` emits `FetchProgress` events via the `progress_sink`. The sink is `ManualFetchRuntime.accept_progress`, which stores the event in `_latest_progress`. The UI polls this via `_refresh_runtime_view`. Verify that the progress fraction is correctly computed: `files_completed / files_requested`. Check that the UI does not divide by zero when `files_requested` is 0.

2. **Stop button → cooperative cancellation.** When the user clicks "Stop Fetch", `fetch_runtime.request_stop()` is called. This sets the coordinator's cancellation event. Verify that the UI correctly reflects the stop state: `stop_requested` becomes True, the stop button is disabled, and the status label shows "Stopping...". Check that after the fetch completes (or is cancelled), the UI transitions to the idle state.

3. **Fetch completion → availability calendar refresh.** After a fetch completes, the availability calendar should refresh to reflect the new data. Verify that `_refresh_runtime_view` triggers `_refresh_availability_calendar` when `fetch_snapshot.completion_sequence` changes. Check that the calendar refresh is debounced (not called on every poll tick).

### B. Automatic Fetch → UI State

4. **Automatic fetch start/stop → button states.** When automatic fetch is running, the "Start Automatic Fetch" button should be disabled and the "Stop Automatic Fetch" button should be enabled. Verify that `_refresh_runtime_view` correctly reads `automatic_snapshot.is_running` and `automatic_snapshot.enabled`. Check that the buttons are re-enabled after automatic fetch stops.

5. **Automatic fetch target hour display.** The UI shows the current target hour (`automatic_snapshot.current_target_hour_utc`). Verify that this is displayed in the user's timezone, not UTC. Check that when the target is None (all hours complete), the UI shows a meaningful message.

6. **Automatic fetch → manual operation conflict.** If automatic fetch is running and the user clicks "Manual Fetch", the manual fetch should be rejected with `FetchOperationBusyError`. Verify that the UI displays this error via `persistent_notify`. Check that the automatic fetch is not interrupted by the failed manual fetch attempt.

### C. Processing Runtime → UI Progress

7. **Processing progress → phase display.** `ProcessingRuntimeProgress` includes `coordinator_phase` (e.g., `planning`, `discovering_checkpoint`, `sampling_target`). Verify that the UI displays this phase in the status label. Check that the phase is cleared when processing completes.

8. **Processing depth fraction inputs.** The UI has two inputs for depth lower/upper fraction. Verify that `_parse_depth_band` correctly validates the inputs: lower ≥ 0, upper < 1, lower ≤ upper, both finite. Check that invalid inputs produce a user-friendly error message, not a stack trace.

9. **Processing completion → result display.** After processing completes, the UI shows the result: status, selected count, completed count, failed count. Verify that the result label is updated correctly. Check that a `no_work` result (no downloaded sources) displays a meaningful message.

### D. Availability Calendar → Analysis Handoff

10. **Calendar cell colors and states.** Each cell in the availability calendar is colored by state: UNKNOWN=grey, MISSING=red, PARTIAL=orange, DOWNLOADED=blue, MATERIALIZED=secondary, ANALYZABLE=green. Verify that the color mapping in `_STATE_COLORS` matches the `HourAvailabilityState` enum. Check that all states have a color.

11. **Contiguous window → handoff button.** The "Use latest window in Analysis" button is enabled only when a contiguous analyzable window exists. Verify that the button is disabled when `base_snapshot.handoff` is None. Check that clicking the button correctly calls `_apply_analysis_handoff` and switches to the Analysis tab.

12. **Handoff → Analysis tab state.** `_apply_analysis_handoff` sets the base, preset, start/end dates, and times in the Analysis controls. Verify that the preset selector is updated to the handoff's preset hash. Check that if the preset is no longer enabled, the handoff fails with a clear error message.

### E. Shutdown Sequencing

13. **Shutdown → fetch/processing/analysis stop.** `shutdown_runtime` stops fetch → processing → analysis → tasks → engine. Verify that each stop is cooperative (grace period) and that forced cancellation is used only if the grace period expires. Check that the shutdown sequence is idempotent (calling it twice does not double-stop).

14. **Shutdown → UI state.** After shutdown starts, all operation buttons should be disabled. Verify that `_refresh_runtime_view` checks `state.shutdown_started` and disables all controls. Check that the UI does not attempt to start new operations after shutdown has begun.

15. **Shutdown → NiceGUI server stop.** `shutdown_runtime` optionally calls `_request_nicegui_shutdown`. Verify that this stops the NiceGUI server gracefully. Check that the browser connection is closed and that the user sees a meaningful message (or the browser simply disconnects).

### F. Operation Lock and Mutual Exclusion

16. **Operation lock → UI button states.** When any operation is active (`state.active_operation_name` is non-empty), all operation-start buttons should be disabled. Verify that `_refresh_runtime_view` checks `state.active_operation_name` and disables the appropriate buttons. Check that the lock is released when the operation completes, and the buttons are re-enabled.

17. **Concurrent operation rejection.** If the user clicks "Manual Fetch" while processing is active, the fetch should be rejected. Verify that the rejection message includes the name of the active operation. Check that the rejection does not corrupt the active operation's state.

### G. Edge Cases

18. **Fetch with zero files.** If the requested range produces no source files (e.g., a future date), the fetch should complete with `files_requested=0`. Verify that the UI handles this gracefully. Check that the progress bar does not divide by zero.

19. **Availability calendar with no enabled presets.** If no data presets are enabled, the calendar should show a message. Verify that `AvailabilityCalendarBaseSnapshot` with `preset_hash=None` produces an empty hours list and a meaningful UI message.

20. **Rapid start/stop cycling.** If the user rapidly clicks "Manual Fetch" and "Stop Fetch", the runtime should handle this gracefully. Verify that the second start is rejected (operation already active) and that the stop is idempotent. Check that the UI does not enter an inconsistent state.

---

## 7. Core 1 + Core 6

**Focus on Core 1 + Core 6. Treat the rest of the repository as available dependency/context.**

You are inspecting the intersection of **database/persistence/identity/provenance/diagnostics/maintenance** (Core 1) and **cross-cutting contracts/invariants/integration** (Core 6). These two cores define the foundational invariants that bind the entire application together: identity, provenance, determinism, state machines, error classification, and configuration. Look for bugs that violate these invariants across any layer.

### A. Identity Invariants

1. **Source identity uniqueness.** `SourceFileSpec.identity_tuple` is `(provider, venue, data_kind, symbol, hour_utc)`. This must be unique across the entire system. Verify that the DB unique constraint `uq_source_hours_identity` matches this tuple. Check that no code path creates two `SourceFileSpec` objects with the same identity but different `remote_path` values.

2. **Preset hash as immutable identity.** `LiquidityDataPreset.preset_hash` is a SHA-256 over canonical JSON. Once computed, it must never change. Verify that no code path modifies the preset's fields after hash computation. Check that `DataPreset.config_json` in the DB is never updated after insertion (except for `enabled` state, which is operational, not semantic).

3. **Analysis ID as deterministic identity.** `liquidity_movement_analysis_id` is a SHA-256 over a canonical payload. Verify that the payload includes all semantically relevant inputs and excludes all presentation-only inputs. Check that changing a display-only setting (e.g., chart colors) does not change the analysis ID. Verify that changing a semantic setting (e.g., confirmation fraction) does change it.

4. **Checkpoint content-address identity.** Checkpoints are stored at a path derived from `(provider, venue, instrument, through_hour_utc, content_sha256)`. Verify that two checkpoints with the same identity but different content produce different paths (because `content_sha256` differs). Check that `CheckpointStore.publish` rejects conflicting content for the same identity.

### B. Provenance Preservation

5. **L2 provenance source-hour completeness.** Every L2 hourly row must have provenance that includes at least one source hour for the analytical hour. Verify that `L2HourlyProvenance.validate_for_hour` enforces this. Check that the processing coordinator always includes the target hour's source in the provenance, even when the target was initialized from a checkpoint.

6. **Price provenance source-hour policy.** Price provenance allows sources at offset -1, 0, +1. Verify that the processing coordinator records exactly the sources it used, not all available sources. Check that if `include_adjacent_sources=False`, only the current source is recorded.

7. **Provenance canonical encoding.** `L2HourlyProvenance.to_dict()` and `PriceHourlyProvenance.to_dict()` must produce canonical JSON. Verify that source hours are sorted by identity, datetimes use ISO-8601 with `Z` suffix, and no local filesystem paths are included. Check that the `from_dict` decoder rejects non-canonical input.

8. **Aggregate provenance component tracking.** `AggregateL2Series` records component preset hashes and hourly content hashes. Verify that these are sufficient to reproduce the aggregate. Check that changing a component's content changes the aggregate's `aggregate_content_sha256`.

### C. State Machine Completeness

9. **Source-hour status: all transitions covered.** Verify that every possible `(current_status, target_status)` pair is either explicitly allowed or explicitly rejected. Check that there are no unreachable states (e.g., a status that can be entered but never exited). Verify that the `DISCOVERED` state is only used for initial insertion and that all subsequent transitions go through `DOWNLOADING`.

10. **Fetch run status: terminal state immutability.** Once a fetch run reaches a terminal status (`ok`, `partial_ok`, `error`, `stopped`), it must not be modified. Verify that `complete_fetch_run` raises if the run is not in `running` status. Check that no code path updates a terminal fetch run.

11. **Processing cancellation reset: explicit permission.** `PROCESSING → DOWNLOADED` requires `allow_processing_cancellation_reset=True`. Verify that this flag is only passed in the cancellation paths of `l2_coordinator._reset_cancelled_source` and `price_coordinator._reset_cancelled_source`. Check that the reset clears `processed_at` and `error_text`.

### D. Error Classification and Propagation

12. **Exception hierarchy consistency.** `AcquisitionError` → `RemoteFileNotFoundError`, `DownloadIntegrityError`, etc. `ProcessingError` → `ProcessingContractError`, `ProcessingCancelledError`, etc. `ReplayContractError`, `StreamedParquetReadError`, `HourlyLiquidityError`, `TradeOHLCError`. Verify that each layer catches only its own exception types and that unexpected exceptions are wrapped, not propagated raw. Check that `maintenance_actions._safe_exception_name` returns only the type name, not the message.

13. **Secret safety in error messages.** Verify that no exception message contains API keys, database passwords, file paths with credentials, or HTTP headers. Check that `CryptoHFTDownloader` catches `httpx.HTTPError` and wraps it in `RemoteRequestError` without including the URL (which may contain `api_key`). Verify that `AcquisitionRepository` uses `bounded_diagnostic_text` with `diagnostic_secrets` to redact known secrets.

14. **Error propagation across async/sync boundary.** Analysis runs in a worker thread. If the worker raises an exception, it must be caught by the async wrapper and stored in `ManualAnalysisRuntime._last_error`. Verify that the error message is secret-safe (type name only for unexpected errors). Check that the error is displayed via `persistent_notify` and not via a raw traceback.

### E. Configuration → Identity → Hash Reproducibility

15. **Configuration changes → identity changes.** Changing `app.timezone` should not change any data identity (it affects display only). Changing `analysis.lm.confirmation_retracement_fraction` should change the analysis ID. Changing `storage.raw_retention_hours` should not change any data identity. Verify that the identity computation includes only semantically relevant configuration.

16. **Environment variable override → consistent behavior.** If `L2SHOCK__DATABASE__PORT` is set to a different value than `config.yaml`, the application should use the environment variable. Verify that the override is applied consistently across all database connections (engine, alembic, diagnostics). Check that the override does not affect the `alembic.ini` placeholder URL (which is overridden by `alembic_config()`).

17. **Default values → production behavior.** Every configuration field has a default. Verify that the defaults produce a working production configuration. Check that `app.host` defaults to `127.0.0.1` (loopback only) and that changing it to `0.0.0.0` is rejected by validation.

### F. Determinism and Reproducibility

18. **DB row ordering.** Queries that return multiple rows must use explicit `ORDER BY`. Verify that `list_presets` orders by `(base, created_at, preset_hash)`. Check that `list_l2_hours` orders by `hour_utc`. Verify that `list_price_hours` orders by `hour_utc`. If any query lacks an `ORDER BY`, the result order is non-deterministic and could cause flaky tests or inconsistent UI.

19. **JSON serialization determinism.** All JSON output must use `sort_keys=True`, `ensure_ascii=True`, `allow_nan=False`, and `separators=(",", ":")`. Verify that `report_json_bytes`, `analysis_export_json_bytes`, `LiquidityDataPreset.canonical_json_bytes`, and `L2HourlyProvenance.to_dict` all follow this convention. Check that no code path uses `json.dumps` without these options for identity-critical output.

20. **Hash computation determinism.** All SHA-256 hashes must be computed over canonical byte sequences. Verify that `LiquidityDataPreset.preset_hash` uses `canonical_json_bytes`. Check that `checkpoint_content_sha256` uses the complete encoded checkpoint bytes. Verify that `aggregate_content_sha256` uses canonical JSON. Check that no hash is computed over a Python object's `repr()` or `str()`.

### G. Cross-Module Invariant Violations

21. **Timezone invariant violation: naive datetime in DB.** If any code path inserts a naive datetime into a `DateTime(timezone=True)` column, PostgreSQL will assume the server's timezone. Verify that all datetime values are timezone-aware UTC before insertion. Check that `now_utc()` is used for all default timestamps. Verify that `require_aware_utc` is called on all datetime inputs.

22. **Decimal invariant violation: float in DB.** If any code path converts a `Decimal` to `float` before storing it in a JSONB column, precision is lost. Verify that all liquidity values in JSONB are stored as strings (canonical decimal text), not as floats. Check that `quality_summary_json` and `provenance_json` do not contain float values.

23. **Identity invariant violation: case sensitivity.** Source identities use uppercase symbols (`BTCUSDT`). Preset hashes use lowercase hex. Checkpoint identities use lowercase provider/venue and uppercase instrument. Verify that all comparisons are case-sensitive and that no code path silently normalizes case. Check that `SourceFileSpec.__post_init__` enforces uppercase symbols and lowercase venues.

24. **Uniqueness invariant violation: duplicate analytical rows.** `l2_hourly_series` has a primary key on `(base, hour_utc, preset_hash)`. Verify that `AnalyticalRepository.write_l2_hour` uses `ON CONFLICT DO NOTHING` and that a conflicting write with different content raises `HourlySeriesConflictError`. Check that no code path bypasses the repository and inserts directly.

25. **Referential integrity: preset deletion with L2 rows.** `DataPreset.preset_hash` is referenced by `L2HourlySeries.preset_hash` via a foreign key. Verify that `delete_preset_if_unreferenced` checks for referencing L2 rows before deletion. Check that the foreign key constraint is `ON DELETE NO ACTION` (not `CASCADE`). Verify that attempting to delete a referenced preset raises `PresetDeletionBlockedError`.

---

These prompts are designed to be sent verbatim alongside the corresponding core file lists from `project_cores.md`. Each prompt instructs the AI model to treat the remaining repository as available context while focusing its bug-finding effort on the specific interactions, invariants, and failure modes unique to that core combination.