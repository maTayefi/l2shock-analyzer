# l2shock/remote/headless_processing.py
"""Pure headless construction of remote processed-hour artifacts.

This module is the shared processing boundary for the future command-line
worker. It deliberately performs no:

- source download;
- Hugging Face network access;
- GitHub Actions scheduling;
- PostgreSQL access;
- source-hour status mutation;
- NiceGUI work.

It consumes explicit, already downloaded ``ProcessingSourceArchive`` objects
and calls the same replay, sampling, liquidity, price, checkpoint, and compact
encoding modules used by the local application.

There is no second L2 or price implementation here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType

from l2shock.acquisition import SourceDataKind, SourceFileSpec
from l2shock.ingest import (
    ReplayCancelledError,
    StreamedParquetReadCancelled,
    StreamedParquetReadError,
    checkpoint_encoding_info,
    decode_checkpoint,
    encode_checkpoint,
)
from l2shock.liquidity import (
    DepthLiquidityCancelledError,
    encode_hourly_liquidity_block,
    liquidity_quality_summary_to_dict,
    sample_liquidity_archives,
)
from l2shock.presets import LiquidityDataPreset
from l2shock.price import (
    TradeOHLCCancelledError,
    encode_hourly_trade_ohlc_block,
    stream_trade_ohlc_hour,
    trade_ohlc_quality_summary_to_dict,
)
from l2shock.processing import (
    ProcessingCancellationProbe,
    ProcessingCancelledError,
    ProcessingContractError,
    ProcessingSourceArchive,
    verify_processing_source_archive,
)
from l2shock.remote.artifact_codec import (
    RemoteL2ProcessedArtifact,
    RemotePriceProcessedArtifact,
    read_remote_artifact_file,
    write_remote_artifact_file,
)

import logging

log = logging.getLogger(__name__)
from l2shock.remote.contracts import (
    RemoteArtifactKey,
    RemoteArtifactKind,
    RemoteArtifactManifest,
    RemoteSourceHourReference,
)


def _positive_integer(
    field_name: str,
    value: object,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProcessingContractError(f"{field_name} must be a positive integer")

    return value


def _source_reference(
    archive: ProcessingSourceArchive,
) -> RemoteSourceHourReference:
    return RemoteSourceHourReference(
        provider=archive.spec.provider,
        venue=archive.spec.venue,
        instrument=archive.spec.symbol,
        data_kind=archive.spec.data_kind,
        hour_utc=archive.spec.hour_utc,
        content_sha256=archive.content_sha256,
    )


def _validate_l2_preset(
    target: SourceFileSpec,
    preset: LiquidityDataPreset,
) -> None:
    if not isinstance(preset, LiquidityDataPreset):
        raise TypeError("preset must be a LiquidityDataPreset")

    if target.data_kind is not SourceDataKind.ORDERBOOK:
        raise ProcessingContractError(
            "Headless L2 processing requires an orderbook target"
        )

    if preset.base != target.base:
        raise ProcessingContractError("L2 preset base does not match the target source")

    if len(preset.eligible_markets) != 1:
        raise ProcessingContractError(
            "Headless L2 processing requires exactly one component market"
        )

    market = preset.eligible_markets[0]

    if (
        market.provider != target.provider
        or market.venue != target.venue
        or market.instrument != target.symbol
    ):
        raise ProcessingContractError(
            "L2 preset market identity does not match the target source"
        )


def _checkpoint_bytes_or_none(
    value: bytes | bytearray | memoryview | None,
) -> bytes | None:
    if value is None:
        return None

    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError("input_checkpoint_bytes must be bytes-like or null")

    result = bytes(value)

    if not result:
        raise ProcessingContractError("input_checkpoint_bytes cannot be empty")

    return result


@dataclass(frozen=True, slots=True)
class HeadlessL2ProcessingOutput:
    """Complete pure result for one headlessly processed L2 source hour."""

    artifact: RemoteL2ProcessedArtifact
    quality_summary: Mapping[str, object]
    replay_event_count: int
    replay_snapshot_count: int
    replay_continuity_mismatch_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, RemoteL2ProcessedArtifact):
            raise TypeError("artifact must be RemoteL2ProcessedArtifact")

        for field_name in (
            "replay_event_count",
            "replay_snapshot_count",
            "replay_continuity_mismatch_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProcessingContractError(
                    f"{field_name} must be a non-negative integer"
                )

        object.__setattr__(
            self,
            "quality_summary",
            MappingProxyType(dict(self.quality_summary)),
        )


@dataclass(frozen=True, slots=True)
class HeadlessPriceProcessingOutput:
    """Complete pure result for one headlessly processed trade-price hour."""

    artifact: RemotePriceProcessedArtifact
    quality_summary: Mapping[str, object]
    input_trade_count: int
    accepted_trade_count: int
    exact_duplicate_trade_count: int
    outside_target_hour_trade_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.artifact, RemotePriceProcessedArtifact):
            raise TypeError("artifact must be RemotePriceProcessedArtifact")

        for field_name in (
            "input_trade_count",
            "accepted_trade_count",
            "exact_duplicate_trade_count",
            "outside_target_hour_trade_count",
        ):
            value = getattr(self, field_name)

            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProcessingContractError(
                    f"{field_name} must be a non-negative integer"
                )

        if (
            self.accepted_trade_count
            + self.exact_duplicate_trade_count
            + self.outside_target_hour_trade_count
            != self.input_trade_count
        ):
            raise ProcessingContractError(
                "Headless price trade accounting is inconsistent"
            )

        object.__setattr__(
            self,
            "quality_summary",
            MappingProxyType(dict(self.quality_summary)),
        )


def process_l2_archive_headlessly(
    target_archive: ProcessingSourceArchive,
    preset: LiquidityDataPreset,
    *,
    input_checkpoint_bytes: bytes | bytearray | memoryview | None = None,
    producer_git_commit: str | None = None,
    cancellation_probe: ProcessingCancellationProbe | None = None,
    batch_size: int = 131_072,
    cancellation_check_interval_rows: int = 4_096,
    cancellation_check_interval_levels: int = 1_024,
    imbalance_decimal_precision: int = 34,
) -> HeadlessL2ProcessingOutput:
    log.info(
        "HEADLESS L2 PROCESSING START: venue=%s symbol=%s hour=%s "
        "has_input_checkpoint=%s local_path=%s",
        target_archive.spec.venue,
        target_archive.spec.symbol,
        target_archive.spec.hour_utc.isoformat(),
        input_checkpoint_bytes is not None,
        target_archive.local_path,
    )
    """Process one explicit local order-book archive without PostgreSQL.

    The optional checkpoint must belong to the immediately preceding UTC hour
    for the same provider, venue, and instrument.

    If no checkpoint is supplied, the target archive must establish its own
    snapshot or the resulting analytical hour will remain explicitly invalid.
    """

    if not isinstance(target_archive, ProcessingSourceArchive):
        raise TypeError("target_archive must be ProcessingSourceArchive")

    target = target_archive.spec
    _validate_l2_preset(
        target,
        preset,
    )

    for field_name, value in (
        ("batch_size", batch_size),
        (
            "cancellation_check_interval_rows",
            cancellation_check_interval_rows,
        ),
        (
            "cancellation_check_interval_levels",
            cancellation_check_interval_levels,
        ),
        (
            "imbalance_decimal_precision",
            imbalance_decimal_precision,
        ),
    ):
        _positive_integer(field_name, value)

    verify_processing_source_archive(
        target_archive,
        cancellation_probe=cancellation_probe,
    )

    checkpoint_bytes = _checkpoint_bytes_or_none(input_checkpoint_bytes)

    if checkpoint_bytes is None:
        input_checkpoint = None
        input_checkpoint_sha256 = None
    else:
        input_checkpoint = decode_checkpoint(checkpoint_bytes)
        input_checkpoint.validate_for_source(target)
        input_checkpoint_sha256 = checkpoint_encoding_info(
            checkpoint_bytes
        ).content_sha256

    try:
        try:
            sampled = sample_liquidity_archives(
                ((target_archive.local_path, target_archive.spec),),
                preset.band,
                initial_checkpoint=input_checkpoint,
                batch_size=batch_size,
                final_source_content_sha256=target_archive.content_sha256,
                cancellation_probe=cancellation_probe,
                cancellation_check_interval_rows=(cancellation_check_interval_rows),
                cancellation_check_interval_levels=(cancellation_check_interval_levels),
                imbalance_decimal_precision=imbalance_decimal_precision,
            )
        except Exception as exc:
            log.error(
                "HEADLESS L2 REPLAY FAILED: venue=%s symbol=%s hour=%s "
                "file=%s error_type=%s error_msg=%s",
                target_archive.spec.venue,
                target_archive.spec.symbol,
                target_archive.spec.hour_utc.isoformat(),
                target_archive.local_path.name,
                type(exc).__name__,
                str(exc),
                exc_info=True,
            )
            raise
    except StreamedParquetReadError as exc:
        if isinstance(exc, StreamedParquetReadCancelled) or isinstance(
            exc.__cause__,
            (
                ReplayCancelledError,
                DepthLiquidityCancelledError,
                StreamedParquetReadCancelled,
            ),
        ):
            raise ProcessingCancelledError(
                "Headless L2 processing was cancelled"
            ) from exc
        raise
    except (
        ReplayCancelledError,
        DepthLiquidityCancelledError,
    ) as exc:
        raise ProcessingCancelledError("Headless L2 processing was cancelled") from exc

    # The Parquet reader reopened the pathname after initial verification.
    # Reverify after streaming and before constructing any publishable artifact.
    verify_processing_source_archive(
        target_archive,
        cancellation_probe=cancellation_probe,
    )

    if len(sampled.hours) != 1:
        raise ProcessingContractError(
            "Headless target-only L2 processing did not produce one hour"
        )

    block = sampled.hours[0]

    if block.spec != target:
        raise ProcessingContractError(
            "Headless L2 block does not belong to its target source"
        )

    encoded = encode_hourly_liquidity_block(block)
    quality_summary = liquidity_quality_summary_to_dict(block.quality_summary)

    final_checkpoint = sampled.replay_report.final_checkpoint

    if final_checkpoint is None:
        output_checkpoint_bytes = None
        output_checkpoint_sha256 = None
    else:
        output_checkpoint_bytes = encode_checkpoint(final_checkpoint)
        output_checkpoint_sha256 = checkpoint_encoding_info(
            output_checkpoint_bytes
        ).content_sha256

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.L2,
        provider=target.provider,
        venue=target.venue,
        instrument=target.symbol,
        hour_utc=target.hour_utc,
        preset_hash=preset.preset_hash,
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=(_source_reference(target_archive),),
        content_sha256=encoded.content_sha256,
        l2_preset=preset,
        input_checkpoint_content_sha256=input_checkpoint_sha256,
        output_checkpoint_content_sha256=(output_checkpoint_sha256),
        producer_git_commit=producer_git_commit,
    )
    artifact = RemoteL2ProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
        output_checkpoint=output_checkpoint_bytes,
    )

    report = sampled.replay_report.archives[0]

    log.info(
        "HEADLESS L2 PROCESSING COMPLETE: venue=%s symbol=%s hour=%s "
        "finally_valid=%s has_output_checkpoint=%s events=%s snapshots=%s "
        "continuity_mismatches=%s valid_seconds=%s invalid_seconds=%s",
        target_archive.spec.venue,
        target_archive.spec.symbol,
        target_archive.spec.hour_utc.isoformat(),
        report.finally_valid,
        artifact.output_checkpoint is not None,
        report.events_seen,
        report.reader_report.snapshot_event_count,
        report.reader_report.continuity_mismatch_count,
        sum(1 for obs in block.observations if obs.quality.value == "VALID"),
        sum(1 for obs in block.observations if obs.quality.value == "INVALID"),
    )

    return HeadlessL2ProcessingOutput(
        artifact=artifact,
        quality_summary=quality_summary,
        replay_event_count=report.events_seen,
        replay_snapshot_count=report.reader_report.snapshot_event_count,
        replay_continuity_mismatch_count=(
            report.reader_report.continuity_mismatch_count
        ),
    )


def process_price_archives_headlessly(
    target: SourceFileSpec,
    source_archives: Sequence[ProcessingSourceArchive],
    *,
    producer_git_commit: str | None = None,
    cancellation_probe: ProcessingCancellationProbe | None = None,
    batch_size: int = 131_072,
    cancellation_check_interval_rows: int = 4_096,
    cancellation_check_interval_records: int = 4_096,
) -> HeadlessPriceProcessingOutput:
    """Process one explicit Binance trade-time hour without PostgreSQL.

    ``source_archives`` may contain the immediately previous, current, and
    immediately following source archives. The current target archive is
    mandatory. Source selection is explicit and therefore cannot change merely
    because a neighboring archive later becomes available.
    """

    if not isinstance(target, SourceFileSpec):
        raise TypeError("target must be a SourceFileSpec")

    if target.data_kind is not SourceDataKind.TRADES:
        raise ProcessingContractError(
            "Headless price processing requires a trades target"
        )

    if target.venue != "binance_futures":
        raise ProcessingContractError(
            "Version 1 headless price processing requires Binance Futures"
        )

    for field_name, value in (
        ("batch_size", batch_size),
        (
            "cancellation_check_interval_rows",
            cancellation_check_interval_rows,
        ),
        (
            "cancellation_check_interval_records",
            cancellation_check_interval_records,
        ),
    ):
        _positive_integer(field_name, value)

    archives = tuple(source_archives)

    if not archives:
        raise ProcessingContractError(
            "Headless price processing requires at least one source archive"
        )

    if any(not isinstance(archive, ProcessingSourceArchive) for archive in archives):
        raise TypeError("source_archives must contain ProcessingSourceArchive objects")

    if (
        tuple(
            sorted(
                archives,
                key=lambda archive: archive.spec.hour_utc,
            )
        )
        != archives
    ):
        raise ProcessingContractError(
            "Headless price source archives must be in chronological order"
        )

    identities = [archive.spec.identity_tuple for archive in archives]

    if len(set(identities)) != len(identities):
        raise ProcessingContractError(
            "Headless price source archives contain a duplicate identity"
        )

    for archive in archives:
        spec = archive.spec

        if (
            spec.provider != target.provider
            or spec.venue != target.venue
            or spec.symbol != target.symbol
            or spec.data_kind is not SourceDataKind.TRADES
        ):
            raise ProcessingContractError(
                "Headless price source identity does not match the target"
            )

        distance = spec.hour_utc - target.hour_utc

        if distance not in {
            -timedelta(hours=1),
            timedelta(0),
            timedelta(hours=1),
        }:
            raise ProcessingContractError(
                "Headless price processing may use only immediately "
                "adjacent source archives"
            )

        verify_processing_source_archive(
            archive,
            cancellation_probe=cancellation_probe,
        )

    current_count = sum(
        archive.spec.identity_tuple == target.identity_tuple for archive in archives
    )

    if current_count != 1:
        raise ProcessingContractError(
            "Headless price processing requires exactly one current " "target archive"
        )

    try:
        streamed = stream_trade_ohlc_hour(
            tuple(
                (
                    archive.local_path,
                    archive.spec,
                )
                for archive in archives
            ),
            target_hour_utc=target.hour_utc,
            batch_size=batch_size,
            cancellation_probe=cancellation_probe,
            cancellation_check_interval_rows=(cancellation_check_interval_rows),
            cancellation_check_interval_records=(cancellation_check_interval_records),
        )
    except StreamedParquetReadError as exc:
        if isinstance(exc, StreamedParquetReadCancelled) or isinstance(
            exc.__cause__,
            (TradeOHLCCancelledError, StreamedParquetReadCancelled),
        ):
            raise ProcessingCancelledError(
                "Headless price processing was cancelled"
            ) from exc
        raise
    except (
        TradeOHLCCancelledError,
        StreamedParquetReadCancelled,
    ) as exc:
        raise ProcessingCancelledError(
            "Headless price processing was cancelled"
        ) from exc

    # Every selected archive may contribute trades to the target hour.
    # Reverify all of them after streaming and before creating the remote
    # analytical artifact and provenance manifest.
    for archive in archives:
        verify_processing_source_archive(
            archive,
            cancellation_probe=cancellation_probe,
        )

    block = streamed.block

    if (
        block.base != target.base
        or block.symbol != target.symbol
        or block.source_venue != target.venue
        or block.hour_utc != target.hour_utc
    ):
        raise ProcessingContractError(
            "Headless price block identity does not match its target"
        )

    encoded = encode_hourly_trade_ohlc_block(block)
    quality_summary = trade_ohlc_quality_summary_to_dict(block.quality_summary)

    key = RemoteArtifactKey(
        kind=RemoteArtifactKind.PRICE,
        provider=target.provider,
        venue=target.venue,
        instrument=target.symbol,
        hour_utc=target.hour_utc,
    )
    manifest = RemoteArtifactManifest(
        key=key,
        source_hours=tuple(_source_reference(archive) for archive in archives),
        content_sha256=encoded.content_sha256,
        producer_git_commit=producer_git_commit,
    )
    artifact = RemotePriceProcessedArtifact(
        manifest=manifest,
        encoded=encoded,
    )

    return HeadlessPriceProcessingOutput(
        artifact=artifact,
        quality_summary=quality_summary,
        input_trade_count=block.input_trade_count,
        accepted_trade_count=block.accepted_trade_count,
        exact_duplicate_trade_count=(block.exact_duplicate_trade_count),
        outside_target_hour_trade_count=(block.outside_target_hour_trade_count),
    )


__all__ = [
    "HeadlessL2ProcessingOutput",
    "HeadlessPriceProcessingOutput",
    "process_l2_archive_headlessly",
    "process_price_archives_headlessly",
]
