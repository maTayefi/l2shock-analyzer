# l2shock/processing/target_planning.py
"""Materialization-aware source target planning.

Normal processing has two responsibilities:

1. first-time processing of durable ``downloaded`` source archives;
2. materialization of a newly requested depth configuration from an existing
   ``processed`` raw order-book archive when that exact component preset row
   does not yet exist.

Processed Binance trade sources are selected only when their fixed price-hour
row is absent.

Already materialized identical outputs are skipped. A processed source whose
raw file was explicitly pruned is not selected because it has no local archive
to replay. Raw rehydration remains a separate operation.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import select

from l2shock.acquisition.models import (
    SourceDataKind,
    SourceFileSpec,
)
from l2shock.acquisition.persistence import SourceHourStatus
from l2shock.acquisition.planning import (
    plan_production_source_files,
)
from l2shock.db.engine import session_scope
from l2shock.db.models import (
    L2HourlySeries,
    PriceHourlySeries,
    SourceHour,
)
from l2shock.presets import (
    LiquidityDataPreset,
    build_binance_futures_data_preset,
    build_bybit_data_preset,
    build_okx_futures_data_preset,
)
from l2shock.timeutils import require_aware_utc


def _canonical_sha256_or_none(
    value: object,
) -> str | None:
    digest = str(value or "").strip()

    if (
        digest != digest.lower()
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        return None

    return digest


def _source_identity_tuple_from_row(
    row: SourceHour,
) -> tuple[str, str, str, str, datetime]:
    return (
        str(row.provider),
        str(row.venue),
        str(row.data_kind),
        str(row.instrument),
        require_aware_utc(
            "source hour_utc",
            row.hour_utc,
        ),
    )


def _processing_order_key(
    spec: SourceFileSpec,
) -> tuple[int, str, str, datetime]:
    """Return dependency-friendly deterministic processing order.

    Order books are processed first, grouped by venue/instrument and then by
    chronological hour. This maximizes checkpoint reuse independently for each
    supported market. Binance trade archives are processed afterward.
    """

    kind_order = 0 if spec.data_kind is SourceDataKind.ORDERBOOK else 1

    return (
        kind_order,
        spec.venue,
        spec.symbol,
        spec.hour_utc,
    )


def _single_market_preset_for_target(
    target: SourceFileSpec,
    *,
    lower_depth_fraction: Decimal,
    upper_depth_fraction: Decimal,
) -> LiquidityDataPreset:
    """Build the exact component preset owned by an order-book target."""

    if not isinstance(target, SourceFileSpec):
        raise TypeError("target must be a SourceFileSpec")

    if target.data_kind is not SourceDataKind.ORDERBOOK:
        raise ValueError(
            "A single-market liquidity preset requires an order-book target"
        )

    if not isinstance(lower_depth_fraction, Decimal):
        raise TypeError("lower_depth_fraction must be Decimal")

    if not isinstance(upper_depth_fraction, Decimal):
        raise TypeError("upper_depth_fraction must be Decimal")

    builders = {
        "binance_futures": build_binance_futures_data_preset,
        "bybit": build_bybit_data_preset,
        "okx_futures": build_okx_futures_data_preset,
    }

    try:
        builder = builders[target.venue]
    except KeyError as exc:
        raise ValueError(
            "No approved single-market preset builder exists for "
            f"venue {target.venue!r}"
        ) from exc

    preset = builder(
        base=target.base,
        lower_fraction=lower_depth_fraction,
        upper_fraction=upper_depth_fraction,
    )

    market = preset.eligible_markets[0]

    if (
        market.provider != target.provider
        or market.venue != target.venue
        or market.instrument != target.symbol
    ):
        raise RuntimeError(
            "Generated component preset does not match its processing target"
        )

    return preset


def _has_replayable_local_metadata(
    row: SourceHour,
) -> bool:
    """Return whether durable metadata claims a replayable local archive.

    Complete filesystem size and SHA-256 verification remains the processing
    coordinator's responsibility immediately before replay.
    """

    local_path = str(row.local_path or "").strip()

    if not local_path:
        return False

    size = row.file_size_bytes

    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        return False

    return _canonical_sha256_or_none(row.content_sha256) is not None


def _should_process_target(
    target: SourceFileSpec,
    *,
    source_status: SourceHourStatus | str,
    has_replayable_local_archive: bool,
    l2_materialized: bool,
    price_materialized: bool,
) -> bool:
    """Apply the materialization-aware target-selection policy."""

    if not isinstance(target, SourceFileSpec):
        raise TypeError("target must be a SourceFileSpec")

    try:
        status = SourceHourStatus(source_status)
    except (TypeError, ValueError) as exc:
        raise ValueError("source_status is unsupported") from exc

    if not isinstance(
        has_replayable_local_archive,
        bool,
    ):
        raise TypeError("has_replayable_local_archive must be bool")

    if not isinstance(l2_materialized, bool):
        raise TypeError("l2_materialized must be bool")

    if not isinstance(price_materialized, bool):
        raise TypeError("price_materialized must be bool")

    if not has_replayable_local_archive:
        return False

    if status is SourceHourStatus.DOWNLOADED:
        # A downloaded row still needs terminal processing/finalization, even
        # if an analytical artifact happens to exist after an earlier partial
        # or recovered operation.
        return True

    if status is not SourceHourStatus.PROCESSED:
        return False

    if target.data_kind is SourceDataKind.ORDERBOOK:
        return not l2_materialized

    if target.data_kind is SourceDataKind.TRADES:
        return not price_materialized

    return False


def load_materialization_processing_targets(
    start_utc: datetime,
    end_utc: datetime,
    lower_depth_fraction: Decimal,
    upper_depth_fraction: Decimal,
) -> tuple[SourceFileSpec, ...]:
    """Return source targets requiring first processing or materialization.

    The requested UTC range uses half-open ``[start_utc, end_utc)`` semantics.

    Order-book output identity depends on the supplied exact depth fractions.
    A processed order-book source is selected only when its exact component
    single-market preset row is absent.

    Binance trade-price output has one fixed identity per base/hour. A
    processed trade source is selected only when that price-hour row is absent.
    """

    start = require_aware_utc(
        "start_utc",
        start_utc,
    )
    end = require_aware_utc(
        "end_utc",
        end_utc,
    )

    if end <= start:
        raise ValueError("end_utc must be after start_utc")

    if not isinstance(lower_depth_fraction, Decimal):
        raise TypeError("lower_depth_fraction must be Decimal")

    if not isinstance(upper_depth_fraction, Decimal):
        raise TypeError("upper_depth_fraction must be Decimal")

    planned = plan_production_source_files(
        start,
        end,
    )

    if not planned:
        return ()

    preset_by_target_identity: dict[
        tuple[str, str, str, str, datetime],
        LiquidityDataPreset,
    ] = {}

    for target in planned:
        if target.data_kind is not SourceDataKind.ORDERBOOK:
            continue

        preset_by_target_identity[target.identity_tuple] = (
            _single_market_preset_for_target(
                target,
                lower_depth_fraction=lower_depth_fraction,
                upper_depth_fraction=upper_depth_fraction,
            )
        )

    component_hashes = tuple(
        sorted({preset.preset_hash for preset in preset_by_target_identity.values()})
    )

    first_hour = min(target.hour_utc for target in planned)
    final_exclusive = max(target.hour_utc for target in planned) + timedelta(hours=1)

    expected_venues = tuple(sorted({target.venue for target in planned}))
    expected_instruments = tuple(sorted({target.symbol for target in planned}))
    planned_identities = {target.identity_tuple for target in planned}

    with session_scope() as session:
        source_rows = session.scalars(
            select(SourceHour).where(
                SourceHour.provider == "cryptohftdata",
                SourceHour.venue.in_(expected_venues),
                SourceHour.instrument.in_(expected_instruments),
                SourceHour.data_kind.in_(
                    (
                        SourceDataKind.ORDERBOOK.value,
                        SourceDataKind.TRADES.value,
                    )
                ),
                SourceHour.hour_utc >= first_hour,
                SourceHour.hour_utc < final_exclusive,
                SourceHour.status.in_(
                    (
                        SourceHourStatus.DOWNLOADED.value,
                        SourceHourStatus.PROCESSED.value,
                    )
                ),
            )
        ).all()

        source_by_identity = {
            _source_identity_tuple_from_row(row): row
            for row in source_rows
            if _source_identity_tuple_from_row(row) in planned_identities
        }

        if component_hashes:
            l2_rows = session.execute(
                select(
                    L2HourlySeries.base,
                    L2HourlySeries.hour_utc,
                    L2HourlySeries.preset_hash,
                ).where(
                    L2HourlySeries.preset_hash.in_(component_hashes),
                    L2HourlySeries.hour_utc >= first_hour,
                    L2HourlySeries.hour_utc < final_exclusive,
                )
            ).all()
        else:
            l2_rows = ()

        price_rows = session.execute(
            select(
                PriceHourlySeries.base,
                PriceHourlySeries.hour_utc,
            ).where(
                PriceHourlySeries.hour_utc >= first_hour,
                PriceHourlySeries.hour_utc < final_exclusive,
            )
        ).all()

    materialized_l2 = {
        (
            str(base),
            require_aware_utc(
                "L2 hour_utc",
                hour_utc,
            ),
            str(preset_hash),
        )
        for base, hour_utc, preset_hash in l2_rows
    }

    materialized_price = {
        (
            str(base),
            require_aware_utc(
                "price hour_utc",
                hour_utc,
            ),
        )
        for base, hour_utc in price_rows
    }

    selected: list[SourceFileSpec] = []

    for target in planned:
        source_row = source_by_identity.get(target.identity_tuple)

        if source_row is None:
            continue

        if target.data_kind is SourceDataKind.ORDERBOOK:
            preset = preset_by_target_identity[target.identity_tuple]
            l2_exists = (
                target.base,
                target.hour_utc,
                preset.preset_hash,
            ) in materialized_l2
            price_exists = False
        else:
            l2_exists = False
            price_exists = (
                target.base,
                target.hour_utc,
            ) in materialized_price

        if _should_process_target(
            target,
            source_status=source_row.status,
            has_replayable_local_archive=(_has_replayable_local_metadata(source_row)),
            l2_materialized=l2_exists,
            price_materialized=price_exists,
        ):
            selected.append(target)

    selected.sort(key=_processing_order_key)

    return tuple(selected)


def load_downloaded_processing_targets(
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[SourceFileSpec, ...]:
    """Return only durable downloaded targets.

    This compatibility boundary retains the original first-processing query.
    New application processing uses
    :func:`load_materialization_processing_targets`.
    """

    start = require_aware_utc(
        "start_utc",
        start_utc,
    )
    end = require_aware_utc(
        "end_utc",
        end_utc,
    )

    if end <= start:
        raise ValueError("end_utc must be after start_utc")

    planned = plan_production_source_files(
        start,
        end,
    )

    if not planned:
        return ()

    first_hour = min(target.hour_utc for target in planned)

    final_exclusive = max(target.hour_utc for target in planned) + timedelta(hours=1)
    planned_identities = {target.identity_tuple for target in planned}

    with session_scope() as session:
        rows = session.scalars(
            select(SourceHour).where(
                SourceHour.provider == "cryptohftdata",
                SourceHour.hour_utc >= first_hour,
                SourceHour.hour_utc < final_exclusive,
                SourceHour.status == SourceHourStatus.DOWNLOADED.value,
            )
        ).all()

    available = {
        _source_identity_tuple_from_row(row)
        for row in rows
        if (
            _source_identity_tuple_from_row(row) in planned_identities
            and _has_replayable_local_metadata(row)
        )
    }

    selected = [target for target in planned if target.identity_tuple in available]
    selected.sort(key=_processing_order_key)

    return tuple(selected)


__all__ = [
    "load_downloaded_processing_targets",
    "load_materialization_processing_targets",
]
