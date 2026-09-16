#!/usr/bin/env python3
"""
Deep local inspection of Bybit and Bitget orderbook archives.
Reads directly from cache_multi_venue/ — no network access needed.
"""

from __future__ import annotations
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

CACHE = Path("cache_multi_venue")
OUTPUT = "bybit_bitget_deep_local_inspection.json"

FILES = {
    "bybit_BTC_ob_12": CACHE / "bybit_2026-09-09_12_BTCUSDT_orderbook.parquet",
    "bybit_BTC_ob_13": CACHE / "bybit_2026-09-09_13_BTCUSDT_orderbook.parquet",
    "bybit_ETH_ob_12": CACHE / "bybit_2026-09-09_12_ETHUSDT_orderbook.parquet",
    "bybit_ETH_ob_13": CACHE / "bybit_2026-09-09_13_ETHUSDT_orderbook.parquet",
    "bitget_BTC_ob_12": CACHE
    / "bitget_futures_2026-09-09_12_BTCUSDT_orderbook.parquet",
    "bitget_BTC_ob_13": CACHE
    / "bitget_futures_2026-09-09_13_BTCUSDT_orderbook.parquet",
    "bitget_ETH_ob_12": CACHE
    / "bitget_futures_2026-09-09_12_ETHUSDT_orderbook.parquet",
    "bitget_ETH_ob_13": CACHE
    / "bitget_futures_2026-09-09_13_ETHUSDT_orderbook.parquet",
}


def inspect_file(path: Path, label: str) -> dict:
    if not path.exists():
        return {"error": f"File not found: {path}"}

    pf = pq.ParquetFile(path)
    total_rows = pf.metadata.num_rows
    schema_names = list(pf.schema_arrow.names)

    # Stream all rows
    event_types = Counter()
    field_null = {name: 0 for name in schema_names}
    events = []
    current_event_key = None
    current_event_rows = []
    row_number = 0

    # Sequence tracking
    final_ids = []
    last_ids = []
    first_ids = []
    prev_final_ids = []
    transaction_times = []
    received_times = []
    event_times = []

    # Snapshot detection
    snapshot_count = 0
    snapshot_rows_sample = []

    # Quantity/price samples
    quantity_samples = []
    price_samples = []

    for batch in pf.iter_batches(batch_size=65536, use_threads=True):
        for row in batch.to_pylist():
            row_number += 1

            # Null tracking
            for name in schema_names:
                if row.get(name) is None:
                    field_null[name] += 1

            et = row.get("event_type", "")
            event_types[et] += 1

            # Collect IDs
            fid = row.get("final_update_id")
            lid = row.get("last_update_id")
            fiuid = row.get("first_update_id")
            pfuid = row.get("prev_final_update_id")
            tt = row.get("transaction_time")
            rt = row.get("received_time")
            evt = row.get("event_time")

            if fid is not None:
                final_ids.append(fid)
            if lid is not None:
                last_ids.append(lid)
            if fiuid is not None:
                first_ids.append(fiuid)
            if pfuid is not None:
                prev_final_ids.append(pfuid)
            if tt is not None:
                transaction_times.append(tt)
            if rt is not None:
                received_times.append(rt)
            if evt is not None:
                event_times.append(evt)

            if et == "snapshot":
                snapshot_count += 1
                if len(snapshot_rows_sample) < 10:
                    snapshot_rows_sample.append(dict(row))

            # Quantity/price samples
            q = row.get("quantity")
            p = row.get("price")
            if q is not None and len(quantity_samples) < 30:
                quantity_samples.append(str(q))
            if p is not None and len(price_samples) < 30:
                price_samples.append(str(p))

            # Event grouping
            event_key = (
                row.get("symbol"),
                et,
                rt,
                evt,
                tt,
                fiuid,
                fid,
                pfuid,
                lid,
            )
            if event_key != current_event_key:
                if current_event_rows:
                    events.append(
                        {
                            "key_summary": {
                                "event_type": current_event_key[1],
                                "received_time": current_event_key[2],
                                "event_time": current_event_key[3],
                                "transaction_time": current_event_key[4],
                                "first_update_id": current_event_key[5],
                                "final_update_id": current_event_key[6],
                                "prev_final_update_id": current_event_key[7],
                                "last_update_id": current_event_key[8],
                            },
                            "row_count": len(current_event_rows),
                            "first_row": (
                                current_event_rows[0] if current_event_rows else None
                            ),
                        }
                    )
                current_event_key = event_key
                current_event_rows = [dict(row)]
            else:
                current_event_rows.append(dict(row))

    # Final event
    if current_event_rows:
        events.append(
            {
                "key_summary": {
                    "event_type": current_event_key[1],
                    "received_time": current_event_key[2],
                    "event_time": current_event_key[3],
                    "transaction_time": current_event_key[4],
                    "first_update_id": current_event_key[5],
                    "final_update_id": current_event_key[6],
                    "prev_final_update_id": current_event_key[7],
                    "last_update_id": current_event_key[8],
                },
                "row_count": len(current_event_rows),
                "first_row": current_event_rows[0] if current_event_rows else None,
            }
        )

    # Sequence analysis
    final_id_increments_by_one = True
    non_one_increments = 0
    for i in range(1, min(len(final_ids), 200000)):
        diff = final_ids[i] - final_ids[i - 1]
        if diff != 1 and diff != 0:
            final_id_increments_by_one = False
            non_one_increments += 1
            if non_one_increments <= 5:
                pass  # just count

    # received_time monotonicity
    received_regressions = 0
    for i in range(1, min(len(received_times), 200000)):
        if received_times[i] < received_times[i - 1]:
            received_regressions += 1

    # Cross-hour boundary info
    first_event = events[0] if events else None
    last_event = events[-1] if events else None

    return {
        "label": label,
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "total_rows": total_rows,
        "row_group_count": pf.metadata.num_row_groups,
        "schema_columns": schema_names,
        "event_type_counts": dict(sorted(event_types.items())),
        "event_count": len(events),
        "field_null_counts": {k: v for k, v in sorted(field_null.items()) if v > 0},
        "field_null_percentages": {
            k: round(v / total_rows * 100, 2)
            for k, v in sorted(field_null.items())
            if v > 0
        },
        "has_snapshot": snapshot_count > 0,
        "snapshot_row_count": snapshot_count,
        "snapshot_rows_sample": snapshot_rows_sample[:5],
        "sequence_analysis": {
            "final_update_id_count": len(final_ids),
            "final_update_id_first": final_ids[0] if final_ids else None,
            "final_update_id_last": final_ids[-1] if final_ids else None,
            "final_update_id_increments_by_one": final_id_increments_by_one,
            "final_update_id_non_one_increment_count": non_one_increments,
            "last_update_id_count": len(last_ids),
            "last_update_id_first": last_ids[0] if last_ids else None,
            "last_update_id_last": last_ids[-1] if last_ids else None,
            "first_update_id_count": len(first_ids),
            "prev_final_update_id_count": len(prev_final_ids),
            "transaction_time_count": len(transaction_times),
            "transaction_time_first": (
                transaction_times[0] if transaction_times else None
            ),
            "transaction_time_last": (
                transaction_times[-1] if transaction_times else None
            ),
            "received_time_regression_count": received_regressions,
        },
        "first_event": first_event,
        "last_event": last_event,
        "quantity_samples": quantity_samples[:20],
        "price_samples": price_samples[:20],
    }


def cross_hour_analysis(results: dict) -> list[dict]:
    """Analyze cross-hour continuity between hour 12 and hour 13."""
    analyses = []
    venues = [
        ("bybit_BTC", "bybit_BTC_ob_12", "bybit_BTC_ob_13"),
        ("bybit_ETH", "bybit_ETH_ob_12", "bybit_ETH_ob_13"),
        ("bitget_BTC", "bitget_BTC_ob_12", "bitget_BTC_ob_13"),
        ("bitget_ETH", "bitget_ETH_ob_12", "bitget_ETH_ob_13"),
    ]
    for name, h12_key, h13_key in venues:
        h12 = results.get(h12_key, {})
        h13 = results.get(h13_key, {})
        if "error" in h12 or "error" in h13:
            continue

        seq12 = h12.get("sequence_analysis", {})
        seq13 = h13.get("sequence_analysis", {})

        last_final_12 = seq12.get("final_update_id_last")
        first_final_13 = seq13.get("final_update_id_first")
        last_last_12 = seq12.get("last_update_id_last")
        first_last_13 = seq13.get("last_update_id_first")

        analysis = {
            "venue": name,
            "hour_12_last_final_update_id": last_final_12,
            "hour_13_first_final_update_id": first_final_13,
            "hour_12_last_update_id": last_last_12,
            "hour_13_first_update_id": first_last_13,
            "hour_13_starts_with_snapshot": h13.get("has_snapshot", False),
            "hour_12_event_count": h12.get("event_count"),
            "hour_13_event_count": h13.get("event_count"),
        }

        if last_final_12 is not None and first_final_13 is not None:
            analysis["final_id_gap"] = first_final_13 - last_final_12
            analysis["final_id_continuous"] = first_final_13 == last_final_12 + 1

        if last_last_12 is not None and first_last_13 is not None:
            analysis["last_id_gap"] = first_last_13 - last_last_12

        analyses.append(analysis)

    return analyses


def main():
    print("=== Deep Local Inspection: Bybit & Bitget ===\n")

    results = {}
    for label, path in FILES.items():
        print(f"Inspecting: {label} ...")
        results[label] = inspect_file(path, label)
        r = results[label]
        if "error" not in r:
            print(
                f"  rows={r['total_rows']}, events={r['event_count']}, "
                f"snapshot={'YES' if r['has_snapshot'] else 'NO'}, "
                f"final_id+1={'YES' if r['sequence_analysis']['final_update_id_increments_by_one'] else 'NO'}"
            )
        else:
            print(f"  ERROR: {r['error']}")

    # Cross-hour analysis
    print("\n=== Cross-Hour Continuity ===")
    cross = cross_hour_analysis(results)
    for c in cross:
        print(
            f"  {c['venue']}: "
            f"final_gap={c.get('final_id_gap', 'N/A')}, "
            f"continuous={c.get('final_id_continuous', 'N/A')}, "
            f"snapshot_h13={c.get('hour_13_starts_with_snapshot', False)}"
        )

    # Summary
    print("\n=== SUMMARY FOR GPT ===")
    for venue_prefix in ["bybit", "bitget"]:
        print(f"\n--- {venue_prefix.upper()} ---")
        for label, r in results.items():
            if not label.startswith(venue_prefix):
                continue
            if "error" in r:
                continue
            seq = r["sequence_analysis"]
            print(f"  {label}:")
            print(f"    has_snapshot: {r['has_snapshot']}")
            print(f"    event_types: {r['event_type_counts']}")
            print(
                f"    transaction_time present: {seq['transaction_time_count']} / {r['total_rows']}"
            )
            print(
                f"    first_update_id present: {seq['first_update_id_count']} / {r['total_rows']}"
            )
            print(
                f"    prev_final_update_id present: {seq['prev_final_update_id_count']} / {r['total_rows']}"
            )
            print(
                f"    last_update_id present: {seq['last_update_id_count']} / {r['total_rows']}"
            )
            print(
                f"    final_update_id range: {seq['final_update_id_first']} -> {seq['final_update_id_last']}"
            )
            print(
                f"    final_update_id increments by 1: {seq['final_update_id_increments_by_one']}"
            )
            print(
                f"    non-one increments: {seq['final_update_id_non_one_increment_count']}"
            )
            print(
                f"    received_time regressions: {seq['received_time_regression_count']}"
            )
            if r["quantity_samples"]:
                print(f"    quantity samples: {r['quantity_samples'][:5]}")
            if r["price_samples"]:
                print(f"    price samples: {r['price_samples'][:5]}")

    # Write full JSON report
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "local cache_multi_venue files",
        "files_inspected": {k: str(v) for k, v in FILES.items()},
        "results": results,
        "cross_hour_analysis": cross,
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\nFull report written to: {OUTPUT}")
    print(f"Send this file to GPT for Bybit/Bitget adapter design.")


if __name__ == "__main__":
    main()
