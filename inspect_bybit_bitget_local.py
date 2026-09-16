#!/usr/bin/env python3
"""
Deep venue contract inspection from LOCAL cached files.
No network access required — uses files already downloaded by inspect_multi_venue.py.
"""

import json
import sys
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq

CACHE_DIR = Path("cache_multi_venue")
OUTPUT = "venue_contract_deep_inspection_local.json"

# Map of what inspect_multi_venue.py saved
LOCAL_FILES = {
    "bybit_BTC": {
        "ob_h12": CACHE_DIR / "bybit_BTC_ob_h12.parquet",
        "ob_h13": CACHE_DIR / "bybit_BTC_ob_h13.parquet",
        "tr_h13": CACHE_DIR / "bybit_BTC_trades_h13.parquet",
    },
    "bybit_ETH": {
        "ob_h12": CACHE_DIR / "bybit_ETH_ob_h12.parquet",
        "ob_h13": CACHE_DIR / "bybit_ETH_ob_h13.parquet",
        "tr_h13": CACHE_DIR / "bybit_ETH_trades_h13.parquet",
    },
    "bitget_BTC": {
        "ob_h12": CACHE_DIR / "bitget_BTC_ob_h12.parquet",
        "ob_h13": CACHE_DIR / "bitget_BTC_ob_h13.parquet",
        "tr_h13": CACHE_DIR / "bitget_BTC_trades_h13.parquet",
    },
    "bitget_ETH": {
        "ob_h12": CACHE_DIR / "bitget_ETH_ob_h12.parquet",
        "ob_h13": CACHE_DIR / "bitget_ETH_ob_h13.parquet",
        "tr_h13": CACHE_DIR / "bitget_ETH_trades_h13.parquet",
    },
}


def inspect_file(path: Path) -> dict:
    """Deep inspection of one local Parquet file."""
    if not path.exists():
        return {"error": f"File not found: {path}", "path": str(path)}

    pf = pq.ParquetFile(path)
    schema_names = list(pf.schema_arrow.names)
    total_rows = int(pf.metadata.num_rows)

    # Stream all rows for deep analysis
    event_types = Counter()
    field_null_counts = {name: 0 for name in schema_names}
    first_rows = []
    last_rows = []
    rows_read = 0

    # Track update IDs for continuity analysis
    final_update_ids = []
    last_update_ids = []
    first_update_ids = []
    prev_final_update_ids = []
    transaction_times = []
    received_times = []

    # Track snapshots
    snapshot_count = 0
    snapshot_rows_sample = []

    for batch in pf.iter_batches(batch_size=65536, use_threads=True):
        for row in batch.to_pylist():
            rows_read += 1

            # Null tracking
            for name in schema_names:
                if row.get(name) is None:
                    field_null_counts[name] += 1

            et = row.get("event_type")
            if et:
                event_types[et] += 1

            if et == "snapshot":
                snapshot_count += 1
                if len(snapshot_rows_sample) < 5:
                    snapshot_rows_sample.append(dict(row))

            # Collect IDs for continuity analysis
            if row.get("final_update_id") is not None:
                final_update_ids.append(row["final_update_id"])
            if row.get("last_update_id") is not None:
                last_update_ids.append(row["last_update_id"])
            if row.get("first_update_id") is not None:
                first_update_ids.append(row["first_update_id"])
            if row.get("prev_final_update_id") is not None:
                prev_final_update_ids.append(row["prev_final_update_id"])
            if row.get("transaction_time") is not None:
                transaction_times.append(row["transaction_time"])
            if row.get("received_time") is not None:
                received_times.append(row["received_time"])

            if len(first_rows) < 5:
                first_rows.append(dict(row))
            if rows_read > total_rows - 5:
                last_rows.append(dict(row))

    # Continuity analysis for final_update_id
    final_id_gaps = 0
    final_id_increments_by_one = True
    for i in range(1, min(len(final_update_ids), 100000)):
        diff = final_update_ids[i] - final_update_ids[i - 1]
        if diff != 1 and diff != 0:  # allow same-event duplicates
            final_id_gaps += 1
            if diff > 1:
                final_id_increments_by_one = False

    # received_time monotonicity
    received_regressions = 0
    for i in range(1, min(len(received_times), 100000)):
        if received_times[i] < received_times[i - 1]:
            received_regressions += 1

    # Quantity samples
    quantity_samples = []
    price_samples = []
    for row in first_rows[:10]:
        if row.get("quantity") is not None:
            quantity_samples.append(str(row["quantity"]))
        if row.get("price") is not None:
            price_samples.append(str(row["price"]))

    return {
        "path": str(path),
        "file_size_bytes": path.stat().st_size,
        "total_rows": total_rows,
        "row_group_count": int(pf.metadata.num_row_groups),
        "schema_columns": schema_names,
        "event_type_counts": dict(sorted(event_types.items())),
        "field_null_counts": {
            k: v for k, v in sorted(field_null_counts.items()) if v > 0
        },
        "field_null_percentages": {
            k: round(v / total_rows * 100, 2)
            for k, v in sorted(field_null_counts.items())
            if v > 0
        },
        "has_snapshot": snapshot_count > 0,
        "snapshot_event_count": snapshot_count,
        "snapshot_sample_rows": snapshot_rows_sample[:3],
        "first_rows": first_rows[:3],
        "last_rows": last_rows[:3],
        "id_analysis": {
            "final_update_id_count": len(final_update_ids),
            "final_update_id_first": final_update_ids[0] if final_update_ids else None,
            "final_update_id_last": final_update_ids[-1] if final_update_ids else None,
            "final_update_id_increments_by_one": final_id_increments_by_one,
            "final_update_id_gap_count": final_id_gaps,
            "last_update_id_count": len(last_update_ids),
            "last_update_id_first": last_update_ids[0] if last_update_ids else None,
            "last_update_id_last": last_update_ids[-1] if last_update_ids else None,
            "first_update_id_count": len(first_update_ids),
            "prev_final_update_id_count": len(prev_final_update_ids),
            "transaction_time_count": len(transaction_times),
        },
        "received_time_regressions": received_regressions,
        "quantity_samples": quantity_samples[:10],
        "price_samples": price_samples[:10],
    }


def main():
    print("=== Deep Venue Contract Inspection (Local Files) ===\n")

    results = {}
    for venue_key, files in LOCAL_FILES.items():
        print(f"--- {venue_key} ---")
        venue_result = {}
        for label, path in files.items():
            print(f"  {label}: {path}")
            result = inspect_file(path)
            venue_result[label] = result
            if "error" not in result:
                print(
                    f"    rows={result['total_rows']}, "
                    f"snapshot={result['has_snapshot']}, "
                    f"final_id+1={result['id_analysis']['final_update_id_increments_by_one']}"
                )
            else:
                print(f"    ERROR: {result['error']}")
        results[venue_key] = venue_result
        print()

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"Report written to: {OUTPUT}")


if __name__ == "__main__":
    main()
