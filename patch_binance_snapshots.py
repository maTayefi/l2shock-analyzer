import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
from pathlib import Path

raw_base = Path(r"data\raw\cryptohftdata\binance_futures")
files = list(raw_base.rglob("*_orderbook.parquet"))

print(f"Found {len(files)} Binance orderbook files to check/patch.")

for p in files:
    try:
        table = pq.read_table(p)
        schema = table.schema
        changed = False

        # 1. Fix transaction_time: fill nulls with event_time
        if "transaction_time" in schema.names:
            tt = table.column("transaction_time")
            if tt.null_count > 0:
                et = table.column("event_time")
                new_tt = pc.if_else(pc.is_null(tt), et, tt)
                table = table.set_column(
                    schema.get_field_index("transaction_time"),
                    "transaction_time",
                    new_tt,
                )
                changed = True

        # 2. Fix order_count: fill nulls with 0
        if "order_count" in schema.names:
            oc = table.column("order_count")
            if oc.null_count > 0:
                zero_scalar = pa.scalar(0, type=oc.type)
                new_oc = pc.if_else(pc.is_null(oc), zero_scalar, oc)
                table = table.set_column(
                    schema.get_field_index("order_count"), "order_count", new_oc
                )
                changed = True

        if changed:
            pq.write_table(table, p, compression="zstd")
            print(f"  -> PATCHED & SAVED: {p.relative_to(raw_base)}")
        else:
            print(f"  -> Already clean: {p.relative_to(raw_base)}")

    except Exception as e:
        print(f"  -> ERROR processing {p.name}: {e}")
