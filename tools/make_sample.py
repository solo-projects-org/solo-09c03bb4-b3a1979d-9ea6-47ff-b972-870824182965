"""Generate the bundled sample used by the "载入样例" button.

The sample deliberately contains:

* data-0001.parquet: five row groups.  Ticket 4242 lives in row group 4
  (zero-based), so row groups 0-3 are skipped by min/max statistics before
  the ticket is found.
* data-0002.parquet: written without column statistics.  Ticket 8888 is in
  its second row group and the checker must read both row groups because the
  statistics are missing.
* delete-pos-0001.parquet: positional delete for 4242 at row 99.
* delete-pos-0002.parquet: positional delete for 8888 at row 19.
* delete-pos-stale-0001.parquet: a tempting delete for 4242/row 99, but with
  sequence number 6 < data file sequence number 7; it must be ignored.
* An equality-delete manifest entry with no uploaded replica; it must be
  ignored because only positional deletes are processed.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample"

BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"
DATA1_PATH = f"{BASE}/data/data-0001.parquet"
DATA2_PATH = f"{BASE}/data/data-0002.parquet"
DELETE1_PATH = f"{BASE}/delete-pos/delete-pos-0001.parquet"
DELETE2_PATH = f"{BASE}/delete-pos/delete-pos-0002.parquet"
STALE_DELETE_PATH = f"{BASE}/delete-pos/delete-pos-stale-0001.parquet"
EQ_DELETE_PATH = f"{BASE}/delete-eq/delete-eq-0001.parquet"

DATA_SCHEMA = pa.schema(
    [
        ("ticket_no", pa.int64()),
        ("passenger_name", pa.string()),
        ("fare_cents", pa.int64()),
        ("booking_status", pa.string()),
        ("voyage_date", pa.string()),
    ]
)

DELETE_SCHEMA = pa.schema(
    [
        ("file_path", pa.string()),
        ("pos", pa.int64()),
    ]
)


def data_rows(tickets: list[int]) -> list[dict]:
    return [
        {
            "ticket_no": ticket,
            "passenger_name": f"P-{ticket}",
            "fare_cents": 1500 + (ticket % 700),
            "booking_status": "checked_in",
            "voyage_date": "2026-11-05",
        }
        for ticket in tickets
    ]


def write_data1() -> None:
    tickets = (
        list(range(1000, 1020))
        + list(range(2000, 2020))
        + list(range(3000, 3020))
        + list(range(4000, 4020))
        + list(range(4200, 4219))
        + [4242]
    )
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # 20 rows per row group -> exactly five row groups.
    pq.write_table(
        table,
        SAMPLE / "data-0001.parquet",
        row_group_size=20,
        compression="snappy",
    )


def write_data2() -> None:
    tickets = list(range(7000, 7010)) + list(range(8000, 8009)) + [8888]
    table = pa.Table.from_pylist(data_rows(tickets), schema=DATA_SCHEMA)
    # No statistics at all: the checker must fall back to reading row groups.
    pq.write_table(
        table,
        SAMPLE / "data-0002.parquet",
        row_group_size=10,
        write_statistics=False,
        compression="snappy",
    )


def write_delete(path: Path, records: list[tuple[str, int]]) -> None:
    table = pa.Table.from_pylist(
        [{"file_path": file_path, "pos": pos} for file_path, pos in records],
        schema=DELETE_SCHEMA,
    )
    pq.write_table(table, path, row_group_size=10, compression="snappy")


def write_manifest() -> None:
    lines = [
        "original_path,uploaded_filename,file_type,data_sequence_number",
        f"{DATA1_PATH},data-0001.parquet,data,7",
        f"{DATA2_PATH},data-0002.parquet,data,7",
        f"{DELETE1_PATH},delete-pos-0001.parquet,position_delete,12",
        f"{DELETE2_PATH},delete-pos-0002.parquet,position_delete,8",
        f"{STALE_DELETE_PATH},delete-pos-stale-0001.parquet,position_delete,6",
        f"{EQ_DELETE_PATH},delete-eq-0001.parquet,equality_delete,12",
    ]
    (SAMPLE / "manifest.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    SAMPLE.mkdir(parents=True, exist_ok=True)
    write_data1()
    write_data2()
    write_delete(
        SAMPLE / "delete-pos-0001.parquet",
        [
            (DATA1_PATH, 3),   # ticket 1003
            (DATA1_PATH, 99),  # ticket 4242, last row of row group 4
            (DATA2_PATH, 99),  # wrong path / out-of-range; must not match
        ],
    )
    write_delete(
        SAMPLE / "delete-pos-0002.parquet",
        [(DATA2_PATH, 19)],  # ticket 8888
    )
    write_delete(
        SAMPLE / "delete-pos-stale-0001.parquet",
        [
            (DATA1_PATH, 99),  # matches 4242 by path/row but sequence is 6 < 7
        ],
    )
    write_manifest()

    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
        "manifest.csv",
    ):
        print(f"wrote {SAMPLE / name}")


if __name__ == "__main__":
    main()
