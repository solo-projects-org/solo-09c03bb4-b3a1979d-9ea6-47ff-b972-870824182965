from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.engine import run_check
from checker.errors import CheckError
from checker.manifest import parse_manifest

SAMPLE = Path(__file__).resolve().parents[1] / "sample"
BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"


def sample_inputs():
    manifest = parse_manifest((SAMPLE / "manifest.csv").read_bytes())
    uploads = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    return manifest, uploads


def test_target_lives_in_later_row_group_and_delete_is_matched():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [4242])
    ticket = result.tickets[0]

    assert ticket.found and ticket.deleted
    occurrence = ticket.occurrences[0]
    assert occurrence.file_entry.uploaded_filename == "data-0001.parquet"
    # Zero-based row in the whole file, not in the row group or filtered table.
    assert occurrence.row_index == 99
    assert occurrence.row_group_index == 4
    assert occurrence.row_group_start == 80
    assert occurrence.local_row_index == 19

    matched = occurrence.matched_deletes
    assert len(matched) == 1
    assert matched[0].entry.uploaded_filename == "delete-pos-0001.parquet"
    assert matched[0].entry.sequence_number == 12
    assert matched[0].source_row == 1

    # The earlier row groups were skipped by statistics, yet the row number
    # remains the original 99.
    scan = next(
        s for s in ticket.file_scans
        if s.file_entry.uploaded_filename == "data-0001.parquet"
    )
    assert [rg.row_group_index for rg in scan.row_groups if rg.skipped] == [
        0,
        1,
        2,
        3,
    ]
    assert scan.read_count == 1
    assert scan.row_groups[4].matched_rows == [99]


def test_stale_delete_file_is_ignored_because_sequence_is_lower():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [4242])
    occurrence = result.tickets[0].occurrences[0]

    assert occurrence.deleted
    assert [m.entry.uploaded_filename for m in occurrence.matched_deletes] == [
        "delete-pos-0001.parquet"
    ]
    assert [
        ignored.record.entry.uploaded_filename
        for ignored in occurrence.ignored_deletes
    ] == ["delete-pos-stale-0001.parquet"]
    assert "小于" in occurrence.ignored_deletes[0].reason


def test_missing_statistics_cause_row_groups_to_be_read():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [8888])
    ticket = result.tickets[0]

    assert ticket.found and ticket.deleted
    occurrence = ticket.occurrences[0]
    assert occurrence.file_entry.uploaded_filename == "data-0002.parquet"
    assert occurrence.row_index == 19
    assert occurrence.row_group_index == 1
    assert occurrence.row_group_start == 10

    scan = next(
        s for s in ticket.file_scans
        if s.file_entry.uploaded_filename == "data-0002.parquet"
    )
    assert scan.read_count == 2
    assert all(not rg.stats_present for rg in scan.row_groups)
    assert all(not rg.skipped for rg in scan.row_groups)
    assert all("缺失行组统计" in rg.reason for rg in scan.row_groups)


def test_first_row_group_and_row_number():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [1003])
    occurrence = result.tickets[0].occurrences[0]
    assert occurrence.row_index == 3
    assert occurrence.row_group_index == 0
    assert occurrence.deleted
    assert occurrence.matched_deletes[0].source_row == 0


def test_present_but_not_deleted():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [4218])
    ticket = result.tickets[0]
    assert ticket.found and not ticket.deleted
    assert ticket.occurrences[0].row_index == 98


def test_ticket_not_found():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [9999])
    ticket = result.tickets[0]
    assert not ticket.found
    assert ticket.occurrences == []


def test_equality_delete_entry_is_ignored():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [4242])
    assert len(result.equality_delete_files) == 1
    assert result.equality_delete_files[0].uploaded_filename == (
        "delete-eq-0001.parquet"
    )
    # No equality delete replica was supplied; the check still succeeds.
    assert "delete-eq-0001.parquet" not in uploads


def test_missing_replica_aborts_with_file_name():
    manifest, uploads = sample_inputs()
    del uploads["data-0002.parquet"]
    with pytest.raises(CheckError) as excinfo:
        run_check(manifest, uploads, [4242])
    assert "data-0002.parquet" in excinfo.value.files
    assert "缺失" in excinfo.value.message


def test_corrupt_data_file_aborts_with_file_name(tmp_path: Path):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"this is not parquet")
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"s3://bucket/voyage/data/bad.parquet,bad.parquet,data,1\n"
    )
    manifest = parse_manifest(manifest_text)
    with pytest.raises(CheckError) as excinfo:
        run_check(manifest, {"bad.parquet": bad}, [4242])
    assert "bad.parquet" in excinfo.value.files
    assert "无法读取数据文件" in excinfo.value.message


def test_corrupt_delete_file_aborts_with_file_name(tmp_path: Path):
    bad = tmp_path / "bad-delete.parquet"
    bad.write_bytes(b"this is not parquet")
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data-0001.parquet,data-0001.parquet,data,7\n"
        f"{BASE}/delete-pos/bad-delete.parquet,bad-delete.parquet,"
        "position_delete,9\n"
    )
    manifest = parse_manifest(manifest_text)
    uploads = {
        "data-0001.parquet": SAMPLE / "data-0001.parquet",
        "bad-delete.parquet": bad,
    }
    with pytest.raises(CheckError) as excinfo:
        run_check(manifest, uploads, [4242])
    assert "bad-delete.parquet" in excinfo.value.files
    assert "无法读取位置删除文件" in excinfo.value.message


def test_delete_file_missing_pos_column_aborts(tmp_path: Path):
    bad = tmp_path / "wrong-schema.parquet"
    pq.write_table(
        pa.table({"file_path": [f"{BASE}/data/data-0001.parquet"]}),
        bad,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/data-0001.parquet,data-0001.parquet,data,7\n"
        f"{BASE}/delete-pos/wrong-schema.parquet,wrong-schema.parquet,"
        "position_delete,9\n"
    )
    manifest = parse_manifest(manifest_text)
    uploads = {
        "data-0001.parquet": SAMPLE / "data-0001.parquet",
        "wrong-schema.parquet": bad,
    }
    with pytest.raises(CheckError) as excinfo:
        run_check(manifest, uploads, [4242])
    assert "wrong-schema.parquet" in excinfo.value.files
    assert "pos" in excinfo.value.message


def test_data_file_missing_ticket_column_aborts(tmp_path: Path):
    data = tmp_path / "no-ticket.parquet"
    pq.write_table(pa.table({"other": [1, 2]}), data)
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"{BASE}/data/no-ticket.parquet,no-ticket.parquet,data,3\n"
    )
    manifest = parse_manifest(manifest_text)
    with pytest.raises(CheckError) as excinfo:
        run_check(manifest, {"no-ticket.parquet": data}, [4242])
    assert "no-ticket.parquet" in excinfo.value.files
    assert "ticket_no" in excinfo.value.message


def test_delete_must_match_original_path(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(pa.table({"ticket_no": [123]}), data_path)
    pq.write_table(
        pa.table(
            {
                "file_path": ["s3://bucket/voyage/data/other.parquet"],
                "pos": [0],
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/voyage/data/data.parquet,data.parquet,data,5\n"
        "s3://bucket/voyage/delete/delete.parquet,delete.parquet,"
        "position_delete,9\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_check(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        [123],
    )
    ticket = result.tickets[0]
    assert ticket.found
    assert not ticket.deleted
    assert ticket.occurrences[0].row_index == 0


def test_delete_sequence_equal_to_data_sequence_is_accepted(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(pa.table({"ticket_no": [123]}), data_path)
    pq.write_table(
        pa.table(
            {
                "file_path": ["s3://bucket/voyage/data/data.parquet"],
                "pos": [0],
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/voyage/data/data.parquet,data.parquet,data,5\n"
        "s3://bucket/voyage/delete/delete.parquet,delete.parquet,"
        "position_delete,5\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_check(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        [123],
    )
    assert result.tickets[0].deleted


def test_row_number_is_original_even_with_sparse_groups(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    # Three row groups of 10.  Ticket 777 is the 8th row of row group 2.
    table = pa.table(
        {
            "ticket_no": list(range(0, 10))
            + list(range(100, 110))
            + list(range(200, 207))
            + [777]
            + list(range(208, 210))
        }
    )
    pq.write_table(table, data_path, row_group_size=10)
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/voyage/data/data.parquet,data.parquet,data,5\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_check(manifest, {"data.parquet": data_path}, [777])
    occurrence = result.tickets[0].occurrences[0]
    assert occurrence.row_group_index == 2
    assert occurrence.row_group_start == 20
    assert occurrence.local_row_index == 7
    assert occurrence.row_index == 27


def test_tickets_are_deduplicated_in_input_order():
    manifest, uploads = sample_inputs()
    result = run_check(manifest, uploads, [4242, 1003, 4242])
    assert [t.ticket for t in result.tickets] == [4242, 1003]
