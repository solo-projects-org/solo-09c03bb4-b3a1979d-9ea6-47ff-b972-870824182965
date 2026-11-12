"""Iceberg v2 positional-delete verification.

The engine scans the Parquet replicas of the supplied valid-file manifest.

* Only data files and positional delete files are read.  Equality delete
  entries are ignored.
* For a ticket number, each Parquet row group is checked against the
  ``ticket_no`` column statistics.  A row group is skipped only when its
  min/max range provably excludes the ticket.  If statistics are missing,
  the row group is read.
* Row numbers are always computed from the original Parquet row-group
  offsets.  Filtering a row group never renumbers rows.
* A delete record applies only when ``file_path`` equals the manifest's
  original path, ``pos`` equals the zero-based row index in the data file,
  and the delete file's data sequence number is >= the data file's.

Any missing required replica or any read failure aborts the whole check
with a ``CheckError`` naming the exact file; no partial results are returned.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import CheckError
from .manifest import FileKind, Manifest, ManifestEntry

DELETE_PATH_COLUMN = "file_path"
DELETE_POS_COLUMN = "pos"


@dataclass
class DeleteRecord:
    entry: ManifestEntry
    source_row: int  # zero-based row in the positional delete file


@dataclass
class IgnoredDelete:
    record: DeleteRecord
    reason: str


@dataclass
class Occurrence:
    ticket: int
    file_entry: ManifestEntry
    row_index: int  # zero-based row in the data file
    row_group_index: int
    row_group_start: int  # zero-based first row of that row group
    local_row_index: int  # zero-based row in the row group
    matched_deletes: list[DeleteRecord] = field(default_factory=list)
    ignored_deletes: list[IgnoredDelete] = field(default_factory=list)

    @property
    def deleted(self) -> bool:
        return bool(self.matched_deletes)


@dataclass
class RowGroupScan:
    row_group_index: int
    start_row: int
    num_rows: int
    stats_present: bool
    stats_min: int | None
    stats_max: int | None
    skipped: bool
    reason: str
    matched_rows: list[int] = field(default_factory=list)


@dataclass
class FileScan:
    file_entry: ManifestEntry
    row_groups: list[RowGroupScan]

    @property
    def skipped_count(self) -> int:
        return sum(1 for rg in self.row_groups if rg.skipped)

    @property
    def read_count(self) -> int:
        return sum(1 for rg in self.row_groups if not rg.skipped)

    @property
    def matched_rows(self) -> list[int]:
        return [row for rg in self.row_groups for row in rg.matched_rows]


@dataclass
class TicketResult:
    ticket: int
    occurrences: list[Occurrence]
    file_scans: list[FileScan]

    @property
    def found(self) -> bool:
        return bool(self.occurrences)

    @property
    def deleted(self) -> bool:
        return any(o.deleted for o in self.occurrences)

    @property
    def ignored_deletes(self) -> list[IgnoredDelete]:
        return [item for o in self.occurrences for item in o.ignored_deletes]


@dataclass
class ManifestRow:
    entry: ManifestEntry
    uploaded: bool
    note: str = ""


@dataclass
class CheckResult:
    manifest: Manifest
    ticket_column: str
    tickets: list[TicketResult]
    manifest_rows: list[ManifestRow]
    unused_uploads: list[str]
    warnings: list[str] = field(default_factory=list)

    @property
    def data_files(self) -> list[ManifestEntry]:
        return self.manifest.data_files

    @property
    def position_delete_files(self) -> list[ManifestEntry]:
        return self.manifest.position_delete_files

    @property
    def equality_delete_files(self) -> list[ManifestEntry]:
        return self.manifest.equality_delete_files

    @property
    def found_count(self) -> int:
        return sum(1 for t in self.tickets if t.found)

    @property
    def deleted_count(self) -> int:
        return sum(1 for t in self.tickets if t.deleted)


@dataclass
class _DataHandle:
    entry: ManifestEntry
    path: Path
    parquet: pq.ParquetFile
    column_index: int = -1


def _find_parquet_column(metadata: pq.FileMetaData, name: str) -> int | None:
    """Return the leaf-column index in row-group metadata for ``name``."""
    for index in range(metadata.num_columns):
        if metadata.schema.column(index).path == name:
            return index
    return None


def _require_integer_column(
    schema: pa.Schema, name: str, *, file_label: str, role: str
) -> pa.DataType:
    try:
        field = schema.field(name)
    except KeyError:
        raise CheckError(
            f"{role} “{file_label}” 缺少列 “{name}”。",
            files=[file_label],
        ) from None
    if not pa.types.is_integer(field.type):
        raise CheckError(
            f"{role} “{file_label}” 的列 “{name}” 类型为 {field.type}，"
            "不是整数类型。",
            files=[file_label],
        )
    return field.type


def _require_string_column(
    schema: pa.Schema, name: str, *, file_label: str
) -> None:
    try:
        field = schema.field(name)
    except KeyError:
        raise CheckError(
            f"删除文件 “{file_label}” 缺少列 “{name}”。位置删除文件必须包含 "
            "file_path 和 pos。",
            files=[file_label],
        ) from None
    if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
        raise CheckError(
            f"删除文件 “{file_label}” 的列 “{name}” 类型为 {field.type}，"
            "不是字符串类型。",
            files=[file_label],
        )


def _open_data_files(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> list[_DataHandle]:
    handles: list[_DataHandle] = []
    for entry in manifest.data_files:
        path = uploaded_paths[entry.uploaded_filename]
        label = entry.uploaded_filename
        try:
            parquet = pq.ParquetFile(str(path))
        except Exception as exc:
            raise CheckError(
                f"无法读取数据文件 “{label}”（原始路径 {entry.original_path}）："
                f"{exc}",
                files=[label],
            ) from None
        handles.append(
            _DataHandle(entry=entry, path=path, parquet=parquet)
        )
    return handles


def _bind_ticket_column(
    handles: list[_DataHandle], ticket_column: str
) -> None:
    """Validate the requested ticket column in every data file."""
    for handle in handles:
        entry = handle.entry
        label = entry.uploaded_filename
        schema = handle.parquet.schema_arrow
        _require_integer_column(
            schema, ticket_column, file_label=label, role="数据文件"
        )
        column_index = _find_parquet_column(
            handle.parquet.metadata, ticket_column
        )
        if column_index is None:
            raise CheckError(
                f"数据文件 “{label}” 的 Parquet 元数据中找不到列 "
                f"“{ticket_column}”。",
                files=[label],
            )
        handle.column_index = column_index


def _load_position_deletes(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> dict[tuple[str, int], list[DeleteRecord]]:
    index: dict[tuple[str, int], list[DeleteRecord]] = {}
    for entry in manifest.position_delete_files:
        path = uploaded_paths[entry.uploaded_filename]
        label = entry.uploaded_filename
        try:
            parquet = pq.ParquetFile(str(path))
            _require_string_column(
                parquet.schema_arrow, DELETE_PATH_COLUMN, file_label=label
            )
            _require_integer_column(
                parquet.schema_arrow,
                DELETE_POS_COLUMN,
                file_label=label,
                role="删除文件",
            )
            table = parquet.read(
                columns=[DELETE_PATH_COLUMN, DELETE_POS_COLUMN]
            )
        except CheckError:
            raise
        except Exception as exc:
            raise CheckError(
                f"无法读取位置删除文件 “{label}”（原始路径 "
                f"{entry.original_path}）：{exc}",
                files=[label],
            ) from None

        # These files are normally small, and the index has to be complete
        # before any conclusion is produced.
        paths = table.column(DELETE_PATH_COLUMN).to_pylist()
        positions = table.column(DELETE_POS_COLUMN).to_pylist()
        for row_index, (record_path, record_pos) in enumerate(
            zip(paths, positions)
        ):
            if record_path is None or str(record_path) == "":
                raise CheckError(
                    f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                    "的 file_path 为空。",
                    files=[label],
                )
            if not isinstance(record_path, str):
                raise CheckError(
                    f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                    "的 file_path 不是字符串。",
                    files=[label],
                )
            if record_pos is None or not isinstance(record_pos, int):
                raise CheckError(
                    f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                    "的 pos 不是整数。",
                    files=[label],
                )
            if record_pos < 0:
                raise CheckError(
                    f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）"
                    f"的 pos={record_pos} 为负数。",
                    files=[label],
                )
            index.setdefault((record_path, record_pos), []).append(
                DeleteRecord(entry=entry, source_row=row_index)
            )
    return index


def _extract_stats(column_metadata) -> tuple[bool, int | None, int | None]:
    stats = column_metadata.statistics
    if stats is None or not getattr(stats, "has_min_max", False):
        return False, None, None
    try:
        minimum = stats.min
        maximum = stats.max
        if minimum is None or maximum is None:
            return False, None, None
        return True, int(minimum), int(maximum)
    except (TypeError, ValueError, OverflowError):
        return False, None, None


def _find_local_matches(
    handle: _DataHandle,
    row_group_index: int,
    ticket: int,
    ticket_column: str,
    expected_rows: int,
) -> list[int]:
    """Read one row group and return original zero-based local row indices."""
    label = handle.entry.uploaded_filename
    try:
        table = handle.parquet.read_row_group(
            row_group_index, columns=[ticket_column]
        )
    except Exception as exc:
        raise CheckError(
            f"读取数据文件 “{label}”（原始路径 "
            f"{handle.entry.original_path}）的行组 {row_group_index} 失败："
            f"{exc}",
            files=[label],
        ) from None
    if table.num_rows != expected_rows:
        raise CheckError(
            f"数据文件 “{label}” 的行组 {row_group_index} 元数据说 "
            f"{expected_rows} 行，实际读取 {table.num_rows} 行。无法可靠计算"
            "文件内行号。",
            files=[label],
        )
    column = table.column(ticket_column)
    try:
        mask = pc.equal(column, pa.scalar(ticket, type=column.type))
        indices = pc.indices_nonzero(mask)
        return [int(i) for i in indices.to_pylist()]
    except Exception as exc:
        raise CheckError(
            f"数据文件 “{label}” 的行组 {row_group_index} 无法按票号列 "
            f"“{ticket_column}” 过滤：{exc}",
            files=[label],
        ) from None


def _scan_file(
    handle: _DataHandle,
    ticket: int,
    ticket_column: str,
    delete_index: Mapping[tuple[str, int], list[DeleteRecord]],
) -> tuple[FileScan, list[Occurrence]]:
    file_scan = FileScan(file_entry=handle.entry, row_groups=[])
    occurrences: list[Occurrence] = []
    start_row = 0

    try:
        metadata = handle.parquet.metadata
    except Exception as exc:  # pragma: no cover - caught during preflight
        raise CheckError(
            f"无法读取数据文件 “{handle.entry.uploaded_filename}” 的元数据：{exc}",
            files=[handle.entry.uploaded_filename],
        ) from None

    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        num_rows = int(row_group.num_rows)
        column_meta = row_group.column(handle.column_index)
        stats_present, stats_min, stats_max = _extract_stats(column_meta)

        if stats_present and (
            ticket < stats_min or ticket > stats_max
        ):
            row_scan = RowGroupScan(
                row_group_index=row_group_index,
                start_row=start_row,
                num_rows=num_rows,
                stats_present=True,
                stats_min=stats_min,
                stats_max=stats_max,
                skipped=True,
                reason=(
                    f"统计范围 [{stats_min}, {stats_max}] 不含票号 {ticket}"
                ),
            )
        else:
            if stats_present:
                reason = (
                    f"统计范围 [{stats_min}, {stats_max}] 含票号 {ticket}，"
                    "读取行组"
                )
            else:
                reason = "缺失行组统计，读取行组"
            local_indices = _find_local_matches(
                handle,
                row_group_index,
                ticket,
                ticket_column,
                num_rows,
            )
            global_matches = [start_row + i for i in local_indices]
            row_scan = RowGroupScan(
                row_group_index=row_group_index,
                start_row=start_row,
                num_rows=num_rows,
                stats_present=stats_present,
                stats_min=stats_min,
                stats_max=stats_max,
                skipped=False,
                reason=reason,
                matched_rows=global_matches,
            )
            if local_indices:
                for local_index in local_indices:
                    row_index = start_row + local_index
                    occurrence = Occurrence(
                        ticket=ticket,
                        file_entry=handle.entry,
                        row_index=row_index,
                        row_group_index=row_group_index,
                        row_group_start=start_row,
                        local_row_index=local_index,
                    )
                    for record in delete_index.get(
                        (handle.entry.original_path, row_index), []
                    ):
                        if (
                            record.entry.sequence_number
                            >= handle.entry.sequence_number
                        ):
                            occurrence.matched_deletes.append(record)
                        else:
                            occurrence.ignored_deletes.append(
                                IgnoredDelete(
                                    record=record,
                                    reason=(
                                        "删除文件的数据序列号 "
                                        f"{record.entry.sequence_number} 小于"
                                        "数据文件的 "
                                        f"{handle.entry.sequence_number}"
                                    ),
                                )
                            )
                    occurrences.append(occurrence)

        file_scan.row_groups.append(row_scan)
        start_row += num_rows

    return file_scan, occurrences


def _check_presence(
    manifest: Manifest, uploaded_paths: Mapping[str, Path]
) -> None:
    missing: list[str] = []
    for entry in manifest.required_entries:
        path = uploaded_paths.get(entry.uploaded_filename)
        if path is None or not Path(path).is_file():
            missing.append(entry.uploaded_filename)
    if missing:
        raise CheckError(
            "清单中以下文件的上传副本缺失，无法给出结论。",
            files=missing,
        )


def run_check(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    tickets: list[int],
    ticket_column: str = "ticket_no",
) -> CheckResult:
    """Run the all-or-nothing check.

    ``uploaded_paths`` maps the manifest's ``uploaded_filename`` values to
    local Parquet replica paths.  Tickets are checked independently; each
    result includes every occurrence and the row-group pruning diagnostics.
    """
    if not tickets:
        raise CheckError("没有可核对的票号。")
    if not ticket_column:
        raise CheckError("票号列名不能为空。")
    if not manifest.data_files:
        raise CheckError(
            "清单中没有 data 类型的数据文件，无法核对票号。"
        )

    _check_presence(manifest, uploaded_paths)
    data_handles = _open_data_files(manifest, uploaded_paths)
    _bind_ticket_column(data_handles, ticket_column)
    delete_index = _load_position_deletes(manifest, uploaded_paths)

    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused_uploads = sorted(
        name for name in uploaded_paths if name not in referenced
    )

    results: list[TicketResult] = []
    for ticket in dict.fromkeys(tickets):
        file_scans: list[FileScan] = []
        occurrences: list[Occurrence] = []
        for handle in data_handles:
            scan, file_occurrences = _scan_file(
                handle, ticket, ticket_column, delete_index
            )
            file_scans.append(scan)
            occurrences.extend(file_occurrences)
        results.append(
            TicketResult(
                ticket=ticket,
                occurrences=occurrences,
                file_scans=file_scans,
            )
        )

    rows: list[ManifestRow] = []
    for entry in manifest.entries:
        uploaded = entry.uploaded_filename in uploaded_paths
        note = ""
        if entry.kind is FileKind.EQUALITY_DELETE:
            note = "仅处理位置删除，此文件已忽略"
        rows.append(ManifestRow(entry=entry, uploaded=uploaded, note=note))

    warnings: list[str] = []
    if unused_uploads:
        warnings.append(
            "以下上传文件未出现在清单中，已忽略："
            + "、".join(unused_uploads)
        )

    return CheckResult(
        manifest=manifest,
        ticket_column=ticket_column,
        tickets=results,
        manifest_rows=rows,
        unused_uploads=unused_uploads,
        warnings=warnings,
    )
