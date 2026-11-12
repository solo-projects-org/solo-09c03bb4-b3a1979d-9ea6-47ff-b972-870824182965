"""Iceberg v2 positional-delete verification.

The engine scans the Parquet replicas of the supplied valid-file manifest.

* Only data files and positional delete files are read.  Equality delete
  entries are ignored.
* Tickets are checked as a batch.  A row group is read at most once: it is
  skipped only when its ``ticket_no`` min/max statistics provably exclude
  **every** requested ticket.  If at least one ticket could be in the row
  group, its ticket column is read once and matched against all tickets
  before the temporary column is released.
* Row numbers are always computed from the original Parquet row-group
  offsets.  Filtering a row group never renumbers rows, and each ticket keeps
  the original row number and the delete records that matched it.
* A delete record applies only when ``file_path`` equals the manifest's
  original path, ``pos`` equals the zero-based row index in the data file,
  and the delete file's data sequence number is >= the data file's.

The shared row-group metadata is retained, but the scanned ticket column is
not.  Per-ticket skip diagnostics are derived lazily from that shared
metadata, using the current ticket's statistics, so a row group read for a
different ticket is never reported as required for this one.

Any missing required replica or any read failure aborts the whole check
with a ``CheckError`` naming the exact file; no partial results are returned.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

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


@dataclass(frozen=True)
class RowGroupInfo:
    """Shared, ticket-independent row-group metadata.

    ``batch_read`` records whether the batch scan had to read the row group
    for at least one ticket.  It is deliberately not used when deciding
    whether the row group is skippable for a particular ticket.
    """

    row_group_index: int
    start_row: int
    num_rows: int
    stats_present: bool
    stats_min: int | None
    stats_max: int | None
    batch_read: bool

    def excludes(self, ticket: int) -> bool:
        """Whether statistics prove ``ticket`` is absent from this group."""
        if not self.stats_present:
            return False
        if self.stats_min is None or self.stats_max is None:
            return False
        return ticket < self.stats_min or ticket > self.stats_max

    def excludes_all(self, tickets: Iterable[int]) -> bool:
        """Whether statistics prove every ticket is absent from this group."""
        if not self.stats_present:
            return False
        if self.stats_min is None or self.stats_max is None:
            return False
        ordered = sorted(set(tickets))
        if not ordered:
            return True
        return _stats_exclude_all(self, ordered)


def _stats_exclude_all(
    info: RowGroupInfo, ordered_tickets: list[int]
) -> bool:
    """True when no ticket in a sorted list falls inside the stats range."""
    if not info.stats_present:
        return False
    if info.stats_min is None or info.stats_max is None:
        return False
    index = bisect_left(ordered_tickets, info.stats_min)
    return (
        index == len(ordered_tickets)
        or ordered_tickets[index] > info.stats_max
    )


@dataclass
class FileScan:
    """Shared row-group metadata for one data file."""

    file_entry: ManifestEntry
    row_groups: list[RowGroupInfo]

    @property
    def batch_skipped_count(self) -> int:
        return sum(1 for rg in self.row_groups if not rg.batch_read)

    @property
    def batch_read_count(self) -> int:
        return sum(1 for rg in self.row_groups if rg.batch_read)

    # Compatibility aliases: on the shared scan these describe physical
    # batch I/O, not a particular ticket's diagnostics.
    @property
    def skipped_count(self) -> int:
        return self.batch_skipped_count

    @property
    def read_count(self) -> int:
        return self.batch_read_count


@dataclass
class RowGroupScan:
    """Per-ticket view of a row group, generated from shared metadata."""

    row_group_index: int
    start_row: int
    num_rows: int
    stats_present: bool
    stats_min: int | None
    stats_max: int | None
    skipped: bool
    reason: str
    matched_rows: list[int] = field(default_factory=list)
    batch_read: bool = False


@dataclass
class TicketFileScan:
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
class FileSummary:
    """Cheap per-file counts for the collapsed result page."""

    file_entry: ManifestEntry
    total_row_groups: int
    skipped_row_groups: int
    read_row_groups: int


@dataclass
class TicketResult:
    ticket: int
    occurrences: list[Occurrence]
    _shared_scans: list[FileScan] = field(default_factory=list, repr=False)
    _diagnostics: list[TicketFileScan] | None = field(
        default=None, repr=False
    )

    @property
    def found(self) -> bool:
        return bool(self.occurrences)

    @property
    def deleted(self) -> bool:
        return any(o.deleted for o in self.occurrences)

    @property
    def ignored_deletes(self) -> list[IgnoredDelete]:
        return [item for o in self.occurrences for item in o.ignored_deletes]

    @property
    def file_scans(self) -> list[TicketFileScan]:
        """Lazily generated per-ticket diagnostics.

        This uses only the shared row-group metadata and the occurrences
        already found during the batch scan; it never opens a Parquet file.
        """
        if self._diagnostics is None:
            self._diagnostics = _build_ticket_diagnostics(
                self.ticket, self._shared_scans, self.occurrences
            )
        return self._diagnostics

    @property
    def file_summaries(self) -> list[FileSummary]:
        """Per-file skip counts for this ticket, without detail rows."""
        summaries: list[FileSummary] = []
        for scan in self._shared_scans:
            total = len(scan.row_groups)
            skipped = sum(
                1 for rg in scan.row_groups if rg.excludes(self.ticket)
            )
            summaries.append(
                FileSummary(
                    file_entry=scan.file_entry,
                    total_row_groups=total,
                    skipped_row_groups=skipped,
                    read_row_groups=total - skipped,
                )
            )
        return summaries

    @property
    def matches_by_file(self) -> dict[str, list[int]]:
        """Original row numbers of this ticket, grouped by uploaded file."""
        matches: dict[str, list[int]] = {}
        for occurrence in self.occurrences:
            matches.setdefault(
                occurrence.file_entry.uploaded_filename, []
            ).append(occurrence.row_index)
        return matches


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
    file_scans: list[FileScan]
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

    @property
    def shared_row_group_metadata(self) -> dict:
        """One ticket-independent payload for the result page."""
        return {
            "ticket_column": self.ticket_column,
            "files": [
                {
                    "uploaded_filename": scan.file_entry.uploaded_filename,
                    "original_path": scan.file_entry.original_path,
                    "sequence_number": scan.file_entry.sequence_number,
                    "row_groups": [
                        {
                            "index": rg.row_group_index,
                            "start_row": rg.start_row,
                            "num_rows": rg.num_rows,
                            "stats_present": rg.stats_present,
                            "stats_min": rg.stats_min,
                            "stats_max": rg.stats_max,
                            # Exact decimal strings avoid JavaScript
                            # precision loss for 64-bit ticket numbers.
                            "stats_min_text": (
                                None
                                if rg.stats_min is None
                                else str(rg.stats_min)
                            ),
                            "stats_max_text": (
                                None
                                if rg.stats_max is None
                                else str(rg.stats_max)
                            ),
                            "batch_read": rg.batch_read,
                        }
                        for rg in scan.row_groups
                    ],
                }
                for scan in self.file_scans
            ],
        }


@dataclass
class _DataHandle:
    entry: ManifestEntry
    path: Path
    parquet: pq.ParquetFile
    column_index: int = -1
    ticket_type: pa.DataType | None = None


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
        ticket_type = _require_integer_column(
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
        handle.ticket_type = ticket_type


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
        # The delete index keeps only the small (path, pos) tuples.
        del table, paths, positions
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


def _representable_tickets(
    tickets: Iterable[int], ticket_type: pa.DataType
) -> list[int]:
    """Drop tickets that cannot possibly occur in a column of this type."""
    values = list(tickets)
    if pa.types.is_unsigned_integer(ticket_type):
        values = [value for value in values if value >= 0]
        width = getattr(ticket_type, "bit_width", None)
        if width:
            maximum = (1 << width) - 1
            values = [value for value in values if value <= maximum]
    elif pa.types.is_signed_integer(ticket_type):
        width = getattr(ticket_type, "bit_width", None)
        if width:
            minimum = -(1 << (width - 1))
            maximum = (1 << (width - 1)) - 1
            values = [
                value
                for value in values
                if minimum <= value <= maximum
            ]
    return values


def _find_batch_matches(
    handle: _DataHandle,
    row_group_index: int,
    ticket_column: str,
    tickets: Iterable[int],
    expected_rows: int,
) -> dict[int, list[int]]:
    """Read one row group's ticket column once and match every ticket.

    Returns zero-based local row indices per ticket.  The temporary Arrow
    table/column is released before returning; only small integer lists are
    retained.
    """
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
    value_pool = _representable_tickets(tickets, column.type)
    if not value_pool:
        del table, column
        return {}

    try:
        value_set = pa.array(value_pool, type=column.type)
        mask = pc.is_in(column, value_set=value_set)
        indices = pc.indices_nonzero(mask)
        if len(indices) == 0:
            del table, column, value_set, mask, indices
            return {}
        matched_values = pc.take(column, indices).to_pylist()
    except Exception as exc:
        raise CheckError(
            f"数据文件 “{label}” 的行组 {row_group_index} 无法按票号列 "
            f"“{ticket_column}” 过滤：{exc}",
            files=[label],
        ) from None

    grouped: dict[int, list[int]] = {}
    for local_index, value in zip(indices.to_pylist(), matched_values):
        if value is not None:
            grouped.setdefault(int(value), []).append(int(local_index))

    # Explicitly drop the scanned column data; nothing Arrow-shaped survives
    # this function except the small per-ticket row-index lists.
    del table, column, value_set, mask, indices, matched_values
    return grouped


def _scan_file(
    handle: _DataHandle,
    tickets: list[int],
    ticket_column: str,
    delete_index: Mapping[tuple[str, int], list[DeleteRecord]],
) -> tuple[FileScan, dict[int, list[Occurrence]]]:
    """Scan one data file once for the whole batch of tickets."""
    file_scan = FileScan(file_entry=handle.entry, row_groups=[])
    occurrences: dict[int, list[Occurrence]] = {
        ticket: [] for ticket in tickets
    }
    start_row = 0
    ordered_tickets = sorted(tickets)

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

        info = RowGroupInfo(
            row_group_index=row_group_index,
            start_row=start_row,
            num_rows=num_rows,
            stats_present=stats_present,
            stats_min=stats_min,
            stats_max=stats_max,
            batch_read=False,
        )

        # The only reason to skip physical I/O is that statistics exclude the
        # *entire* batch.  Missing statistics always require a read.
        if _stats_exclude_all(info, ordered_tickets):
            file_scan.row_groups.append(info)
            start_row += num_rows
            continue

        matches = _find_batch_matches(
            handle,
            row_group_index,
            ticket_column,
            tickets,
            num_rows,
        )
        info = RowGroupInfo(
            row_group_index=info.row_group_index,
            start_row=info.start_row,
            num_rows=info.num_rows,
            stats_present=info.stats_present,
            stats_min=info.stats_min,
            stats_max=info.stats_max,
            batch_read=True,
        )
        file_scan.row_groups.append(info)

        for ticket, local_indices in matches.items():
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
                occurrences[ticket].append(occurrence)

        start_row += num_rows

    return file_scan, occurrences


def _build_ticket_diagnostics(
    ticket: int,
    shared_scans: list[FileScan],
    occurrences: list[Occurrence],
) -> list[TicketFileScan]:
    """Build per-ticket diagnostics from shared metadata, without I/O."""
    matches: dict[tuple[str, int], list[int]] = {}
    for occurrence in occurrences:
        key = (
            occurrence.file_entry.uploaded_filename,
            occurrence.row_group_index,
        )
        matches.setdefault(key, []).append(occurrence.row_index)

    diagnostics: list[TicketFileScan] = []
    for scan in shared_scans:
        row_groups: list[RowGroupScan] = []
        for info in scan.row_groups:
            skipped = info.excludes(ticket)
            matched_rows = matches.get(
                (scan.file_entry.uploaded_filename, info.row_group_index), []
            )
            if skipped:
                reason = (
                    f"统计范围 [{info.stats_min}, {info.stats_max}] "
                    f"不含票号 {ticket}"
                )
                if info.batch_read:
                    reason += (
                        "；本票无需读取，该行组在批量扫描中为其他票号读取"
                    )
                matched_rows = []
            elif info.stats_present:
                reason = (
                    f"统计范围 [{info.stats_min}, {info.stats_max}] "
                    f"含票号 {ticket}，读取行组"
                )
            else:
                reason = "缺失行组统计，读取行组"

            row_groups.append(
                RowGroupScan(
                    row_group_index=info.row_group_index,
                    start_row=info.start_row,
                    num_rows=info.num_rows,
                    stats_present=info.stats_present,
                    stats_min=info.stats_min,
                    stats_max=info.stats_max,
                    skipped=skipped,
                    reason=reason,
                    matched_rows=matched_rows,
                    batch_read=info.batch_read,
                )
            )
        diagnostics.append(
            TicketFileScan(file_entry=scan.file_entry, row_groups=row_groups)
        )
    return diagnostics


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
    """Run the all-or-nothing batch check.

    ``uploaded_paths`` maps the manifest's ``uploaded_filename`` values to
    local Parquet replica paths.  Each data file is scanned once for the
    whole batch.  Every ticket result keeps its original row numbers and
    matched/ignored delete evidence; per-ticket diagnostics are derived
    lazily from the shared row-group metadata.
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

    unique_tickets = list(dict.fromkeys(tickets))
    shared_scans: list[FileScan] = []
    occurrences_by_ticket: dict[int, list[Occurrence]] = {
        ticket: [] for ticket in unique_tickets
    }
    for handle in data_handles:
        scan, file_occurrences = _scan_file(
            handle, unique_tickets, ticket_column, delete_index
        )
        shared_scans.append(scan)
        for ticket in unique_tickets:
            occurrences_by_ticket[ticket].extend(
                file_occurrences[ticket]
            )

    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused_uploads = sorted(
        name for name in uploaded_paths if name not in referenced
    )

    results = [
        TicketResult(
            ticket=ticket,
            occurrences=occurrences_by_ticket[ticket],
            _shared_scans=shared_scans,
        )
        for ticket in unique_tickets
    ]

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
        file_scans=shared_scans,
        warnings=warnings,
    )
