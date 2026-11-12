"""Parsing of the Iceberg v2 valid-file manifest.

Supported input is either CSV or JSON.  Every entry must describe:

* ``original_path`` - the path stored in Iceberg (and in positional delete rows)
* ``uploaded_filename`` - the name of the uploaded Parquet replica
* ``file_type`` - data / position delete / equality delete
* ``data_sequence_number`` - the Iceberg data sequence number of that file

Only data files and *position* delete files become inputs to the checker.
Equality delete entries are recorded and explicitly ignored.
"""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from enum import Enum

from .errors import CheckError


class FileKind(str, Enum):
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"


_DATA_TYPES = {"data", "data_file", "0"}
_POSITION_TYPES = {
    "position_delete",
    "position_deletes",
    "positional_delete",
    "positional_deletes",
    "position_deletion",
    "pos_delete",
    "pos_deletes",
    "delete_position",
    "delete_positional",
    "delete",
    "1",
}
_EQUALITY_TYPES = {
    "equality_delete",
    "equality_deletes",
    "eq_delete",
    "eq_deletes",
    "delete_equality",
    "2",
}

_ALIASES = {
    "original_path": {
        "original_path",
        "original_file_path",
        "source_path",
        "data_file_path",
        "file_path",
        "path",
    },
    "uploaded_filename": {
        "uploaded_filename",
        "uploaded_file",
        "upload_name",
        "upload_file_name",
        "filename",
    },
    "file_type": {"file_type", "content", "type", "file_content", "kind"},
    "data_sequence_number": {
        "data_sequence_number",
        "sequence_number",
        "seq",
        "data_seq",
    },
}


@dataclass(frozen=True)
class ManifestEntry:
    original_path: str
    uploaded_filename: str
    kind: FileKind
    sequence_number: int
    row_label: str
    raw_file_type: str

    @property
    def is_data(self) -> bool:
        return self.kind is FileKind.DATA

    @property
    def is_position_delete(self) -> bool:
        return self.kind is FileKind.POSITION_DELETE


@dataclass
class Manifest:
    entries: list[ManifestEntry]

    @property
    def data_files(self) -> list[ManifestEntry]:
        return [e for e in self.entries if e.is_data]

    @property
    def position_delete_files(self) -> list[ManifestEntry]:
        return [e for e in self.entries if e.is_position_delete]

    @property
    def equality_delete_files(self) -> list[ManifestEntry]:
        return [e for e in self.entries if e.kind is FileKind.EQUALITY_DELETE]

    @property
    def required_entries(self) -> list[ManifestEntry]:
        """Entries whose uploaded Parquet replicas must be present."""
        return [e for e in self.entries if e.is_data or e.is_position_delete]


def _normalise_key(value: object) -> str:
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _field(row: dict[str, object], name: str) -> object | None:
    for alias in _ALIASES[name]:
        if alias in row:
            value = row[alias]
            if value is not None and str(value).strip() != "":
                return value
    return None


def _require_field(row: dict[str, object], name: str, row_label: str) -> object:
    value = _field(row, name)
    if value is None:
        raise CheckError(
            f"{row_label} 缺少必需列 “{name}”。清单列应为 original_path、"
            "uploaded_filename、file_type、data_sequence_number。"
        )
    return value


def _parse_kind(raw: object, row_label: str) -> FileKind:
    text = _normalise_key(raw)
    if text in _DATA_TYPES:
        return FileKind.DATA
    if text in _POSITION_TYPES:
        return FileKind.POSITION_DELETE
    if text in _EQUALITY_TYPES:
        return FileKind.EQUALITY_DELETE
    raise CheckError(
        f"{row_label} 的文件类型 “{raw}” 无法识别。可选值：data、"
        "position_delete（或 positional_delete / 1）、"
        "equality_delete（或 2）。"
    )


def _parse_sequence(raw: object, row_label: str) -> int:
    text = _clean_text(raw)
    try:
        value = int(text, 10)
    except (TypeError, ValueError):
        raise CheckError(
            f"{row_label} 的数据序列号 “{raw}” 不是整数。"
        ) from None
    if value < 0:
        raise CheckError(f"{row_label} 的数据序列号不能为负数。")
    return value


def _entry_from_row(row: dict[str, object], row_label: str) -> ManifestEntry:
    original_path = _clean_text(
        _require_field(row, "original_path", row_label)
    )
    uploaded_filename = _clean_text(
        _require_field(row, "uploaded_filename", row_label)
    )
    raw_kind = _require_field(row, "file_type", row_label)
    sequence = _parse_sequence(
        _require_field(row, "data_sequence_number", row_label), row_label
    )

    if not original_path:
        raise CheckError(f"{row_label} 的原始路径为空。")
    if not uploaded_filename:
        raise CheckError(f"{row_label} 的上传文件名为空。")

    return ManifestEntry(
        original_path=original_path,
        uploaded_filename=uploaded_filename,
        kind=_parse_kind(raw_kind, row_label),
        sequence_number=sequence,
        row_label=row_label,
        raw_file_type=_clean_text(raw_kind),
    )


def _parse_csv(text: str) -> list[ManifestEntry]:
    try:
        reader = csv.DictReader(io.StringIO(text))
        raw_headers = reader.fieldnames
        if not raw_headers:
            raise CheckError("清单为空或缺少表头。")
        headers = [_normalise_key(h) for h in raw_headers]
        if len(set(headers)) != len(headers):
            raise CheckError("清单表头在规范化后出现重复列名。")

        entries: list[ManifestEntry] = []
        for row in reader:
            # DictReader keeps the original header keys; normalise them now.
            normalised = {
                _normalise_key(k): v for k, v in row.items() if k is not None
            }
            if not any(_clean_text(v) for v in normalised.values()):
                continue
            row_label = f"清单第 {reader.line_num} 行"
            entries.append(_entry_from_row(normalised, row_label))
        return entries
    except csv.Error as exc:
        raise CheckError(f"CSV 清单解析失败：{exc}") from None


def _parse_json(text: str) -> list[ManifestEntry]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CheckError(f"JSON 清单解析失败：{exc}") from None

    if isinstance(payload, dict):
        keys = {_normalise_key(k): v for k, v in payload.items()}
        for key in ("files", "entries", "valid_files", "rows"):
            if key in keys and isinstance(keys[key], list):
                payload = keys[key]
                break
        else:
            if any(
                key in _ALIASES["original_path"] for key in keys
            ):
                payload = [payload]
            else:
                raise CheckError(
                    "JSON 清单应为数组，或包含 files/entries 数组的对象。"
                )
    if not isinstance(payload, list):
        raise CheckError("JSON 清单顶层必须是数组。")

    entries: list[ManifestEntry] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise CheckError(f"清单第 {index} 条不是对象。")
        normalised = {_normalise_key(k): v for k, v in item.items()}
        entries.append(_entry_from_row(normalised, f"清单第 {index} 条"))
    return entries


def _validate_uniqueness(entries: list[ManifestEntry]) -> None:
    paths: dict[str, ManifestEntry] = {}
    uploads: dict[str, ManifestEntry] = {}
    for entry in entries:
        other = paths.get(entry.original_path)
        if other is not None:
            raise CheckError(
                f"{entry.row_label} 的原始路径重复：{entry.original_path}"
                f"（也出现在 {other.row_label}）。",
                files=[entry.uploaded_filename],
            )
        paths[entry.original_path] = entry

        other = uploads.get(entry.uploaded_filename)
        if other is not None:
            raise CheckError(
                f"{entry.row_label} 的上传文件名重复：{entry.uploaded_filename}"
                f"（也出现在 {other.row_label}）。",
                files=[entry.uploaded_filename],
            )
        uploads[entry.uploaded_filename] = entry


def parse_manifest(data: bytes | str) -> Manifest:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CheckError(
                f"清单不是有效的 UTF-8 文本：{exc}"
            ) from None
    else:
        text = data
    stripped = text.lstrip("﻿ \t\r\n")
    if not stripped:
        raise CheckError("清单为空。")
    if stripped.startswith(("[", "{")):
        entries = _parse_json(text)
    else:
        entries = _parse_csv(text)
    if not entries:
        raise CheckError("清单中没有文件条目。")
    _validate_uniqueness(entries)
    return Manifest(entries=entries)
