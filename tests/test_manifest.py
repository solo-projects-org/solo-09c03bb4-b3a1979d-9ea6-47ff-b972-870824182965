from __future__ import annotations

import json
from pathlib import Path

import pytest

from checker.errors import CheckError
from checker.manifest import FileKind, parse_manifest

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


def test_sample_csv_manifest():
    manifest = parse_manifest((SAMPLE / "manifest.csv").read_bytes())
    assert len(manifest.entries) == 6
    assert len(manifest.data_files) == 2
    assert len(manifest.position_delete_files) == 3
    assert len(manifest.equality_delete_files) == 1

    data1 = manifest.data_files[0]
    assert data1.original_path.endswith("/data/data-0001.parquet")
    assert data1.uploaded_filename == "data-0001.parquet"
    assert data1.kind is FileKind.DATA
    assert data1.sequence_number == 7


def test_json_manifest_and_aliases():
    payload = [
        {
            "path": "s3://bucket/data/a.parquet",
            "filename": "a.parquet",
            "type": "0",
            "seq": 3,
        },
        {
            "path": "s3://bucket/delete/d.parquet",
            "filename": "d.parquet",
            "content": "1",
            "sequence_number": 5,
        },
    ]
    manifest = parse_manifest(json.dumps(payload))
    assert [e.kind for e in manifest.entries] == [
        FileKind.DATA,
        FileKind.POSITION_DELETE,
    ]


def test_json_object_with_files_key():
    payload = {
        "files": [
            {
                "original_path": "s3://bucket/a.parquet",
                "uploaded_filename": "a.parquet",
                "file_type": "data",
                "data_sequence_number": 2,
            }
        ]
    }
    manifest = parse_manifest(json.dumps(payload))
    assert len(manifest.data_files) == 1


def test_unknown_file_type_is_rejected():
    csv = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/a.parquet,a.parquet,data,1\n"
        "s3://bucket/x.parquet,x.parquet,manifest,2\n"
    )
    with pytest.raises(CheckError, match="无法识别"):
        parse_manifest(csv)


def test_duplicate_paths_are_rejected():
    csv = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/a.parquet,a.parquet,data,1\n"
        "s3://bucket/a.parquet,b.parquet,data,2\n"
    )
    with pytest.raises(CheckError, match="原始路径重复"):
        parse_manifest(csv)


def test_duplicate_uploaded_names_are_rejected():
    csv = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://bucket/a.parquet,a.parquet,data,1\n"
        "s3://bucket/b.parquet,a.parquet,data,2\n"
    )
    with pytest.raises(CheckError, match="上传文件名重复"):
        parse_manifest(csv)


@pytest.mark.parametrize("sequence", ["abc", "1.5", "-1"])
def test_bad_sequence_is_rejected(sequence):
    csv = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        f"s3://bucket/a.parquet,a.parquet,data,{sequence}\n"
    )
    with pytest.raises(CheckError, match="序列号"):
        parse_manifest(csv)


def test_missing_column_is_rejected():
    csv = (
        "original_path,uploaded_filename,file_type\n"
        "s3://bucket/a.parquet,a.parquet,data\n"
    )
    with pytest.raises(CheckError, match="缺少必需列"):
        parse_manifest(csv)


def test_empty_manifest_is_rejected():
    with pytest.raises(CheckError, match="清单为空"):
        parse_manifest("")
