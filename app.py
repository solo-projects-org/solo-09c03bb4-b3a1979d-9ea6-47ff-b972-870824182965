"""Flask front end for the Iceberg positional-delete checker."""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

from flask import Flask, render_template, request

from checker.engine import CheckResult, run_check
from checker.errors import CheckError
from checker.manifest import parse_manifest

ROOT = Path(__file__).resolve().parent
SAMPLE_DIR = ROOT / "sample"
SAMPLE_TICKETS = "4242, 1003, 4218, 8888, 9999"
SAMPLE_TICKET_COLUMN = "ticket_no"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = (
    int(os.environ.get("MAX_UPLOAD_MB", "512")) * 1024 * 1024
)


def parse_tickets(text: str) -> list[int]:
    tokens = [
        token
        for token in re.split(r"[,\s，、;；]+", (text or "").strip())
        if token
    ]
    if not tokens:
        raise CheckError("请输入至少一个整数票号。")
    tickets: list[int] = []
    for token in tokens:
        if not re.fullmatch(r"[+-]?\d+", token):
            raise CheckError(f"票号 “{token}” 不是整数。")
        tickets.append(int(token))
    return tickets


def _save_uploads(file_storages, temp_dir: Path) -> dict[str, Path]:
    uploaded: dict[str, Path] = {}
    for file_storage in file_storages:
        if file_storage is None or not file_storage.filename:
            continue
        # Browsers occasionally send a client-side path; keep only the name.
        name = os.path.basename(file_storage.filename.replace("\\", "/")).strip()
        if not name:
            raise CheckError("上传的 Parquet 文件名为空。")
        if name in uploaded:
            raise CheckError(
                f"上传文件名重复：{name}。清单中的 uploaded_filename 必须唯一。",
                files=[name],
            )
        destination = temp_dir / f"upload-{len(uploaded) + 1:04d}.parquet"
        try:
            file_storage.save(destination)
        except Exception as exc:
            raise CheckError(
                f"无法保存上传文件 “{name}”：{exc}", files=[name]
            ) from None
        if destination.stat().st_size == 0:
            raise CheckError(f"上传文件 “{name}” 为空。", files=[name])
        uploaded[name] = destination
    return uploaded


def _sample_uploaded_paths(manifest) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for entry in manifest.required_entries:
        candidate = SAMPLE_DIR / entry.uploaded_filename
        if candidate.is_file():
            paths[entry.uploaded_filename] = candidate
    return paths


def _render(
    *,
    result: CheckResult | None = None,
    error: CheckError | None = None,
    tickets_text: str = "",
    ticket_column: str = "ticket_no",
    source: str = "",
    status: int = 200,
):
    return (
        render_template(
            "index.html",
            result=result,
            error=error,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source=source,
            sample_tickets=SAMPLE_TICKETS,
            max_upload_mb=app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024),
        ),
        status,
    )


@app.get("/")
def index():
    return _render()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/sample")
def sample():
    try:
        manifest = parse_manifest((SAMPLE_DIR / "manifest.csv").read_bytes())
        result = run_check(
            manifest,
            _sample_uploaded_paths(manifest),
            parse_tickets(SAMPLE_TICKETS),
            SAMPLE_TICKET_COLUMN,
        )
    except CheckError as exc:
        return _render(
            error=exc,
            tickets_text=SAMPLE_TICKETS,
            ticket_column=SAMPLE_TICKET_COLUMN,
            source="sample",
            status=400,
        )
    return _render(
        result=result,
        tickets_text=SAMPLE_TICKETS,
        ticket_column=SAMPLE_TICKET_COLUMN,
        source="sample",
    )


@app.post("/check")
def check():
    tickets_text = request.form.get("tickets", "")
    ticket_column = request.form.get("ticket_column", "ticket_no").strip()
    manifest_file = request.files.get("manifest")
    parquet_files = request.files.getlist("files")

    if manifest_file is None or not manifest_file.filename:
        return _render(
            error=CheckError("请上传 Iceberg v2 有效文件清单（CSV 或 JSON）。"),
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    try:
        tickets = parse_tickets(tickets_text)
        manifest = parse_manifest(manifest_file.read())
        with tempfile.TemporaryDirectory(prefix="iceberg-pos-delete-") as temp:
            uploaded_paths = _save_uploads(parquet_files, Path(temp))
            result = run_check(
                manifest, uploaded_paths, tickets, ticket_column
            )
    except CheckError as exc:
        return _render(
            error=exc,
            tickets_text=tickets_text,
            ticket_column=ticket_column,
            source="upload",
            status=400,
        )

    return _render(
        result=result,
        tickets_text=tickets_text,
        ticket_column=ticket_column,
        source="upload",
    )


@app.errorhandler(413)
def payload_too_large(_error):
    return _render(
        error=CheckError(
            "上传内容超过大小限制 "
            f"（{app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB）。"
        ),
        status=413,
    )


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        debug=bool(os.environ.get("FLASK_DEBUG")),
    )
