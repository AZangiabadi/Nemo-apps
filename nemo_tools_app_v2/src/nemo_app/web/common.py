from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Any

from flask import current_app, redirect, request, url_for
from werkzeug.datastructures import FileStorage

from nemo_app.jobs.store import JobStore


def config():
    return current_app.extensions["nemo_config"]


def job_store() -> JobStore:
    return current_app.extensions["job_store"]


def save_job_upload(
    job_id: str,
    upload: FileStorage,
    *,
    allowed_suffixes: set[str],
    sequence: int = 1,
) -> tuple[str, str]:
    original = upload.filename or ""
    suffix = Path(original).suffix.lower()
    if suffix not in allowed_suffixes:
        raise ValueError("Upload one of: " + ", ".join(sorted(allowed_suffixes)))
    input_dir = config().jobs_dir / job_id / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    stored_name = f"input_{sequence}{suffix}"
    upload.save(input_dir / stored_name)
    return stored_name, original


def enqueue_upload_job(
    kind: str,
    *,
    title: str,
    upload_specs: list[tuple[FileStorage, set[str]]],
    payload: dict[str, Any],
    secrets: dict[str, str] | None = None,
) -> str:
    job_id = str(uuid.uuid4())
    try:
        stored = [
            save_job_upload(job_id, upload, allowed_suffixes=suffixes, sequence=index)
            for index, (upload, suffixes) in enumerate(upload_specs, 1)
        ]
        payload = {**payload, "stored_uploads": stored}
        job_store().enqueue(
            kind,
            payload=payload,
            secrets=secrets,
            title=title,
            job_id=job_id,
        )
    except Exception:
        shutil.rmtree(config().jobs_dir / job_id, ignore_errors=True)
        raise
    return job_id


def job_redirect(job_id: str):
    return redirect(url_for("jobs.detail", job_id=job_id))


def checkbox(name: str, *, default: bool = False) -> bool:
    if name not in request.form:
        return default
    return request.form.get(name) == "on"
