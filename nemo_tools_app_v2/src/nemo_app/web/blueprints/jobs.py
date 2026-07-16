from __future__ import annotations

from pathlib import Path

from flask import Blueprint, abort, jsonify, render_template, send_file, url_for

from ..common import config, job_store

jobs_blueprint = Blueprint("jobs", __name__)


@jobs_blueprint.get("/jobs/<job_id>")
def detail(job_id: str):
    job = job_store().get(job_id)
    if not job:
        abort(404)
    return render_template("job.html", job=job)


@jobs_blueprint.get("/jobs/<job_id>/status")
def status(job_id: str):
    job = job_store().get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    files = [
        {
            "label": Path(relative).name,
            "url": url_for("jobs.download", job_id=job_id, file_index=index),
        }
        for index, relative in enumerate(job.result.get("files", []))
    ]
    return jsonify(
        {
            "id": job.id,
            "title": job.title,
            "summary": job.summary,
            "status": job.status,
            "current": job.current,
            "total": job.total,
            "finished": job.finished,
            "error": job.error,
            "logs": job_store().logs(job.id),
            "files": files,
            "data": job.result.get("data", {}),
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
        }
    )


@jobs_blueprint.get("/jobs/<job_id>/files/<int:file_index>")
def download(job_id: str, file_index: int):
    job = job_store().get(job_id)
    if not job or job.status != "completed":
        abort(404)
    files = list(job.result.get("files", []))
    if file_index < 0 or file_index >= len(files):
        abort(404)
    job_dir = (config().jobs_dir / job_id).resolve()
    path = (job_dir / files[file_index]).resolve()
    if not path.is_relative_to(job_dir) or not path.is_file():
        abort(404)
    return send_file(path, as_attachment=True, download_name=path.name)
