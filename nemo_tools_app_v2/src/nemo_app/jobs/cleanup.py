from __future__ import annotations

import datetime as dt
import shutil

from nemo_app.config import AppConfig

from .store import JobStore, create_job_store


def cleanup(config: AppConfig, *, store: JobStore | None = None) -> int:
    store = store or create_job_store(config)
    cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(days=config.output_retention_days)
    identifiers = store.expire_before(cutoff)
    for job_id in identifiers:
        shutil.rmtree(config.jobs_dir / job_id, ignore_errors=True)
    return len(identifiers)


def main() -> None:
    count = cleanup(AppConfig.from_env())
    print(f"Expired {count} generated job(s).")


if __name__ == "__main__":
    main()
