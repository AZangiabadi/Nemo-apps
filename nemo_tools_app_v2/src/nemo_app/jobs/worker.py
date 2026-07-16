from __future__ import annotations

import argparse
import os
import signal
import socket
import time
import traceback

from nemo_app.config import AppConfig

from .cleanup import cleanup
from .handlers import run_job
from .store import create_job_store


def work(*, once: bool = False, poll_seconds: float = 1.0) -> int:
    config = AppConfig.from_env()
    config.validate()
    store = create_job_store(config)
    worker_id = f"{socket.gethostname()}:{os.getpid()}"
    stopping = False
    next_recovery = 0.0
    next_cleanup = 0.0

    def stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    processed = 0
    while not stopping:
        monotonic_now = time.monotonic()
        if monotonic_now >= next_recovery:
            store.recover_stale_jobs(config.job_stale_seconds)
            next_recovery = monotonic_now + 60
        if monotonic_now >= next_cleanup:
            cleanup(config, store=store)
            next_cleanup = monotonic_now + 24 * 60 * 60
        job = store.claim_next(worker_id)
        if job is None:
            if once:
                break
            time.sleep(poll_seconds)
            continue
        try:
            run_job(config, store, job)
        except Exception as exc:
            store.fail(
                job.id,
                error=f"{type(exc).__name__}: {exc}",
                log_message=traceback.format_exc(),
            )
        processed += 1
        if once:
            break
    return processed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Process at most one job")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    arguments = parser.parse_args()
    work(once=arguments.once, poll_seconds=arguments.poll_seconds)


if __name__ == "__main__":
    main()
