from __future__ import annotations

import datetime as dt
import time
from typing import Any

import requests


class AngstromLogClient:
    """Read indexed gold-deposition runs from the Angstrom log dashboard."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15.0,
        poll_interval: float = 0.5,
        max_wait: float = 90.0,
    ):
        if not base_url.strip():
            raise ValueError("Angstrom log application URL is required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    @staticmethod
    def _json(response: requests.Response) -> dict[str, Any]:
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Angstrom log application returned an unexpected response")
        return payload

    def fetch_gold_runs(self, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
        if start > end:
            raise ValueError("Angstrom log query start must be on or before end")
        created = self._json(
            self.session.post(
                self._url("api/scans"),
                json={"start": start.isoformat(), "end": end.isoformat()},
                timeout=self.timeout,
            )
        )
        job_id = str(created.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("Angstrom log application did not return a scan job ID")

        deadline = time.monotonic() + self.max_wait
        while time.monotonic() < deadline:
            job = self._json(
                self.session.get(self._url(f"api/scans/{job_id}"), timeout=self.timeout)
            )
            state = str(job.get("state") or "").strip().lower()
            if state == "complete":
                result = job.get("result")
                runs = result.get("runs") if isinstance(result, dict) else None
                if not isinstance(runs, list):
                    raise ValueError("Angstrom log scan completed without a run list")
                return [run for run in runs if isinstance(run, dict)]
            if state == "error":
                raise RuntimeError(str(job.get("message") or "Angstrom log scan failed"))
            time.sleep(self.poll_interval)
        raise TimeoutError("Timed out waiting for the Angstrom log scan")
