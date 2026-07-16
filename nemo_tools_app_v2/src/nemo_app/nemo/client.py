from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any
from urllib.parse import urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class NemoClient:
    """Single NEMO REST client with pagination, retries, and dry-run writes."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = "https://nemo.cni.columbia.edu/api/",
        dry_run: bool = False,
        timeout: float = 60.0,
    ):
        self.token = token.strip()
        self.base_url = base_url.rstrip("/") + "/"
        self.dry_run = dry_run
        self.timeout = timeout
        self.actions: list[str] = []
        self._dry_run_ids: dict[str, int] = {}
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Token {self.token}", "Accept": "application/json"}
        )
        retry = Retry(
            total=3,
            connect=3,
            read=2,
            backoff_factor=0.4,
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    @property
    def identity_hash(self) -> str:
        material = f"{self.base_url}|{self.token}".encode()
        return hashlib.sha256(material).hexdigest()

    def url(self, endpoint: str) -> str:
        return self.base_url + endpoint.lstrip("/")

    def fetch_all(self, endpoint: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        next_url: str | None = self.url(endpoint)
        expected_origin = urlsplit(self.base_url)[:2]
        while next_url:
            response = self.session.get(next_url, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                page = payload
                next_url = None
            elif isinstance(payload, dict) and "results" in payload:
                page = payload.get("results") or []
                next_value = payload.get("next")
                next_url = urljoin(next_url, str(next_value)) if next_value else None
                if next_url and urlsplit(next_url)[:2] != expected_origin:
                    raise ValueError("NEMO pagination attempted to leave the configured API host")
            else:
                raise ValueError(f"Unexpected response from {endpoint}: {type(payload).__name__}")
            records.extend(item for item in page if isinstance(item, dict))
        return records

    def fetch_by_ids(
        self, endpoint: str, ids: Iterable[int], *, chunk_size: int = 100
    ) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        sorted_ids = sorted(set(ids))
        for offset in range(0, len(sorted_ids), chunk_size):
            query = ",".join(str(value) for value in sorted_ids[offset : offset + chunk_size])
            for record in self.fetch_all(f"{endpoint}?id__in={query}"):
                record_id = record.get("id")
                if isinstance(record_id, int):
                    result[record_id] = record
        return result

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.dry_run:
            next_id = self._dry_run_ids.get(endpoint, -1)
            self._dry_run_ids[endpoint] = next_id - 1
            result = {"id": next_id, **payload}
            self.actions.append(f"POST {endpoint} {payload!r}")
            return result
        response = self.session.post(self.url(endpoint), json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def patch(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.dry_run:
            self.actions.append(f"PATCH {endpoint} {payload!r}")
            return dict(payload)
        response = self.session.patch(self.url(endpoint), json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
