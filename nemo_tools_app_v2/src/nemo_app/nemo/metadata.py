from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nemo_app.billing.invoice_model import PIInfo
from nemo_app.billing.text import normalize_item

from .cache import JsonTTLCache
from .client import NemoClient


class MetadataRepository:
    def __init__(self, client: NemoClient, cache: JsonTTLCache):
        self.client = client
        self.cache = cache

    def _load(
        self,
        name: str,
        loader: Callable[[], Any],
        *,
        use_cache: bool,
    ) -> Any:
        key = f"{self.client.identity_hash}:{name}"
        return self.cache.get_or_load(key, loader, use_cache=use_cache)

    def projects(self, *, use_cache: bool = True) -> dict[str, dict[str, Any]]:
        def load() -> dict[str, dict[str, Any]]:
            return {
                str(record["name"]): record
                for record in self.client.fetch_all("projects/")
                if record.get("name")
            }

        return self._load("projects", load, use_cache=use_cache)

    def tools(self, *, use_cache: bool = True) -> dict[int, str]:
        def load() -> dict[str, str]:
            return {
                str(record["id"]): str(record["name"])
                for record in self.client.fetch_all("tools/")
                if isinstance(record.get("id"), int) and record.get("name")
            }

        payload = self._load("tools", load, use_cache=use_cache)
        return {int(key): value for key, value in payload.items()}

    def adjustments(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        return self._load(
            "adjustments",
            lambda: self.client.fetch_all("adjustment_requests/"),
            use_cache=use_cache,
        )

    def consumable_labs(self, *, use_cache: bool = True) -> dict[str, str]:
        def load() -> dict[str, str]:
            result: dict[str, str] = {}
            for record in self.client.fetch_all("consumables/"):
                category = record.get("category")
                lab = "SMCL" if category == 4 else "Cleanroom" if category in {1, 2, 3} else None
                name = normalize_item(record.get("name"))
                if name and lab:
                    result[name] = lab
            return result

        return self._load("consumables", load, use_cache=use_cache)

    def users(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        return self._load("users", lambda: self.client.fetch_all("users/"), use_cache=use_cache)


def _last_first(name: str) -> str:
    clean = (name or "").strip()
    if not clean or "," in clean:
        return clean
    parts = clean.split()
    return parts[0] if len(parts) == 1 else f"{parts[-1]}, {' '.join(parts[:-1])}"


def project_pi(project_name: str, projects: dict[str, dict[str, Any]]) -> PIInfo:
    project = projects.get(project_name)
    fallback = project_name.split()[-1] if project_name.split() else "UNKNOWN_PI"
    if not project:
        return PIInfo(fallback, fallback)
    email = str(project.get("contact_email") or "").strip().lower()
    name = _last_first(str(project.get("contact_name") or ""))
    return PIInfo(email or fallback, name or email or fallback, email)
