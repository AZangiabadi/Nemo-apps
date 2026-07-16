from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


class JsonTTLCache:
    """Small process-safe-enough metadata cache using atomic file replacement."""

    def __init__(self, directory: Path, ttl_seconds: int = 21600):
        self.directory = directory
        self.ttl_seconds = max(0, ttl_seconds)
        self._lock = threading.RLock()

    def _path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.directory / f"{digest}.json"

    def get(self, key: str) -> Any | None:
        if not self.ttl_seconds:
            return None
        path = self._path(key)
        if not path.exists():
            return None
        with self._lock:
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                if float(record["expires_at"]) <= time.time():
                    path.unlink(missing_ok=True)
                    return None
                return record["value"]
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                path.unlink(missing_ok=True)
                return None

    def set(self, key: str, value: Any) -> None:
        if not self.ttl_seconds:
            return
        with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self._path(key)
            temporary = path.with_suffix(f".{os.getpid()}.{threading.get_ident()}.tmp")
            temporary.write_text(
                json.dumps(
                    {"expires_at": time.time() + self.ttl_seconds, "value": value},
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            temporary.replace(path)

    def get_or_load(self, key: str, loader: Callable[[], Any], *, use_cache: bool) -> Any:
        if use_cache:
            cached = self.get(key)
            if cached is not None:
                return cached
        value = loader()
        self.set(key, value)
        return value
