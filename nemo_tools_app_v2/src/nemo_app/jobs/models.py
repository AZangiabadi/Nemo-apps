from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

TERMINAL_STATUSES = {"completed", "failed", "expired"}


@dataclass(frozen=True, slots=True)
class Job:
    id: str
    kind: str
    status: str
    title: str
    summary: str
    current: int
    total: int
    payload: dict[str, Any]
    result: dict[str, Any]
    error: str
    created_at: str
    started_at: str
    finished_at: str
    worker_id: str
    secrets: dict[str, str] = field(default_factory=dict, repr=False)

    @property
    def finished(self) -> bool:
        return self.status in TERMINAL_STATUSES
