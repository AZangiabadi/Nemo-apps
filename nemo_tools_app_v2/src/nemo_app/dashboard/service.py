from __future__ import annotations

import datetime as dt
import threading
from dataclasses import dataclass
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from nemo_app.nemo.client import NemoClient


@dataclass(frozen=True, slots=True)
class DashboardReport:
    generated_at: str
    current_usage: tuple[tuple[str, str, str], ...]
    upcoming: tuple[tuple[str, str, str, str, str], ...]
    cancellations: tuple[tuple[str, str, str, str, str], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "current_usage": self.current_usage,
            "upcoming": self.upcoming,
            "cancellations": self.cancellations,
        }


def _datetime(value: object, timezone: ZoneInfo) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(value)) if value else None
    except ValueError:
        return None
    if parsed is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone)
    return parsed


def _format(value: object, timezone: ZoneInfo) -> str:
    parsed = _datetime(value, timezone)
    return parsed.astimezone(timezone).strftime("%a, %b %d, %Y %I:%M %p") if parsed else "—"


class DashboardService:
    def __init__(self, *, timezone: ZoneInfo, cache_seconds: int = 15):
        self.timezone = timezone
        self.cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._cached: tuple[dt.datetime, str, DashboardReport] | None = None

    def report(self, client: NemoClient) -> DashboardReport:
        now = dt.datetime.now(self.timezone)
        with self._lock:
            if self._cached and self._cached[1] == client.identity_hash and now < self._cached[0]:
                return self._cached[2]
        report = self._build(client, now)
        with self._lock:
            self._cached = (
                now + dt.timedelta(seconds=self.cache_seconds),
                client.identity_hash,
                report,
            )
        return report

    def _build(self, client: NemoClient, now: dt.datetime) -> DashboardReport:
        today = dt.datetime.combine(now.date(), dt.time.min, tzinfo=self.timezone)
        tomorrow = today + dt.timedelta(days=1)
        day_after = today + dt.timedelta(days=2)
        usage = client.fetch_all("usage_events/?end__isnull=true")
        query = urlencode({"start__gte": today.isoformat(), "start__lt": day_after.isoformat()})
        reservations = client.fetch_all(f"reservations/?{query}")
        user_ids = {
            value
            for record in [*usage, *reservations]
            for field in ("user", "operator", "creator", "cancelled_by")
            if isinstance((value := record.get(field)), int)
        }
        tool_ids = {
            record["tool"]
            for record in [*usage, *reservations]
            if isinstance(record.get("tool"), int)
        }
        users = client.fetch_by_ids("users/", user_ids)
        tools = client.fetch_by_ids("tools/", tool_ids)

        def username(value: object) -> str:
            return (
                str(users.get(value, {}).get("username") or f"User {value}")
                if isinstance(value, int)
                else "—"
            )

        def tool(value: object) -> str:
            return (
                str(tools.get(value, {}).get("name") or f"Tool {value}")
                if isinstance(value, int)
                else "—"
            )

        current = []
        for event in sorted(
            usage,
            key=lambda value: _datetime(value.get("start"), self.timezone) or now,
        ):
            user_id = (
                event.get("user") if isinstance(event.get("user"), int) else event.get("operator")
            )
            current.append(
                (
                    username(user_id),
                    tool(event.get("tool")),
                    _format(event.get("start"), self.timezone),
                )
            )
        upcoming: list[tuple[dt.datetime, tuple[str, str, str, str, str]]] = []
        cancellations: list[tuple[dt.datetime, tuple[str, str, str, str, str]]] = []
        for reservation in reservations:
            start = _datetime(reservation.get("start"), self.timezone)
            if not start:
                continue
            local_start = start.astimezone(self.timezone)
            if not reservation.get("cancelled") and local_start >= now and local_start < day_after:
                upcoming.append(
                    (
                        local_start,
                        (
                            "Today" if local_start < tomorrow else "Tomorrow",
                            username(reservation.get("user")),
                            tool(reservation.get("tool")),
                            _format(start, self.timezone),
                            _format(reservation.get("end"), self.timezone),
                        ),
                    )
                )
            if reservation.get("cancelled") and today <= local_start < tomorrow:
                cancellation_time = (
                    _datetime(reservation.get("cancellation_time"), self.timezone) or local_start
                )
                cancellations.append(
                    (
                        cancellation_time,
                        (
                            username(reservation.get("user")),
                            tool(reservation.get("tool")),
                            "Missed reservation" if reservation.get("missed") else "User Cancelled",
                            _format(start, self.timezone),
                            _format(reservation.get("cancellation_time"), self.timezone),
                        ),
                    )
                )
        upcoming.sort(key=lambda value: value[0])
        cancellations.sort(key=lambda value: value[0], reverse=True)
        return DashboardReport(
            now.strftime("%a, %b %d, %Y %I:%M %p"),
            tuple(current),
            tuple(value for _, value in upcoming),
            tuple(value for _, value in cancellations),
        )
