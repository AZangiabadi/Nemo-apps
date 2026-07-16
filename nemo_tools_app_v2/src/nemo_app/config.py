from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = PACKAGE_DIR.parents[1]
REPOSITORY_DIR = PROJECT_DIR.parent


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw_value = os.environ.get(name, str(default)).strip()
    try:
        return max(minimum, int(raw_value))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, not {raw_value!r}") from exc


def _timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"NEMO_TIMEZONE is not a known timezone: {name!r}") from exc


@dataclass(frozen=True, slots=True)
class AppConfig:
    environment: str
    base_dir: Path
    data_dir: Path
    asset_dir: Path
    nemo_base_url: str
    timezone_name: str
    timezone: ZoneInfo
    port: int
    debug: bool
    access_password: str
    flask_secret_key: str
    job_secret_key: str
    jumbotron_api_token: str
    jumbotron_refresh_seconds: int
    jumbotron_cache_seconds: int
    jumbotron_scroll_step_px: int
    jumbotron_scroll_interval_ms: int
    output_retention_days: int
    metadata_cache_seconds: int
    job_stale_seconds: int

    @classmethod
    def from_env(cls, *, base_dir: Path | None = None) -> AppConfig:
        root = (base_dir or PROJECT_DIR).resolve()
        load_dotenv(root / ".env")
        load_dotenv(REPOSITORY_DIR / ".env")
        timezone_name = (
            os.environ.get("NEMO_TIMEZONE", "America/New_York").strip() or "America/New_York"
        )
        data_dir = Path(os.environ.get("NEMO_DATA_DIR", root / "data")).expanduser()
        asset_dir = Path(os.environ.get("NEMO_ASSET_DIR", REPOSITORY_DIR)).expanduser()
        return cls(
            environment=os.environ.get("NEMO_ENV", "development").strip().lower(),
            base_dir=root,
            data_dir=data_dir.resolve(),
            asset_dir=asset_dir.resolve(),
            nemo_base_url=os.environ.get("NEMO_BASE_URL", "https://nemo.cni.columbia.edu").rstrip(
                "/"
            ),
            timezone_name=timezone_name,
            timezone=_timezone(timezone_name),
            port=_env_int("PORT", 8000, minimum=1),
            debug=_env_bool("FLASK_DEBUG"),
            access_password=os.environ.get("NEMO_APP_ACCESS_PASSWORD", ""),
            flask_secret_key=os.environ.get("NEMO_FLASK_SECRET_KEY", "dev-only-secret"),
            job_secret_key=os.environ.get("NEMO_JOB_SECRET_KEY", ""),
            jumbotron_api_token=os.environ.get("NEMO_JUMBOTRON_API_TOKEN", ""),
            jumbotron_refresh_seconds=_env_int("NEMO_JUMBOTRON_REFRESH_SECONDS", 15, minimum=5),
            jumbotron_cache_seconds=_env_int("NEMO_JUMBOTRON_CACHE_SECONDS", 15),
            jumbotron_scroll_step_px=_env_int("NEMO_JUMBOTRON_SCROLL_STEP_PX", 1, minimum=1),
            jumbotron_scroll_interval_ms=_env_int(
                "NEMO_JUMBOTRON_SCROLL_INTERVAL_MS", 50, minimum=10
            ),
            output_retention_days=_env_int("NEMO_OUTPUT_RETENTION_DAYS", 14),
            metadata_cache_seconds=_env_int("NEMO_METADATA_CACHE_SECONDS", 21600),
            job_stale_seconds=_env_int("NEMO_JOB_STALE_SECONDS", 3600, minimum=60),
        )

    def validate(self) -> None:
        if self.environment != "production":
            return
        errors: list[str] = []
        if not self.access_password:
            errors.append("NEMO_APP_ACCESS_PASSWORD is required")
        if len(self.flask_secret_key) < 32 or self.flask_secret_key == "dev-only-secret":
            errors.append("NEMO_FLASK_SECRET_KEY must contain at least 32 random characters")
        if not self.job_secret_key:
            errors.append("NEMO_JOB_SECRET_KEY is required")
        if not self.nemo_base_url.startswith("https://"):
            errors.append("NEMO_BASE_URL must use HTTPS")
        if errors:
            raise ValueError("Invalid production configuration: " + "; ".join(errors))

    @property
    def api_base_url(self) -> str:
        return f"{self.nemo_base_url}/api/"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "jobs.sqlite3"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def jobs_dir(self) -> Path:
        return self.data_dir / "jobs"

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
