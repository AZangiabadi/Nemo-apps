import csv
import copy
import hashlib
import json
import random
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import openpyxl
import requests

if TYPE_CHECKING:
    from tkinter import Tk


BASE_URL = "https://nemo.cni.columbia.edu/api/"
TIMEOUT = 240
IMPORT_LOOKUP_CACHE_TTL_SECONDS = 300
IMPORT_LOOKUP_CACHE_DIR = Path(__file__).resolve().parent / ".import_lookup_cache"


ACCOUNT_TYPE_MAP = {
    "cdg": 1,
    "industry": 2,
    "external academic": 3,
    "local": 4,
}

USER_TYPE_MAP = {
    "local": 1,
    "cdg": 1,
    "external academic": 2,
    "industry": 4,
}

PROJECT_TYPE_MAP = {
    "cdg": 2,
    "local": 3,
    "external academic": 4,
    "industry": 5,
}

PROJECT_CATEGORY_MAP = {
    "cdg": 6,
    "external academic": 1,
    "industry": 2,
    "local": 3,
}

EXPECTED_HEADERS = {
    "name",
    "uni",
    "email",
    "pi",
    "account type",
    "project number",
}

StatusCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int, str], None]

IMPORT_LOOKUP_CACHE_LOCK = threading.Lock()
IMPORT_LOOKUP_CACHE: dict[tuple[str, str], tuple[float, tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
]]] = {}


@dataclass
class SpreadsheetRow:
    row_number: int
    name: str
    uni: str
    email: str
    pi_value: str
    account_type_label: str
    project_number: str

    @property
    def normalized_account_type(self) -> str:
        return normalize_account_type(self.account_type_label)

    @property
    def is_pi_row(self) -> bool:
        return self.pi_value.strip().lower() == "pi"


class NemoClient:
    def __init__(self, token: str, base_url: str = BASE_URL, dry_run: bool = False):
        self.token = token.strip()
        self.base_url = base_url.rstrip("/") + "/"
        self.dry_run = dry_run
        self._dry_run_ids = {
            "accounts/": -1,
            "users/": -1,
            "projects/": -1,
        }
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Token {self.token}",
                "Content-Type": "application/json",
            }
        )

    def _url(self, endpoint: str) -> str:
        return self.base_url + endpoint.lstrip("/")

    def cache_key(self) -> tuple[str, str]:
        token = self.session.headers.get("Authorization", "")
        return (
            self.base_url,
            hashlib.sha256(token.encode("utf-8")).hexdigest(),
        )

    def fetch_all(self, endpoint: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        url = self._url(endpoint)

        while url:
            response = self.session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                items.extend(data)
                url = ""
            elif isinstance(data, dict) and "results" in data:
                items.extend(data.get("results", []))
                url = data.get("next") or ""
            else:
                raise ValueError(f"Unexpected response shape from {endpoint}: {data!r}")

        return items

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.dry_run:
            next_id = self._dry_run_ids.setdefault(endpoint, -1)
            self._dry_run_ids[endpoint] = next_id - 1
            preview_payload = {"id": next_id, **payload}
            print(f"[DRY RUN] POST {endpoint} -> {preview_payload}")
            return preview_payload
        response = self.session.post(self._url(endpoint), json=payload, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()

    def patch(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.dry_run:
            print(f"[DRY RUN] PATCH {endpoint} -> {payload}")
            return payload
        response = self.session.patch(
            self._url(endpoint), json=payload, timeout=TIMEOUT
        )
        response.raise_for_status()
        return response.json()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value)).strip()
    return str(value).strip()


def normalize_email(value: Any) -> str:
    return normalize_text(value).lower()


def normalize_account_type(value: Any) -> str:
    normalized = normalize_text(value).lower()
    return " ".join(normalized.split())


def split_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in normalize_text(full_name).split() if part]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def unique_sorted(values: list[int] | set[int]) -> list[int]:
    return sorted({value for value in values if value is not None})


def initials_for_generated_uni(full_name: str) -> str:
    first_name, last_name = split_name(full_name)
    first_initial = first_name[:1].lower() or "x"
    last_initial = last_name[:1].lower() or first_initial
    return f"{first_initial}{last_initial}"


def generate_missing_uni(full_name: str, used_usernames: set[str]) -> str:
    initials = initials_for_generated_uni(full_name)
    while True:
        candidate = f"xx{initials}{random.randint(10, 99)}"
        if candidate.lower() not in used_usernames:
            used_usernames.add(candidate.lower())
            return candidate


def fill_missing_unis(rows: list[SpreadsheetRow], used_usernames: set[str]) -> None:
    generated_by_email: dict[str, str] = {}

    for row in rows:
        if row.uni:
            used_usernames.add(row.uni.lower())
            continue

        generated_uni = generated_by_email.get(row.email)
        if not generated_uni:
            generated_uni = generate_missing_uni(row.name, used_usernames)
            generated_by_email[row.email] = generated_uni

        row.uni = generated_uni
        print(f"Generated UNI '{generated_uni}' for {row.email} (row {row.row_number})")


class Tee:
    def __init__(self, *streams: Any):
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


def create_dialog_root() -> "Tk":
    from tkinter import Tk

    root = Tk()
    try:
        # On macOS, parenting dialogs to a withdrawn root can leave the first
        # modal stuck behind the active app. Keep a tiny transparent root alive
        # so Tk has a real foregroundable window to attach dialogs to.
        root.title("NEMO Importer")
        root.geometry("1x1+0+0")
        root.attributes("-alpha", 0.0)
        root.deiconify()
        root.attributes("-topmost", True)
        root.lift()
        root.focus_force()
        root.update()
        root.after_idle(lambda: root.attributes("-topmost", False))
    except Exception:
        pass
    return root


def prepare_dialog(root: "Tk") -> None:
    try:
        root.lift()
        root.focus_force()
        root.update()
    except Exception:
        pass


def choose_input_file(root: "Tk") -> str:
    from tkinter import filedialog

    prepare_dialog(root)
    return filedialog.askopenfilename(
        parent=root,
        title="Select Excel or CSV file",
        filetypes=[
            ("Spreadsheet files", "*.xlsx *.csv"),
            ("Excel files", "*.xlsx"),
            ("CSV files", "*.csv"),
            ("All files", "*.*"),
        ],
    )


def prompt_for_token(root: "Tk") -> str | None:
    from tkinter import simpledialog

    prepare_dialog(root)
    return simpledialog.askstring(
        "NEMO Token",
        "Enter your NEMO API token:",
        parent=root,
        show="*",
    )


def prompt_for_dry_run(root: "Tk") -> bool | None:
    from tkinter import messagebox

    prepare_dialog(root)
    return messagebox.askyesnocancel(
        "Dry Run",
        "Run in Dry Run mode?\n\n"
        "Yes: preview API actions only\n"
        "No: send changes to NEMO\n"
        "Cancel: exit",
        parent=root,
    )


def read_rows_from_csv(path: Path) -> list[list[Any]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if not rows:
        raise ValueError("The CSV file is empty.")
    return rows


def read_rows_from_excel(path: Path) -> list[list[Any]]:
    workbook = openpyxl.load_workbook(path, data_only=True)
    sheet = workbook.active
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        raise ValueError("The Excel file is empty.")
    return [list(row) for row in rows]


def locate_header_row(rows: list[list[Any]]) -> tuple[int, dict[str, int]]:
    for row_index, row in enumerate(rows):
        headers = [normalize_text(cell) for cell in row]
        header_index: dict[str, int] = {}
        for index, header in enumerate(headers):
            normalized = normalize_text(header).lower()
            if normalized:
                header_index[normalized] = index

        if EXPECTED_HEADERS.issubset(header_index):
            return row_index, header_index

    raise ValueError(
        "Could not find a header row containing: " + ", ".join(sorted(EXPECTED_HEADERS))
    )


def load_spreadsheet(path: Path) -> list[SpreadsheetRow]:
    if path.suffix.lower() == ".csv":
        rows = read_rows_from_csv(path)
    else:
        rows = read_rows_from_excel(path)

    header_row_index, header_index = locate_header_row(rows)
    parsed_rows: list[SpreadsheetRow] = []

    for offset, row in enumerate(
        rows[header_row_index + 1 :], start=header_row_index + 2
    ):
        name = normalize_text(
            row[header_index["name"]] if len(row) > header_index["name"] else ""
        )
        uni = normalize_text(
            row[header_index["uni"]] if len(row) > header_index["uni"] else ""
        )
        email = normalize_email(
            row[header_index["email"]] if len(row) > header_index["email"] else ""
        )
        pi_value = normalize_text(
            row[header_index["pi"]] if len(row) > header_index["pi"] else ""
        )
        account_type_label = normalize_text(
            row[header_index["account type"]]
            if len(row) > header_index["account type"]
            else ""
        )
        project_number = normalize_text(
            row[header_index["project number"]]
            if len(row) > header_index["project number"]
            else ""
        )

        if not any([name, uni, email, pi_value, account_type_label, project_number]):
            continue

        parsed_rows.append(
            SpreadsheetRow(
                row_number=offset,
                name=name,
                uni=uni,
                email=email,
                pi_value=pi_value,
                account_type_label=account_type_label,
                project_number=project_number,
            )
        )

    if not parsed_rows:
        raise ValueError("No usable data rows were found in the spreadsheet.")

    return parsed_rows


def validate_rows(rows: list[SpreadsheetRow]) -> list[str]:
    errors: list[str] = []

    for row in rows:
        if not row.name and not row.uni:
            errors.append(f"Row {row.row_number}: missing Name and UNI")
        if not row.email and not row.uni:
            errors.append(f"Row {row.row_number}: missing email and UNI")
        if not row.project_number:
            errors.append(f"Row {row.row_number}: missing Project Number")
        normalized_type = row.normalized_account_type
        if normalized_type not in ACCOUNT_TYPE_MAP:
            errors.append(
                f"Row {row.row_number}: unsupported Account type '{row.account_type_label}'"
            )

    return errors


def emit_status(callback: StatusCallback | None, message: str) -> None:
    if callback:
        callback(message)


def _clone_existing_maps(
    value: tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        set[str],
    ],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
]:
    accounts_by_name, users_by_email, users_by_username, projects_by_name, usernames = value
    return (
        copy.deepcopy(accounts_by_name),
        copy.deepcopy(users_by_email),
        copy.deepcopy(users_by_username),
        copy.deepcopy(projects_by_name),
        set(usernames),
    )


def _persistent_import_cache_path(cache_key: tuple[str, str]) -> Path:
    base_url, token_hash = cache_key
    stable_name = hashlib.sha256(
        f"{base_url}|{token_hash}".encode("utf-8")
    ).hexdigest()
    return IMPORT_LOOKUP_CACHE_DIR / f"{stable_name}.json"


def _serialize_existing_maps(
    value: tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        set[str],
    ],
) -> dict[str, Any]:
    (
        accounts_by_name,
        users_by_email,
        users_by_username,
        projects_by_name,
        usernames,
    ) = value
    return {
        "accounts_by_name": accounts_by_name,
        "users_by_email": users_by_email,
        "users_by_username": users_by_username,
        "projects_by_name": projects_by_name,
        "usernames": sorted(usernames),
    }


def _deserialize_existing_maps(
    payload: dict[str, Any],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
]:
    return (
        dict(payload.get("accounts_by_name", {})),
        dict(payload.get("users_by_email", {})),
        dict(payload.get("users_by_username", {})),
        dict(payload.get("projects_by_name", {})),
        set(payload.get("usernames", [])),
    )


def load_persistent_cached_existing_maps(
    client: NemoClient,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
] | None:
    cache_key = client.cache_key()
    cache_path = _persistent_import_cache_path(cache_key)
    if not cache_path.exists():
        return None

    try:
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    expires_at = cached.get("expires_at")
    if not isinstance(expires_at, (int, float)) or expires_at <= time.time():
        cache_path.unlink(missing_ok=True)
        return None

    payload = _deserialize_existing_maps(dict(cached.get("payload", {})))
    with IMPORT_LOOKUP_CACHE_LOCK:
        IMPORT_LOOKUP_CACHE[cache_key] = (
            float(expires_at),
            _clone_existing_maps(payload),
        )
    return _clone_existing_maps(payload)


def load_cached_existing_maps(
    client: NemoClient,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
] | None:
    cache_key = client.cache_key()
    now = time.time()
    with IMPORT_LOOKUP_CACHE_LOCK:
        cached = IMPORT_LOOKUP_CACHE.get(cache_key)
        if not cached:
            cached = None
        else:
            expires_at, payload = cached
            if expires_at <= now:
                IMPORT_LOOKUP_CACHE.pop(cache_key, None)
                cached = None
            else:
                return _clone_existing_maps(payload)
    return load_persistent_cached_existing_maps(client)


def store_cached_existing_maps(
    client: NemoClient,
    value: tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        set[str],
    ],
) -> None:
    cache_key = client.cache_key()
    payload = _clone_existing_maps(value)
    with IMPORT_LOOKUP_CACHE_LOCK:
        IMPORT_LOOKUP_CACHE[cache_key] = (
            time.time() + IMPORT_LOOKUP_CACHE_TTL_SECONDS,
            payload,
        )
    try:
        IMPORT_LOOKUP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path = _persistent_import_cache_path(cache_key)
        expires_at = time.time() + IMPORT_LOOKUP_CACHE_TTL_SECONDS
        serialized = {
            "expires_at": expires_at,
            "payload": _serialize_existing_maps(payload),
        }
        temp_path = cache_path.with_name(
            f"{cache_path.stem}.{threading.get_ident()}.tmp"
        )
        temp_path.write_text(json.dumps(serialized), encoding="utf-8")
        temp_path.replace(cache_path)
    except Exception:
        # Keep the in-memory cache working even if disk persistence is unavailable.
        pass


def get_existing_maps(
    client: NemoClient,
    use_cache: bool = True,
    status_callback: StatusCallback | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    set[str],
]:
    if use_cache:
        cached_maps = load_cached_existing_maps(client)
        if cached_maps is not None:
            emit_status(
                status_callback,
                "Using cached accounts, users, and projects from a recent NEMO import",
            )
            return cached_maps

    emit_status(status_callback, "Fetching existing accounts from the NEMO API")
    accounts = client.fetch_all("accounts/")
    emit_status(status_callback, "Fetching existing users from the NEMO API")
    users = client.fetch_all("users/")
    emit_status(status_callback, "Fetching existing projects from the NEMO API")
    projects = client.fetch_all("projects/")

    accounts_by_name = {
        normalize_text(account.get("name")).lower(): account
        for account in accounts
        if normalize_text(account.get("name"))
    }
    users_by_email = {
        normalize_email(user.get("email")): user
        for user in users
        if normalize_email(user.get("email"))
    }
    users_by_username = {
        normalize_text(user.get("username")).lower(): user
        for user in users
        if normalize_text(user.get("username"))
    }
    usernames = {
        normalize_text(user.get("username")).lower()
        for user in users
        if normalize_text(user.get("username"))
    }
    projects_by_name = {
        normalize_text(project.get("name")).lower(): project
        for project in projects
        if normalize_text(project.get("name"))
    }
    result = (
        accounts_by_name,
        users_by_email,
        users_by_username,
        projects_by_name,
        usernames,
    )
    store_cached_existing_maps(client, result)
    return _clone_existing_maps(result)


def existing_user_for_row(
    row: SpreadsheetRow,
    users_by_email: dict[str, dict[str, Any]],
    users_by_username: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if row.email:
        existing_user = users_by_email.get(row.email)
        if existing_user:
            return existing_user
    if row.uni:
        existing_user = users_by_username.get(row.uni.lower())
        if existing_user:
            print(
                f"Matched existing user by UNI for row {row.row_number}: "
                f"username={row.uni}"
            )
        return existing_user
    return None


def display_name_for_user(user: dict[str, Any]) -> str:
    first_name = normalize_text(user.get("first_name"))
    last_name = normalize_text(user.get("last_name"))
    full_name = " ".join(part for part in (first_name, last_name) if part)
    return full_name


def populate_missing_row_fields_from_existing_user(
    row: SpreadsheetRow,
    existing_user: dict[str, Any],
) -> None:
    existing_email = normalize_email(existing_user.get("email"))
    existing_username = normalize_text(existing_user.get("username"))
    existing_name = display_name_for_user(existing_user)

    if not row.email and existing_email:
        row.email = existing_email
        print(
            f"Filled missing email for row {row.row_number} from existing user "
            f"'{existing_username or existing_email}'."
        )
    if not row.uni and existing_username:
        row.uni = existing_username
    if not row.name and existing_name:
        row.name = existing_name
        print(
            f"Filled missing name for row {row.row_number} from existing user "
            f"'{existing_username or existing_email}'."
        )


def populate_missing_row_fields_from_existing_users(
    rows: list[SpreadsheetRow],
    users_by_email: dict[str, dict[str, Any]],
    users_by_username: dict[str, dict[str, Any]],
) -> None:
    for row in rows:
        existing_user = existing_user_for_row(row, users_by_email, users_by_username)
        if not existing_user:
            continue

        populate_missing_row_fields_from_existing_user(row, existing_user)


def build_account_payload(project_number: str, account_type: str) -> dict[str, Any]:
    return {
        "name": project_number,
        "note": "",
        "start_date": date.today().isoformat(),
        "active": True,
        "type": ACCOUNT_TYPE_MAP[account_type],
    }


def build_user_payload(
    row: SpreadsheetRow,
    user_type: int,
    managed_account_ids: list[int] | None = None,
    project_ids: list[int] | None = None,
    managed_project_ids: list[int] | None = None,
) -> dict[str, Any]:
    first_name, last_name = split_name(row.name)
    now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
    return {
        "username": row.uni,
        "first_name": first_name,
        "last_name": last_name,
        "email": row.email,
        "domain": "",
        "notes": "",
        "badge_number": None,
        "access_expiration": None,
        "is_active": True,
        "is_staff": False,
        "is_user_office": False,
        "is_accounting_officer": False,
        "is_service_personnel": False,
        "is_technician": False,
        "is_facility_manager": False,
        "is_superuser": False,
        "training_required": False,
        "date_joined": now_iso,
        "type": user_type,
        "projects": unique_sorted(project_ids or []),
        "managed_projects": unique_sorted(managed_project_ids or []),
        "managed_accounts": unique_sorted(managed_account_ids or []),
        "managed_users": [],
        "emergency_contact": "",
    }


def build_project_payload(
    row: SpreadsheetRow,
    account_id: int,
    pi_user_id: int,
) -> dict[str, Any]:
    return {
        "principal_investigators": [pi_user_id],
        "users": [pi_user_id],
        "name": row.project_number,
        "application_identifier": row.account_type_label,
        "start_date": date.today().isoformat(),
        "active": True,
        "allow_consumable_withdrawals": True,
        "allow_staff_charges": True,
        "account": account_id,
        "discipline": None,
        "project_types": [PROJECT_TYPE_MAP[row.normalized_account_type]],
        "only_allow_tools": [],
        "project_name": None,
        "contact_name": row.name,
        "contact_phone": None,
        "contact_email": row.email,
        "addressee": f"{row.name}\r\n{row.email}",
        "comments": "",
        "no_charge": False,
        "no_tax": False,
        "no_cap": False,
        "category": PROJECT_CATEGORY_MAP[row.normalized_account_type],
        "institution": None,
        "department": None,
        "staff_host": None,
    }


def merge_user_relationships(
    user: dict[str, Any],
    *,
    project_ids: list[int] | None = None,
    managed_project_ids: list[int] | None = None,
    managed_account_ids: list[int] | None = None,
) -> dict[str, list[int]]:
    payload: dict[str, list[int]] = {}
    if project_ids is not None:
        payload["projects"] = unique_sorted(
            list(user.get("projects", [])) + project_ids
        )
    if managed_project_ids is not None:
        payload["managed_projects"] = unique_sorted(
            list(user.get("managed_projects", [])) + managed_project_ids
        )
    if managed_account_ids is not None:
        payload["managed_accounts"] = unique_sorted(
            list(user.get("managed_accounts", [])) + managed_account_ids
        )
    return payload


def patch_if_changed(
    client: NemoClient,
    endpoint: str,
    user: dict[str, Any],
    patch_payload: dict[str, list[int]],
) -> dict[str, Any]:
    changed_payload: dict[str, list[int]] = {}
    for key, value in patch_payload.items():
        current_value = unique_sorted(user.get(key, []))
        if current_value != value:
            changed_payload[key] = value

    if not changed_payload:
        return user

    return client.patch(endpoint, changed_payload)


def account_id_for_project(
    account_ids_by_project_number: dict[str, int], project_number: str
) -> int:
    account_id = account_ids_by_project_number.get(project_number)
    if account_id is not None:
        return account_id

    lowered = project_number.lower()
    account_id = account_ids_by_project_number.get(lowered)
    if account_id is None:
        raise KeyError(f"No account id found for project '{project_number}'")
    return account_id


def refresh_accounts(client: NemoClient) -> dict[str, dict[str, Any]]:
    if client.dry_run:
        return {}
    accounts = client.fetch_all("accounts/")
    return {
        normalize_text(account.get("name")).lower(): account
        for account in accounts
        if normalize_text(account.get("name"))
    }


def refresh_users(client: NemoClient) -> dict[str, dict[str, Any]]:
    if client.dry_run:
        return {}
    users = client.fetch_all("users/")
    return {
        normalize_email(user.get("email")): user
        for user in users
        if normalize_email(user.get("email"))
    }


def refresh_projects(client: NemoClient) -> dict[str, dict[str, Any]]:
    if client.dry_run:
        return {}
    projects = client.fetch_all("projects/")
    return {
        normalize_text(project.get("name")).lower(): project
        for project in projects
        if normalize_text(project.get("name"))
    }


def refresh_existing_maps_cache(client: NemoClient) -> None:
    print("Refreshing in-memory NEMO import cache in the background...")
    accounts_by_name = refresh_accounts(client)
    users_by_email = refresh_users(client)
    users_by_username = {
        normalize_text(user.get("username")).lower(): user
        for user in users_by_email.values()
        if normalize_text(user.get("username"))
    }
    projects_by_name = refresh_projects(client)
    usernames = {
        normalize_text(user.get("username")).lower()
        for user in users_by_email.values()
        if normalize_text(user.get("username"))
    }
    store_cached_existing_maps(
        client,
        (
            accounts_by_name,
            users_by_email,
            users_by_username,
            projects_by_name,
            usernames,
        ),
    )
    print("Background NEMO import cache refresh complete.")


def import_accounts(
    client: NemoClient,
    rows: list[SpreadsheetRow],
    accounts_by_name: dict[str, dict[str, Any]],
    status_callback: StatusCallback | None = None,
) -> dict[str, int]:
    account_ids_by_project_number: dict[str, int] = {}
    seen_projects: set[str] = set()
    unique_project_count = len({row.project_number.lower() for row in rows if row.project_number})
    emit_status(
        status_callback,
        f"Processing {unique_project_count} project account(s)",
    )

    for row in rows:
        project_number = row.project_number
        project_key = project_number.lower()
        if project_key in seen_projects:
            continue
        seen_projects.add(project_key)

        existing_account = accounts_by_name.get(project_key)
        if existing_account:
            print(
                f"Account already exists for '{project_number}': "
                f"{existing_account.get('name')} (id={existing_account.get('id')})"
            )
            account_ids_by_project_number[project_number] = existing_account["id"]
            account_ids_by_project_number[project_key] = existing_account["id"]
            continue

        payload = build_account_payload(project_number, row.normalized_account_type)
        created_account = client.post("accounts/", payload)
        print(
            f"Created account '{created_account.get('name')}' "
            f"(id={created_account.get('id')})"
        )
        account_ids_by_project_number[project_number] = created_account["id"]
        account_ids_by_project_number[project_key] = created_account["id"]

    return account_ids_by_project_number


def import_pis(
    client: NemoClient,
    rows: list[SpreadsheetRow],
    users_by_email: dict[str, dict[str, Any]],
    users_by_username: dict[str, dict[str, Any]],
    account_ids_by_project_number: dict[str, int],
    status_callback: StatusCallback | None = None,
) -> dict[str, dict[str, Any]]:
    pi_rows = [row for row in rows if row.is_pi_row]
    emit_status(status_callback, f"Processing {len(pi_rows)} PI row(s)")

    for row in pi_rows:
        account_id = account_id_for_project(
            account_ids_by_project_number, row.project_number
        )
        user_type = USER_TYPE_MAP[row.normalized_account_type]
        existing_user = existing_user_for_row(row, users_by_email, users_by_username)

        if existing_user:
            populate_missing_row_fields_from_existing_user(row, existing_user)
            print(
                f"PI already exists: {row.email} "
                f"(id={existing_user.get('id')}, username={existing_user.get('username')})"
            )
            patch_payload = merge_user_relationships(
                existing_user, managed_account_ids=[account_id]
            )
            updated_user = patch_if_changed(
                client,
                f"users/{existing_user['id']}/",
                existing_user,
                patch_payload,
            )
            refreshed_user = {**existing_user, **updated_user}
            if row.email:
                users_by_email[row.email] = refreshed_user
            if row.uni:
                users_by_username[row.uni.lower()] = refreshed_user
            continue

        if not row.email:
            raise ValueError(
                f"Row {row.row_number}: email is missing and no existing PI was found "
                f"for UNI '{row.uni}'."
            )

        payload = build_user_payload(
            row,
            user_type,
            managed_account_ids=[account_id],
        )
        created_user = client.post("users/", payload)
        print(
            f"Created PI '{created_user.get('email')}' "
            f"(id={created_user.get('id')})"
        )
        users_by_email[row.email] = created_user
        if row.uni:
            users_by_username[row.uni.lower()] = created_user

    return users_by_email


def import_projects(
    client: NemoClient,
    rows: list[SpreadsheetRow],
    projects_by_name: dict[str, dict[str, Any]],
    users_by_email: dict[str, dict[str, Any]],
    account_ids_by_project_number: dict[str, int],
    status_callback: StatusCallback | None = None,
) -> dict[str, dict[str, Any]]:
    pi_rows = [row for row in rows if row.is_pi_row]
    emit_status(status_callback, f"Creating or validating {len(pi_rows)} project(s)")

    for row in pi_rows:
        project_key = row.project_number.lower()
        if project_key in projects_by_name:
            existing_project = projects_by_name[project_key]
            print(
                f"Project already exists: {existing_project.get('name')} "
                f"(id={existing_project.get('id')})"
            )
            continue

        pi_user = users_by_email.get(row.email)
        if not pi_user:
            raise ValueError(
                f"PI user '{row.email}' was not found after user creation."
            )

        account_id = account_id_for_project(
            account_ids_by_project_number, row.project_number
        )
        payload = build_project_payload(row, account_id, pi_user["id"])
        created_project = client.post("projects/", payload)
        print(
            f"Created project '{created_project.get('name')}' "
            f"(id={created_project.get('id')})"
        )
        projects_by_name[project_key] = created_project

    return projects_by_name


def update_pi_project_links(
    client: NemoClient,
    rows: list[SpreadsheetRow],
    users_by_email: dict[str, dict[str, Any]],
    projects_by_name: dict[str, dict[str, Any]],
    account_ids_by_project_number: dict[str, int],
    status_callback: StatusCallback | None = None,
) -> dict[str, dict[str, Any]]:
    pi_rows = [row for row in rows if row.is_pi_row]
    emit_status(
        status_callback,
        f"Linking PI users to projects and accounts for {len(pi_rows)} project(s)",
    )

    for row in pi_rows:
        user = users_by_email.get(row.email)
        project = projects_by_name.get(row.project_number.lower())
        if not user or not project:
            continue

        patch_payload = merge_user_relationships(
            user,
            project_ids=[project["id"]],
            managed_project_ids=[project["id"]],
            managed_account_ids=[
                account_id_for_project(
                    account_ids_by_project_number, row.project_number
                )
            ],
        )
        updated_user = patch_if_changed(
            client,
            f"users/{user['id']}/",
            user,
            patch_payload,
        )
        users_by_email[row.email] = {**user, **updated_user}
        print(
            f"Updated PI links for {row.email}: "
            f"project {project['id']} and account "
            f"{account_id_for_project(account_ids_by_project_number, row.project_number)}"
        )

    return users_by_email


def import_other_users(
    client: NemoClient,
    rows: list[SpreadsheetRow],
    users_by_email: dict[str, dict[str, Any]],
    users_by_username: dict[str, dict[str, Any]],
    projects_by_name: dict[str, dict[str, Any]],
    status_callback: StatusCallback | None = None,
) -> dict[str, dict[str, Any]]:
    non_pi_rows = [row for row in rows if not row.is_pi_row]
    emit_status(status_callback, f"Processing {len(non_pi_rows)} non-PI user row(s)")

    for row in non_pi_rows:
        project = projects_by_name.get(row.project_number.lower())
        if not project:
            raise ValueError(
                f"Project '{row.project_number}' was not found after project creation."
            )

        user_type = USER_TYPE_MAP[row.normalized_account_type]
        existing_user = existing_user_for_row(row, users_by_email, users_by_username)

        if existing_user:
            populate_missing_row_fields_from_existing_user(row, existing_user)
            print(
                f"User already exists: {row.email} "
                f"(id={existing_user.get('id')}, username={existing_user.get('username')})"
            )
            patch_payload = merge_user_relationships(
                existing_user,
                project_ids=[project["id"]],
            )
            updated_user = patch_if_changed(
                client,
                f"users/{existing_user['id']}/",
                existing_user,
                patch_payload,
            )
            refreshed_user = {**existing_user, **updated_user}
            if row.email:
                users_by_email[row.email] = refreshed_user
            if row.uni:
                users_by_username[row.uni.lower()] = refreshed_user
            continue

        if not row.email:
            raise ValueError(
                f"Row {row.row_number}: email is missing and no existing user was found "
                f"for UNI '{row.uni}'."
            )

        payload = build_user_payload(
            row,
            user_type,
            project_ids=[project["id"]],
        )
        created_user = client.post("users/", payload)
        print(
            f"Created user '{created_user.get('email')}' "
            f"(id={created_user.get('id')})"
        )
        users_by_email[row.email] = created_user
        if row.uni:
            users_by_username[row.uni.lower()] = created_user

    return users_by_email


def summarize(rows: list[SpreadsheetRow]) -> str:
    pi_count = sum(1 for row in rows if row.is_pi_row)
    project_count = len(
        {row.project_number.lower() for row in rows if row.project_number}
    )
    user_count = len({row.email for row in rows if row.email})
    return (
        f"Rows loaded: {len(rows)}\n"
        f"Unique projects: {project_count}\n"
        f"Unique users: {user_count}\n"
        f"PI rows: {pi_count}"
    )


def run_import(
    file_path: str,
    token: str,
    dry_run: bool = False,
    *,
    use_cache: bool = True,
    status_callback: StatusCallback | None = None,
    progress_callback: ProgressCallback | None = None,
) -> None:
    total_steps = 7
    current_step = 0

    def advance(label: str) -> None:
        nonlocal current_step
        current_step += 1
        emit_status(status_callback, label)
        if progress_callback:
            progress_callback(current_step, total_steps, label)

    emit_status(status_callback, "Reading uploaded spreadsheet")
    spreadsheet_path = Path(file_path)
    rows = load_spreadsheet(spreadsheet_path)
    if progress_callback:
        progress_callback(0, total_steps, "Reading uploaded spreadsheet")
    errors = validate_rows(rows)
    if errors:
        raise ValueError("\n".join(errors[:20]))

    print(f"Loaded file: {spreadsheet_path}")
    print(summarize(rows))
    pi_count = sum(1 for row in rows if row.is_pi_row)
    project_count = len({row.project_number.lower() for row in rows if row.project_number})
    user_count = len({row.email for row in rows if row.email})
    emit_status(
        status_callback,
        f"Loaded {len(rows)} row(s) covering {project_count} project(s), {user_count} user(s), and {pi_count} PI row(s)",
    )
    advance("Spreadsheet loaded and validated")

    client = NemoClient(token, dry_run=dry_run)
    advance("Fetching current accounts, users, and projects from NEMO")
    (
        accounts_by_name,
        users_by_email,
        users_by_username,
        projects_by_name,
        existing_usernames,
    ) = (
        get_existing_maps(
            client,
            use_cache=use_cache,
            status_callback=status_callback,
        )
    )
    emit_status(
        status_callback,
        "Filling missing spreadsheet fields from existing NEMO users where possible",
    )
    populate_missing_row_fields_from_existing_users(
        rows,
        users_by_email,
        users_by_username,
    )
    emit_status(status_callback, "Generating missing UNIs where needed")
    fill_missing_unis(rows, existing_usernames)
    advance("Account import phase started")

    account_ids_by_project_number = import_accounts(
        client,
        rows,
        accounts_by_name,
        status_callback=status_callback,
    )
    advance("PI import phase started")
    users_by_email = import_pis(
        client,
        rows,
        users_by_email,
        users_by_username,
        account_ids_by_project_number,
        status_callback=status_callback,
    )
    advance("Project import phase started")
    projects_by_name = import_projects(
        client,
        rows,
        projects_by_name,
        users_by_email,
        account_ids_by_project_number,
        status_callback=status_callback,
    )
    advance("PI relationship update phase started")
    users_by_email = update_pi_project_links(
        client,
        rows,
        users_by_email,
        projects_by_name,
        account_ids_by_project_number,
        status_callback=status_callback,
    )
    advance("Other user import phase started")
    users_by_email = import_other_users(
        client,
        rows,
        users_by_email,
        users_by_username,
        projects_by_name,
        status_callback=status_callback,
    )
    _ = users_by_email

    if dry_run:
        print("Dry run complete. No changes were sent to NEMO.")
        advance("Dry run complete")
    else:
        print("Import complete.")
        advance("Import complete")
        threading.Thread(
            target=refresh_existing_maps_cache,
            args=(client,),
            daemon=True,
        ).start()


def main() -> None:
    from tkinter import messagebox

    root = create_dialog_root()
    log_path = Path(__file__).with_name("nemo_user_importer_output.txt")

    try:
        dry_run = prompt_for_dry_run(root)
        if dry_run is None:
            messagebox.showerror("Cancelled", "No mode selected.")
            return

        token = prompt_for_token(root)
        if not token:
            messagebox.showerror("Cancelled", "No API token entered.")
            return

        file_path = choose_input_file(root)
        if not file_path:
            messagebox.showerror("Cancelled", "No spreadsheet selected.")
            return

        with log_path.open("w", encoding="utf-8") as log_file:
            tee_stdout = Tee(sys.stdout, log_file)
            tee_stderr = Tee(sys.stderr, log_file)
            original_stdout = sys.stdout
            original_stderr = sys.stderr
            sys.stdout = tee_stdout
            sys.stderr = tee_stderr
            try:
                print(
                    f"Run started: {datetime.now().isoformat(timespec='seconds')}\n"
                    f"Output file: {log_path}\n"
                )
                try:
                    run_import(file_path, token, dry_run=dry_run)
                except requests.HTTPError as error:
                    response = error.response
                    details = response.text if response is not None else str(error)
                    messagebox.showerror(
                        "API Error",
                        f"Request failed.\n\n{details}",
                    )
                    raise
                except Exception as error:
                    messagebox.showerror("Import Failed", str(error))
                    raise
                else:
                    messagebox.showinfo(
                        "Finished",
                        (
                            "Dry run finished.\nSee the terminal output for the preview."
                            if dry_run
                            else "Accounts, PIs, projects, and other users have been processed.\n"
                        )
                        + "\n"
                        f"See the terminal output and {log_path.name} for details.",
                    )
            finally:
                sys.stdout = original_stdout
                sys.stderr = original_stderr
    finally:
        root.destroy()


if __name__ == "__main__":
    main()
