# Refactored NEMO Tools Hub

This is an isolated, from-scratch rewrite of the NEMO application. The legacy
Python files in the repository root are intentionally untouched, so this version
can be tested and rolled out without disrupting the current app.

The rewrite provides the same application areas: invoice generation, Excel to
PDF conversion, user/account/project import, account/project replacement,
detailed financial reporting, usage-cap analysis, user/PI reporting, missed
reservations, active lab users, and the jumbotron dashboard.

## Why this structure is easier to maintain

- One `NemoClient` owns authentication, pagination, retries, timeouts, and dry-run writes.
- Billing rules live in one framework-independent package.
- Excel and PDF invoices consume the same `InvoiceDocument`; totals are calculated once.
- Flask routes only validate requests and enqueue work.
- A separate worker executes durable SQLite jobs; web restarts do not lose queued work.
- Operator API tokens are encrypted at rest and removed when a job finishes.
- Configuration, paths, retention, and cache timing come from one typed settings object.
- Feature modules are small enough to review without navigating one multi-thousand-line file.

See [Architecture](docs/ARCHITECTURE.md), [Development](docs/DEVELOPMENT.md), and
[Migration](docs/MIGRATION.md) for the design rules and rollout order.

## Local setup

Install the locked dependencies from this directory:

```bash
uv sync --frozen
```

The checked-in `.python-version` keeps local development and production on
Python 3.12.13.

Start the durable worker:

```bash
uv run nemo-tools-worker
```

In a second terminal, start the web application:

```bash
uv run nemo-tools-web
```

Open `https://127.0.0.1:8000`. The development command uses a temporary local
TLS certificate. An application password is optional in development but strongly
recommended whenever the service is reachable by another computer.

## Local Docker test alongside the legacy app

Keep the production hostname and its ports unchanged while testing v2. After
creating `.env` from `.env.example`, start only the v2 web and worker services
with the local override:

```bash
docker compose -p nemo-tools-v2-local \
  -f compose.yaml -f compose.local.yaml \
  up -d --build web worker
```

Open `http://127.0.0.1:8001`. The override publishes v2 only on the local
loopback interface; it does not start Caddy or claim host ports 80 and 443.

Inspect or stop this isolated stack with:

```bash
docker compose -p nemo-tools-v2-local \
  -f compose.yaml -f compose.local.yaml \
  logs -f web worker

docker compose -p nemo-tools-v2-local \
  -f compose.yaml -f compose.local.yaml \
  down
```

## Quality checks

```bash
uv run python -m unittest discover -s tests -v
uv run ruff format --check src tests
uv run ruff check src tests
uv run pip-audit --path .venv/lib/python3.12/site-packages --progress-spinner off
```

The tests cover billing behavior, exact-cent cap allocation, invoice agreement,
spreadsheet imports, report workbooks, encrypted durable jobs, a real worker
conversion, authentication/CSRF, upload queueing, and all web pages.

The same checks and a production container build run automatically in GitHub
Actions for v2 pull requests and changes to `main`. Dependabot checks the Python,
Docker, and GitHub Actions dependencies weekly.

## Production with Docker Compose

Create the production environment file and replace every placeholder:

```bash
cp .env.example .env
uv run python -c "import secrets; print(secrets.token_urlsafe(48))"
uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then validate and start the stack:

```bash
docker compose -p nemo-tools-v2-production \
  -f compose.yaml -f compose.local.yaml -f compose.production.yaml \
  config --quiet

docker compose -p nemo-tools-v2-production \
  -f compose.yaml -f compose.local.yaml -f compose.production.yaml \
  up -d --build

docker compose -p nemo-tools-v2-production \
  -f compose.yaml -f compose.local.yaml -f compose.production.yaml \
  ps
```

The stack runs Caddy for HTTPS, Gunicorn for web requests, and a separate worker.
Caddy publishes production ports 80 and 443. The local override also publishes
the web service on `127.0.0.1:8001` for host-only diagnostics. Web and worker
processes run as a non-root user and share `./data` for SQLite, input files,
outputs, encrypted secrets, and cache. The production override reuses the
existing `nemo-apps_caddy_data` and `nemo-apps_caddy_config` volumes.

During the rollback window, the legacy application may remain running without
public traffic. To return traffic to it:

```bash
docker compose -p nemo-tools-v2-production \
  -f compose.yaml -f compose.local.yaml -f compose.production.yaml \
  stop proxy

cd ..
docker compose -p nemo-apps -f docker-compose.yml start caddy
```

Production mode refuses to start without a long Flask secret, an app password, a
Fernet job key, and an HTTPS NEMO base URL. `NEMO_JUMBOTRON_API_TOKEN` is optional;
without it, only the jumbotron is unavailable.

Set `NEMO_READ_ONLY_API_TOKEN` to a NEMO service-account token with only the
required `Can view` model permissions. When configured, invoice generation,
detailed financials, user/PI reports, and active-user reports use it without
asking the operator for an API token. Write-capable import and replacement jobs
never use this token and continue to require an operator token. The NEMO client
also rejects `POST` and `PATCH` calls whenever it is using the read-only token.

For a dedicated kiosk, set `NEMO_JUMBOTRON_KIOSK_TOKEN` to a separate random
secret of at least 32 characters and launch:

```text
https://tools.example.edu/jumbotron?kiosk_token=THE_KIOSK_TOKEN
```

The server exchanges that URL for a browser session and immediately redirects
to the clean `/jumbotron` URL. That session can access only `/jumbotron` and its
live-data endpoint; all other tools still require `NEMO_APP_ACCESS_PASSWORD`.
Do not reuse the main application password as the kiosk token.

The jumbotron auto-scroll defaults match the legacy app: one pixel every 50 ms.
Adjust `NEMO_JUMBOTRON_SCROLL_STEP_PX` or
`NEMO_JUMBOTRON_SCROLL_INTERVAL_MS` to change its display speed.

## Data lifecycle

Completed and failed job directories are retained for 14 days by default. The
worker performs cleanup at startup and every 24 hours. Change
`NEMO_OUTPUT_RETENTION_DAYS` when local data-retention policy requires a different
window. Back up `data/jobs.sqlite3` and the configured `NEMO_JOB_SECRET_KEY`
together if pending jobs must survive a host migration.
