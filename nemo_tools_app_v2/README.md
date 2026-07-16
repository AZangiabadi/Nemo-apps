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
- API tokens are encrypted at rest and removed when a job finishes.
- Configuration, paths, retention, and cache timing come from one typed settings object.
- Feature modules are small enough to review without navigating one multi-thousand-line file.

See [Architecture](docs/ARCHITECTURE.md), [Development](docs/DEVELOPMENT.md), and
[Migration](docs/MIGRATION.md) for the design rules and rollout order.

## Local setup

Install the locked dependencies from this directory:

```bash
uv sync --frozen
```

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

## Quality checks

```bash
uv run python -m unittest discover -s tests -v
uvx ruff format --check src tests
uvx ruff check src tests
```

The tests cover billing behavior, exact-cent cap allocation, invoice agreement,
spreadsheet imports, report workbooks, encrypted durable jobs, a real worker
conversion, authentication/CSRF, upload queueing, and all web pages.

## Production with Docker Compose

Create the production environment file and replace every placeholder:

```bash
cp .env.example .env
uv run python -c "import secrets; print(secrets.token_urlsafe(48))"
uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then validate and start the stack:

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

The stack runs Caddy for HTTPS, Gunicorn for web requests, and a separate worker.
Only Caddy publishes host ports. Web and worker processes run as a non-root user
and share `./data` for SQLite, input files, outputs, encrypted secrets, and cache.

Production mode refuses to start without a long Flask secret, an app password, a
Fernet job key, and an HTTPS NEMO base URL. `NEMO_JUMBOTRON_API_TOKEN` is optional;
without it, only the jumbotron is unavailable.

The jumbotron auto-scroll defaults match the legacy app: one pixel every 50 ms.
Adjust `NEMO_JUMBOTRON_SCROLL_STEP_PX` or
`NEMO_JUMBOTRON_SCROLL_INTERVAL_MS` to change its display speed.

## Data lifecycle

Completed and failed job directories are retained for 14 days by default. The
worker performs cleanup at startup and every 24 hours. Change
`NEMO_OUTPUT_RETENTION_DAYS` when local data-retention policy requires a different
window. Back up `data/jobs.sqlite3` and the configured `NEMO_JOB_SECRET_KEY`
together if pending jobs must survive a host migration.
