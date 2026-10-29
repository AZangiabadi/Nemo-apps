# Refactored NEMO Tools Hub

## Revenue Analysis

Open `/tools/revenue` for monthly calculated invoice revenue, daily charge and
gold-surcharge charts, project-type totals, and PI spending rankings. Analysis
runs through the same `calculate_invoices` service used by Excel/PDF generation,
including waived/no-charge exclusions, adjustments, minimum-quantity filtering,
project/session caps, recorded gold deposition charges, and default access fees.
The session-cap checkbox defaults off to match the invoice form; project caps
and the forced BET limit still apply. Custom invoice fee overrides are not
inferred from previously generated documents.

Charts represent calculated invoice amounts, not collected payments. Daily
charges use their billing completion date; monthly access fees are allocated
to month-end. Gold surcharge revenue is already included in total revenue.
CSV export includes all PI rankings and daily/project-type totals. Results are
saved in the persistent database independently of background-job retention.
Show analysis loads the saved month/cap-setting result without calling NEMO.
Previous/next month arrows also load saved results; Next stops at the current month.
Re-analyze uses the protected billing API token and replaces the saved result
only after successful completion. Existing completed revenue jobs are migrated
to saved results automatically. The original calculation timestamp is retained.

## NEMO Lookup Data

Invoices and financial reports use tool categories from `tools/` for lab
classification, with local historical tool mappings as fallbacks. Active lab
user reports use the same category rules. Consumable lab policy maps category
names from `consumable_categories/` to labs; category IDs are resolved at runtime.
Unknown consumable categories are not assigned a lab by this lookup.

User imports resolve account types, project types, and billing categories by
name from `account_types/`, `project_types/`, and `billing/rate_categories/`.
Qualification imports resolve the existing `24hrs Access` policy by name from
`qualification_levels/`. Missing or ambiguous required lookup names stop imports
before writes. Import tokens need read access to these endpoints in addition to
their existing record creation/update permissions. User type codes remain fixed
serializer choices; they are separate from the API account-type records.

Session caps, project caps, fees, subsidies, and gold surcharge rules remain
local billing policy.

## Newsletter Sync

To reuse a server-managed key, set `MAILCHIMP_API_KEY` in the private V2 `.env`
file, then recreate the web and worker containers with the production Compose
files. The Newsletter Sync page will show that a saved key is configured without
displaying the key. If unset, the page accepts a per-comparison key instead.

Open `/tools/newsletter` and enter a Mailchimp Marketing API key in the protected
form, along with the target audience ID (CNI default: `9205f7ea97`). The comparison
uses fresh active NEMO users and the complete Mailchimp audience, matching unique
normalized email addresses. It does not send messages or change subscriptions.
The review shows last NEMO login and last attributed tool use across the available
usage history, in the configured facility timezone, including ongoing usage.
Unavailable tool history is labeled explicitly rather than treated as no activity.
The review page lets an operator select missing contacts, confirm their newsletter
permission, and record where it was obtained. Confirmation email (`pending`) is
the default action. Direct subscription is available only for an audience without
double opt-in and still requires recorded permission. Existing contacts are
always skipped, including unsubscribed, bounced, and pending contacts. Each action
rechecks the user's active status and current audience membership before a
create-only request. Failed or interrupted jobs may have partially completed;
review their per-contact logs and run a fresh comparison before retrying.

Mailchimp credentials are encrypted in the job database and never returned by
the job status API. Comparison jobs retain encrypted credentials for approved
follow-ups until job expiration; other completed/failed jobs discard credentials.
The existing cleanup task removes retained comparison credentials on expiration.
Newsletter opt-in must be collected separately (for example during onboarding);
an active NEMO account is not itself a record of newsletter consent.

Invoice generation defaults to fetching a selected month from
`/api/billing/billing_data/`, using the protected read-only service token.
The token's NEMO account needs `NEMO.use_billing_api`. Projects marked
`no_charge` are excluded from API billing and CSV invoice input. CSV upload
remains available as an alternative. API jobs provide `billing_source.csv`
alongside the invoice files for reviewing the fetched quantities, rates, and
costs. The existing invoice caps, access fees, and appearance settings apply
to either input source.

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

## Angstrom gold audit

Set `ANGSTROM_LOG_APP_URL` to the private HTTPS address of the Angstrom gold
thickness dashboard. Each invoice run then adds a gold-deposition audit workbook
to the output ZIP. The workbook compares Au thickness reported in NEMO usage
events with the closest same-instrument log records, highlights mismatches and
missing reports, and totals both measurements.

## Data lifecycle

Completed and failed job directories are retained for 14 days by default. The
worker performs cleanup at startup and every 24 hours. Change
`NEMO_OUTPUT_RETENTION_DAYS` when local data-retention policy requires a different
window. Back up `data/jobs.sqlite3` and the configured `NEMO_JOB_SECRET_KEY`
together if pending jobs must survive a host migration.
