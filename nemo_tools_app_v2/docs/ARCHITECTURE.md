# Architecture

## Dependency direction

The important rule is that dependencies point inward toward business behavior:

```text
web routes ─────┐
worker handlers ├──> application services ──> billing rules / NEMO client
CLI commands ───┘              │
                               └──> renderers and durable job store
```

Billing, reporting, import, replacement, and invoice services do not import
Flask. This keeps calculations testable and prevents presentation concerns from
spreading into the core code.

## Package map

| Package | Responsibility |
|---|---|
| `nemo_app.config` | Environment parsing, paths, production validation |
| `nemo_app.nemo` | HTTP session, retries, pagination, dry runs, metadata cache |
| `nemo_app.billing` | CSV classification, approved adjustments, session caps, project caps, invoice model |
| `nemo_app.invoices` | Excel/PDF rendering, workbook parsing, contacts, ZIP archives |
| `nemo_app.reports` | Financial, cap, PI, missed-reservation, and active-user reports |
| `nemo_app.imports` | Spreadsheet validation and phased NEMO record creation |
| `nemo_app.replacements` | New-project cloning and existing-project migration |
| `nemo_app.dashboard` | Read-only live usage and reservation view |
| `nemo_app.jobs` | SQLite queue, encrypted secrets, worker, recovery, retention |
| `nemo_app.web` | Application factory, security, Blueprints, templates, static CSS |

## Request and job lifecycle

1. A Blueprint validates the form, file suffix, required token, and CSRF token.
2. The upload is stored under `data/jobs/<job-id>/input`.
3. A pending row is committed to SQLite. Per-job secrets are Fernet-encrypted.
4. The worker atomically claims one pending row and records its heartbeat.
5. A handler calls a framework-independent service and writes to the job output folder.
6. Every declared output is verified before the job is marked completed.
7. The encrypted token is deleted from SQLite on completion or failure.
8. The browser polls job status and exposes only verified files through an indexed route.
9. Retention cleanup expires the database result and removes the whole job folder.

Running jobs are not blindly reset when another worker starts. Only jobs whose
heartbeat exceeds `NEMO_JOB_STALE_SECONDS` are recovered, which prevents a second
worker from stealing active work.

## Billing invariants

- Raw columns are normalized once in `prepare_usage_dataframe`.
- Staff-time lab association happens after charge classification.
- Approved adjustments are applied before caps.
- Session caps run before project caps unless explicitly disabled.
- Project-cap allocation uses integer cents and largest remainders, so totals are exact.
- Staff charges are excluded from project-cap reduction, matching legacy behavior.
- Invoice access fees, project summaries, lab totals, and grand totals are properties of
  `InvoiceDocument`; renderers are not allowed to recalculate them independently.

These invariants are characterized by tests and should be changed only with an
explicit billing-policy decision and corresponding fixture updates.

## Security boundaries

- The app has password-gated sessions, CSRF protection, secure cookies, and safe redirects.
- Uploaded filenames never become filesystem paths; generated names are sanitized.
- Download paths are resolved and checked against the owning job directory.
- Tokens are encrypted in SQLite and never stored in session cookies or output metadata.
- The reverse proxy is the only published production service.
- Production startup validates required secrets and HTTPS API configuration.

The metadata cache and uploaded source files can contain institutional data. They
are intentionally stored only under the configured data directory and are subject
to the same host access controls and retention policy as generated reports.
