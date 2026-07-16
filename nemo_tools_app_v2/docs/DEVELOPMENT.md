# Development guide

## Working agreement

Keep routes thin. A route may validate HTTP input, save an upload, and enqueue a
job; calculations belong in a service package. A service should accept ordinary
Python values, paths, DataFrames, or a `NemoClient`, and return a typed result.

Keep NEMO access behind `NemoClient` and `MetadataRepository`. Do not add a direct
`requests.get` to a report or route. Keep invoice totals in `InvoiceDocument` so
Excel and PDF cannot drift apart.

## Adding a feature

1. Add a small characterization fixture for the expected behavior.
2. Implement or extend a framework-independent service.
3. Add a job handler and verify each output file.
4. Add the smallest Blueprint route and Jinja template needed for input.
5. Exercise both success and failure behavior in tests.
6. Run formatting, lint, tests, and a clean dependency sync.

## Commands

```bash
uv sync --frozen
uv run python -m unittest discover -s tests -v
uvx ruff format --check src tests
uvx ruff check src tests
uv run python -m compileall -q src tests
```

Ruff is the static quality gate. Broad DataFrame type checking is intentionally
not configured: current pandas and openpyxl annotations produce large numbers of
false positives that obscure useful findings. Critical data behavior is guarded
with executable fixtures instead.

## Test boundaries

- Billing tests assert transformations and exact totals, not private helper calls.
- Renderer tests inspect actual workbooks and PDFs.
- Worker tests claim a durable job and verify its generated file.
- Web tests assert routes, queueing, encryption, login redirect safety, and CSRF.
- NEMO write workflows use dry-run or fake clients; automated tests never call live NEMO.

## Style

Prefer domain names such as `InvoiceDocument`, `ProjectSummary`, and
`ExistingRecords` over generic dictionaries. Use dataclasses for service results.
Keep functions focused, preserve source ordering when business output depends on
it, and add comments only when they explain a policy or non-obvious constraint.
