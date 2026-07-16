# Migration and rollout

The rewrite is deliberately located in `refactored_nemo_app`. It does not import
the legacy modules, and it does not modify the original application files.

## Recommended order of work

The implementation followed this order so high-risk billing behavior was fixed
before presentation and deployment work:

1. Characterize session caps, project caps, staff association, filtering, and totals.
2. Create shared configuration, NEMO client, metadata repository, and cache.
3. Isolate billing preparation and exact-cent cap logic.
4. Build one invoice document model and render both Excel and PDF from it.
5. Move reports, imports, replacements, and dashboard queries into services.
6. Replace in-process daemon threads with SQLite jobs and a separate worker.
7. Split the web layer into feature Blueprints and Jinja templates.
8. Add authentication, CSRF, encrypted tokens, safe downloads, and retention.
9. Lock dependencies and add the production container stack.
10. Run artifact, worker, route, lint, and clean-environment verification.

## Safe rollout sequence

1. Keep the legacy service running.
2. Deploy this app on a separate hostname or non-production host.
3. Use dry-run mode for user import and account/project replacement comparisons.
4. Generate invoices from a previously completed billing month and compare PI count,
   row count, access fees, lab totals, project totals, and final invoice totals.
5. Compare each report workbook's row counts and aggregate totals.
6. Let billing administrators perform acceptance testing with representative files.
7. Back up the new `data` directory and document the secret-storage location.
8. Change the production hostname only after acceptance criteria pass.
9. Keep the legacy deployment available read-only for one billing cycle.
10. Archive rather than delete the legacy source after the rollback window closes.

## Acceptance criteria

- The same supported application identifiers are invoiced.
- Missed reservations and one-minute tool rows follow the characterized filters.
- Staff time appears in the intended lab and is excluded from project-cap reduction.
- Hourly caps preserve original quantity/cost fields for auditing.
- Every project cap ends at the exact configured cent total.
- Excel, PDF, contact workbook, and cap report totals agree.
- Dry runs perform no live NEMO writes and do not seed caches with synthetic IDs.
- A web restart does not lose pending work, and a worker interruption can recover stale work.
- API tokens do not appear in SQLite plaintext, logs, URLs, or downloads.

## Rollback

Rollback is operational rather than destructive: stop this Compose stack and
return traffic to the legacy hostname. The original code and its data remain
untouched. Preserve the new `data` directory until queued or completed work has
been reconciled.
