# Reproducibility and release scope

## Offline example

Use Python 3.11 or 3.12 from the repository root:

```text
python -m pip install --require-hashes -r requirements.lock
python -m pytest tests -q
python -m scripts.demo --output reports/demo
```

The example writes JSON and an HTML table from the explicitly synthetic fixture. It is an executable calculation example, not market evidence. The full tests include temporary SQLite and mocked network responses. A historical database test is skipped when private data is absent.

## Official data reconstruction

```text
python -m scripts.rebuild_research --download
```

This downloads the registered FRED, ECB and LBMA batches, verifies normalized file paths and SHA-256 hashes against their manifests, initializes the research tables, loads the files, creates unique views and entity mappings, then rebuilds the four domain pages, daily dashboard and snapshot. Download batches retain their declared historical request windows. Some sources may be unavailable; a failure must be resolved before claiming successful reproduction. An isolated live reconstruction and subsequent refresh both completed successfully on 2026-09-07; this is a dated observation, not a continuing availability guarantee.

If the downloaded normalized files already exist, use `python -m scripts.rebuild_research` without `--download`. The reconstruction path uses existing research tables and may reload a batch; use a new exported tree for an isolated rebuild. The loaders retain their historical batch behavior and are not a data migration framework.

Refresh after reconstruction:

```text
python -m scripts.daily_official_refresh
```

Refresh fills missing dates from FRED, ECB and LBMA without replacing existing observations. A changed value on an existing source/date is a revision conflict: the source fails visibly and the raw response is retained under a content-hashed filename for review. This conservative policy prevents revised and old prices from silently becoming duplicate modeling rows. Metadata includes observation age; age is informational because different series have different publication calendars and some have ended. Unsupported sources are reported as skipped. Source or output failures produce a nonzero exit code and a `partial_failed` report. Successful source rows may already have been committed if another source fails; report publication is then skipped.

Rebuild and refresh share an operating-system database lock. Publication creates a SQLite backup and all six outputs read that same snapshot. Only after every builder succeeds does `reports/current.json` switch atomically to a complete directory in `reports/generations/`. Failure preserves the previous pointer. Start `python -m scripts.serve_reports` and open `http://127.0.0.1:8000` to follow the current generation. While this local server runs, its own local thread starts the official refresh once per day at 20:00 local time; it stops when the server exits. Use `--refresh-time HH:MM` to change the time or `--no-refresh` to disable it for a browser-only session. The local server serves complete report generations only, without database access or directory listings. Older root-level HTML files are not the current-generation entry point.

Run `python -m scripts.check_sources --all` for provider diagnostics. The 2026-09-07 run passed 58 configured endpoint checks using a historical sample window (LBMA uses its JSON series); this does not establish current freshness. The real refresh checked 89 asset/source entries with no failures. Its union reaches 2026-09-04, while the fixed official basket's complete common window ends on 2025-12-30. Changing membership to extend that window is a research decision, not an automatic maintenance step.

No forward fill is used for the official common window. A domain needs every configured member and at least three complete dates. A missing or partial basket must not be represented as the complete official basket.

## Export

The local scheduler uses Python only and needs no AI service. Closing a browser tab does not stop the server: use Ctrl+C in its terminal. Shutdown cancels future scheduled work and waits for any active refresh to finish safely. There is no catch-up run for schedules missed while the server was off; invoke the refresh command manually when needed. Refresh dependencies are checked at startup; `--no-refresh` remains available for report-only viewing. Windows users can run `scripts\start_site.bat`, which prefers the project's `.venv` interpreter. The EN / ZH button changes chart language and remembers the choice in browser storage.

```text
python -m scripts.export_public --output dist/candidate
```

The output directory must not already exist. It contains `SVU/`, `SVU-source.zip`, and `SHA256.json`. Only individually allowlisted files are copied; Git history, databases, raw data, generated reports, internal notes and legacy prototypes are excluded. The exporter checks local static imports and some private-content patterns; this is not a substitute for a dedicated secret scanner or review of the final Git history.

Test the exported tree, run its demo, and initialize a new Git repository only inside that clean tree when preparing an actual publication. Export does not create a remote, push, or reuse the internal repository's history. Any external release checklist must be updated to this exact file list and rerun before publication.

## Naming and limits

SVU is fixed at 100. ICATI and Domain ICATI are dynamic indices. `domain_svu` and `computed_domain_svu` in existing JSON remain compatibility aliases; they do not introduce another value unit. Historical experiment runners and optional Web APIs are outside this release.

Dependency versions, including transitive packages, are constrained in `constraints.txt` and their release artifact hashes are locked in `requirements.lock`. After an intentional version update, run `python -m scripts.lock_dependencies` and review the lock diff. Hashes protect artifact integrity, not the trustworthiness of a package. Windows CPython 3.12 and Linux CPython 3.11 wheel resolution and hash checks passed locally. Linux runtime execution remains a CI check.

## Offline browser regression

Use Node.js 22 and pnpm 11.19.0:

```text
pnpm install --frozen-lockfile --ignore-scripts
pnpm exec playwright install chromium
python -m scripts.demo --gallery --output reports/demo-gallery
pnpm run test:browser reports/demo-gallery dist/browser-evidence
```

On Linux, use `pnpm exec playwright install --with-deps chromium` to install required browser system libraries. The gallery is explicitly simulated. Checks exercise the daily chart and four domain pages at desktop and mobile widths, including window controls, selection reset and tooltip recovery after an empty window. HTTP requests are blocked during these local-page tests. `pnpm audit` checks the browser tooling dependencies; CI also scans the Python lock with pip-audit. Passing tests establishes the tested software behavior, not economic validity or data redistribution rights.
