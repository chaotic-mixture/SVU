# SVU (Standard Value Unit)

[简体中文](README.md) | English

Research code for calculating cross-asset common trends and relative deviations from traceable prices. The public version is **diagnostic-v0.1-candidate**, a research candidate rather than a production index, forecasting system, or investment tool.

| Name | Definition |
|---|---|
| SVU | A display coordinate fixed at 100 |
| ICATI-CB | A dynamic common trend with equal category weights and equal asset weights within each category |
| ICATI-EW | A dynamic common trend with equal economic-entity weights, used for comparison |
| Domain ICATI | An equal-weight dynamic common trend within one asset domain |

Asset curves show deviations relative to ICATI; they do not measure how many SVU an asset is worth. See the [mathematical specification](docs/SVU_ICATI_SPEC.md).

## Quick start: offline, no database required

Python 3.11 and 3.12 are supported. Run these commands from the project root:

```text
python -m pip install --require-hashes -r requirements.lock
python -m pytest tests -q
python -m scripts.demo --output reports/demo
```

Open `reports/demo/demo.html`; the machine-readable result is `reports/demo/demo.json`. Inputs come from a synthetic fixture explicitly marked `SIMULATED` and must not be treated as market evidence. Tests cover mathematics, invalid inputs, missing-date insertion, ECB cross rates, loading into a fresh directory, and public export. Tests requiring the private historical database are skipped when it is absent.

## Official data and charts

```text
python -m scripts.rebuild_research --download
python -m scripts.daily_official_refresh
python -m scripts.serve_reports
```

The rebuild command downloads and verifies registered FRED, ECB, and LBMA batches in order, creates research views, and generates the daily dashboard and four domain pages. It requires network access and available official sources. Historical batches use the request windows declared in their scripts.

Once the server is running, open `http://127.0.0.1:8000`. It automatically refreshes official data every day at **20:00 local machine time** while running. Set `--refresh-time HH:MM` to change the schedule, or use `--no-refresh` to serve reports only.

Pages are generated under `reports/generations/` from a single database snapshot. Once every output succeeds, `reports/current.json` is updated atomically. A failed refresh preserves the previous complete publication. Resolve source revisions, empty responses, missing basket members, and output failures before claiming complete reproduction.

Run `python -m scripts.check_sources --all` to test access to historical samples from configured sources. This does not prove that every series is current. An isolated reconstruction and refresh succeeded on 2026-09-07, but the fixed basket's complete common window still ended on **2025-12-30**. New observations for individual assets are not new complete-basket observations.

Use the **EN / ZH** button in the top-right corner of a chart to switch between English and Chinese. On Windows, `scripts\start_site.bat` starts the server and prefers the project's `.venv` environment. Scheduling uses Python only and does not depend on AI. Closing a browser tab does not stop the server; press **Ctrl+C** in the server terminal. Shutdown cancels future scheduled work and waits for an active refresh to finish. Schedules missed while the server was off are not automatically retried; run the refresh command manually when needed. A source-only installation must complete the initial download and database rebuild before serving official-data charts.

See the [reproduction guide](docs/REPRODUCIBILITY.md) for complete steps, failure behavior, and limitations. Passing mathematical tests does not establish current provider availability or economic validity.

## Public distribution scope

`config/public_release.json` defines the public distribution file by file. It includes mathematical functions, supported official-data scripts, chart templates, configuration, tests, and documentation. A development workspace may also contain historical experiments, legacy Web/GNN prototypes, and internal material; these are outside the supported public distribution.

To create a new public candidate directory:

```text
python -m scripts.export_public --output dist/candidate
```

The output contains a clean source tree, a ZIP archive, and a SHA-256 file manifest. It excludes Git history, downloaded market data, databases, internal notes, and historical prototypes. The output directory must not already exist. Prepare a public repository from this independent source tree; do not push the internal workspace or reuse its private history. Exporting does not connect to GitHub or publish a release.

## License and contributions

The code is licensed under MIT, with copyright attributed to chaotic-mixture; see [LICENSE](LICENSE). Downloaded market data are not distributed. FRED, ECB, and LBMA data are governed by their respective terms; the code license does not cover third-party data. Synthetic fixtures are not market data.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contributions and issue reporting.
