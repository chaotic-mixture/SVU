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

Run `python -m scripts.check_sources --all` to test access to historical samples from configured sources. This does not prove that every series is current. The latest published v0.4 complete window ends on **2026-09-18**. The 2026-10-23 refresh inserted 1,322 asset observations, but both LBMA endpoints returned HTTP 403, so the complete market publication was retained. Partial asset updates do not extend the published complete basket. LBMA historical tables now require portal access with the relevant licence; see [source limitations](docs/REPRODUCIBILITY.md#current-source-access-limitations-2026-10-23).

Use the **EN / ZH** button in the top-right corner of a chart to switch between English and Chinese. On Windows, `scripts\start_site.bat` starts the server and prefers the project's `.venv` environment. Scheduling uses Python only and does not depend on AI. Closing a browser tab does not stop the server; press **Ctrl+C** in the server terminal. Shutdown cancels future scheduled work and waits for an active refresh to finish. Schedules missed while the server was off are not automatically retried; run the refresh command manually when needed. A source-only installation must complete the initial download and database rebuild before serving official-data charts.

See the [reproduction guide](docs/REPRODUCIBILITY.md) for complete steps, failure behavior, and limitations. Passing mathematical tests does not establish current provider availability or economic validity.

## Project structure

See [project structure](docs/PROJECT_STRUCTURE.md) for directory responsibilities, commands and generated files. Offline examples, independent labour refresh and complete market reconstruction are separate entry points.

## Public distribution scope

`config/public_release.json` defines the public distribution file by file. It includes mathematical functions, supported official-data scripts, chart templates, configuration, tests, and documentation. A development workspace may also contain historical experiments, legacy Web/GNN prototypes, and internal material; these are outside the supported public distribution.

To create a new public candidate directory:

```text
python -m scripts.export_public --output dist/candidate
```

The output contains a clean source tree, a ZIP archive, and a SHA-256 file manifest. It excludes Git history, downloaded market data, databases, internal notes, and historical prototypes. The output directory must not already exist. Prepare a public repository from this independent source tree; do not push the internal workspace or reuse its private history. Exporting does not connect to GitHub or publish a release.

## License and contributions

The code is licensed under MIT, with copyright attributed to chaotic-mixture; see [LICENSE](LICENSE). Downloaded market data are not distributed. FRED, ECB, LBMA, BLS and ILOSTAT data are governed by their respective terms; the code license does not cover third-party data. Synthetic fixtures are not market data.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contributions and issue reporting.

Current calculations use **v0.4 (37 members)**, excluding the discontinued BGN series. The v0.3 configuration and prior pages remain available. History is independently recalculated, not spliced to v0.3. See [basket policy](docs/BASKET_POLICY.md) for membership, calendar alignment and freshness rules.

## Labour and earnings (independent multi-country page)

Run `python -m scripts.refresh_labor`, then open `reports/labor.html` or `/labor.html` on the local server. No market database or API key is required. The page supports Chinese and English.

- **ILOSTAT annual data**: mean hourly and monthly employee earnings in local currency, and mean weekly hours actually worked per employee. Select a country/territory, indicator and survey. Surveys are never spliced; local-currency amounts are not ranked across countries. The 2026-10-23 snapshot covers 203 countries and territories with at least one indicator, not necessarily all indicators or recent observations.
- **US BLS monthly tracker**: CES0500000003 (gross nominal USD/hour) and CES0500000002 (paid hours/week), all employees in the private nonfarm sector, seasonally adjusted. Fetches the latest ten calendar years and preserves preliminary flags. Paid hours are not hours actually worked.

The existing server scheduler refreshes this page before asset data without AI. Start it with `python -m scripts.serve_reports`; the default check is daily at 20:00 local time, configurable using `--refresh-time HH:MM`. Scheduling stops with the server, not the browser tab. Startup does not catch up missed checks; run the refresh command above for the initial page.

Daily checks do not imply daily observations. Any labour-source retrieval or validation failure retains the entire previous page. Retrieval time, observation years/months and official status flags remain visible. Missing values are not filled; gaps and official breaks are not connected on charts. Successful snapshots incorporate revisions. Raw responses are archived by SHA-256 under `data/raw/labor/`, with integrity records on the page. The SVU asset basket is unaffected. Official survey names and statistical notes remain in their original English and are labelled as such.

Official interfaces and definitions: [ILOSTAT](https://ilostat.ilo.org/data/bulk/) and [BLS CES](https://www.bls.gov/opub/hom/ces/concepts.htm). Localized country display names use Unicode CLDR territory codes and runtime Intl.DisplayNames; official English names remain in the payload. BLS unregistered API limits apply.


### Reference-unit earnings and shared controls

The labour page uses the same sidebar, date inputs/sliders, 1/3/5-year shortcuts, select-all/clear/reset controls and language preference as the main dashboard. Charts support pointer inspection and arrow keys. Main/domain pages link to labour; labour links back to the same published generation.

Earnings are shown in original currency, currency-category reference units and SVU reference units. Both units cost USD 1 on the first complete-basket date. Subsequent USD prices follow the equal-weight geometric currency trend and full-basket category-balanced ICATI-CB, respectively. This labour-view pricing convention does not redefine `SVU = 100`, establish official exchange rates or measure real purchasing power.

Annual conversion uses ILOSTAT USD earnings matched by country, survey and year; US monthly conversion uses BLS USD earnings. Divide earnings by the arithmetic period-average USD price of the reference unit, using actual complete-basket dates from the published snapshot without filling gaps. Eligible months must be closed, contain at least 10 observations, start by day 10 and end on/after day 20; eligible years require all 12 months. Missing USD values or eligible periods produce an explicit unavailable state. Window changes rebase chart trends to 100 without changing the fixed unit pricing scale. Hours are not converted into monetary units.

After successful labour and market updates, the scheduler recalculates conversions against the newly published market snapshot without downloading labour data again. Benchmark version, generation, base date and source-page SHA-256 remain traceable.

The labour page supports searchable country checkboxes and separate survey choices. Local-currency earnings compare trends rebased to 100 per country; currency-category and SVU reference units compare amounts on a shared scale. Use `reports/index.html` as the stable local home page; successful publication updates it while dated generations remain historical snapshots.

The multi-country view uses one main chart with one line per country. The comparison-basis selector switches between normalized original-currency trends, currency-category reference units and SVU reference units. Single-country charts and records are collapsed by default under Single-country details.
