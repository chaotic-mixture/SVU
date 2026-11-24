# Project structure

The supported public source tree is defined file by file in `config/public_release.json`. Historical prototypes and experiments in the development workspace are excluded from the supported release.

| Path | Responsibility |
|---|---|
| `config/` | Versioned baskets, country display names and the release allowlist |
| `scripts/` | Official-source downloads, refresh, publication, local serving and clean export |
| `validation/*.py` | Index calculations and dashboard builders |
| `validation/*.html` | Standalone bilingual dashboard templates |
| `validation/labor_*.js` | Labour-page controls, country comparisons, benchmarks and rendering |
| `tests/` | Offline regression tests and explicitly synthetic fixtures |
| `docs/` | Mathematical definitions, reproduction, basket policy and release notes |
| `.github/workflows/` | Automated source checks and dependency audits |

## Main commands

| Command | Purpose |
|---|---|
| `python -m scripts.demo` | Run an offline mathematical example |
| `python -m scripts.rebuild_research --download` | Download configured history and build the initial market database and dashboards; requires accessible sources |
| `python -m scripts.refresh_labor` | Refresh BLS/ILOSTAT independently; reference-unit conversions also require a complete published basket |
| `python -m scripts.daily_official_refresh` | Refresh labour and assets; recalculate benchmarks after successful market publication |
| `python -m scripts.serve_reports` | Serve local reports and run the daily Python refresh schedule |
| `python -m scripts.export_public --output dist/candidate` | Create a clean source tree, ZIP archive and SHA-256 manifest |

## Generated state

- `reports/index.html`: stable local homepage and module navigation.
- `reports/labor.html`: latest validated labour snapshot.
- `reports/current.json` and `reports/generations/`: pointer to the complete market publication and preserved historical generations.
- `data/raw/`, `svu.db` and `dist/`: downloaded responses, local database and disposable export/test outputs.

Generated state is excluded from GitHub source uploads. When moving a runtime module, update its imports, templates, tests and release allowlist. Provider access is a runtime prerequisite; passing source-code tests does not prove that providers are reachable or observations are current. See [Reproducibility and source limitations](REPRODUCIBILITY.md).
