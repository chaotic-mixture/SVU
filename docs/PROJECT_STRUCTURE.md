# Project structure / 项目结构

The supported public source tree is defined by `config/public_release.json`.
公开源码逐文件列入该清单；开发工作区中的历史原型和实验不随发行上传。

| Directory / 目录 | Responsibility / 职责 |
|---|---|
| `config/` | Versioned baskets, country display names and release allowlist / 版本化篮子、国家名称与发布清单 |
| `scripts/` | Official-source ingestion, refresh, publication, local server and export commands / 数据采集、更新、发布、服务与导出入口 |
| `validation/*.py` | Index mathematics and dashboard builders / 指数计算与图表构建 |
| `validation/*.html` | Standalone bilingual page templates / 独立双语页面模板 |
| `validation/labor_*.js` | Labour page controls, country comparisons, benchmarks and annual/monthly rendering / 劳动页交互与展示 |
| `tests/` | Offline regression tests and explicitly synthetic fixtures / 离线测试与明确标识的合成夹具 |
| `docs/` | Definitions, reproduction, basket policy and release notes / 定义、复现、篮子政策及发布说明 |
| `.github/workflows/` | Public-source checks and dependency audits / 公开源码检查与依赖审计 |

## Entry points / 常用入口

| Command / 命令 | Purpose / 用途 |
|---|---|
| `python -m scripts.demo` | Offline mathematical example / 离线数学示例 |
| `python -m scripts.rebuild_research --download` | Initial market database and dashboard build; requires available source access / 首次行情库与图表构建，需要可访问的数据源 |
| `python -m scripts.refresh_labor` | Independent BLS/ILOSTAT refresh; monetary comparisons additionally need a complete published basket / 独立劳动数据更新，参考单位换算另需已发布完整篮子 |
| `python -m scripts.daily_official_refresh` | Labour refresh, incremental asset update, then benchmark recalculation after successful market publication / 劳动更新、资产增量更新及成功发布后的换算重算 |
| `python -m scripts.serve_reports` | Local website and daily Python scheduler / 本地网站与每日 Python 调度 |
| `python -m scripts.export_public --output dist/candidate` | New clean source tree, ZIP and SHA-256 manifest / 新建干净源码目录、ZIP 和校验清单 |

## Generated state / 生成内容

- `reports/index.html`: stable local homepage, with module navigation / 固定本地首页与模块入口。
- `reports/labor.html`: latest validated labour snapshot / 最近一次通过校验的劳动快照。
- `reports/current.json` and `reports/generations/`: authoritative complete market publication and preserved historical generations / 完整资产发布指针与保留的历史版本。
- `data/raw/`, `svu.db`, `dist/`: downloaded source responses, local database and disposable release/test artifacts / 原始响应、本地数据库及发布和测试产物。

Generated state is excluded from GitHub source uploads. Do not move a runtime
module without updating its imports, templates, tests and release allowlist.
生成内容不进入 GitHub 源码；调整模块路径时须同步更新导入、模板、测试及发布清单。

Official-source access is a runtime prerequisite. A successful source-code test
does not establish that providers are accessible or observations are current.
See [reproduction and source limitations](REPRODUCIBILITY.md).
