# SVU（Standard Value Unit）

研究代码：从可追溯价格计算跨资产共同趋势与相对偏离。公开版本为 **diagnostic-v0.1-candidate**，不是生产指数、预测器或投资工具。

| 名称 | 定义 |
|---|---|
| SVU | 恒为 100 的显示坐标 |
| ICATI-CB | 类别先等权、类内再等权的动态共同趋势 |
| ICATI-EW | 经济实体等权的动态共同趋势，用于对照 |
| Domain ICATI | 单领域的等权动态共同趋势 |

资产曲线表示相对 ICATI 的偏离，不是“资产值多少 SVU”。数学定义见 [规范](docs/SVU_ICATI_SPEC.md)。

## 快速开始（离线、无需数据库）

支持 Python 3.11 / 3.12。在项目根目录运行：

```text
python -m pip install --require-hashes -r requirements.lock
python -m pytest tests -q
python -m scripts.demo --output reports/demo
```

打开 `reports/demo/demo.html`；机器结果为 `reports/demo/demo.json`。输入来自标记为 `SIMULATED` 的合成夹具，不能作为市场证据。测试覆盖数学、无效输入、补洞、ECB 交叉汇率、新目录加载和公开导出；私有数据库相关测试在没有数据时跳过。

## 官方数据与图表

```text
python -m scripts.rebuild_research --download
python -m scripts.daily_official_refresh
python -m scripts.serve_reports
```

重建命令按顺序下载并校验 FRED / ECB / LBMA 批次、创建研究视图、生成日级图和四个领域页面。需要网络及可用的官方数据源；历史批次采用各脚本声明的请求窗口。运行本地服务后打开 `http://127.0.0.1:8000`。服务运行期间会在本机当地时间每天 20:00 自动运行官方数据刷新；关闭服务即停止调度。可用 `--refresh-time HH:MM` 修改时间，或用 `--no-refresh` 只浏览报告。页面位于 `reports/generations/`，全部从同一数据库快照生成，成功后原子更新 `reports/current.json`；失败时保留上一份完整页面。数据修订、空响应、缺少篮子成员和输出失败必须先解决，不能用部分结果宣称完整复现。

可运行 `python -m scripts.check_sources --all` 检查配置来源的历史样本是否可访问；它不证明各序列已更新至今天。2026-09-07 的隔离重建和刷新均成功，但固定篮子的完整共同窗口仍截止 2025-12-30，不能把部分资产的新数据当作完整篮子的新值。

图表右上角的 EN / ZH 按钮切换中英文。Windows 可使用 `scripts\start_site.bat` 启动服务，优先使用项目 `.venv` 环境。调度完全由 Python 执行，不依赖 AI；关闭浏览器标签页不会关闭后台服务，需在服务终端按 Ctrl+C。退出会停止安排新更新，并等待正在执行的刷新完成。服务关闭期间错过的更新时间不会自动补跑，可手动执行刷新命令。尚未重建数据库的公开源码包需要先完成上述下载重建。

完整步骤、错误语义和边界见 [复现说明](docs/REPRODUCIBILITY.md)。数学测试通过不代表所有官方来源当前可用，也不代表经济有效性得到验证。

## 公开发行范围

公开内容由 `config/public_release.json` 逐文件定义，包括数学函数、支持的官方数据脚本、图表模板、配置、测试和说明。开发工作区可能保留历史实验、旧 Web/GNN 原型和内部资料；它们不属于受支持的发行功能。

生成一个新的公开候选目录：

```text
python -m scripts.export_public --output dist/candidate
```

输出包括干净源码树、ZIP 和 SHA-256 文件清单，不包含 Git 历史、行情、数据库、内部记录或历史原型。输出目录必须不存在。应从该独立源码树准备新仓库，不应直接推送内部工作区或沿用其历史。导出不会连接 GitHub 或发布版本。

## 许可证与贡献

代码为 MIT，版权主体为 chaotic-mixture，见 [LICENSE](LICENSE)。不分发下载行情；FRED、ECB、LBMA 数据受各自条款约束，代码许可证不覆盖第三方数据。合成夹具不是市场数据。

贡献与报告问题见 [CONTRIBUTING.md](CONTRIBUTING.md)。
