"""Generate a read-only SVU relative dashboard for one asset domain."""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import sqlite3
from pathlib import Path

import pandas as pd
import yaml

from validation.domain_view import (
    add_numeraire_reference,
    build_domain_view,
    load_domain_panel,
)

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "validation" / "domain_view.html"

DOMAIN_TITLES = {
    "currency": "货币领域相对视图",
    "energy": "能源领域相对视图",
    "monetary_hedge": "贵金属对冲领域相对视图",
    "equity_index": "股票指数领域相对视图",
}

DOMAIN_TITLES_EN = {
    "currency": "Currency relative view", "energy": "Energy relative view",
    "monetary_hedge": "Monetary-hedge relative view", "equity_index": "Equity-index relative view",
}

MEASUREMENT_NOTES = {
    "currency": "原始数据是以美元计价的汇率序列；图中比较基线是计算出的货币领域 ICATI，不是美元。",
    "energy": "原始价格单位为美元/桶；图中比较基线是计算出的能源领域 ICATI，不是美元。",
    "monetary_hedge": "原始价格单位为美元/盎司；图中比较基线是计算出的贵金属对冲领域 ICATI，不是美元。",
    "equity_index": "原始数据是指数点；图中比较基线是计算出的股票指数领域 ICATI，不是美元。",
}

MEASUREMENT_NOTES_EN = {
    "currency": "Raw inputs are USD-quoted exchange-rate series; the chart baseline is the computed currency-domain ICATI, not USD.",
    "energy": "Raw prices are USD per barrel; the chart baseline is the computed energy-domain ICATI, not USD.",
    "monetary_hedge": "Raw prices are USD per ounce; the chart baseline is the computed monetary-hedge ICATI, not USD.",
    "equity_index": "Raw inputs are index points; the chart baseline is the computed equity-index ICATI, not USD.",
}

LABELS = {
    "AUDUSD": "澳元",
    "BGNUSD_ECB": "保加利亚列弗",
    "BRLUSD": "巴西雷亚尔",
    "CADUSD": "加元",
    "CHFUSD": "瑞士法郎",
    "CNYUSD": "人民币",
    "CZKUSD_ECB": "捷克克朗",
    "DKKUSD": "丹麦克朗",
    "EURUSD": "欧元",
    "GBPUSD": "英镑",
    "HKDUSD": "港币",
    "HUFUSD_ECB_DISCOVERED": "匈牙利福林",
    "IDRUSD_ECB": "印尼盾",
    "ILSUSD_ECB_DISCOVERED": "以色列谢克尔",
    "INRUSD": "印度卢比",
    "JPYUSD": "日元",
    "KRWUSD": "韩元",
    "MXNUSD": "墨西哥比索",
    "MYRUSD": "马来西亚林吉特",
    "NOKUSD": "挪威克朗",
    "NZDUSD_ECB": "新西兰元",
    "PHPUSD_ECB_DISCOVERED": "菲律宾比索",
    "PLNUSD_ECB": "波兰兹罗提",
    "RONUSD_ECB": "罗马尼亚列伊",
    "SEKUSD_ECB": "瑞典克朗",
    "SGDUSD": "新加坡元",
    "THBUSD": "泰铢",
    "TRYUSD_ECB_DISCOVERED": "土耳其里拉",
    "TWDUSD": "新台币",
    "ZARUSD": "南非兰特",
    "BRENT_USD_BBL": "布伦特原油",
    "WTI_USD_BBL": "WTI 原油",
    "GOLD_LBMA_PM_USD_OZ": "黄金",
    "SILVER_LBMA_USD_OZ": "白银",
    "DJIA": "道琼斯",
    "NASDAQ100": "纳斯达克100",
    "NIKKEI225": "日经225",
    "SP500": "标普500",
}


def canonical_domain_assets(domain: str, db_path=None) -> list[str]:
    config = yaml.safe_load(
        (ROOT / "config" / "svu_v03_candidate.yaml").read_text(encoding="utf-8")
    )
    symbols = list(config["category_groups"][domain])
    with closing(sqlite3.connect(Path(db_path or ROOT / "svu.db").resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        mapping = pd.read_sql_query(
            "SELECT symbol, entity_id FROM research_asset_entity_map", connection
        )
        entities = pd.read_sql_query(
            "SELECT entity_id, canonical_symbol FROM research_entity_registry", connection
        )
    assets = []
    for symbol in symbols:
        hit = mapping[mapping.symbol == symbol]
        if hit.empty:
            assets.append(symbol)
            continue
        entity = entities[entities.entity_id == hit.entity_id.iloc[0]]
        assets.append(entity.canonical_symbol.iloc[0] if not entity.empty else symbol)
    return list(dict.fromkeys(assets))


def render_report(data: dict, output_path: Path) -> str:
    return "\n".join(
        [
            f"# {data['title']}",
            "",
            "> 这是 SVU 主项目的只读派生视图，不改变 SVU、ICATI-CB 或 ICATI-EW 的定义。",
            "",
            "## 已验证事实",
            f"- 领域：`{data['domain_key']}`（L1：现行配置与数据库读取）",
            f"- 资产数：{len(data['assets'])}（L1）",
            f"- 共同有效日期：{data['n_observations']}（L1）",
            f"- 共同窗口：`{data['start']}` 至 `{data['end']}`（L1）",
            f"- 领域 ICATI 末值：{data['domain_svu'][-1]:.8f}（L2：公式计算）",
            f"- 当前领域内离散标准差：{data['diagnostics']['current_dispersion']['std']:.8f}（L2：当前末期相对值计算）",
            f"- 当前领域内最大跨度：{data['diagnostics']['current_dispersion']['spread']:.8f}（L2：当前末期相对值计算）",
            "",
            "## 领域诊断",
            "- 留一资产结果位于机器 JSON 的 `diagnostics.leave_one_out`，用于观察领域 ICATI 对组成的敏感性。",
            "- 诊断指标不代表预测能力、绝对价值或经济好坏。",
            "",
            "## 数学定义",
            "领域动态共同趋势使用领域内资产的等权几何平均，并在共同窗口首日归一化为 100：",
            "`DomainICATI_g,t = 100 × exp(mean_i(log(P_i,t / P_i,0)))`（L2）",
            "",
            "资产展示值为：",
            "`AssetRelative_i,t = 100 × (P_i,t / P_i,0) / (DomainICATI_g,t / 100)`（L2）",
            "",
            "因此图中的水平线 100 是固定显示基线，不是新的绝对价值单位。",
            "",
            "## 基线与计价单位",
            f"- 比较基线：计算出的 `{data['domain_key']}` 领域 ICATI；不是美元或其他报价单位。",
            f"- 原始计量说明：{data['measurement_note']}",
            "- 货币页面的 USD 线是显示专用的恒定计价参照，不加入货币领域 ICATI 的计算篮子。"
            if data["domain_key"] == "currency" else "",
            "",
            "## 数据处理",
            "- 只使用 SVU 主项目唯一实体观测视图。",
            "- 只保留领域内所有资产均有报价的共同日期。",
            "- 不前向填充，不把缺失值当作最新值。",
            "- 资产名称、来源和主体定义继续由 SVU 主项目统一管理。",
            "",
            "## 解释边界",
            "- 高于 100 只表示相对领域共同趋势走强，低于 100 只表示相对走弱。",
            "- 不同领域的 100 不能直接相互比较，因为领域成员和共同窗口可能不同。",
            "- 领域资产数量不足或共同窗口不足时，不输出稳定性结论。",
            "",
            f"页面：`{output_path.as_posix()}`",
            f"机器结果：`reports/domain_{data['domain_key']}_latest.json`",
            "",
        ]
    )


def build(domain: str, output_dir=None, db_path=None, panel=None, assets=None) -> dict:
    if domain not in DOMAIN_TITLES:
        raise ValueError(f"未知领域: {domain}")
    assets = canonical_domain_assets(domain, db_path) if assets is None else assets
    panel = load_domain_panel(db_path or ROOT / "svu.db", assets) if panel is None else panel
    result = build_domain_view(panel, assets, domain_key=domain)
    if domain == "currency":
        result = add_numeraire_reference(result, symbol="USD")
    result.update(
        {
            "title": DOMAIN_TITLES[domain],
            "title_en": DOMAIN_TITLES_EN[domain],
            "labels": {
                asset: LABELS.get(asset, asset) for asset in result["display_assets"]
            },
            "labels_en": {asset: asset for asset in result["display_assets"]},
            "measurement_note": MEASUREMENT_NOTES[domain],
            "measurement_note_en": MEASUREMENT_NOTES_EN[domain],
            "status": "derived_readonly_domain_view",
            "source_project": "SVU-v0.3-candidate",
            "source_table": "research_entity_observations_unique",
            "main_definition_unchanged": True,
            "derived_layer_only": True,
            "comparison_warning": "different_domain_windows_are_not_cross_comparable",
        }
    )
    if domain == "currency":
        result["labels"]["USD"] = "美元（相对货币领域 ICATI）"
        result["labels_en"]["USD"] = "USD (relative to currency ICATI)"
    reports = Path(output_dir) if output_dir is not None else ROOT / "reports"
    output = reports / "domains" / f"{domain}.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("__TITLE__", DOMAIN_TITLES[domain])
    html = html.replace("__DOMAIN__", domain)
    html = html.replace(
        "__PAYLOAD__", json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    )
    output.write_text(html, encoding="utf-8")

    machine_report = {
        "experiment": f"SVU-DV-{domain.upper()}-001",
        **result,
        "file": output.relative_to(reports).as_posix(),
    }
    report_json = reports / f"domain_{domain}_latest.json"
    report_md = reports / f"domain_{domain}_latest.md"
    report_json.write_text(
        json.dumps(machine_report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    report_md.write_text(render_report(machine_report, output.relative_to(reports)), encoding="utf-8")
    return machine_report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--domain", default="currency", choices=sorted(DOMAIN_TITLES))
    args = parser.parse_args()
    result = build(args.domain)
    print(
        json.dumps(
            {
                "experiment": result["experiment"],
                "status": result["status"],
                "file": result["file"],
                "assets": len(result["assets"]),
                "observations": result["n_observations"],
                "start": result["start"],
                "end": result["end"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
