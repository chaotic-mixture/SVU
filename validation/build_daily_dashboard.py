"""SVU-BL-037：日级仪表盘。完整历史入库展示；时间轴随勾选序列扩展。"""
from __future__ import annotations

import json
from contextlib import closing
import numpy as np
from pathlib import Path

import pandas as pd
import sqlite3
import yaml

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "validation" / "dashboard_daily.html"

LABELS = {
    "SVU": "SVU（固定 100）",
    "ICATI-CB": "ICATI-CB 类别平衡指数",
    "ICATI-EW": "ICATI-EW 实体等权指数",
    "USD": "美元 / SVU",
    "currency baseline": "货币类别基线",
    "energy baseline": "能源类别基线",
    "monetary_hedge baseline": "贵金属对冲类别基线",
    "equity_index baseline": "股票指数类别基线",
    "AUDUSD": "澳元 / SVU",
    "BRLUSD": "巴西雷亚尔 / SVU",
    "CNYUSD": "人民币 / SVU",
    "HKDUSD": "港币 / SVU",
    "HUFUSD_ECB_DISCOVERED": "匈牙利福林 / SVU",
    "IDRUSD_ECB": "印尼盾 / SVU",
    "INRUSD": "印度卢比 / SVU",
    "JPYUSD": "日元 / SVU",
    "MXNUSD": "墨西哥比索 / SVU",
    "PHPUSD_ECB_DISCOVERED": "菲律宾比索 / SVU",
    "TRYUSD_ECB_DISCOVERED": "土耳其里拉 / SVU",
    "TWDUSD": "新台币 / SVU",
    "EURUSD": "欧元 / SVU",
    "GBPUSD": "英镑 / SVU",
    "CADUSD": "加元 / SVU",
    "CHFUSD": "瑞士法郎 / SVU",
    "DKKUSD": "丹麦克朗 / SVU",
    "KRWUSD": "韩元 / SVU",
    "MYRUSD": "马来西亚林吉特 / SVU",
    "NOKUSD": "挪威克朗 / SVU",
    "SGDUSD": "新加坡元 / SVU",
    "THBUSD": "泰铢 / SVU",
    "ZARUSD": "南非兰特 / SVU",
    "BGNUSD_ECB": "保加利亚列弗 / SVU",
    "CZKUSD_ECB": "捷克克朗 / SVU",
    "ILSUSD_ECB_DISCOVERED": "以色列谢克尔 / SVU",
    "NZDUSD_ECB": "新西兰元 / SVU",
    "PLNUSD_ECB": "波兰兹罗提 / SVU",
    "RONUSD_ECB": "罗马尼亚列伊 / SVU",
    "SEKUSD_ECB": "瑞典克朗 / SVU",
    "BRENT_USD_BBL": "布伦特原油 / SVU",
    "WTI_USD_BBL": "WTI 原油 / SVU",
    "GOLD_LBMA_PM_USD_OZ": "黄金 / SVU",
    "SILVER_LBMA_USD_OZ": "白银 / SVU",
    "DJIA": "道琼斯 / SVU",
    "NASDAQ100": "纳斯达克100 / SVU",
    "NIKKEI225": "日经225 / SVU",
    "SP500": "标普500 / SVU",
    "NASDAQ": "纳斯达克综合 / SVU",
}

GROUP_TITLES = {
    "baseline": "基准与共同趋势",
    "currency": "货币",
    "energy": "能源",
    "monetary_hedge": "贵金属对冲",
    "equity_index": "股票指数",
}

GROUP_TITLES_EN = {
    "baseline": "Benchmarks and common trends",
    "currency": "Currencies",
    "energy": "Energy",
    "monetary_hedge": "Monetary hedges",
    "equity_index": "Equity indices",
}

LABELS_EN = {
    "SVU": "SVU (fixed at 100)", "ICATI-CB": "ICATI-CB category-balanced index",
    "ICATI-EW": "ICATI-EW entity-equal-weight index", "USD": "USD / SVU",
    "currency baseline": "Currency category baseline", "energy baseline": "Energy category baseline",
    "monetary_hedge baseline": "Monetary-hedge category baseline",
    "equity_index baseline": "Equity-index category baseline",
    "BRENT_USD_BBL": "Brent crude / SVU", "WTI_USD_BBL": "WTI crude / SVU",
    "GOLD_LBMA_PM_USD_OZ": "Gold / SVU", "SILVER_LBMA_USD_OZ": "Silver / SVU",
    "DJIA": "Dow Jones / SVU", "NASDAQ100": "Nasdaq-100 / SVU",
    "NIKKEI225": "Nikkei 225 / SVU", "SP500": "S&P 500 / SVU", "NASDAQ": "Nasdaq Composite / SVU",
}


def resolve_groups(cfg_groups, mapping, entities):
    groups = {}
    for group, symbols in (cfg_groups or {}).items():
        groups[group] = []
        for symbol in symbols:
            hit = mapping[mapping.symbol == symbol]
            if hit.empty:
                groups[group].append(symbol)
                continue
            rec = entities[entities.entity_id == hit.entity_id.iloc[0]]
            groups[group].append(rec.canonical_symbol.iloc[0] if not rec.empty else symbol)
    return groups


def load_panel(db_path=None):
    cfg = yaml.safe_load((ROOT / "config/svu_v03_candidate.yaml").read_text(encoding="utf-8"))
    con = sqlite3.connect(Path(db_path or ROOT / "svu.db").resolve().as_uri() + "?mode=ro", uri=True)
    try:
        obs = pd.read_sql_query(
            "select entity_id, canonical_symbol, observed_at, value from research_entity_observations_unique",
            con,
            parse_dates=["observed_at"],
        )
        mapping = pd.read_sql_query("select symbol, entity_id from research_asset_entity_map", con)
        entities = pd.read_sql_query("select entity_id, canonical_symbol from research_entity_registry", con)
    finally:
        con.close()
    groups = resolve_groups(cfg["category_groups"], mapping, entities)
    control = resolve_groups(cfg.get("long_history_groups") or {}, mapping, entities)
    assets = sorted(set(sum(groups.values(), []) + sum(control.values(), [])))
    panel = (
        obs[obs.canonical_symbol.isin(assets)]
        .pivot(index="observed_at", columns="canonical_symbol", values="value")
        .sort_index()
    )
    return panel, groups, control


def series_values(column: pd.Series) -> list:
    out = []
    for value in column.tolist():
        if pd.isna(value):
            out.append(None)
        else:
            out.append(round(float(value), 8))
    return out


def coverage(panel: pd.DataFrame) -> dict:
    rows = {}
    for col in panel.columns:
        valid = panel[col].dropna()
        if valid.empty:
            continue
        rows[col] = {
            "start": valid.index.min().strftime("%Y-%m-%d"),
            "end": valid.index.max().strftime("%Y-%m-%d"),
            "n": int(len(valid)),
        }
    return rows


def main(output_dir=None, db_path=None, panel_data=None) -> None:
    panel, groups, control = panel_data if panel_data is not None else load_panel(db_path)
    values = panel.to_numpy(dtype=float)
    if np.isinf(values).any() or (values <= 0).any():
        raise ValueError("chart prices must be finite and positive where present")
    reports = Path(output_dir) if output_dir is not None else ROOT / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    cols = [c for c in panel.columns if panel[c].notna().any()]
    missing = sorted(set(sum(groups.values(), [])) - set(cols))
    if missing:
        raise ValueError(f"official basket members missing: {missing}")
    official = [a for xs in groups.values() for a in xs if a in cols]
    control_assets = [a for xs in control.values() for a in xs if a in cols]
    common = panel[official].dropna() if official else panel[cols].dropna()
    if len(common) < 3:
        raise ValueError("official basket requires at least three complete observations")
    kinds = {"SVU": "baseline", "ICATI-CB": "baseline", "ICATI-EW": "baseline", "USD": "currency"}
    for group, members in groups.items():
        kinds[f"{group} baseline"] = group
        for asset in members:
            if asset in cols:
                kinds[asset] = group
    if "NASDAQ" in cols:
        kinds["NASDAQ"] = "equity_index"
    payload = {
        "dates": [x.strftime("%Y-%m-%d") for x in panel.index],
        "prices": {col: series_values(panel[col]) for col in cols},
        "groups": groups,
        "control_groups": control,
        "official_assets": official,
        "control_assets": control_assets,
        "kinds": kinds,
        "labels": {key: LABELS.get(key, key) for key in list(kinds)},
        "labels_en": {key: LABELS_EN.get(key, key) for key in list(kinds)},
        "group_titles": GROUP_TITLES,
        "group_titles_en": GROUP_TITLES_EN,
        "coverage": coverage(panel[cols]),
        "earliest": panel.index.min().strftime("%Y-%m-%d"),
        "latest": panel.index.max().strftime("%Y-%m-%d"),
        "complete_start": common.index.min().strftime("%Y-%m-%d") if len(common) else None,
        "complete_end": common.index.max().strftime("%Y-%m-%d") if len(common) else None,
        "default": ["SVU", "USD", "ICATI-CB", "ICATI-EW"] + [f"{k} baseline" for k in groups],
        "default_mode": "long",
        "no_fill_missing_as_today": True,
        "official_conclusions_from": "complete_basket_only",
    }
    page = TEMPLATE.read_text(encoding="utf-8").replace(
        "__PAYLOAD__", json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    )
    out = reports / "svu_v03_dashboard.html"
    daily = reports / "svu_daily.html"
    out.write_text(page, encoding="utf-8")
    daily.write_text(page, encoding="utf-8")
    result = {
        "experiment": "SVU-BL-039",
        "status": "incomplete_basket_long_chart",
        "file": str(daily),
        "legacy_file": str(out),
        "build": "041",
        "series_count": len(kinds),
        "official_n": len(official),
        "control_n": len(control_assets),
        "union_earliest": payload["earliest"],
        "union_latest": payload["latest"],
        "union_days": len(payload["dates"]),
        "complete_basket_earliest": payload["complete_start"],
        "complete_basket_latest": payload["complete_end"],
        "no_fill_missing_as_today": True,
        "default_mode": "long",
    }
    (reports / "baseline_bl039.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports / "baseline_bl039.md").write_text(
        "\n".join(
            [
                "# SVU-BL-039 不完整篮子长图",
                "",
                "> 默认按并集/在场重标绘图。正式结论仍只使用篮子齐全窗口。不填充缺失日。",
                "",
                f"- 并集：`{payload['earliest']}` 至 `{payload['latest']}`（L1）",
                f"- 正式齐全窗口：`{payload['complete_start']}` 至 `{payload['complete_end']}`（L1）",
                f"- 正式篮子 {len(official)} 个；长历史对照篮 {len(control_assets)} 个（无道指/标普/纳指100）",
                "",
                "页面：`reports/svu_daily.html`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
