"""获取并登记黄金与债券价格/总回报数据集 SVU-DATA-004。"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "supplemental"
PROCESSED = ROOT / "data" / "processed"
START, END = "2015-01-01", "2025-12-31"


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    PROCESSED.mkdir(parents=True, exist_ok=True)
    gold_url = "https://prices.lbma.org.uk/json/gold_pm.json"
    gold_resp = requests.get(gold_url, timeout=90)
    gold_resp.raise_for_status()
    gold_bytes = gold_resp.content
    gold_raw = RAW / "gold_pm.json"
    gold_raw.write_bytes(gold_bytes)
    gold = pd.DataFrame([
        {"timestamp": row["d"], "value": row["v"][0]}
        for row in gold_resp.json()
        if row.get("v") and row["v"][0] is not None
    ])
    gold["timestamp"] = pd.to_datetime(gold["timestamp"], errors="coerce")
    gold["value"] = pd.to_numeric(gold["value"], errors="coerce")
    gold = gold.dropna().query("timestamp >= @START and timestamp <= @END and value > 0")
    gold["symbol"], gold["source"], gold["unit"] = "GOLD_LBMA_PM_USD_OZ", "LBMA:gold_pm", "USD per troy ounce"

    silver_url = "https://prices.lbma.org.uk/json/silver.json"
    silver_resp = requests.get(silver_url, timeout=90)
    silver_resp.raise_for_status()
    silver_bytes = silver_resp.content
    silver_raw = RAW / "silver.json"
    silver_raw.write_bytes(silver_bytes)
    silver = pd.DataFrame([
        {"timestamp": row["d"], "value": row["v"][0]}
        for row in silver_resp.json()
        if row.get("v") and row["v"][0] is not None
    ])
    silver["timestamp"] = pd.to_datetime(silver["timestamp"], errors="coerce")
    silver["value"] = pd.to_numeric(silver["value"], errors="coerce")
    silver = silver.dropna().query("timestamp >= @START and timestamp <= @END and value > 0")
    silver["symbol"], silver["source"], silver["unit"] = "SILVER_LBMA_USD_OZ", "LBMA:silver", "USD per troy ounce"

    bond_url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLCC0A0CMTRIV"
    bond_resp = requests.get(bond_url, timeout=90)
    bond_resp.raise_for_status()
    bond_bytes = bond_resp.content
    bond_raw = RAW / "us_corporate_bond_total_return_fred.csv"
    bond_raw.write_bytes(bond_bytes)
    bond = pd.read_csv(BytesIO(bond_bytes))
    bond.columns = ["timestamp", "value"]
    bond["timestamp"] = pd.to_datetime(bond["timestamp"], errors="coerce")
    bond["value"] = pd.to_numeric(bond["value"], errors="coerce")
    bond = bond.dropna().query("timestamp >= @START and timestamp <= @END and value > 0")
    bond["symbol"], bond["source"], bond["unit"] = "US_CORP_BOND_TR", "FRED:BAMLCC0A0CMTRIV", "total return index"

    combined = pd.concat([gold, silver, bond], ignore_index=True).sort_values(["timestamp", "symbol"])
    output = PROCESSED / "supplemental_prices.csv"
    combined.to_csv(output, index=False, date_format="%Y-%m-%d")
    manifest = {
        "dataset_id": "SVU-DATA-004",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_window": {"start": START, "end": END},
        "frequency": "daily/available observation; common-date alignment at analysis time",
        "numeraire": "USD",
        "assets": [
            {"symbol": "GOLD_LBMA_PM_USD_OZ", "source": gold_url, "provider_series": "LBMA gold PM", "unit": "USD per troy ounce", "raw_file": str(gold_raw.relative_to(ROOT)), "raw_sha256": hashlib.sha256(gold_bytes).hexdigest(), "rows": int(len(gold)), "start": gold.timestamp.min().strftime("%Y-%m-%d"), "end": gold.timestamp.max().strftime("%Y-%m-%d")},
            {"symbol": "SILVER_LBMA_USD_OZ", "source": silver_url, "provider_series": "LBMA silver", "unit": "USD per troy ounce", "raw_file": str(silver_raw.relative_to(ROOT)), "raw_sha256": hashlib.sha256(silver_bytes).hexdigest(), "rows": int(len(silver)), "start": silver.timestamp.min().strftime("%Y-%m-%d"), "end": silver.timestamp.max().strftime("%Y-%m-%d")},
            {"symbol": "US_CORP_BOND_TR", "source": bond_url, "provider_series": "BAMLCC0A0CMTRIV", "unit": "total return index", "raw_file": str(bond_raw.relative_to(ROOT)), "raw_sha256": hashlib.sha256(bond_bytes).hexdigest(), "rows": int(len(bond)), "start": bond.timestamp.min().strftime("%Y-%m-%d"), "end": bond.timestamp.max().strftime("%Y-%m-%d")},
        ],
        "processed_file": str(output.relative_to(ROOT)),
        "processed_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "limitations": [
            "黄金为 LBMA PM 基准价格；债券为美国投资级公司债总回报指数，不是单只债券价格。",
            "两序列发布时间和频率不同，分析时需共同日期或月末对齐。",
        ],
    }
    manifest_path = PROCESSED / "supplemental_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"dataset_id": manifest["dataset_id"], "rows": len(combined), "assets": ["GOLD_LBMA_PM_USD_OZ", "SILVER_LBMA_USD_OZ", "US_CORP_BOND_TR"], "processed_sha256": manifest["processed_sha256"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
