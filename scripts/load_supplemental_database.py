"""将 SVU-DATA-004 补充数据幂等写入 svu.db。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "svu.db"
MANIFEST = ROOT / "data" / "processed" / "supplemental_manifest.json"
DATA = ROOT / "data" / "processed" / "supplemental_prices.csv"


def main() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    data = pd.read_csv(DATA, parse_dates=["timestamp"])
    con = sqlite3.connect(DB)
    con.execute("INSERT OR REPLACE INTO research_datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
        manifest["dataset_id"], manifest["retrieved_at_utc"], manifest["numeraire"], manifest["frequency"],
        None, None, manifest["processed_file"], manifest["processed_sha256"],
    ))
    type_by_symbol = {"GOLD_LBMA_PM_USD_OZ": "commodity", "SILVER_LBMA_USD_OZ": "commodity", "US_CORP_BOND_TR": "bond_index"}
    for entry in manifest["assets"]:
        con.execute("INSERT OR REPLACE INTO research_assets VALUES (?, ?, ?, ?, ?, ?)", (
            entry["symbol"], type_by_symbol[entry["symbol"]], entry["unit"], entry["source"], "identity", manifest["dataset_id"],
        ))
    con.executemany("INSERT OR REPLACE INTO research_price_observations VALUES (?, ?, ?, ?, ?, ?)", [
        (manifest["dataset_id"], r.symbol, r.timestamp.strftime("%Y-%m-%d"), float(r.value), r.source, r.unit)
        for r in data.itertuples()
    ])
    con.commit()
    result = {
        "operation": "load_supplemental_research_data",
        "database": str(DB),
        "dataset_id": manifest["dataset_id"],
        "assets": con.execute("SELECT count(*) FROM research_assets WHERE symbol in (SELECT DISTINCT symbol FROM research_price_observations WHERE dataset_id=?)", (manifest["dataset_id"],)).fetchone()[0],
        "price_observations": con.execute("SELECT count(*) FROM research_price_observations WHERE dataset_id=?", (manifest["dataset_id"],)).fetchone()[0],
        "idempotent": True,
    }
    con.close()
    (ROOT / "reports" / "database_supplemental_load_latest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
