"""幂等加载 SVU-DATA-010 官方日频能源商品。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    man = json.loads((ROOT / "data" / "processed" / "official_energy_manifest.json").read_text(encoding="utf-8"))
    df = pd.read_csv(ROOT / "data" / "processed" / "official_energy.csv", parse_dates=["timestamp"])
    if df.empty:
        raise RuntimeError("official_energy.csv is empty")
    con = sqlite3.connect(ROOT / "svu.db")
    con.execute(
        "insert or replace into research_datasets values (?,?,?,?,?,?,?,?)",
        (man["dataset_id"], man["retrieved_at_utc"], man["numeraire"], man["frequency"], man["processed_file"], man["processed_sha256"], None, None),
    )
    asset_rows = []
    for entry in man["assets"]:
        asset_rows.append(
            (entry["symbol"], entry["asset_type"], entry["unit"], f"FRED:{entry['fred_series']}", entry["transform"], man["dataset_id"])
        )
    con.executemany("insert or replace into research_assets values (?,?,?,?,?,?)", asset_rows)
    obs_rows = [
        (man["dataset_id"], r.symbol, r.timestamp.strftime("%Y-%m-%d"), float(r.value), r.source, r.unit)
        for r in df.itertuples()
    ]
    con.executemany("insert or replace into research_price_observations values (?,?,?,?,?,?)", obs_rows)
    con.commit()
    result = {
        "operation": "load_official_energy",
        "dataset_id": man["dataset_id"],
        "assets": con.execute("select count(distinct symbol) from research_price_observations where dataset_id=?", (man["dataset_id"],)).fetchone()[0],
        "rows": con.execute("select count(*) from research_price_observations where dataset_id=?", (man["dataset_id"],)).fetchone()[0],
        "symbols": [row[0] for row in con.execute("select distinct symbol from research_price_observations where dataset_id=? order by 1", (man["dataset_id"],))],
        "idempotent": True,
    }
    con.close()
    (ROOT / "reports" / "official_energy_load_latest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
