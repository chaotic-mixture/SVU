"""将公开研究数据幂等加载到 canonical svu.db 的独立研究表。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "svu.db"
MANIFEST = ROOT / "data" / "processed" / "public_manifest.json"
PRICES = ROOT / "data" / "processed" / "public_prices.csv"
MACRO = ROOT / "data" / "processed" / "macro_indicators.csv"


def asset_type(symbol: str) -> str:
    if symbol in {"EURUSD", "JPYUSD", "CADUSD", "AUDUSD", "GBPUSD", "CHFUSD", "CNYUSD"}:
        return "currency"
    if symbol in {"SP500", "NASDAQ", "NIKKEI225", "DJIA"}:
        return "equity_index"
    return "commodity"


def main() -> None:
    (ROOT / "reports").mkdir(parents=True, exist_ok=True)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    prices = pd.read_csv(PRICES, parse_dates=["timestamp"])
    macro = pd.read_csv(MACRO, parse_dates=["timestamp"])
    con = sqlite3.connect(DB)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS research_datasets (
        dataset_id TEXT PRIMARY KEY,
        retrieved_at_utc TEXT NOT NULL,
        numeraire TEXT,
        frequency TEXT,
        normalized_file TEXT,
        normalized_sha256 TEXT,
        macro_file TEXT,
        macro_sha256 TEXT
    );
    CREATE TABLE IF NOT EXISTS research_assets (
        symbol TEXT PRIMARY KEY,
        asset_type TEXT NOT NULL,
        unit TEXT NOT NULL,
        source TEXT NOT NULL,
        transform TEXT,
        dataset_id TEXT NOT NULL REFERENCES research_datasets(dataset_id)
    );
    CREATE TABLE IF NOT EXISTS research_price_observations (
        dataset_id TEXT NOT NULL REFERENCES research_datasets(dataset_id),
        symbol TEXT NOT NULL REFERENCES research_assets(symbol),
        observed_at TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        unit TEXT NOT NULL,
        PRIMARY KEY (dataset_id, symbol, observed_at)
    );
    CREATE TABLE IF NOT EXISTS research_macro_observations (
        dataset_id TEXT NOT NULL REFERENCES research_datasets(dataset_id),
        symbol TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        value REAL NOT NULL,
        source TEXT NOT NULL,
        unit TEXT NOT NULL,
        PRIMARY KEY (dataset_id, symbol, observed_at)
    );
    CREATE INDEX IF NOT EXISTS idx_research_prices_symbol_date ON research_price_observations(symbol, observed_at);
    CREATE INDEX IF NOT EXISTS idx_research_macro_symbol_date ON research_macro_observations(symbol, observed_at);
    """)
    con.execute("INSERT OR REPLACE INTO research_datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (
        manifest["dataset_id"], manifest["retrieved_at_utc"], manifest["numeraire"], manifest["frequency"],
        manifest["normalized_file"], manifest["normalized_sha256"], manifest["macro_file"], manifest["macro_sha256"],
    ))
    for entry in manifest["assets"]:
        con.execute("INSERT OR REPLACE INTO research_assets VALUES (?, ?, ?, ?, ?, ?)", (
            entry["symbol"], asset_type(entry["symbol"]), entry["unit"], f"FRED:{entry['fred_series']}", entry.get("transform", "identity"), manifest["dataset_id"],
        ))
    con.executemany("INSERT OR REPLACE INTO research_price_observations VALUES (?, ?, ?, ?, ?, ?)", [
        (manifest["dataset_id"], r.symbol, r.timestamp.strftime("%Y-%m-%d"), float(r.value), r.source, r.unit)
        for r in prices.itertuples()
    ])
    con.executemany("INSERT OR REPLACE INTO research_macro_observations VALUES (?, ?, ?, ?, ?, ?)", [
        (manifest["dataset_id"], r.symbol, r.timestamp.strftime("%Y-%m-%d"), float(r.value), r.source, r.unit)
        for r in macro.itertuples()
    ])
    con.commit()
    counts = {
        "datasets": con.execute("SELECT count(*) FROM research_datasets").fetchone()[0],
        "assets": con.execute("SELECT count(*) FROM research_assets").fetchone()[0],
        "price_observations": con.execute("SELECT count(*) FROM research_price_observations WHERE dataset_id=?", (manifest["dataset_id"],)).fetchone()[0],
        "macro_observations": con.execute("SELECT count(*) FROM research_macro_observations WHERE dataset_id=?", (manifest["dataset_id"],)).fetchone()[0],
    }
    con.close()
    result = {"operation": "load_public_research_data", "database": str(DB), "dataset_id": manifest["dataset_id"], "counts": counts, "idempotent": True}
    report = ROOT / "reports" / "database_load_latest.json"
    report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
