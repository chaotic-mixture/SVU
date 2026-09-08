"""获取 SVU-BL-002 的公开 FRED 日频数据并生成血缘清单。"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "public"
OUT_FILE = ROOT / "data" / "processed" / "public_prices.csv"
MANIFEST = ROOT / "data" / "processed" / "public_manifest.json"
START_DATE = "2015-01-01"
END_DATE = "2025-12-31"

SERIES = {
    # FRED series are public downloads; transformations are recorded below.
    "EURUSD": {"fred": "DEXUSEU", "unit": "USD per EUR", "transform": "identity"},
    "JPYUSD": {"fred": "DEXJPUS", "unit": "USD per JPY", "transform": "inverse"},
    "WTI_USD_BBL": {"fred": "DCOILWTICO", "unit": "USD per barrel", "transform": "identity"},
    "SP500": {"fred": "SP500", "unit": "index points", "transform": "identity"},
    "NASDAQ": {"fred": "NASDAQCOM", "unit": "index points", "transform": "identity"},
    "CADUSD": {"fred": "DEXCAUS", "unit": "USD per CAD", "transform": "inverse"},
    "AUDUSD": {"fred": "DEXUSAL", "unit": "USD per AUD", "transform": "identity"},
    "NIKKEI225": {"fred": "NIKKEI225", "unit": "index points", "transform": "identity"},
    "GBPUSD": {"fred": "DEXUSUK", "unit": "USD per GBP", "transform": "identity"},
    "CHFUSD": {"fred": "DEXSZUS", "unit": "USD per CHF", "transform": "inverse"},
    "CNYUSD": {"fred": "DEXCHUS", "unit": "USD per CNY", "transform": "inverse"},
    "DJIA": {"fred": "DJIA", "unit": "index points", "transform": "identity"},
    "BRENT_USD_BBL": {"fred": "DCOILBRENTEU", "unit": "USD per barrel", "transform": "identity"},
}

MACRO_SERIES = {
    "CPI_US": {"fred": "CPIAUCSL", "unit": "index, 1982-84=100"},
    "UNRATE_US": {"fred": "UNRATE", "unit": "percent"},
    "GS10_US": {"fred": "GS10", "unit": "percent"},
    "DGS10_US": {"fred": "DGS10", "unit": "percent"},
    "STLFSI4_US": {"fred": "STLFSI4", "unit": "index"},
    "VIX_US": {"fred": "VIXCLS", "unit": "index"},
    "NFCI_US": {"fred": "NFCI", "unit": "index"},
    "DOLLAR_BROAD_US": {"fred": "DTWEXBGS", "unit": "index"},
    "M2_US": {"fred": "M2SL", "unit": "billions of dollars"},
    "FEDFUNDS_US": {"fred": "FEDFUNDS", "unit": "percent"},
    "PCE_US": {"fred": "PCEPI", "unit": "index, 2017=100"},
    "REAL_GDP_US": {"fred": "GDPC1", "unit": "billions of chained 2017 dollars"},
    "INDPRO_US": {"fred": "INDPRO", "unit": "index, 2017=100"},
    "UMCSENT_US": {"fred": "UMCSENT", "unit": "index"},
    "T10Y2Y_US": {"fred": "T10Y2Y", "unit": "percent"},
    "HY_SPREAD_US": {"fred": "BAMLH0A0HYM2", "unit": "percent"},
    "IG_SPREAD_US": {"fred": "BAMLC0A0CM", "unit": "percent"},
}


def fetch_series(symbol: str, spec: dict) -> tuple[pd.DataFrame, dict]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={spec['fred']}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    raw_bytes = response.content
    raw_path = RAW_DIR / f"{symbol}_{spec['fred']}.csv"
    raw_path.write_bytes(raw_bytes)
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()

    from io import BytesIO
    frame = pd.read_csv(BytesIO(raw_bytes))
    frame.columns = ["timestamp", "value"]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna().sort_values("timestamp")
    frame = frame[(frame["timestamp"] >= START_DATE) & (frame["timestamp"] <= END_DATE)]
    if spec["transform"] == "inverse":
        frame["value"] = 1.0 / frame["value"]
    frame = frame[frame["value"] > 0]
    frame["symbol"] = symbol
    frame["type"] = "currency" if symbol in {"EURUSD", "JPYUSD"} else ("commodity" if symbol != "SP500" else "index")
    frame["source"] = f"FRED:{spec['fred']}"
    frame["unit"] = spec["unit"]
    frame["confidence"] = 1.0
    return frame[["symbol", "type", "timestamp", "value", "source", "unit", "confidence"]], {
        "symbol": symbol,
        "fred_series": spec["fred"],
        "url": url,
        "unit": spec["unit"],
        "transform": spec["transform"],
        "raw_file": str(raw_path.relative_to(ROOT)),
        "raw_sha256": raw_hash,
        "rows_after_cleaning": int(len(frame)),
        "start": frame["timestamp"].min().strftime("%Y-%m-%d"),
        "end": frame["timestamp"].max().strftime("%Y-%m-%d"),
    }


def fetch_macro_series(symbol: str, spec: dict) -> tuple[pd.DataFrame, dict]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={spec['fred']}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    raw_bytes = response.content
    raw_path = RAW_DIR / f"{symbol}_{spec['fred']}.csv"
    raw_path.write_bytes(raw_bytes)
    frame = pd.read_csv(__import__("io").BytesIO(raw_bytes))
    frame.columns = ["timestamp", "value"]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna().sort_values("timestamp")
    frame = frame[(frame["timestamp"] >= START_DATE) & (frame["timestamp"] <= END_DATE)]
    frame["symbol"] = symbol
    frame["source"] = f"FRED:{spec['fred']}"
    frame["unit"] = spec["unit"]
    return frame[["symbol", "timestamp", "value", "source", "unit"]], {
        "symbol": symbol,
        "fred_series": spec["fred"],
        "url": url,
        "unit": spec["unit"],
        "raw_file": str(raw_path.relative_to(ROOT)),
        "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "rows_after_cleaning": int(len(frame)),
        "start": frame["timestamp"].min().strftime("%Y-%m-%d"),
        "end": frame["timestamp"].max().strftime("%Y-%m-%d"),
    }


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    frames, entries = [], []
    for symbol, spec in SERIES.items():
        frame, entry = fetch_series(symbol, spec)
        frames.append(frame)
        entries.append(entry)
    combined = pd.concat(frames, ignore_index=True).sort_values(["timestamp", "symbol"])
    combined.to_csv(OUT_FILE, index=False, date_format="%Y-%m-%d")
    output_hash = hashlib.sha256(OUT_FILE.read_bytes()).hexdigest()
    macro_frames, macro_entries = [], []
    for symbol, spec in MACRO_SERIES.items():
        frame, entry = fetch_macro_series(symbol, spec)
        macro_frames.append(frame)
        macro_entries.append(entry)
    macro_file = ROOT / "data" / "processed" / "macro_indicators.csv"
    pd.concat(macro_frames, ignore_index=True).sort_values(["timestamp", "symbol"]).to_csv(macro_file, index=False, date_format="%Y-%m-%d")
    macro_hash = hashlib.sha256(macro_file.read_bytes()).hexdigest()
    manifest = {
        "dataset_id": "SVU-DATA-003",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_window": {"start": START_DATE, "end": END_DATE},
        "frequency": "daily observation, common-date alignment at analysis time",
        "numeraire": "USD",
        "assets": entries,
        "macro_indicators": macro_entries,
        "normalized_file": str(OUT_FILE.relative_to(ROOT)),
        "macro_file": str(macro_file.relative_to(ROOT)),
        "normalized_sha256": output_hash,
        "macro_sha256": macro_hash,
        "limitations": [
            "FRED series have different publication calendars and missing dates.",
            "JPY series is inverted from JPY per USD to USD per JPY.",
            "SP500 is an index level, not a total-return index.",
            "This dataset is a research baseline, not a claim of universal value.",
        ],
    }
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"dataset_id": manifest["dataset_id"], "rows": len(combined), "assets": list(SERIES), "normalized_sha256": output_hash}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
