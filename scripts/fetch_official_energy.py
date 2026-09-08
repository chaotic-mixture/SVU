"""获取 SVU-DATA-010：官方 FRED 日频能源商品，失败序列不入库。"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "official_energy"
OUT = ROOT / "data" / "processed" / "official_energy.csv"
MAN = ROOT / "data" / "processed" / "official_energy_manifest.json"

ASSETS = [
    {"symbol": "NG_HH_USD_MMBTU", "fred": "DHHNGSP", "asset_type": "commodity", "unit": "USD per million BTU", "transform": "identity"},
    {"symbol": "HO_NY_USD_GAL", "fred": "DHOILNYH", "asset_type": "commodity", "unit": "USD per gallon", "transform": "identity"},
    {"symbol": "GAS_NY_USD_GAL", "fred": "DGASNYH", "asset_type": "commodity", "unit": "USD per gallon", "transform": "identity"},
    {"symbol": "GAS_GULF_USD_GAL", "fred": "DGASUSGULF", "asset_type": "commodity", "unit": "USD per gallon", "transform": "identity"},
    {"symbol": "ULSD_GULF_USD_GAL", "fred": "DJFUELUSGULF", "asset_type": "commodity", "unit": "USD per gallon", "transform": "identity"},
    {"symbol": "PROPANE_MB_USD_GAL", "fred": "DPROPANEMBTX", "asset_type": "commodity", "unit": "USD per gallon", "transform": "identity"},
]


def fetch_one(spec: dict) -> tuple[pd.DataFrame, dict]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={spec['fred']}"
    last_error = None
    resp = None
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=90)
            resp.raise_for_status()
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            time.sleep(2 * attempt)
    if last_error is not None or resp is None:
        raise last_error or RuntimeError(f"failed to fetch {spec['fred']}")
    raw_bytes = resp.content
    RAW.mkdir(parents=True, exist_ok=True)
    raw_path = RAW / f"{spec['symbol']}_{spec['fred']}.csv"
    raw_path.write_bytes(raw_bytes)
    frame = pd.read_csv(BytesIO(raw_bytes))
    frame = frame.iloc[:, :2]
    frame.columns = ["timestamp", "value"]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna().sort_values("timestamp")
    if spec["transform"] == "inverse":
        frame["value"] = 1.0 / frame["value"]
    frame = frame[frame["value"] > 0]
    if frame.empty:
        raise ValueError(f"{spec['symbol']} has no positive observations")
    frame["symbol"] = spec["symbol"]
    frame["source"] = f"FRED:{spec['fred']}"
    frame["unit"] = spec["unit"]
    frame = frame[["timestamp", "value", "symbol", "source", "unit"]]
    meta = {
        "symbol": spec["symbol"],
        "fred_series": spec["fred"],
        "asset_type": spec["asset_type"],
        "url": url,
        "unit": spec["unit"],
        "transform": spec["transform"],
        "raw_file": str(raw_path.relative_to(ROOT)),
        "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "rows": int(len(frame)),
        "start": frame.timestamp.min().strftime("%Y-%m-%d"),
        "end": frame.timestamp.max().strftime("%Y-%m-%d"),
        "status": "loaded",
    }
    return frame, meta


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    frames = []
    assets = []
    failures = []
    for spec in ASSETS:
        try:
            frame, meta = fetch_one(spec)
            frames.append(frame)
            assets.append(meta)
        except Exception as exc:
            failures.append({"symbol": spec["symbol"], "fred": spec["fred"], "error": str(exc)[:400], "status": "failed_not_loaded"})
    if not frames:
        raise RuntimeError("SVU-DATA-010 fetched zero successful series")
    df = pd.concat(frames, ignore_index=True, sort=False)
    df.to_csv(OUT, index=False, date_format="%Y-%m-%d")
    man = {
        "dataset_id": "SVU-DATA-010",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_window": {"start": "native_series_start", "end": "latest_official_observation"},
        "frequency": "daily/available observation",
        "numeraire": "USD",
        "selection_rule": "official FRED EIA energy spot prices; skip FX padding; skip short-history ICE bond TR; skip monthly OECD share prices",
        "assets": assets,
        "failures": failures,
        "deferred": [
            {"symbol": "US_AAA_CORP_TR", "fred": "BAMLCC0A1AAATRIV", "reason": "starts 2023-09; short-history diagnostic only, duplicate of existing US_CORP_BOND_TR family"},
            {"symbol": "UK_SHARE_OECD", "fred": "SPASTT01GBM661N", "reason": "monthly OECD share prices; needs frequency-aware audit before load"},
            {"symbol": "EA_SHARE_OECD", "fred": "SPASTT01EZM661N", "reason": "monthly OECD share prices; needs frequency-aware audit before load"},
        ],
        "processed_file": str(OUT.relative_to(ROOT)),
        "processed_sha256": hashlib.sha256(OUT.read_bytes()).hexdigest(),
        "rows": int(len(df)),
    }
    MAN.write_text(json.dumps(man, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"dataset_id": man["dataset_id"], "assets": len(assets), "rows": man["rows"], "failures": len(failures), "sha256": man["processed_sha256"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
