"""SVU 官方数据日更：追加新观测，不覆盖历史，并重建分领域派生页面。"""
from __future__ import annotations

import json
import hashlib
import re
import sqlite3
import sys
from contextlib import closing
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import numpy as np
import requests

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "svu.db"
RAW = ROOT / "data" / "raw" / "daily_updates"
LIVE = "SVU-LIVE-DAILY"
DOMAIN_KEYS = ["currency", "energy", "monetary_hedge", "equity_index"]


def clean_frame(frame):
    frame = frame.copy()
    frame["observed_at"] = pd.to_datetime(frame["observed_at"], errors="coerce").dt.normalize()
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna(subset=["observed_at", "value"])
    frame = frame[np.isfinite(frame.value) & (frame.value > 0)]
    if frame.empty:
        raise ValueError("source returned no finite positive observations")
    if frame.observed_at.duplicated().any():
        raise ValueError("source returned duplicate observation dates")
    return frame.sort_values("observed_at")


def ecb_currency(source):
    match = re.fullmatch(r"ECB:EXR\.(?:D\.)?([A-Z]{3})(?:\.EUR\.SP00\.A)?", source)
    if not match:
        raise ValueError("unsupported ECB series")
    return match.group(1)


def save_raw(name, extension, payload):
    RAW.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(payload).hexdigest()
    path = RAW / f"{name}_{date.today().isoformat()}_{digest}.{extension}"
    if not path.exists():
        path.write_bytes(payload)
    return path


def fetch_ecb(source, cache=None):
    """Use the same EUR cross-rate convention as the initial ECB loaders."""
    currency = ecb_currency(source)
    cache = {} if cache is None else cache
    def series(currency):
        if currency in cache:
            return cache[currency].copy()
        url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.{currency}.EUR.SP00.A?format=csvdata"
        response = requests.get(url, timeout=90)
        response.raise_for_status()
        save_raw(f"ECB_{currency}", "csv", response.content)
        frame = pd.read_csv(BytesIO(response.content)).rename(columns={"TIME_PERIOD": "observed_at", "OBS_VALUE": "value"})
        cache[currency] = clean_frame(frame[["observed_at", "value"]])
        return cache[currency].copy()
    local = series(currency).rename(columns={"value": "local"})
    usd = series("USD").rename(columns={"value": "usd"})
    cross = local.merge(usd, on="observed_at", validate="one_to_one")
    cross["value"] = cross.usd / cross.local
    return clean_frame(cross[["observed_at", "value"]])


def rebuild_domain_dashboards(builder=None):
    """Rebuild every registered domain view after the database commit."""
    if builder is None:
        from validation.build_domain_dashboard import build

        builder = build
    results = []
    for domain in DOMAIN_KEYS:
        try:
            results.append(builder(domain))
        except Exception as exc:  # keep the refresh report fail-visible per domain
            results.append(
                {
                    "domain_key": domain,
                    "status": "failed",
                    "error": str(exc)[:500],
                }
            )
    return results


def fred_id(source: str) -> str | None:
    series = None
    if source.startswith("FRED:") and "http" not in source:
        series = source.split(":", 1)[1]
    if urlparse(source).hostname == "fred.stlouisfed.org" and "id=" in source:
        ids = parse_qs(urlparse(source).query).get("id") or []
        series = ids[0] if ids else None
    return series if series and re.fullmatch(r"[A-Za-z0-9_]+", series) else None


def source_identity(source):
    series = fred_id(source)
    if series:
        return f"FRED:{series}"
    if source.startswith("ECB:EXR."):
        return f"ECB:EXR.D.{ecb_currency(source)}.EUR.SP00.A"
    if urlparse(source).hostname == "prices.lbma.org.uk":
        return "LBMA:" + Path(urlparse(source).path).stem
    return source


def fetch_fred(series: str, transform: str) -> pd.DataFrame:
    if not re.fullmatch(r"[A-Za-z0-9_]+", series) or transform not in {'identity', 'inverse'}:
        raise ValueError('unsupported FRED series or transformation')
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    raw = resp.content
    save_raw(series, "csv", raw)
    df = pd.read_csv(BytesIO(raw))
    df.columns = ["observed_at", "value"]
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna()
    df = df[df["value"] > 0]
    if transform == "inverse":
        df["value"] = 1.0 / df["value"]
    return clean_frame(df)


def fetch_lbma(kind: str) -> pd.DataFrame:
    if kind not in {'gold_pm', 'silver'}:
        raise ValueError('unsupported LBMA series')
    url = f"https://prices.lbma.org.uk/json/{kind}.json"
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    save_raw(f"LBMA_{kind}", "json", resp.content)
    rows = []
    for item in resp.json():
        values = item.get("v") or []
        if not values or values[0] is None:
            continue
        rows.append({"observed_at": item.get("d"), "value": values[0]})
    df = pd.DataFrame(rows, columns=["observed_at", "value"])
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna()
    return clean_frame(df)


def insert_new(con, symbol, source, unit, frame):
    frame = clean_frame(frame)
    existing = {}
    source = source_identity(source)
    for observed, value, old_source in con.execute(
        "select observed_at,value,source from research_price_observations where symbol=? and unit=?",
        (symbol, unit),
    ):
        if source_identity(old_source) == source:
            existing.setdefault(observed, set()).add(float(value))
    conflicts = [row.observed_at.strftime("%Y-%m-%d") for row in frame.itertuples()
                 if row.observed_at.strftime("%Y-%m-%d") in existing
                 and not any(np.isclose(float(row.value), old, rtol=1e-12, atol=0)
                             for old in existing[row.observed_at.strftime("%Y-%m-%d")])]
    if conflicts:
        # Raw response is retained; never silently choose a revision or change history.
        raise ValueError(f"source revisions require review: {len(conflicts)} dates, first={conflicts[0]}")
    inserted = 0
    for row in frame.itertuples():
        if row.observed_at.strftime("%Y-%m-%d") in existing:
            continue
        cur = con.execute(
            "insert or ignore into research_price_observations values (?,?,?,?,?,?)",
            (LIVE, symbol, row.observed_at.strftime("%Y-%m-%d"), float(row.value), source, unit),
        )
        inserted += cur.rowcount
    return len(frame), inserted


def rebuild_outputs():
    from scripts.publication import publish
    return publish(ROOT)


def main() -> int:
    if not DB.is_file():
        raise FileNotFoundError("research database missing; run python -m scripts.rebuild_research first")
    RAW.mkdir(parents=True, exist_ok=True)
    (ROOT / "reports").mkdir(parents=True, exist_ok=True)
    from scripts.publication import operation_lock
    with operation_lock(ROOT / "reports" / ".database.lock"):
        with closing(sqlite3.connect(DB)) as con:
            return run_refresh(con)


def run_refresh(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS research_refresh_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT, dataset_id TEXT, run_date TEXT, source TEXT,
        status TEXT, records_seen INTEGER, records_inserted INTEGER, error TEXT, created_at TEXT)"""
    )
    con.execute(
        "insert or ignore into research_datasets values (?,?,?,?,?,?,?,?)",
        (LIVE, datetime.now(timezone.utc).isoformat(), "USD", "daily incremental official refresh", None, None, None, None),
    )
    assets = con.execute("select symbol, asset_type, unit, source, transform from research_assets").fetchall()
    stats = []
    total = 0
    checked = 0
    ecb_cache = {}
    for symbol, asset_type, unit, source, transform in assets:
        if "monthly" in unit.lower():
            stats.append({"symbol": symbol, "source": source, "status": "skipped", "reason": "monthly data excluded from daily refresh"})
            continue
        series = fred_id(source)
        kind = None
        if series:
            kind = "fred"
        elif source.startswith("ECB:EXR."):
            kind = "ecb"
        elif source_identity(source) == "LBMA:gold_pm":
            kind = "gold"
        elif source_identity(source) == "LBMA:silver":
            kind = "silver"
        else:
            stats.append({"symbol": symbol, "source": source, "status": "skipped", "reason": "unsupported source"})
            continue
        checked += 1
        try:
            con.execute("SAVEPOINT refresh_asset")
            if kind == "fred":
                df = fetch_fred(series, transform or "identity")
            elif kind == "ecb":
                df = fetch_ecb(source, cache=ecb_cache)
                source = f"ECB:EXR.D.{ecb_currency(source)}.EUR.SP00.A"
            elif kind == "gold":
                df = fetch_lbma("gold_pm")
            else:
                df = fetch_lbma("silver")
            seen, inserted = insert_new(con, symbol, source, unit, df)
            status = "updated" if inserted else "no_new_data"
            con.execute(
                "insert into research_refresh_runs(dataset_id,run_date,source,status,records_seen,records_inserted,error,created_at) values (?,?,?,?,?,?,?,?)",
                (LIVE, date.today().isoformat(), source, status, seen, inserted, None, datetime.now(timezone.utc).isoformat()),
            )
            con.execute("RELEASE SAVEPOINT refresh_asset")
            stats.append({"symbol": symbol, "source": source, "status": status, "seen": seen, "inserted": inserted,
                          "latest_observation": df.observed_at.max().strftime("%Y-%m-%d"),
                          "age_days": (date.today() - df.observed_at.max().date()).days})
            total += inserted
        except Exception as exc:
            con.execute("ROLLBACK TO SAVEPOINT refresh_asset")
            con.execute("RELEASE SAVEPOINT refresh_asset")
            con.execute(
                "insert into research_refresh_runs(dataset_id,run_date,source,status,records_seen,records_inserted,error,created_at) values (?,?,?,?,?,?,?,?)",
                (LIVE, date.today().isoformat(), source, "failed", 0, 0, str(exc)[:500], datetime.now(timezone.utc).isoformat()),
            )
            stats.append({"symbol": symbol, "source": source, "status": "failed", "error": str(exc)[:500]})
    con.commit()
    result = {
        "operation": "daily_official_refresh",
        "dataset_id": LIVE,
        "run_date": date.today().isoformat(),
        "assets_checked": checked,
        "records_inserted": total,
        "status_counts": pd.Series([x["status"] for x in stats]).value_counts().to_dict() if stats else {},
        "details": stats,
    }
    con.close()
    if any(row['status'] == 'failed' for row in stats) or not checked:
        result["domain_views"] = [{"status": "failed", "reason": "source refresh incomplete; previous generation preserved"}]
    else:
        try:
            result["domain_views"] = rebuild_outputs()
        except Exception as exc:
            result["domain_views"] = [{"status": "failed", "error": str(exc)[:500], "previous_generation_preserved": True}]
    result["domain_view_status_counts"] = pd.Series(
        [row.get("status", "unknown") for row in result["domain_views"]]
    ).value_counts().to_dict()
    failed = not checked or any(x["status"] == "failed" for x in stats + result["domain_views"])
    result["status"] = "partial_failed" if failed else "ok"
    (ROOT / "reports" / "daily_refresh_latest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("operation", "run_date", "assets_checked", "records_inserted", "status_counts", "domain_view_status_counts")}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
