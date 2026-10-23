"""Fetch BLS monthly labour indicators; publish an independent atomic HTML page."""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import uuid
import requests
from scripts.publication import operation_lock
from scripts.labor_ilo import fetch_ilo
from scripts.labor_benchmarks import attach_conversions, load_benchmarks

ROOT = Path(__file__).resolve().parents[1]
URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
SERIES = {"CES0500000003": "earnings", "CES0500000002": "hours"}


def parse_response(payload):
    if payload.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError("BLS request failed: " + str(payload.get("message")))
    result = {}
    for series in payload.get("Results", {}).get("series", []):
        sid = series["seriesID"]
        if sid not in SERIES or sid in result:
            raise ValueError("Unexpected or duplicate BLS series")
        rows = {}
        for row in series.get("data", []):
            period = row["period"]
            if period == "M13":
                continue
            if period not in [f"M{i:02d}" for i in range(1, 13)]:
                raise ValueError("Invalid monthly period")
            year = int(row["year"])
            if not 1900 <= year <= 2100:
                raise ValueError("Invalid year")
            month = f"{year}-{period[1:]}"
            value = float(row["value"].replace(",", ""))
            if month in rows or not math.isfinite(value) or value <= 0:
                raise ValueError("Invalid or duplicate observation")
            rows[month] = {"month": month, "value": value,
                           "preliminary": any(n.get("code") == "P" for n in row.get("footnotes", [])),
                           "footnotes": row.get("footnotes", [])}
        if not rows:
            raise ValueError("Empty BLS series")
        result[sid] = sorted(rows.values(), key=lambda r: r["month"])
    if set(result) != set(SERIES):
        raise ValueError("Missing required BLS series")
    return result


def write_page(root, data):
    reports = root / 'reports'
    from scripts.publication import current_directory
    try:
        data['navigation_generation'] = current_directory(reports).name
    except (OSError, ValueError, KeyError):
        data['navigation_generation'] = None
    html = (root / "validation/labor.html").read_text(encoding="utf-8")
    for token, name in [('__ANNUAL_SCRIPT__', 'labor_annual.js'), ('__BENCHMARK_SCRIPT__', 'labor_benchmarks.js'),
                        ('__MONTHLY_SCRIPT__', 'labor_monthly.js'), ('__CONTROLS_SCRIPT__', 'labor_controls.js'), ('__COMPARE_SCRIPT__', 'labor_compare.js')]:
        html = html.replace(token, (root / 'validation' / name).read_text(encoding='utf-8'))
    html = html.replace('__PAYLOAD__', json.dumps(data, ensure_ascii=False, allow_nan=False).replace('<', '\\u003c'))
    temp = reports / ('.labor-' + uuid.uuid4().hex + '.tmp')
    try:
        temp.write_text(html, encoding='utf-8')
        os.replace(temp, reports / 'labor.html')
    finally:
        temp.unlink(missing_ok=True)


def rebuild_benchmarks(root=ROOT):
    """Reprice the published wage snapshot after a successful market refresh."""
    root = Path(root)
    with operation_lock(root / 'reports/.labor.lock'):
        page = (root / 'reports/labor.html').read_text(encoding='utf-8')
        data = json.JSONDecoder().raw_decode(page.split('const DATA=', 1)[1])[0]
        attach_conversions(data, load_benchmarks(root))
        write_page(root, data)


def refresh(root=ROOT, session=requests):
    root = Path(root)
    reports = root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    with operation_lock(reports / ".labor.lock"):
        now = datetime.now(timezone.utc)
        response = session.post(URL, json={"seriesid": list(SERIES),
                                "startyear": str(now.year - 9), "endyear": str(now.year)}, timeout=60)
        response.raise_for_status()
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        archive = root / "data/raw/labor"
        archive.mkdir(parents=True, exist_ok=True)
        (archive / (digest + ".json")).write_bytes(raw)
        series = parse_response(response.json())
        ilo = fetch_ilo(session, archive, now.year)
        names = json.loads((root / "config/labor_countries.json").read_text(encoding="utf-8"))
        ilo["country_names"] = names
        data = {"retrieved_at": now.isoformat(), "source": URL, "raw_sha256": digest,
                "frequency": "monthly", "seasonal_adjustment": "seasonally_adjusted", "series": series,
                "ilo": ilo}
        attach_conversions(data, load_benchmarks(root))
        write_page(root, data)
        return {"status": "ok", "retrieved_at": data["retrieved_at"],
                "latest": {sid: rows[-1]["month"] for sid, rows in series.items()}, "raw_sha256": digest,
                "countries": len(ilo["countries"])}


def main():
    try:
        print(json.dumps(refresh(), ensure_ascii=False))
        return 0
    except Exception as exc:
        print("Labor refresh failed; previous page retained: " + str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
