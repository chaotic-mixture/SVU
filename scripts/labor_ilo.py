"""ILOSTAT annual observations, separated by country, indicator and survey.

Official metadata: https://ilostat.ilo.org/data/bulk/
No interpolation, currency conversion or splicing of national surveys.
"""
import csv
import hashlib
import io
import math
import re

BASE = "https://rplumber.ilo.org"
INDICATORS = {
    "hourly": "EAR_EHRA_SEX_CUR_NB_A",
    "monthly": "EAR_EMTA_SEX_CUR_NB_A",
    "hours": "HOW_XEES_SEX_NB_A",
}
DICTIONARIES = ("ref_area", "source", "obs_status", "note_indicator", "note_source", "note_classif")


def read_csv(raw, required):
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    if not set(required).issubset(reader.fieldnames or []):
        raise ValueError("Invalid ILOSTAT CSV schema")
    return list(reader)


def parse_observations(raw, metric, dictionaries, year):
    groups = {}
    required = ("ref_area", "source", "indicator", "sex", "time", "obs_value", "obs_status")
    raw_rows = read_csv(raw, required)
    dollars = {}
    for row in raw_rows:
        if metric != 'hours' and row.get('classif1') == 'CUR_TYPE_USD' and row['sex'] == 'SEX_T':
            if row['obs_value'] in ('', 'NA'):
                continue
            key = (row['ref_area'], row['source'], row['time'])
            value = float(row['obs_value'])
            if key in dollars or not math.isfinite(value) or value <= 0:
                raise ValueError('Invalid or duplicate ILOSTAT USD earnings')
            dollars[key] = value
    for row in raw_rows:
        area, period = row["ref_area"], row["time"]
        if row["sex"] != "SEX_T" or not re.fullmatch(r"[A-WYZ][A-Z]{2}", area):
            continue  # Exclude regional aggregates (X...), retain countries/territories.
        if not re.fullmatch(r"\d{4}", period):
            raise ValueError("Invalid annual period")
        if not 1900 <= int(period) <= year:
            continue
        if row["indicator"] != INDICATORS[metric][:-2]:
            raise ValueError("Unexpected ILOSTAT indicator")
        currency = row.get("classif1", "")
        if metric != "hours" and currency != "CUR_TYPE_LCU":
            continue  # Preserve local currency; no currency-converted ranking.
        if row["obs_value"] in ("", "NA"):
            continue
        value = float(row["obs_value"])
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Invalid ILOSTAT observation")
        source = row["source"]
        if area not in dictionaries["ref_area"] or source not in dictionaries["source"]:
            raise ValueError("Unknown ILOSTAT country or source")
        key = (area, source)
        group = groups.setdefault(key, {"country": area, "metric": metric,
                                       "source": source, "rows": {}})
        if period in group["rows"]:
            raise ValueError("Duplicate ILOSTAT country/source/year")
        notes = []
        for field in ("note_classif", "note_indicator", "note_source"):
            for code in filter(None, row.get(field, "").split("_")):
                notes.append({"code": code, "text": dictionaries[field].get(code, code)})
        group["rows"][period] = {"year": period, "value": value,
                                 "usd": dollars.get((area, source, period)),
                                 "status": row["obs_status"], "notes": notes}
    if not groups:
        raise ValueError("No usable ILOSTAT observations")
    return [dict(g, rows=sorted(g["rows"].values(), key=lambda r: r["year"]))
            for _, g in sorted(groups.items())]


def fetch_ilo(session, archive, year):
    hashes = {}

    def fetch(key, endpoint, params):
        response = session.get(BASE + endpoint, params=params, timeout=120)
        response.raise_for_status()
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        (archive / (digest + ".csv")).write_bytes(raw)
        hashes[key] = digest
        return raw

    dictionaries = {}
    for key in DICTIONARIES:
        raw = fetch(key, "/metadata/dic", {"var": key, "lang": "en", "format": ".csv"})
        dictionaries[key] = {r[key]: r[key + ".label"] for r in
                             read_csv(raw, (key, key + ".label")) if r[key]}
    series = []
    for metric, indicator in INDICATORS.items():
        raw = fetch(indicator, "/data/indicator/", {"id": indicator, "lang": "en", "format": ".csv"})
        series.extend(parse_observations(raw, metric, dictionaries, year))
    countries = sorted({g["country"] for g in series})
    sources = {g["source"] for g in series}
    return {"series": series, "countries": {c: dictionaries["ref_area"][c] for c in countries},
            "sources": {s: dictionaries["source"][s] for s in sorted(sources)},
            "statuses": dictionaries["obs_status"], "raw_sha256": hashes,
            "indicators": INDICATORS, "source": BASE}
