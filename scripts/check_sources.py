"""Probe official source responses without changing the research database."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from io import BytesIO
import argparse
import json
from pathlib import Path

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]


def sources(all_series=False):
    fred = {'DEXUSEU', 'NASDAQ100'}
    currencies = {'USD', 'SEK'}
    if all_series:
        from scripts.fetch_public_data import SERIES, MACRO_SERIES
        from scripts.fetch_official_fx_batch import SERIES as FX
        fred |= {spec['fred'] for spec in [*SERIES.values(), *MACRO_SERIES.values()]}
        fred |= set(FX.values()) | {'DCOILBRENTEU', 'DCOILWTICO', 'BAMLCC0A0CMTRIV'}
        config = yaml.safe_load((ROOT / 'config/svu_v03_candidate.yaml').read_text(encoding='utf-8'))
        currencies |= {symbol[:3] for group in config['category_groups'].values() for symbol in group if '_ECB' in symbol}
    rows = [('fred', key, f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={key}&cosd=2025-01-01&coed=2025-01-15') for key in sorted(fred)]
    rows += [('ecb', key, f'https://data-api.ecb.europa.eu/service/data/EXR/D.{key}.EUR.SP00.A?startPeriod=2025-01-01&endPeriod=2025-01-15&format=csvdata') for key in sorted(currencies)]
    rows += [('lbma', key, f'https://prices.lbma.org.uk/json/{key}.json') for key in ['gold_pm', 'silver']]
    return rows


def probe(spec):
    kind, key, url = spec
    result = {'provider': kind, 'series': key, 'url': url}
    try:
        response = requests.get(url, timeout=(10, 25))
        result['http_status'] = response.status_code
        response.raise_for_status()
        if kind == 'lbma':
            data = response.json()
            count = sum(bool(row.get('d') and row.get('v')) for row in data)
        else:
            frame = pd.read_csv(BytesIO(response.content))
            column = 'OBS_VALUE' if kind == 'ecb' else key
            count = int(pd.to_numeric(frame[column], errors='coerce').notna().sum())
        if count == 0:
            raise ValueError('no numeric observations')
        result.update(status='ok', observations=count)
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc)[:250])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--all', action='store_true', help='probe all configured official basket and macro series')
    parser.add_argument('--output', type=Path, default=ROOT / 'reports/source_probe.json')
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(probe, sources(args.all)))
    report = {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'status': 'ok' if all(row['status'] == 'ok' for row in results) else 'failed', 'results': results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'status': report['status'], 'checked': len(results), 'failed': [row for row in results if row['status'] != 'ok']}, ensure_ascii=False))
    return 0 if report['status'] == 'ok' else 1


if __name__ == '__main__':
    raise SystemExit(main())
