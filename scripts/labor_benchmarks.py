"""Explicit research-unit wage conversions using a published complete basket.

One reference unit costs USD 1 on the first complete basket date. This is a
display convention, not an official exchange rate or a change to SVU=100.
"""
import calendar
from collections import defaultdict
from datetime import date
import hashlib
import json
import math

from scripts.publication import current_directory


def calculate_benchmarks(payload, today=None):
    today = today or date.today()
    groups = payload['groups']
    if not groups or not groups.get('currency') or any(not members for members in groups.values()):
        raise ValueError('Missing benchmark groups')
    assets = [a for members in groups.values() for a in members]
    if len(assets) != len(set(assets)) or set(assets) != set(payload['official_assets']):
        raise ValueError('Invalid complete basket membership')
    dates, prices = payload['dates'], payload['prices']
    if dates != sorted(set(dates)) or any(len(prices[a]) != len(dates) for a in assets):
        raise ValueError('Unaligned benchmark prices')
    complete = []
    for i, day in enumerate(dates):
        observed = date.fromisoformat(day)
        values = {a: prices[a][i] for a in assets}
        if observed > today or any(v is None for v in values.values()):
            continue
        if any(not isinstance(v, (float, int)) or not math.isfinite(v) or v <= 0 for v in values.values()):
            raise ValueError('Invalid benchmark price')
        complete.append((day, values))
    if len(complete) < 3:
        raise ValueError('Insufficient complete basket dates')
    base = complete[0][1]
    monthly = defaultdict(list)
    for day, values in complete:
        log_changes = {a: math.log(values[a] / base[a]) for a in assets}
        category_logs = {g: sum(log_changes[a] for a in members) / len(members)
                         for g, members in groups.items()}
        monthly[day[:7]].append({'date': day, 'currency': math.exp(category_logs['currency']),
                                'svu': math.exp(sum(category_logs.values()) / len(groups))})
    months = {}
    for period, rows in sorted(monthly.items()):
        year, month = map(int, period.split('-'))
        end = date(year, month, calendar.monthrange(year, month)[1])
        # Require a closed month and observations spanning it; never extrapolate
        # an incomplete final month or silently fill missing quote dates.
        if end >= today or len(rows) < 10 or int(rows[0]['date'][-2:]) > 10 or int(rows[-1]['date'][-2:]) < 20:
            continue
        months[period] = {k: sum(r[k] for r in rows) / len(rows) for k in ['currency', 'svu']}
        months[period].update(n=len(rows), first=rows[0]['date'], last=rows[-1]['date'])
    years = {}
    for year in sorted({p[:4] for p in months}):
        keys = [f'{year}-{m:02d}' for m in range(1, 13)]
        if not all(k in months for k in keys):
            continue
        n = sum(months[k]['n'] for k in keys)
        years[year] = {v: sum(months[k][v] * months[k]['n'] for k in keys) / n
                       for v in ['currency', 'svu']}
        years[year].update(n=n, first=months[keys[0]]['first'], last=months[keys[-1]]['last'])
    return {'available': True, 'base_date': complete[0][0], 'complete_end': complete[-1][0],
            'months': months, 'years': years, 'groups': groups,
            'basket_revision': payload.get('basket_revision'),
            'convention': 'one_reference_unit_equals_one_USD_on_base_date',
            'aggregation': 'arithmetic_mean_on_actual_complete_basket_dates'}


def load_benchmarks(root):
    try:
        directory = current_directory(root / 'reports')
        raw = (directory / 'svu_daily.html').read_bytes()
        text = raw.decode('utf-8').split('const DATA = ', 1)[1]
        payload = json.JSONDecoder().raw_decode(text)[0]
        from datetime import datetime, timedelta, timezone
        result = calculate_benchmarks(payload, datetime.now(timezone(timedelta(hours=8))).date())
        result.update(generation=directory.name, source_sha256=hashlib.sha256(raw).hexdigest())
        return result
    except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
        # Official wage data remains useful without a market database/generation.
        return {'available': False, 'reason': 'published_complete_basket_unavailable'}


def convert_wage(usd, benchmark):
    if usd is None or benchmark is None:
        return None
    if not math.isfinite(usd) or usd <= 0:
        raise ValueError('Invalid USD earnings')
    return {key: usd / benchmark[key] for key in ['currency', 'svu']}


def attach_conversions(data, benchmarks):
    data['benchmarks'] = benchmarks
    years, months = benchmarks.get('years', {}), benchmarks.get('months', {})
    for group in data['ilo']['series']:
        for row in group['rows']:
            row['conversion'] = convert_wage(row.get('usd'), years.get(row['year'])) if group['metric'] != 'hours' else None
    for row in data['series']['CES0500000003']:
        row['conversion'] = convert_wage(row['value'], months.get(row['month']))
