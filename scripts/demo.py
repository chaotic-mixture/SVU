"""Offline, synthetic-only end-to-end research example."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import numpy as np
import yaml

from validation.icati import calculate_icati, relative_to_icati
from validation.domain_view import build_domain_view

ROOT = Path(__file__).resolve().parents[1]


def build_gallery(output):
    """Exercise the actual chart templates offline using only synthetic prices."""
    from validation.build_daily_dashboard import main as daily
    from validation.build_domain_dashboard import build as domain
    cfg = yaml.safe_load((ROOT / 'config/svu_v03_candidate.yaml').read_text(encoding='utf-8'))
    groups, control = cfg['category_groups'], cfg['long_history_groups']
    assets = sorted(set(sum(groups.values(), []) + sum(control.values(), [])))
    dates = pd.bdate_range('2020-01-01', periods=800)
    t = np.arange(len(dates))
    panel = pd.DataFrame({name: np.exp(.0001 * (j % 5 - 2) * t + .02 * np.sin(t / (10 + j)))
                          for j, name in enumerate(assets)}, index=dates)
    daily(output_dir=output, panel_data=(panel, groups, control))
    for name, members in groups.items():
        domain(name, output_dir=output, panel=panel, assets=members)
    for page in output.rglob('*.html'):
        text = page.read_text(encoding='utf-8').replace('<main>', '<main><p class="warning">SIMULATED: synthetic test prices, not market observations.</p>', 1)
        page.write_text(text, encoding='utf-8')
    for path in output.rglob('*.json'):
        obj = json.loads(path.read_text(encoding='utf-8'))
        obj['source_kind'] = 'SIMULATED'
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def build(output: Path):
    frame = pd.read_csv(ROOT / "tests/fixtures/synthetic_panel.csv", parse_dates=["date"])
    if set(frame.source) != {"SIMULATED"}:
        raise ValueError("demo fixture must be explicitly synthetic")
    panel = frame.set_index("date").drop(columns="source")
    index = calculate_icati(panel, {name: 1 / len(panel.columns) for name in panel.columns})
    relative = relative_to_icati(panel, index)
    view = build_domain_view(panel, list(panel.columns), domain_key="synthetic")
    result = {"status": "SIMULATED", "icati": index.tolist(), "domain": view}
    output.mkdir(parents=True, exist_ok=True)
    (output / "demo.json").write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
    (output / "demo.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>SVU synthetic demo</title>'
        '<h1>SIMULATED — offline calculation example</h1><p>SVU = 100; ICATI is the dynamic trend. '
        'These are synthetic observations, not market evidence.</p>' + relative.to_html() + '</html>',
        encoding="utf-8",
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "reports/demo")
    parser.add_argument("--gallery", action="store_true", help="generate all real chart templates with synthetic prices")
    args = parser.parse_args()
    if args.gallery:
        build_gallery(args.output)
    else:
        build(args.output)


if __name__ == "__main__":
    main()
