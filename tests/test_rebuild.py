import json
import sqlite3

import pandas as pd
import yaml

from scripts import load_public_database as loader
from scripts import rebuild_research as rebuild
from scripts import daily_official_refresh as refresh


def test_initial_load_creates_reports_in_fresh_workspace(tmp_path, monkeypatch):
    manifest = {"dataset_id": "SYNTHETIC", "retrieved_at_utc": "2024-01-01", "numeraire": "USD", "frequency": "daily",
                "normalized_file": "prices.csv", "normalized_sha256": "synthetic", "macro_file": "macro.csv", "macro_sha256": "synthetic",
                "assets": [{"symbol": "EURUSD", "unit": "USD per EUR", "fred_series": "SYNTHETIC"}]}
    man, prices, macro = [tmp_path / name for name in ['manifest.json', 'prices.csv', 'macro.csv']]
    man.write_text(json.dumps(manifest))
    pd.DataFrame([{"timestamp": "2024-01-01", "symbol": "EURUSD", "value": 1., "source": "SIMULATED", "unit": "USD per EUR"}]).to_csv(prices, index=False)
    pd.DataFrame(columns=['timestamp', 'symbol', 'value', 'source', 'unit']).to_csv(macro, index=False)
    for name, value in {'ROOT': tmp_path, 'DB': tmp_path / 'svu.db', 'MANIFEST': man, 'PRICES': prices, 'MACRO': macro}.items():
        monkeypatch.setattr(loader, name, value)
    loader.main()
    loader.main()
    with sqlite3.connect(tmp_path / 'svu.db') as con:
        assert con.execute('select count(*) from research_price_observations').fetchone()[0] == 1
    assert (tmp_path / 'reports/database_load_latest.json').is_file()


def test_rebuild_orders_fetch_load_then_views(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(rebuild, 'ROOT', tmp_path)
    monkeypatch.setattr(rebuild, 'validate_batch', lambda root, loader: None)
    monkeypatch.setattr(refresh, 'rebuild_outputs', lambda: [{'status': 'ok'}])
    rebuild.rebuild(download=True, runner=calls.append)
    assert calls[:2] == ['scripts.fetch_public_data', 'scripts.load_public_database']
    assert calls[-2:] == ['scripts.create_unique_views', 'scripts.create_entity_dedup_layer']


def test_rebuild_rejects_modified_csv_before_loading(tmp_path):
    import hashlib
    import pytest
    folder = tmp_path / 'data/processed'
    folder.mkdir(parents=True)
    data = folder / 'official_energy.csv'
    data.write_bytes(b'original')
    manifest = {'processed_file': 'data/processed/official_energy.csv', 'processed_sha256': hashlib.sha256(data.read_bytes()).hexdigest()}
    (folder / 'official_energy_manifest.json').write_text(json.dumps(manifest))
    rebuild.validate_batch(tmp_path, 'load_official_energy')
    data.write_bytes(b'modified')
    with pytest.raises(ValueError, match='hash mismatch'):
        rebuild.validate_batch(tmp_path, 'load_official_energy')


def test_synthetic_database_to_all_research_outputs(tmp_path, monkeypatch):
    from scripts import create_unique_views, create_entity_dedup_layer
    from validation import build_daily_dashboard, build_domain_dashboard, build_v03_snapshot
    cfg = yaml.safe_load((rebuild.ROOT / 'config/svu_v03_candidate.yaml').read_text(encoding='utf-8'))
    symbols = sorted(set(sum(cfg['category_groups'].values(), []) + sum(cfg['long_history_groups'].values(), [])))
    (tmp_path / 'config').mkdir()
    (tmp_path / 'config/svu_v03_candidate.yaml').write_text(yaml.safe_dump(cfg), encoding='utf-8')
    manifest = {"dataset_id": "SIMULATED", "retrieved_at_utc": "2024-01-01", "numeraire": "USD", "frequency": "daily",
                "normalized_file": "prices.csv", "normalized_sha256": "synthetic", "macro_file": "macro.csv", "macro_sha256": "synthetic",
                "assets": [{"symbol": symbol, "unit": "synthetic", "fred_series": "SIMULATED"} for symbol in symbols]}
    (tmp_path / 'manifest.json').write_text(json.dumps(manifest))
    rows = [{"timestamp": day, "symbol": symbol, "value": 1 + j * .01 + i * .002,
             "source": "SIMULATED", "unit": "synthetic"}
            for j, symbol in enumerate(symbols) for i, day in enumerate(pd.date_range('2024-01-01', periods=8))]
    pd.DataFrame(rows).to_csv(tmp_path / 'prices.csv', index=False)
    pd.DataFrame(columns=['timestamp', 'symbol', 'value', 'source', 'unit']).to_csv(tmp_path / 'macro.csv', index=False)
    for module in [loader, create_unique_views, create_entity_dedup_layer, build_daily_dashboard, build_domain_dashboard, build_v03_snapshot, refresh]:
        monkeypatch.setattr(module, 'ROOT', tmp_path)
        if hasattr(module, 'DB'): monkeypatch.setattr(module, 'DB', tmp_path / 'svu.db')
    monkeypatch.setattr(loader, 'MANIFEST', tmp_path / 'manifest.json')
    monkeypatch.setattr(loader, 'PRICES', tmp_path / 'prices.csv')
    monkeypatch.setattr(loader, 'MACRO', tmp_path / 'macro.csv')
    loader.main()
    create_unique_views.main()
    create_entity_dedup_layer.main()
    result = refresh.rebuild_outputs()
    assert len(result) == 6
    assert all(row['status'] != 'failed' for row in result), result
    from scripts.publication import current_directory
    output = current_directory(tmp_path / 'reports')
    assert (output / 'svu_daily.html').is_file()
    snapshot = json.loads((output / 'svu_v03_snapshot.json').read_text(encoding='utf-8'))
    assert snapshot['database']['economic_entities'] > 30
    assert snapshot['status'] == 'diagnostic_only'
    for domain in refresh.DOMAIN_KEYS:
        assert (output / f'domains/{domain}.html').is_file()
