import sqlite3

import pandas as pd
import pytest

from scripts import daily_official_refresh as refresh


def database():
    con = sqlite3.connect(":memory:")
    con.execute("create table research_price_observations(dataset_id,symbol,observed_at,value,source,unit,primary key(dataset_id,symbol,observed_at))")
    con.executemany("insert into research_price_observations values(?,?,?,?,?,?)", [
        ("old", "A", "2024-01-01", 1., "FRED:A", "USD"),
        ("old", "A", "2024-01-03", 3., "FRED:A", "USD"),
    ])
    return con


def test_refresh_fills_gaps_and_is_idempotent():
    con = database()
    frame = pd.DataFrame({"observed_at": pd.date_range("2024-01-01", periods=4), "value": [1., 2., 3., 4.]})
    assert refresh.insert_new(con, "A", "FRED:A", "USD", frame) == (4, 2)
    assert refresh.insert_new(con, "A", "FRED:A", "USD", frame) == (4, 0)
    assert con.execute("select count(*) from research_price_observations").fetchone()[0] == 4
    con.close()


def test_revision_fails_before_writing():
    con = database()
    frame = pd.DataFrame({"observed_at": pd.to_datetime(["2024-01-02", "2024-01-03"]), "value": [2., 9.]})
    with pytest.raises(ValueError, match="revisions require review"):
        refresh.insert_new(con, "A", "FRED:A", "USD", frame)
    assert con.execute("select count(*) from research_price_observations").fetchone()[0] == 2
    con.close()


def test_ecb_cross_rate_uses_common_dates(monkeypatch, tmp_path):
    class Response:
        def __init__(self, content): self.content = content
        def raise_for_status(self): pass
    def get(url, timeout):
        values = "2024-01-01,1.2\n2024-01-03,1.4" if "D.USD." in url else "2024-01-01,6\n2024-01-02,7"
        return Response(("TIME_PERIOD,OBS_VALUE\n" + values).encode())
    monkeypatch.setattr(refresh.requests, "get", get)
    monkeypatch.setattr(refresh, "RAW", tmp_path)
    result = refresh.fetch_ecb("ECB:EXR.D.CNY.EUR.SP00.A")
    assert result.value.tolist() == pytest.approx([0.2])


def test_empty_source_and_missing_database_fail(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="no finite"):
        refresh.clean_frame(pd.DataFrame(columns=["observed_at", "value"]))
    monkeypatch.setattr(refresh, "DB", tmp_path / "missing.db")
    with pytest.raises(FileNotFoundError): refresh.main()
    assert not refresh.DB.exists()


def test_failed_download_is_recorded_and_returns_failure(monkeypatch, tmp_path):
    con = database()
    con.execute('create table research_datasets(a primary key,b,c,d,e,f,g,h)')
    con.execute('create table research_assets(symbol,asset_type,unit,source,transform)')
    con.execute("insert into research_assets values('A','currency','USD','FRED:A','identity')")
    monkeypatch.setattr(refresh, 'ROOT', tmp_path)
    (tmp_path / 'reports').mkdir()
    monkeypatch.setattr(refresh, 'rebuild_outputs', lambda: [{'status': 'ok'}])
    def fail(*args): raise RuntimeError('synthetic network failure')
    monkeypatch.setattr(refresh, 'fetch_fred', fail)
    assert refresh.run_refresh(con) == 1
    import json
    report = json.loads((tmp_path / 'reports/daily_refresh_latest.json').read_text(encoding='utf-8'))
    assert report['status'] == 'partial_failed'
    assert report['status_counts']['failed'] == 1


def test_legacy_ecb_source_id_remains_readable():
    assert refresh.ecb_currency('ECB:EXR.NZD') == 'NZD'
    assert refresh.ecb_currency('ECB:EXR.D.NZD.EUR.SP00.A') == 'NZD'


def test_equivalent_source_urls_do_not_duplicate_prices():
    con = database()
    frame = pd.DataFrame({'observed_at': pd.to_datetime(['2024-01-01']), 'value': [1.]})
    assert refresh.insert_new(con, 'A', 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=A', 'USD', frame) == (1, 0)
    con.close()


def test_raw_revisions_are_retained_and_daily_dates_are_unique(tmp_path, monkeypatch):
    monkeypatch.setattr(refresh, 'RAW', tmp_path)
    first = refresh.save_raw('TEST', 'csv', b'old')
    second = refresh.save_raw('TEST', 'csv', b'new')
    assert first != second
    assert first.read_bytes() == b'old'
    assert refresh.save_raw('TEST', 'csv', b'new') == second
    duplicate_day = pd.DataFrame({'observed_at': ['2024-01-01 01:00', '2024-01-01 02:00'], 'value': [1., 2.]})
    with pytest.raises(ValueError, match='duplicate'):
        refresh.clean_frame(duplicate_day)


def test_unknown_transformation_rejected_before_network(monkeypatch):
    monkeypatch.setattr(refresh.requests, 'get', lambda *args, **kwargs: pytest.fail('network must not be called'))
    with pytest.raises(ValueError, match='transformation'):
        refresh.fetch_fred('DEXUSEU', 'unsupported')
