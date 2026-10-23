import json
from pathlib import Path
import pytest
from scripts.refresh_labor import parse_response, refresh, SERIES


def payload():
    return {"status": "REQUEST_SUCCEEDED", "Results": {"series": [
        {"seriesID": sid, "data": [{"year": "2026", "period": "M09", "value": "34.4",
         "footnotes": [{"code": "P", "text": "preliminary"}]}]} for sid in SERIES]}}


def test_parse_requires_both_series_and_preserves_preliminary():
    p = payload()
    assert parse_response(p)[next(iter(SERIES))][0]['preliminary']
    p['Results']['series'].pop()
    with pytest.raises(ValueError, match='Missing'):
        parse_response(p)


@pytest.mark.parametrize('value', ['NaN', 'inf', '-1', '0'])
def test_reject_invalid_values(value):
    p = payload(); p['Results']['series'][0]['data'][0]['value'] = value
    with pytest.raises(ValueError): parse_response(p)


def test_duplicate_month_is_rejected():
    p = payload(); rows = p['Results']['series'][0]['data']; rows.append(rows[0].copy())
    with pytest.raises(ValueError): parse_response(p)


def test_atomic_page_and_archive_on_failure(tmp_path, monkeypatch):
    monkeypatch.setattr('scripts.refresh_labor.fetch_ilo', lambda *a: {'countries': {}, 'series': []})
    (tmp_path/'validation').mkdir()
    (tmp_path/'config').mkdir()
    (tmp_path/'config/labor_countries.json').write_text('{}')
    (tmp_path/'validation/labor_annual.js').write_text('')
    (tmp_path/'validation/labor_benchmarks.js').write_text('')
    (tmp_path/'validation/labor_monthly.js').write_text('')
    (tmp_path/'validation/labor_controls.js').write_text('')
    (tmp_path/'validation/labor_compare.js').write_text('')
    (tmp_path/'validation/labor.html').write_text('<script>const DATA=__PAYLOAD__</script>')
    class Response:
        content = json.dumps(payload()).encode()
        def raise_for_status(self): pass
        def json(self): return json.loads(self.content)
    class Session:
        def post(self, *args, **kwargs): return Response()
    refresh(tmp_path, Session())
    page = tmp_path/'reports/labor.html'; old = page.read_bytes()
    assert len(list((tmp_path/'data/raw/labor').glob('*.json'))) == 1
    Response.content = b'{"status":"REQUEST_NOT_PROCESSED"}'
    with pytest.raises(ValueError): refresh(tmp_path, Session())
    assert page.read_bytes() == old


def test_ilo_filters_and_keeps_surveys_separate():
    from scripts.labor_ilo import parse_observations
    dictionaries = {'ref_area': {'CHN': 'China'}, 'source': {'A': 'Survey A', 'B': 'Survey B'},
                    'note_classif': {}, 'note_indicator': {'T:1': 'Coverage'}, 'note_source': {}}
    header = 'ref_area,source,indicator,sex,classif1,time,obs_value,obs_status,note_indicator\n'
    rows = [f'CHN,{s},EAR_EMTA_SEX_CUR_NB,SEX_T,CUR_TYPE_LCU,2024,100,B,T:1' for s in ['A','B']]
    rows += ['XAA,A,EAR_EMTA_SEX_CUR_NB,SEX_T,CUR_TYPE_LCU,2024,200,,',
             'CHN,A,EAR_EMTA_SEX_CUR_NB,SEX_F,CUR_TYPE_LCU,2024,80,,',
             'CHN,A,EAR_EMTA_SEX_CUR_NB,SEX_T,CUR_TYPE_USD,2024,15,,',
             'CHN,A,EAR_EMTA_SEX_CUR_NB,SEX_T,CUR_TYPE_LCU,2027,120,,']
    raw = (header+'\n'.join(rows)).encode()
    groups = parse_observations(raw, 'monthly', dictionaries, 2026)
    assert len(groups)==2 and all(len(g['rows'])==1 for g in groups)
    assert groups[0]['rows'][0]['notes'][0]['text']=='Coverage'
    assert groups[0]['rows'][0]['status']=='B'
    assert groups[0]['rows'][0]['usd']==15
    assert groups[1]['rows'][0]['usd'] is None  # Never borrow another survey's FX conversion.
    with pytest.raises(ValueError,match='Duplicate'):
        parse_observations(raw+b'\n'+rows[0].encode(), 'monthly', dictionaries, 2026)
    with pytest.raises(ValueError,match='Invalid ILOSTAT observation'):
        parse_observations(raw.replace(b',100,',b',NaN,'), 'monthly', dictionaries, 2026)


def test_ilo_failure_keeps_existing_page(tmp_path, monkeypatch):
    (tmp_path/'reports').mkdir()
    page = tmp_path/'reports/labor.html';page.write_text('previous')
    class Response:
        content=json.dumps(payload()).encode()
        def raise_for_status(self): pass
        def json(self): return payload()
    class Session:
        def post(self,*args,**kwargs): return Response()
    def fail(*args): raise ValueError('ILOSTAT unavailable')
    monkeypatch.setattr('scripts.refresh_labor.fetch_ilo',fail)
    with pytest.raises(ValueError,match='ILOSTAT unavailable'): refresh(tmp_path,Session())
    assert page.read_text()=='previous'


def test_labor_route_without_market_database(tmp_path):
    from http.client import HTTPConnection
    from http.server import ThreadingHTTPServer
    from threading import Thread
    from scripts.serve_reports import handler_for
    (tmp_path/'labor.html').write_text('labor')
    (tmp_path/'index.html').write_text('home')
    (tmp_path/'private.json').write_text('private')
    with ThreadingHTTPServer(('127.0.0.1', 0), handler_for(tmp_path)) as server:
        thread = Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            for path, expected in [('/labor.html',200),('/labor.html?lang=en',200),('/index.html',200),('/private.json',503),('/labor.html/../private.json',503)]:
                client = HTTPConnection('127.0.0.1',server.server_port,timeout=5)
                try:
                    client.request('GET',path); response=client.getresponse()
                    assert response.status==expected
                    content=response.read()
                    if expected==200: assert content==(b'home' if path=='/index.html' else b'labor')
                finally: client.close()
        finally: server.shutdown(); thread.join(5)
