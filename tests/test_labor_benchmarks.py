from datetime import date, timedelta
import math
import pytest
from scripts.labor_benchmarks import calculate_benchmarks, convert_wage, attach_conversions, load_benchmarks


def fixture():
    days = [date(2023, 1, 1) + timedelta(days=i) for i in range(731)]
    return {'dates': [d.isoformat() for d in days],
            'groups': {'currency': ['A','B'], 'energy': ['E']},
            'official_assets': ['A','B','E'],
            'prices': {'A': [1.0]+[4.0]*730, 'B': [1.0]*731, 'E': [1.0]+[4.0]*730}}


def test_explicit_units_and_category_balanced_weighting():
    b=calculate_benchmarks(fixture(),date(2025,1,2))
    assert b['base_date']=='2023-01-01'
    feb=b['months']['2023-02']
    assert feb['currency']==pytest.approx(2)
    assert feb['svu']==pytest.approx(math.sqrt(8))
    result=convert_wage(8,feb)
    assert result['currency']==pytest.approx(4)
    assert result['svu']==pytest.approx(math.sqrt(8))
    assert set(b['years'])=={'2023','2024'}


def test_incomplete_periods_and_members_are_never_filled():
    p=fixture()
    for i,day in enumerate(p['dates']):
        if day>='2024-09-19' or day.startswith('2023-06'):
            p['prices']['E'][i]=None
    b=calculate_benchmarks(p,date(2025,1,2))
    assert b['complete_end']=='2024-09-18'
    assert '2024-09' not in b['months']
    assert '2023-06' not in b['months']
    assert not b['years']
    assert convert_wage(20,None) is None
    assert convert_wage(None,b['months']['2023-02']) is None


def test_current_month_not_used_and_invalid_prices_rejected():
    p=fixture();b=calculate_benchmarks(p,date(2024,12,25))
    assert '2024-12' not in b['months']
    p['prices']['A'][10]=float('inf')
    with pytest.raises(ValueError,match='Invalid benchmark price'):
        calculate_benchmarks(p,date(2025,1,2))


def test_missing_published_basket_preserves_original_wages(tmp_path):
    b=load_benchmarks(tmp_path)
    assert b['available'] is False
    data={'ilo':{'series':[{'metric':'monthly','rows':[{'year':'2024','value':300,'usd':40}]}]},
          'series':{'CES0500000003':[{'month':'2024-01','value':20}]}}
    attach_conversions(data,b)
    row=data['ilo']['series'][0]['rows'][0]
    assert row['value']==300 and row['conversion'] is None
    assert data['series']['CES0500000003'][0]['conversion'] is None


@pytest.mark.parametrize('labor_status,market_status', [(0,0),(1,0),(0,1)])
def test_scheduler_reprices_only_after_successful_publication(tmp_path,monkeypatch,labor_status,market_status):
    from scripts import daily_official_refresh as daily
    from scripts import refresh_labor as labor
    db=tmp_path/'market.db';db.touch()
    monkeypatch.setattr(daily,'ROOT',tmp_path)
    monkeypatch.setattr(daily,'DB',db)
    monkeypatch.setattr(daily,'RAW',tmp_path/'raw')
    calls=[]
    def labour(): calls.append('labor');return labor_status
    def market(con): calls.append('market');return market_status
    monkeypatch.setattr(labor,'main',labour)
    monkeypatch.setattr(daily,'run_refresh',market)
    monkeypatch.setattr(labor,'rebuild_benchmarks',lambda root:calls.append('reprice'))
    assert daily.main()==int(bool(labor_status or market_status))
    assert calls==['labor','market']+(['reprice'] if not labor_status and not market_status else [])
