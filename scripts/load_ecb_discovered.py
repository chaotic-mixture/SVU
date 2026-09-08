"""幂等加载 SVU-DATA-008 ECB 自动发现批次。"""
from __future__ import annotations
import json,sqlite3
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
def main():
 man=json.loads((ROOT/'data/processed/ecb_discovered_manifest.json').read_text(encoding='utf-8')); df=pd.read_csv(ROOT/'data/processed/ecb_discovered_fx.csv',parse_dates=['timestamp']); con=sqlite3.connect(ROOT/'svu.db'); con.execute('insert or replace into research_datasets values (?,?,?,?,?,?,?,?)',(man['dataset_id'],man['retrieved_at_utc'],man['numeraire'],man['frequency'],man['processed_file'],man['processed_sha256'],None,None)); con.executemany('insert or replace into research_assets values (?,?,?,?,?,?)',[(e['symbol'],'currency',e['unit'],f"ECB:EXR.D.{e['currency']}.EUR.SP00.A",'cross_rate_usd_eur',man['dataset_id']) for e in man['assets']]); con.executemany('insert or replace into research_price_observations values (?,?,?,?,?,?)',[(man['dataset_id'],r.symbol,r.timestamp.strftime('%Y-%m-%d'),float(r.value),r.source,r.unit) for r in df.itertuples()]); con.commit(); result={'operation':'load_ecb_discovered','dataset_id':man['dataset_id'],'assets':con.execute('select count(distinct symbol) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'rows':con.execute('select count(*) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'failures':len(man['failures']),'idempotent':True}; con.close(); (ROOT/'reports/ecb_discovered_load_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
