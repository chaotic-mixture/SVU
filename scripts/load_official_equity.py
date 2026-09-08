"""幂等加载 SVU-DATA-009 官方权益指数。"""
from __future__ import annotations
import json,sqlite3
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
def main():
 man=json.loads((ROOT/'data/processed/official_equity_manifest.json').read_text(encoding='utf-8')); df=pd.read_csv(ROOT/'data/processed/official_equity.csv',parse_dates=['timestamp']); con=sqlite3.connect(ROOT/'svu.db'); con.execute('insert or replace into research_datasets values (?,?,?,?,?,?,?,?)',(man['dataset_id'],man['retrieved_at_utc'],man['numeraire'],man['frequency'],man['processed_file'],man['processed_sha256'],None,None)); con.executemany('insert or replace into research_assets values (?,?,?,?,?,?)',[(e['symbol'],'equity_index',e['unit'],e['url'],'identity',man['dataset_id']) for e in man['assets']]); con.executemany('insert or replace into research_price_observations values (?,?,?,?,?,?)',[(man['dataset_id'],r.symbol,r.timestamp.strftime('%Y-%m-%d'),float(r.value),r.source,r.unit) for r in df.itertuples()]); con.commit(); result={'operation':'load_official_equity','dataset_id':man['dataset_id'],'assets':con.execute('select count(distinct symbol) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'rows':con.execute('select count(*) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'idempotent':True}; con.close(); (ROOT/'reports/official_equity_load_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
