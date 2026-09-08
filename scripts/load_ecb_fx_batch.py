"""幂等加载 ECB 外汇批次 SVU-DATA-007。"""
from __future__ import annotations
import json,sqlite3
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
def main():
 man=json.loads((ROOT/'data/processed/ecb_fx_manifest.json').read_text(encoding='utf-8')); df=pd.read_csv(ROOT/'data/processed/ecb_fx.csv',parse_dates=['timestamp']); con=sqlite3.connect(ROOT/'svu.db'); con.execute('INSERT OR REPLACE INTO research_datasets VALUES (?,?,?,?,?,?,?,?)',(man['dataset_id'],man['retrieved_at_utc'],man['numeraire'],man['frequency'],man['processed_file'],man['processed_sha256'],None,None)); con.executemany('INSERT OR REPLACE INTO research_assets VALUES (?,?,?,?,?,?)',[(e['symbol'],'currency',e['unit'],f"ECB:EXR.D.{e['currency']}.EUR.SP00.A",'cross_rate_usd_eur',man['dataset_id']) for e in man['assets']]); con.executemany('INSERT OR REPLACE INTO research_price_observations VALUES (?,?,?,?,?,?)',[(man['dataset_id'],r.symbol,r.timestamp.strftime('%Y-%m-%d'),float(r.value),r.source,r.unit) for r in df.itertuples()]); con.commit(); result={'operation':'load_ecb_fx_batch','dataset_id':man['dataset_id'],'database':str(ROOT/'svu.db'),'assets':con.execute('select count(distinct symbol) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'rows':con.execute('select count(*) from research_price_observations where dataset_id=?',(man['dataset_id'],)).fetchone()[0],'idempotent':True}; con.close(); (ROOT/'reports/ecb_fx_load_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
