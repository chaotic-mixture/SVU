"""获取官方 FRED NASDAQ-100 指数，形成 SVU-DATA-009。"""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from io import BytesIO
from pathlib import Path
import pandas as pd,requests
ROOT=Path(__file__).resolve().parents[1]; RAW=ROOT/'data/raw/official_equity'; OUT=ROOT/'data/processed/official_equity.csv'; MAN=ROOT/'data/processed/official_equity_manifest.json'
def main():
 RAW.mkdir(parents=True,exist_ok=True); OUT.parent.mkdir(parents=True,exist_ok=True); url='https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQ100'; resp=requests.get(url,timeout=90); resp.raise_for_status(); b=resp.content; raw=RAW/'NASDAQ100.csv'; raw.write_bytes(b); df=pd.read_csv(BytesIO(b)); df=df.rename(columns={'observation_date':'timestamp','NASDAQ100':'value'}); df.timestamp=pd.to_datetime(df.timestamp,errors='coerce'); df.value=pd.to_numeric(df.value,errors='coerce'); df=df.dropna(); df=df[(df.timestamp>='2015-01-01')&(df.timestamp<='2025-12-31')&(df.value>0)]; df['symbol']='NASDAQ100'; df['source']='FRED:NASDAQ100'; df['unit']='index points'; df=df[['timestamp','value','symbol','source','unit']]; df.to_csv(OUT,index=False,date_format='%Y-%m-%d'); man={'dataset_id':'SVU-DATA-009','retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'requested_window':{'start':'2015-01-01','end':'2025-12-31'},'frequency':'daily/available observation','numeraire':'USD','assets':[{'symbol':'NASDAQ100','fred_series':'NASDAQ100','url':url,'unit':'index points','raw_file':str(raw.relative_to(ROOT)),'raw_sha256':hashlib.sha256(b).hexdigest(),'rows':len(df),'start':df.timestamp.min().strftime('%Y-%m-%d'),'end':df.timestamp.max().strftime('%Y-%m-%d')}],'processed_file':str(OUT.relative_to(ROOT)),'processed_sha256':hashlib.sha256(OUT.read_bytes()).hexdigest()}; MAN.write_text(json.dumps(man,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps({'dataset_id':man['dataset_id'],'assets':1,'rows':len(df),'sha256':man['processed_sha256']},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
