"""官方 FRED 外汇扩展批次 SVU-DATA-006。"""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from io import BytesIO
from pathlib import Path
import pandas as pd,requests
ROOT=Path(__file__).resolve().parents[1]; RAW=ROOT/'data/raw/official_fx'; OUT=ROOT/'data/processed/official_fx.csv'; MAN=ROOT/'data/processed/official_fx_manifest.json'
SERIES={'MXNUSD':'DEXMXUS','BRLUSD':'DEXBZUS','KRWUSD':'DEXKOUS','INRUSD':'DEXINUS','THBUSD':'DEXTHUS','HKDUSD':'DEXHKUS','SGDUSD':'DEXSIUS','TWDUSD':'DEXTAUS','MYRUSD':'DEXMAUS','ZARUSD':'DEXSFUS','NOKUSD':'DEXNOUS','DKKUSD':'DEXDNUS'}
START,END='2015-01-01','2025-12-31'
def main():
 RAW.mkdir(parents=True,exist_ok=True); OUT.parent.mkdir(parents=True,exist_ok=True); frames=[]; entries=[]
 for symbol,fred in SERIES.items():
  url=f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred}'; resp=requests.get(url,timeout=90); resp.raise_for_status(); b=resp.content; raw=RAW/f'{symbol}_{fred}.csv'; raw.write_bytes(b); df=pd.read_csv(BytesIO(b)); df.columns=['timestamp','value']; df.timestamp=pd.to_datetime(df.timestamp,errors='coerce'); df.value=pd.to_numeric(df.value,errors='coerce'); df=df.dropna(); df=df[(df.timestamp>=START)&(df.timestamp<=END)&(df.value>0)]; df.value=1/df.value; df['symbol']=symbol; df['source']=f'FRED:{fred}'; df['unit']=f'USD per {symbol[:3]}'; frames.append(df); entries.append({'symbol':symbol,'fred_series':fred,'url':url,'unit':df.unit.iloc[0],'transform':'inverse','raw_file':str(raw.relative_to(ROOT)),'raw_sha256':hashlib.sha256(b).hexdigest(),'rows':len(df),'start':df.timestamp.min().strftime('%Y-%m-%d'),'end':df.timestamp.max().strftime('%Y-%m-%d')})
 out=pd.concat(frames,ignore_index=True).sort_values(['timestamp','symbol']); out.to_csv(OUT,index=False,date_format='%Y-%m-%d'); man={'dataset_id':'SVU-DATA-006','retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'requested_window':{'start':START,'end':END},'frequency':'daily/available observation','numeraire':'USD','assets':entries,'processed_file':str(OUT.relative_to(ROOT)),'processed_sha256':hashlib.sha256(OUT.read_bytes()).hexdigest()}; MAN.write_text(json.dumps(man,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps({'dataset_id':man['dataset_id'],'assets':len(entries),'rows':len(out),'sha256':man['processed_sha256']},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
