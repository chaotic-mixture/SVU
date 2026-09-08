"""ECB 官方外汇扩展批次 SVU-DATA-007：通过 EUR 交叉汇率换算 USD/本币。"""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from io import BytesIO
from pathlib import Path
import pandas as pd,requests
ROOT=Path(__file__).resolve().parents[1]; RAW=ROOT/'data/raw/ecb_fx'; OUT=ROOT/'data/processed/ecb_fx.csv'; MAN=ROOT/'data/processed/ecb_fx_manifest.json'
CURRENCIES=['NZD','SEK','PLN','CZK','HUF','RON','BGN','TRY','ZAR','BRL','KRW','INR','IDR','ILS','ISK','AED','THB','MYR','PHP','RSD','CLP','COP','PEN','UYU','MAD']
START,END='2015-01-01','2025-12-31'
def get(url):
 r=requests.get(url,timeout=90); r.raise_for_status(); return r.content
def parse(b):
 df=pd.read_csv(BytesIO(b)); df=df.rename(columns={'TIME_PERIOD':'timestamp','OBS_VALUE':'value'}); df.timestamp=pd.to_datetime(df.timestamp,errors='coerce'); df.value=pd.to_numeric(df.value,errors='coerce'); return df.dropna(subset=['timestamp','value'])
def main():
 RAW.mkdir(parents=True,exist_ok=True); OUT.parent.mkdir(parents=True,exist_ok=True); usd_url=f'https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?startPeriod={START}&endPeriod={END}&format=csvdata'; usd_b=get(usd_url); usd=parse(usd_b)[['timestamp','value']].rename(columns={'value':'usd_per_eur'}); frames=[]; entries=[]; failures=[]
 for cur in CURRENCIES:
  url=f'https://data-api.ecb.europa.eu/service/data/EXR/D.{cur}.EUR.SP00.A?startPeriod={START}&endPeriod={END}&format=csvdata'
  try:
   b=get(url); raw=RAW/f'{cur}_per_EUR.csv'; raw.write_bytes(b); df=parse(b)[['timestamp','value']].rename(columns={'value':'local_per_eur'}); df=df[(df.timestamp>=START)&(df.timestamp<=END)&(df.local_per_eur>0)]; df=df.merge(usd,on='timestamp'); df['value']=df.usd_per_eur/df.local_per_eur; df['symbol']=f'{cur}USD_ECB'; df['source']=f'ECB:EXR.D.{cur}.EUR.SP00.A'; df['unit']=f'USD per {cur}'; frames.append(df[['timestamp','value','symbol','source','unit']]); entries.append({'symbol':f'{cur}USD_ECB','currency':cur,'url':url,'cross_rate':'USD/EUR divided by '+cur+'/EUR','unit':f'USD per {cur}','raw_file':str(raw.relative_to(ROOT)),'raw_sha256':hashlib.sha256(b).hexdigest(),'rows':len(df),'start':df.timestamp.min().strftime('%Y-%m-%d'),'end':df.timestamp.max().strftime('%Y-%m-%d')})
  except Exception as exc: failures.append({'currency':cur,'error':type(exc).__name__+': '+str(exc)[:160]})
 if not frames: raise RuntimeError(f'No ECB series succeeded: {failures}')
 out=pd.concat(frames,ignore_index=True).sort_values(['timestamp','symbol']); out.to_csv(OUT,index=False,date_format='%Y-%m-%d'); man={'dataset_id':'SVU-DATA-007','retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'requested_window':{'start':START,'end':END},'frequency':'daily/available observation','numeraire':'USD','cross_rate_reference':{'symbol':'USDEUR','url':usd_url,'sha256':hashlib.sha256(usd_b).hexdigest()},'assets':entries,'failures':failures,'processed_file':str(OUT.relative_to(ROOT)),'processed_sha256':hashlib.sha256(OUT.read_bytes()).hexdigest()}; MAN.write_text(json.dumps(man,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps({'dataset_id':man['dataset_id'],'assets':len(entries),'rows':len(out),'failures':len(failures),'sha256':man['processed_sha256']},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
