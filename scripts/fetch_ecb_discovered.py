"""自动发现并获取 ECB EXR 数据流中的官方货币序列。"""
from __future__ import annotations
import hashlib,json
from datetime import datetime,timezone
from io import BytesIO
from pathlib import Path
import pandas as pd,requests
ROOT=Path(__file__).resolve().parents[1]; RAW=ROOT/'data/raw/ecb_discovered'; OUT=ROOT/'data/processed/ecb_discovered_fx.csv'; MAN=ROOT/'data/processed/ecb_discovered_manifest.json'
START,END='2015-01-01','2025-12-31'
def main():
 RAW.mkdir(parents=True,exist_ok=True); OUT.parent.mkdir(parents=True,exist_ok=True); url=f'https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A?startPeriod={START}&endPeriod={END}&format=csvdata'; resp=requests.get(url,timeout=180); resp.raise_for_status(); b=resp.content; raw=RAW/'all_currency_per_eur.csv'; raw.write_bytes(b); df=pd.read_csv(BytesIO(b), low_memory=False); df=df.rename(columns={'TIME_PERIOD':'timestamp','OBS_VALUE':'value'}); df.timestamp=pd.to_datetime(df.timestamp,errors='coerce'); df.value=pd.to_numeric(df.value,errors='coerce'); df=df.dropna(subset=['timestamp','value']); usd=df[df.CURRENCY=='USD'][['timestamp','value']].rename(columns={'value':'usd_per_eur'}); frames=[]; entries=[]; failures=[]
 for cur,g in df.groupby('CURRENCY'):
  if cur in {'USD','XDR','XAU','XAG'}: continue
  j=g[['timestamp','value']].rename(columns={'value':'local_per_eur'}).merge(usd,on='timestamp'); j=j[(j.local_per_eur>0)&(j.usd_per_eur>0)];
  if len(j)<100: failures.append({'currency':cur,'reason':'less_than_100_observations'}); continue
  j['value']=j.usd_per_eur/j.local_per_eur; j['symbol']=f'{cur}USD_ECB_DISCOVERED'; j['source']=f'ECB:EXR.D.{cur}.EUR.SP00.A'; j['unit']=f'USD per {cur}'; frames.append(j[['timestamp','value','symbol','source','unit']]); entries.append({'symbol':f'{cur}USD_ECB_DISCOVERED','currency':cur,'unit':f'USD per {cur}','rows':len(j),'start':j.timestamp.min().strftime('%Y-%m-%d'),'end':j.timestamp.max().strftime('%Y-%m-%d')})
 if not frames: raise RuntimeError('No discovered currencies')
 out=pd.concat(frames,ignore_index=True).sort_values(['timestamp','symbol']); out.to_csv(OUT,index=False,date_format='%Y-%m-%d'); man={'dataset_id':'SVU-DATA-008','retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'requested_window':{'start':START,'end':END},'frequency':'daily/available observation','numeraire':'USD','discovery_url':url,'raw_file':str(raw.relative_to(ROOT)),'raw_sha256':hashlib.sha256(b).hexdigest(),'assets':entries,'failures':failures,'processed_file':str(OUT.relative_to(ROOT)),'processed_sha256':hashlib.sha256(OUT.read_bytes()).hexdigest()}; MAN.write_text(json.dumps(man,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps({'dataset_id':man['dataset_id'],'assets':len(entries),'rows':len(out),'failures':len(failures),'sha256':man['processed_sha256']},ensure_ascii=False,indent=2))
if __name__=='__main__': main()
