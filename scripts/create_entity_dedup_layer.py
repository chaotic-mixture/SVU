"""建立经济实体去重层：多来源保留，建模默认每实体一个节点。"""
from __future__ import annotations
import json,sqlite3
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; DB=ROOT/'svu.db'
def main():
 con=sqlite3.connect(DB); assets=[r[0] for r in con.execute('select symbol from research_assets').fetchall()]; aset=set(assets)
 con.executescript('''
 CREATE TABLE IF NOT EXISTS research_entity_registry (
   entity_id TEXT PRIMARY KEY,
   canonical_symbol TEXT NOT NULL,
   asset_type TEXT NOT NULL,
   unit TEXT NOT NULL,
   notes TEXT
 );
 CREATE TABLE IF NOT EXISTS research_asset_entity_map (
   symbol TEXT PRIMARY KEY,
   entity_id TEXT NOT NULL REFERENCES research_entity_registry(entity_id),
   source_role TEXT NOT NULL,
   mapping_reason TEXT NOT NULL
 );
 DROP VIEW IF EXISTS research_entity_observations_unique;
 ''')
 con.execute('delete from research_asset_entity_map'); con.execute('delete from research_entity_registry')
 for symbol in assets:
  base=symbol[:-15] if symbol.endswith('_ECB_DISCOVERED') else (symbol[:-4] if symbol.endswith('_ECB') else symbol)
  entity_id=base if base in aset else symbol
  canonical=base if base in aset else symbol
  role='validation_source' if (symbol.endswith('_ECB') or symbol.endswith('_ECB_DISCOVERED')) and base in aset else 'canonical_source'
  reason='same currency/entity, ECB alternate source' if role=='validation_source' else 'primary loaded representation'
  row=con.execute('select asset_type,unit from research_assets where symbol=?',(canonical,)).fetchone()
  con.execute('insert or ignore into research_entity_registry values (?,?,?,?,?)',(entity_id,canonical,row[0],row[1],reason))
  con.execute('insert into research_asset_entity_map values (?,?,?,?)',(symbol,entity_id,role,reason))
 con.execute('''CREATE VIEW research_entity_observations_unique AS
 SELECT m.entity_id, e.canonical_symbol, p.observed_at, p.value, p.source, p.unit, p.canonical_dataset_id
 FROM research_price_observations_unique p
 JOIN research_asset_entity_map m ON m.symbol=p.symbol
 JOIN research_entity_registry e ON e.entity_id=m.entity_id
 WHERE m.source_role='canonical_source' ''')
 duplicates=con.execute("select entity_id,count(*) from research_asset_entity_map group by entity_id having count(*)>1").fetchall()
 result={'operation':'create_entity_dedup_layer','database':str(DB),'raw_assets':len(assets),'economic_entities':con.execute('select count(*) from research_entity_registry').fetchone()[0],'duplicate_source_groups':len(duplicates),'mapping_rows':con.execute('select count(*) from research_asset_entity_map').fetchone()[0],'modeling_view_rows':con.execute('select count(*) from research_entity_observations_unique').fetchone()[0],'history_deleted':False}
 con.commit(); con.close(); (ROOT/'reports/entity_dedup_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
