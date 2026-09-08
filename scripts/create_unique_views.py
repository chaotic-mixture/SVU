"""建立研究数据的去重事实视图，不删除任何历史数据集。"""
from __future__ import annotations
import json, sqlite3
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
DB=ROOT/'svu.db'
def main():
 con=sqlite3.connect(DB)
 con.executescript('''
 DROP VIEW IF EXISTS research_price_observations_unique;
 CREATE VIEW research_price_observations_unique AS
 SELECT symbol, observed_at, value, source, unit, MIN(dataset_id) AS canonical_dataset_id
 FROM research_price_observations
 GROUP BY symbol, observed_at, value, source, unit;
 DROP VIEW IF EXISTS research_macro_observations_unique;
 CREATE VIEW research_macro_observations_unique AS
 SELECT symbol, observed_at, value, source, unit, MIN(dataset_id) AS canonical_dataset_id
 FROM research_macro_observations
 GROUP BY symbol, observed_at, value, source, unit;
 DROP TABLE IF EXISTS research_dataset_status;
 CREATE TABLE research_dataset_status (
   dataset_id TEXT PRIMARY KEY,
   role TEXT NOT NULL,
   duplicate_policy TEXT NOT NULL,
   notes TEXT
 );
 ''')
 rows=[
  ('SVU-BL-002','legacy_experiment','retain_but_exclude_from_unique_totals','历史实验身份；内容与后续公开数据重复'),
  ('SVU-DATA-003','public_asset_dataset','unique_view_default','公开资产与宏观数据主集'),
  ('SVU-DATA-004','supplemental_asset_dataset','unique_view_default','LBMA 贵金属与债券总回报'),
  ('SVU-DATA-005','global_macro_dataset','unique_view_default','全球美元与信用环境指标'),
  ('SVU-DATA-006','official_fx_dataset','unique_view_default','FRED 官方外汇扩展'),
  ('SVU-DATA-007','official_fx_dataset','unique_view_default','ECB 官方交叉汇率'),
  ('SVU-DATA-008','official_fx_dataset','unique_view_default','ECB 发现的额外汇率'),
  ('SVU-DATA-009','official_equity_dataset','unique_view_default','FRED 官方权益指数'),
  ('SVU-DATA-010','official_energy_dataset','unique_view_default','FRED/EIA 日频能源商品'),
  ('SVU-DATA-011','official_coverage_extension','unique_view_default','已核验官方序列向原生起点与最新日扩展'),
  ('SVU-DATA-012','monthly_commodity_dataset','unique_view_default','FRED/IMF 月频金属农产品，不进入日频 ICATI'),
  ('SVU-LIVE-DAILY','incremental_refresh','unique_view_default','官方来源日更追加，不覆盖历史'),
 ]
 con.executemany('INSERT INTO research_dataset_status VALUES (?,?,?,?)',rows)
 result={
  'operation':'create_unique_research_views',
  'database':str(DB),
  'price_rows_raw':con.execute('select count(*) from research_price_observations').fetchone()[0],
  'price_rows_unique':con.execute('select count(*) from research_price_observations_unique').fetchone()[0],
  'macro_rows_raw':con.execute('select count(*) from research_macro_observations').fetchone()[0],
  'macro_rows_unique':con.execute('select count(*) from research_macro_observations_unique').fetchone()[0],
  'views_created':['research_price_observations_unique','research_macro_observations_unique'],
  'history_deleted':False,
 }
 con.commit(); con.close(); (ROOT/'reports/unique_views_latest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
