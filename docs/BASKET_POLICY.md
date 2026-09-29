# Basket versions / 篮子版本

## v0.4 (adopted 2026-09-29)

Active configuration: `config/svu_v04_candidate.yaml`. The 37-member official basket removes BGNUSD_ECB from both the official and long-history control pools. v0.3 remains unchanged in `config/svu_v03_candidate.yaml`; existing report generations and raw observations remain available.

BGN reference rates ended when Bulgaria adopted the euro on 2026-01-01. Source: https://www.ecb.europa.eu/services/using-our-site/technical-updates/html/ecb.technical_update251211.en.html

Each version independently recomputes its entire available historical window with its own membership. v0.4 is not an effective-date splice onto v0.3, and its historical values and currency weights differ. EUR already belongs to the pool; BGN is not replaced by another EUR copy. Category-balanced and entity-equal formulas remain unchanged; weights follow the new membership.

## Rules

- Membership exits require a documented version change, reason and review. Missing or late quotes never automatically remove a member. Preserve the previous configuration and observations.
- Use the intersection of dates with actual observations for every member. Do not forward-fill, interpolate or relabel historical observations as current. Different holidays may leave gaps.
- The latest complete date is the last actual common date, not the refresh date or the minimum of individual end dates. Show per-series coverage and the common window. Official daily releases can be delayed; this is not intraday pricing. Failed refreshes preserve the previous publication.
- Keep retired series in the database for historical research. Do not alter old report generations or silently join indices from different basket versions.

## 中文说明

新版正式篮子为 37 个成员，正式篮子和长历史对照篮均移除已停止发布的保加利亚列弗 BGN。旧 v0.3 配置、历史网页和原始数据保留。新版按新成员构成独立重算全部历史，不表示 2026 年之前已使用此版本，也不与旧指数拼接。货币类内和实体等权权重随成员变化，类别等权公式不变。

成员退出须有依据并显式升级版本；缺测、延迟不能触发自动剔除。仅使用全员实际有报价的日期交集，不补值。更新时间、单序列最新日期与完整篮子截止日期必须区分，日级官方数据不保证盘中实时。
