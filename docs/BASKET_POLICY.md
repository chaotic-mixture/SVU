# Basket policy

## v0.4 (adopted 2026-09-29)

The active configuration is `config/svu_v04_candidate.yaml`. The 37-member official basket removes BGNUSD_ECB from both the official and long-history control pools. The prior v0.3 configuration remains in `config/svu_v03_candidate.yaml`; historical report generations and raw observations are preserved.

Bulgarian lev reference rates ended when Bulgaria adopted the euro on 2026-01-01. See the [ECB technical update](https://www.ecb.europa.eu/services/using-our-site/technical-updates/html/ecb.technical_update251211.en.html).

Each basket version independently recomputes its available historical window using its own membership. v0.4 is not spliced onto v0.3 at an effective date; historical values and currency weights therefore differ. EUR is already a member, so BGN is not replaced by a duplicate EUR series. Category-balanced and entity-equal formulas are unchanged; member weights follow the version's membership.

## Maintenance rules

- Removing a member requires a documented reason, review and explicit basket-version change. Missing or delayed quotes never remove a member automatically.
- Use only dates with actual observations for every member. Do not forward-fill, interpolate or label old observations as current. Different market holidays may create gaps.
- The last complete date is the final actual common date. It is not the refresh date or the earliest individual end date. Report individual series coverage and the basket's common window separately.
- Official daily data may be published with a delay; it is not intraday pricing. A failed refresh preserves the last complete publication.
- Retain retired series for historical research. Do not rewrite old report generations or silently join indices calculated with different basket versions.

`README.zh-CN.md` provides the Chinese-language overview.
