# SVU and ICATI: names and mathematical specification

## SVU: a fixed reference coordinate

```text
SVU_t = 100
```

SVU is a fixed display and comparison reference. It does not circulate in markets and does not move with the asset basket. Currencies such as USD, EUR and JPY are assets measured against the index. USD is the denomination used by source exchange-rate series; it is not a substitute definition of SVU.

## ICATI: a dynamic common-trend index

The International Cross-Asset Trend Index (ICATI) describes the common trend among the assets included in a basket:

```text
ICATI_t = 100 × exp(Σ_i w_i × log(P_i,t / P_i,0))
```

The project currently calculates two variants:

- `ICATI-CB`: category-balanced weights;
- `ICATI-EW`: equal economic-entity weights.

## Asset performance relative to the common trend

```text
Asset/SVU_t = 100 × (P_i,t / P_i,0) / (ICATI_t / ICATI_0)
```

Above 100 means the asset outperformed the ICATI common trend; below 100 means it underperformed; 100 means it moved in line with the trend.

SVU is a fixed coordinate, ICATI is a dynamic common-trend measurement, and Asset/SVU is an asset's deviation relative to that trend. `SVU` is no longer used to name dynamic indices. Legacy report keys `SVU-EW` and `SVU-CB` are interpreted in new output layers as `ICATI-EW` and `ICATI-CB`. Historical files are not rewritten; new reports use current names.

## Domain-relative views

Read-only domain pages do not create another value unit or change the main asset pool, source policies, ICATI-CB or ICATI-EW:

- `SVU-DV-CURRENCY`: currencies;
- `SVU-DV-ENERGY`: energy;
- `SVU-DV-MONETARY_HEDGE`: precious-metal hedges;
- `SVU-DV-EQUITY_INDEX`: equity indices.

Each domain calculates its dynamic common trend over dates where every domain member has an actual observation, then displays relative performance against a fixed baseline of 100:

```text
DomainICATI_g,t = 100 × exp(Σ_i w_i,g × log(P_i,t / P_i,0))
AssetRelative_i,g,t = 100 × (P_i,t / P_i,0) / (DomainICATI_g,t / 100)
```

Weights are equal within a domain by default. Missing dates are not forward-filled. The currency page may show a constant USD-denominated reference relative to the currency-domain ICATI; USD is not included in that domain's calculation basket. A value of 100 in different domains may refer to different members and base windows, so domain values are not directly comparable. Machine-readable reports are under `reports/domains/` and `reports/domain_*_latest.*`.

See `README.zh-CN.md` for a Chinese overview.
