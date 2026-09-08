# SVU 与 ICATI 命名和数学规范

## SVU：固定标准单位

```text
SVU_t = 100
```

SVU 是显示和比较用的固定参照线，不参与市场流通，不随资产篮子涨跌。美元、欧元、日元等货币也属于被 SVU 衡量的资产。美元只是原始汇率数据的计价货币，不是 SVU 的替代基线。

## ICATI：动态共同趋势指数

ICATI（International Cross-Asset Trend Index）用于描述被纳入资产篮子的动态共同趋势：

```text
ICATI_t = 100 × exp(Σ_i w_i × log(P_i,t / P_i,0))
```

当前有两个估计版本：

- `ICATI-CB`：类别平衡；
- `ICATI-EW`：经济实体等权。

## 相对 SVU 的资产曲线

```text
Asset/SVU_t = 100 × (P_i,t / P_i,0) / (ICATI_t / ICATI_0)
```

解释：

- 大于 100：资产跑赢 ICATI 共同趋势；
- 小于 100：资产跑输 ICATI 共同趋势；
- 等于 100：资产与 ICATI 同步。

## 三者关系

```text
SVU：固定坐标
ICATI：动态共同趋势测量
Asset/SVU：资产相对共同趋势的偏离
```

`SVU` 不再用于命名动态指数。历史报告中的 `SVU-EW`/`SVU-CB` 在新输出层统一解释为 `ICATI-EW`/`ICATI-CB`；历史文件不重写，只在新报告中使用新名称。

## 分领域相对视图

SVU 主项目可以提供按领域拆分的只读派生页面。它们不新增价值单位，也不改变主项目的资产池、来源、ICATI-CB 或 ICATI-EW：

- `SVU-DV-CURRENCY`：货币领域；
- `SVU-DV-ENERGY`：能源领域；
- `SVU-DV-MONETARY_HEDGE`：贵金属对冲领域；
- `SVU-DV-EQUITY_INDEX`：股票指数领域。

每个领域先在自身共同有效窗口内计算领域动态共同趋势，再将该趋势显示为固定相对基线 100：

```text
DomainICATI_g,t = 100 × exp(Σ_i w_i,g × log(P_i,t / P_i,0))
AssetRelative_i,g,t = 100 × (P_i,t / P_i,0) / (DomainICATI_g,t / 100)
```

领域内默认等权。只使用所有领域成员均有报价的共同日期，不前向填充。货币页面可额外显示恒定美元计价参照相对于货币领域 ICATI 的曲线；美元不加入货币领域 ICATI 的计算篮子。不同领域的 100 由于成员和窗口可能不同，不作跨领域直接比较。机器页面和报告位于 `reports/domains/` 与 `reports/domain_*_latest.*`。
