"""SVU/ICATI 命名分离：固定标准单位与动态共同趋势指数。"""
from __future__ import annotations
import numpy as np
import pandas as pd
from validation.baseline import calculate_index, validate_panel

def flat_svu(index: pd.Series, base: float = 100.0) -> pd.Series:
    """固定 SVU 显示基线。"""
    return pd.Series(base, index=index.index, name="SVU")

def calculate_icati(panel: pd.DataFrame, weights: dict[str, float], base: float = 100.0) -> pd.Series:
    """计算动态 ICATI，不将结果命名为 SVU。"""
    result = calculate_index(panel, weights, base=base)
    result.name = "ICATI"
    return result

def relative_to_icati(panel: pd.DataFrame, icati: pd.Series, base: float = 100.0) -> pd.DataFrame:
    """将资产曲线转换为相对 ICATI 的固定 SVU 坐标。"""
    validate_panel(panel, base)
    if not panel.index.equals(icati.index) or not np.isfinite(icati).all() or (icati <= 0).any():
        raise ValueError("ICATI must be finite, positive and aligned with the panel")
    normalized = panel / panel.iloc[0] * base
    trend = icati / icati.iloc[0]
    return normalized.div(trend, axis=0)
