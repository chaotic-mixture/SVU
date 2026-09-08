"""SVU v0 的可解释基线：不训练 GNN，只验证指数是否可重复评价。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd


DEFAULT_ASSET_TYPES = {"currency", "crypto", "commodity", "precious_metal", "stock", "index", "fund"}


def load_sqlite_prices(db_path: str | Path) -> pd.DataFrame:
    """从项目 SQLite 读取价格与资产元数据，保留来源字段。"""
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(
            """
            SELECT i.symbol, i.type, p.timestamp, p.price, p.source, p.confidence
            FROM prices p JOIN items i ON i.id = p.item_id
            WHERE i.type != 'svu'
            ORDER BY p.timestamp, i.symbol
            """,
            conn,
        )


def load_csv_prices(csv_path: str | Path) -> pd.DataFrame:
    """读取 fetch_public_data.py 生成的标准化长表。"""
    frame = pd.read_csv(csv_path, parse_dates=["timestamp"])
    return frame.rename(columns={"value": "price"})


def build_price_panel(
    prices: pd.DataFrame,
    min_observations: int = 20,
    allowed_types: Iterable[str] = DEFAULT_ASSET_TYPES,
    require_multiple: bool = True,
) -> Tuple[pd.DataFrame, dict]:
    """过滤正价格、按日期对齐，并只保留有足够观测的资产。"""
    required = {"symbol", "timestamp", "price"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"缺少列: {sorted(missing)}")
    frame = prices.copy()
    if "type" in frame.columns:
        frame = frame[frame["type"].isin(set(allowed_types))]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce").dt.normalize()
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "timestamp", "price"])
    frame = frame[(frame["price"] > 0) & np.isfinite(frame["price"])]
    # 同一资产同一天若有多条记录，只使用该日均值，避免 API/BASE 重复污染。
    daily = frame.groupby(["timestamp", "symbol"], as_index=False)["price"].mean()
    counts = daily.groupby("symbol").size()
    symbols = sorted(counts[counts >= min_observations].index.tolist())
    if (require_multiple and len(symbols) < 2) or (not require_multiple and len(symbols) < 1):
        raise ValueError(f"满足最少观测数的资产不足: {symbols}")
    panel = daily[daily["symbol"].isin(symbols)].pivot(index="timestamp", columns="symbol", values="price")
    panel = panel.sort_index().dropna(how="any")
    if len(panel) < 3:
        raise ValueError("完整对齐后的共同时间点不足 3 个")
    meta = {
        "assets": symbols,
        "n_assets": len(symbols),
        "n_periods": len(panel),
        "start": panel.index.min().strftime("%Y-%m-%d"),
        "end": panel.index.max().strftime("%Y-%m-%d"),
        "source_counts": frame[frame["symbol"].isin(symbols)]["source"].fillna("UNKNOWN").value_counts().to_dict()
        if "source" in frame.columns else {},
    }
    return panel, meta


def equal_weights(returns: pd.DataFrame) -> Dict[str, float]:
    return {column: 1.0 / len(returns.columns) for column in returns.columns}


def inverse_volatility_weights(returns: pd.DataFrame) -> Dict[str, float]:
    volatility = returns.std(ddof=1).replace(0, np.nan).fillna(np.inf)
    inv = 1.0 / volatility.replace(np.inf, np.nan)
    if inv.isna().all():
        return equal_weights(returns)
    inv = inv.fillna(0.0)
    if inv.sum() == 0:
        return equal_weights(returns)
    inv = inv / inv.sum()
    return inv.to_dict()


def pca_weights(returns: pd.DataFrame) -> Dict[str, float]:
    """PCA 一因子载荷的绝对值权重；仅作透明对照，不宣称因果。"""
    x = returns.to_numpy(dtype=float)
    x = x - x.mean(axis=0, keepdims=True)
    _, _, vh = np.linalg.svd(x, full_matrices=False)
    loading = np.abs(vh[0])
    if loading.sum() == 0:
        return equal_weights(returns)
    loading = loading / loading.sum()
    return dict(zip(returns.columns, loading.tolist()))


def validate_panel(panel: pd.DataFrame, base: float = 100.0) -> None:
    """Require an unambiguous, finite positive price panel before logarithms."""
    if panel.empty or not panel.columns.is_unique or not panel.index.is_unique:
        raise ValueError("price panel must be nonempty with unique columns and dates")
    if not panel.index.is_monotonic_increasing:
        raise ValueError("price panel dates must be increasing")
    values = panel.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("prices must be finite and positive")
    if not np.isfinite(base) or base <= 0:
        raise ValueError("base must be finite and positive")


def calculate_index(panel: pd.DataFrame, weights: Dict[str, float], base: float = 100.0) -> pd.Series:
    """几何加权收益指数：I_t = base * exp(sum_i w_i * log(P_i,t/P_i,0))."""
    validate_panel(panel, base)
    if set(panel.columns) != set(weights):
        raise ValueError("权重资产集合与价格面板不一致")
    ordered = np.array([weights[c] for c in panel.columns], dtype=float)
    if not np.isfinite(ordered).all() or np.any(ordered < 0) or not np.isclose(ordered.sum(), 1.0):
        raise ValueError("权重必须非负且和为 1")
    logs = np.log(panel.to_numpy(dtype=float))
    with np.errstate(over="ignore", under="ignore"):
        values = base * np.exp((logs - logs[0]) @ ordered)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("index exceeds finite positive numeric range")
    return pd.Series(values, index=panel.index, name="svu_index")


def robustness_score(panel: pd.DataFrame, weights: Dict[str, float]) -> dict:
    """内部稳健性指标：留一资产指数与全指数的相关性和最大偏离。"""
    full = calculate_index(panel, weights)
    if len(panel) < 3 or len(panel.columns) < 2:
        raise ValueError("robustness requires at least two assets and three observations")
    correlations, deviations = [], []
    skipped = []
    for asset in panel.columns:
        leave = panel.drop(columns=[asset])
        remaining = {k: v for k, v in weights.items() if k != asset}
        total = sum(remaining.values())
        if total <= 0:
            skipped.append(asset)
            continue
        remaining = {k: v / total for k, v in remaining.items()}
        other = calculate_index(leave, remaining)
        full_returns, other_returns = full.pct_change().dropna(), other.pct_change().dropna()
        if full_returns.std() > 0 and other_returns.std() > 0:
            corr = float(full_returns.corr(other_returns))
            if np.isfinite(corr):
                correlations.append(corr)
        deviations.append(float((full / full.iloc[0] - other / other.iloc[0]).abs().max()))
    return {
        "leave_one_out_return_corr_mean": float(np.mean(correlations)) if correlations else None,
        "leave_one_out_return_corr_min": float(np.min(correlations)) if correlations else None,
        "leave_one_out_max_normalized_deviation": float(np.max(deviations)) if deviations else None,
        "undefined_correlation_policy": "null_when_no_variable_return_pairs",
        "skipped_zero_remaining_weight": skipped,
    }
