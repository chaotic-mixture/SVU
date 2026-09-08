"""Build a read-only relative view for one SVU asset domain."""
from __future__ import annotations

from contextlib import closing
import sqlite3
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
from validation.baseline import validate_panel


def _build_diagnostics(
    normalized: pd.DataFrame,
    domain_svu: pd.Series,
    relative: pd.DataFrame,
    base: float,
) -> dict:
    current = relative.iloc[-1]
    current_values = current.to_numpy(dtype=float)
    leave_one_out = {}
    full_normalized = domain_svu / domain_svu.iloc[0]
    for asset in normalized.columns:
        remaining = normalized.drop(columns=[asset])
        loo_svu = base * np.exp(np.log(remaining).mean(axis=1))
        loo_normalized = loo_svu / loo_svu.iloc[0]
        delta = loo_normalized - full_normalized
        leave_one_out[asset] = {
            "domain_svu_end": round(float(loo_svu.iloc[-1]), 8),
            "end_normalized_delta": round(float(delta.iloc[-1]), 8),
            "max_abs_normalized_delta": round(float(delta.abs().max()), 8),
        }
    return {
        "domain_svu_start": round(float(domain_svu.iloc[0]), 8),
        "domain_svu_end": round(float(domain_svu.iloc[-1]), 8),
        "current_relative": {
            asset: round(float(current[asset]), 8) for asset in relative.columns
        },
        "current_dispersion": {
            "std": round(float(np.std(current_values, ddof=0)), 8),
            "spread": round(float(current_values.max() - current_values.min()), 8),
            "mean_abs_deviation_from_100": round(
                float(np.mean(np.abs(current_values - base))), 8
            ),
        },
        "leave_one_out": leave_one_out,
    }


def build_domain_view(
    panel: pd.DataFrame,
    assets: Sequence[str],
    *,
    domain_key: str,
    base: float = 100.0,
    min_common_observations: int = 3,
) -> dict:
    """Return a domain trend and asset series relative to that trend.

    The domain trend is a geometric equal-weight index rebased to ``base``
    on the first common observation.  The displayed relative baseline is a
    constant ``base`` line; it is not a second value unit.
    """
    selected = list(dict.fromkeys(assets))
    if len(selected) < 2:
        raise ValueError("领域视图至少需要两个资产")
    missing = [asset for asset in selected if asset not in panel.columns]
    if missing:
        raise ValueError(f"领域资产不在价格面板中: {missing}")

    common = panel[selected].sort_index().dropna(how="any")
    if len(common) < min_common_observations:
        raise ValueError(
            f"领域共同有效观测不足: {len(common)} < {min_common_observations}"
        )
    if (common <= 0).any().any():
        raise ValueError("领域价格必须全部为正")

    validate_panel(common, base)
    normalized = common / common.iloc[0]
    domain_index = base * np.exp(np.log(normalized).mean(axis=1))
    relative = base * normalized.div(domain_index / base, axis=0)
    diagnostics = _build_diagnostics(normalized, domain_index, relative, base)

    return {
        "domain_key": domain_key,
        "assets": selected,
        "dates": [stamp.strftime("%Y-%m-%d") for stamp in common.index],
        "domain_index": [round(float(value), 8) for value in domain_index],
        "domain_icati": [round(float(value), 8) for value in domain_index],
        "domain_svu": [round(float(value), 8) for value in domain_index],  # Legacy JSON alias.
        "dynamic_index_name": "Domain ICATI",
        "baseline": [round(float(value), 8) for value in domain_index],
        "relative_baseline": [float(base)] * len(common),
        "display_assets": selected.copy(),
        "relative_reference": {asset: "computed_domain_svu" for asset in selected},
        "diagnostics": diagnostics,
        "relative": {
            asset: [round(float(value), 8) for value in relative[asset]]
            for asset in selected
        },
        "start": common.index.min().strftime("%Y-%m-%d"),
        "end": common.index.max().strftime("%Y-%m-%d"),
        "n_observations": int(len(common)),
        "base": float(base),
        "reference_type": "computed_domain_svu",
        "weighting": "equal_weight_geometric_mean_within_domain",
        "missing_policy": "complete_common_observations_only_no_forward_fill",
    }


def add_numeraire_reference(view: dict, *, symbol: str = "USD") -> dict:
    """Add a constant numeraire as a display-only series relative to domain SVU."""
    if symbol in view["assets"]:
        raise ValueError(f"计价参照不能同时是领域篮子资产: {symbol}")
    base = float(view["base"])
    domain_svu = view["domain_svu"]
    result = dict(view)
    result["relative"] = dict(view["relative"])
    result["relative_reference"] = dict(view["relative_reference"])
    result["display_assets"] = [symbol] + list(view["assets"])
    result["relative"][symbol] = [
        round(base / (float(value) / base), 8) for value in domain_svu
    ]
    result["relative_reference"][symbol] = "computed_domain_svu"
    result["numeraire_display_only"] = symbol
    return result


def load_domain_panel(db_path: str | Path, symbols: Sequence[str]) -> pd.DataFrame:
    """Load the requested canonical assets from the unique observation view."""
    with closing(sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        observations = pd.read_sql_query(
            """
            SELECT canonical_symbol, observed_at, value
            FROM research_entity_observations_unique
            WHERE canonical_symbol IN ({placeholders})
            ORDER BY observed_at, canonical_symbol
            """.format(placeholders=", ".join("?" for _ in symbols)),
            connection,
            params=list(symbols),
            parse_dates=["observed_at"],
        )
    if observations.empty:
        raise ValueError("数据库中没有找到请求的领域资产")
    return (
        observations.pivot(index="observed_at", columns="canonical_symbol", values="value")
        .sort_index()
    )
