"""Window rebase for ICATI/SVU display. Calculation starts at the selected start date."""
from __future__ import annotations

import numpy as np
import pandas as pd

from validation.baseline import calculate_index
from validation.icati import flat_svu


def slice_panel(panel: pd.DataFrame, start: str | pd.Timestamp, end: str | pd.Timestamp) -> pd.DataFrame:
    window = panel.loc[pd.Timestamp(start) : pd.Timestamp(end)].copy()
    window = window.dropna(how="any")
    if len(window) < 2:
        raise ValueError("selected window has fewer than 2 complete observations")
    return window


def category_balanced_weights(groups: dict[str, list[str]], columns: list[str]) -> dict[str, float]:
    present = {k: [a for a in xs if a in columns] for k, xs in groups.items()}
    present = {k: xs for k, xs in present.items() if xs}
    if not present:
        raise ValueError("no category members in window")
    members = [a for xs in present.values() for a in xs]
    if len(members) != len(set(members)) or set(members) != set(columns):
        raise ValueError("each asset must belong to exactly one category")
    return {a: 1.0 / (len(present) * len(xs)) for xs in present.values() for a in xs}


def entity_equal_weights(columns: list[str]) -> dict[str, float]:
    return {a: 1.0 / len(columns) for a in columns}


def rebase_display(panel: pd.DataFrame, groups: dict[str, list[str]]) -> pd.DataFrame:
    """Rebuild display series so t0 of the window is 100.

    SVU stays flat 100. ICATI-CB is the window denominator, so it is also
    displayed as 100. Other series are Asset/SVU relative to window-start ICATI-CB.
    """
    cols = list(panel.columns)
    cb = category_balanced_weights(groups, cols)
    ew = entity_equal_weights(cols)
    icb = calculate_index(panel, cb)
    iew = calculate_index(panel, ew)
    svu = flat_svu(icb)
    rel = panel.div(panel.iloc[0], axis=1).mul(100.0)
    trend = icb / icb.iloc[0]
    out = pd.DataFrame(index=panel.index)
    out["SVU"] = svu.to_numpy()
    out["ICATI-CB"] = 100.0
    out["ICATI-EW"] = (iew / iew.iloc[0] / trend * 100.0).to_numpy()
    if "USD" not in panel.columns:
        out["USD"] = (100.0 / trend).to_numpy()
    for col in cols:
        out[col] = (rel[col] / trend).to_numpy()
    for name, members in groups.items():
        xs = [a for a in members if a in panel.columns]
        if not xs:
            continue
        gi = calculate_index(panel[xs], {a: 1.0 / len(xs) for a in xs})
        out[f"{name} baseline"] = (gi / gi.iloc[0] / trend * 100.0).to_numpy()
    return out
