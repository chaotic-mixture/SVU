from pathlib import Path

import pandas as pd

from validation.build_daily_dashboard import load_panel


def test_daily_dashboard_template_has_window_slider_and_zh_controls():
    text = (Path(__file__).resolve().parents[1] / "validation" / "dashboard_daily.html").read_text(encoding="utf-8")
    assert "startSlider" in text
    assert "endSlider" in text
    assert "niceTicks" in text
    assert "thin(" in text
    assert "画法 041" in text
    assert "chainLevel" in text
    assert "plotIndices" in text
    assert "在场重标" in text
    assert "timeTicks" in text
    assert "onPlotMove" in text
    assert "calcAssets" in text
    assert "全部货币" in text
    assert "恢复默认" in text
    assert "axisAssets" in text
    assert "所选全部历史" in text


def test_panel_keeps_history_before_common_basket():
    db = Path(__file__).resolve().parents[1] / "svu.db"
    if not db.exists():
        import pytest

        pytest.skip("svu.db is private and not shipped")
    panel, groups, control = load_panel()
    assert "NIKKEI225" in panel.columns
    assert "GOLD_LBMA_PM_USD_OZ" in panel.columns
    assert "NASDAQ" in panel.columns
    assert panel.index.min() < pd.Timestamp("2016-01-01")
    assert panel["NIKKEI225"].first_valid_index() < pd.Timestamp("2016-01-01")
    assert panel.isna().any().any()
    assert set(groups) == {"currency", "energy", "monetary_hedge", "equity_index"}
    assert "SP500" not in (control.get("equity_index") or [])
    assert "DJIA" not in (control.get("equity_index") or [])
