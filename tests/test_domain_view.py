from pathlib import Path

import pandas as pd
import pytest

from validation.domain_view import add_numeraire_reference, build_domain_view
from scripts.daily_official_refresh import DOMAIN_KEYS, rebuild_domain_dashboards


def test_domain_view_uses_geometric_domain_baseline_and_fixed_relative_line():
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    panel = pd.DataFrame(
        {
            "A": [100.0, 110.0, 121.0],
            "B": [100.0, 90.0, 81.0],
        },
        index=index,
    )

    result = build_domain_view(panel, ["A", "B"], domain_key="test")

    assert result["assets"] == ["A", "B"]
    assert result["baseline"][0] == pytest.approx(100.0)
    assert result["baseline"][1] == pytest.approx(99.498744)
    assert result["relative"]["A"][0] == pytest.approx(100.0)
    assert result["relative"]["A"][1] == pytest.approx(110.554160)
    assert result["relative"]["B"][1] == pytest.approx(90.453404)
    assert result["relative_baseline"] == [100.0, 100.0, 100.0]
    assert result["domain_svu"] == result["domain_index"]
    assert result["reference_type"] == "computed_domain_svu"
    assert result["relative"]["A"][1] != pytest.approx(110.0)
    assert result["diagnostics"]["current_relative"]["A"] == pytest.approx(122.222222)
    assert result["diagnostics"]["current_relative"]["B"] == pytest.approx(81.818182)
    assert result["diagnostics"]["leave_one_out"]

def test_constant_numeraire_is_displayed_relative_to_domain_svu_without_joining_basket():
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    panel = pd.DataFrame(
        {
            "A": [100.0, 120.0, 120.0],
            "B": [100.0, 100.0, 100.0],
        },
        index=index,
    )
    view = build_domain_view(panel, ["A", "B"], domain_key="currency")

    result = add_numeraire_reference(view, symbol="USD")

    assert view["assets"] == ["A", "B"]
    assert result["assets"] == ["A", "B"]
    assert result["display_assets"] == ["USD", "A", "B"]
    assert result["relative"]["USD"][0] == pytest.approx(100.0)
    assert result["relative"]["USD"][1] == pytest.approx(91.2871)
    assert result["relative_reference"]["USD"] == "computed_domain_svu"


def test_domain_view_rejects_single_asset_and_incomplete_common_window():
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    panel = pd.DataFrame(
        {
            "A": [100.0, 110.0, 121.0],
            "B": [100.0, None, 81.0],
        },
        index=index,
    )

    with pytest.raises(ValueError, match="至少需要两个资产"):
        build_domain_view(panel, ["A"], domain_key="test")

    with pytest.raises(ValueError, match="共同有效观测不足"):
        build_domain_view(panel, ["A", "B"], domain_key="test")


def test_daily_refresh_rebuilds_all_registered_domain_views():
    calls = []

    def fake_builder(domain):
        calls.append(domain)
        return {"domain_key": domain, "status": "ok"}

    result = rebuild_domain_dashboards(builder=fake_builder)

    assert calls == DOMAIN_KEYS
    assert [row["domain_key"] for row in result] == DOMAIN_KEYS


def test_domain_view_generator_and_template_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / "validation" / "build_domain_dashboard.py").exists()
    template = (root / "validation" / "domain_view.html").read_text(encoding="utf-8")
    assert (root / "validation" / "domain_view.html").exists()
    assert "domainChart" in template
    assert "DATA.display_assets||DATA.assets" in template
    assert "measurementNote" in template
