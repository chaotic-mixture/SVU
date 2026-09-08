import pandas as pd

from validation.icati import calculate_icati, flat_svu, relative_to_icati


def sample_panel():
    dates = pd.date_range("2024-01-01", periods=6, freq="D")
    return pd.DataFrame(
        {
            "A": [1.0, 1.01, 1.02, 1.03, 1.04, 1.05],
            "B": [2.0, 2.02, 2.01, 2.04, 2.03, 2.06],
        },
        index=dates,
    )


def test_svu_display_line_is_flat_100():
    panel = sample_panel()
    icati = calculate_icati(panel, {"A": 0.5, "B": 0.5})
    svu = flat_svu(icati)
    assert (svu == 100.0).all()
    assert svu.name == "SVU"


def test_dynamic_index_is_named_icati_not_svu():
    panel = sample_panel()
    icati = calculate_icati(panel, {"A": 0.5, "B": 0.5})
    assert icati.name == "ICATI"
    assert icati.iloc[0] == 100.0
    relative = relative_to_icati(panel, icati)
    assert set(relative.columns) == {"A", "B"}
