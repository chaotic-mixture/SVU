from validation.window_rebase import rebase_display, slice_panel


def _panel():
    import pandas as pd

    idx = pd.date_range("2020-01-01", periods=10, freq="D")
    return pd.DataFrame(
        {
            "A": [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9],
            "B": [2.0, 2.0, 2.1, 2.2, 2.1, 2.3, 2.4, 2.3, 2.5, 2.6],
        },
        index=idx,
    )


def test_window_start_rebases_to_100():
    panel = _panel()
    groups = {"g1": ["A"], "g2": ["B"]}
    window = slice_panel(panel, "2020-01-04", "2020-01-08")
    display = rebase_display(window, groups)
    assert display.index.min().strftime("%Y-%m-%d") == "2020-01-04"
    assert display.index.max().strftime("%Y-%m-%d") == "2020-01-08"
    assert abs(display["SVU"].iloc[0] - 100.0) < 1e-9
    assert (display["SVU"] == 100.0).all()
    assert abs(display["ICATI-CB"].iloc[0] - 100.0) < 1e-9
    assert abs(display["A"].iloc[0] - 100.0) < 1e-9
    assert abs(display["B"].iloc[0] - 100.0) < 1e-9


def test_later_window_is_not_the_full_sample_path():
    panel = _panel()
    groups = {"g1": ["A"], "g2": ["B"]}
    full = rebase_display(panel, groups)
    later = rebase_display(slice_panel(panel, "2020-01-06", "2020-01-10"), groups)
    assert abs(later["A"].iloc[0] - 100.0) < 1e-9
    assert abs(full.loc["2020-01-06", "A"] - 100.0) > 1.0
