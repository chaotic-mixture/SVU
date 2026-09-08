import numpy as np
import pandas as pd
import pytest

from validation.baseline import calculate_index, robustness_score
from validation.window_rebase import category_balanced_weights


@pytest.mark.parametrize("bad", [0, -1, np.inf, -np.inf, np.nan])
def test_index_rejects_bad_prices(bad):
    with pytest.raises(ValueError, match="finite and positive"):
        calculate_index(pd.DataFrame({"A": [1., bad, 2.]}), {"A": 1.})


@pytest.mark.parametrize("base", [0, -100, np.inf, np.nan])
def test_index_rejects_bad_base(base):
    with pytest.raises(ValueError, match="base"):
        calculate_index(pd.DataFrame({"A": [1., 2.]}), {"A": 1.}, base=base)


def test_index_rejects_ambiguous_panel():
    for panel in [pd.DataFrame(), pd.DataFrame([[1, 2]], columns=["A", "A"]),
                  pd.DataFrame({"A": [1., 2.]}, index=[0, 0])]:
        with pytest.raises(ValueError):
            calculate_index(panel, {"A": 1.})


def test_robustness_marks_undefined_cases():
    panel = pd.DataFrame({"A": [1., 2., 4.], "B": [1., 1., 1.]})
    result = robustness_score(panel, {"A": 1., "B": 0.})
    assert result["skipped_zero_remaining_weight"] == ["A"]
    assert result["leave_one_out_return_corr_mean"] is None


def test_category_membership_is_a_partition():
    with pytest.raises(ValueError, match="exactly one"):
        category_balanced_weights({"x": ["A"], "y": ["A", "B"]}, ["A", "B"])
