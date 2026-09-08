import numpy as np
import pandas as pd

from validation.baseline import (
    build_price_panel,
    calculate_index,
    equal_weights,
    inverse_volatility_weights,
    pca_weights,
)


def sample_prices():
    dates = pd.date_range("2024-01-01", periods=8, freq="D")
    rows = []
    for j, symbol in enumerate(["A", "B", "C"]):
        for i, date in enumerate(dates):
            rows.append({
                "symbol": symbol,
                "type": "currency",
                "timestamp": date,
                "price": 1 + 0.01 * i + 0.001 * j,
                "source": "TEST",
            })
    return pd.DataFrame(rows)


def test_panel_is_positive_and_aligned():
    panel, meta = build_price_panel(sample_prices(), min_observations=5)
    assert panel.shape == (8, 3)
    assert (panel > 0).all().all()
    assert meta["assets"] == ["A", "B", "C"]


def test_index_is_base_100_and_has_expected_length():
    panel, _ = build_price_panel(sample_prices(), min_observations=5)
    returns = np.log(panel).diff().dropna()
    weights = equal_weights(returns)
    index = calculate_index(panel, weights)
    assert index.iloc[0] == 100.0
    assert len(index) == len(panel)
    assert np.isfinite(index).all()


def test_weight_models_are_normalized():
    panel, _ = build_price_panel(sample_prices(), min_observations=5)
    returns = np.log(panel).diff().dropna()
    for weights in [equal_weights(returns), inverse_volatility_weights(returns), pca_weights(returns)]:
        assert set(weights) == set(panel.columns)
        assert np.isclose(sum(weights.values()), 1.0)
        assert all(value >= 0 for value in weights.values())
