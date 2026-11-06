from __future__ import annotations

import logging
import math
from unittest.mock import Mock

import numpy as np

from ml.train_predict import _evaluate_test_split


def test_empty_test_split_warns_without_pool_or_predict(monkeypatch, caplog) -> None:
    model = Mock()
    pool = Mock(side_effect=AssertionError("Pool must not be created for an empty test split"))
    monkeypatch.setattr("ml.train_predict.Pool", pool)

    with caplog.at_level(logging.WARNING, logger="ml.train_predict"):
        metrics = _evaluate_test_split(model, np.empty((0, 3)), np.empty(0), [0])

    assert all(math.isnan(value) for value in metrics)
    pool.assert_not_called()
    model.predict.assert_not_called()
    assert "Test split is empty" in caplog.text
