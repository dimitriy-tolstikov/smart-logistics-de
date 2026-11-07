"""Unit tests for ML split bounds, feature hygiene, and scoring NULL rules."""

from __future__ import annotations

import inspect
import math
from datetime import date, timedelta

import numpy as np

from ml.train_predict import (
    CAT_COLS,
    INSERT_PRED_SQL,
    NUM_COLS,
    _mask_test,
    _mask_train,
    _parse_region,
    build_model,
    cyclic_date_features,
    load_training_table,
    main,
    ml_split_bounds,
    score_actuals,
)


def test_ml_split_bounds_for_default_as_of() -> None:
    train_start, train_end_excl, test_start, test_end_incl = ml_split_bounds(date(2026, 10, 26))
    assert train_start == date(2026, 7, 26)
    assert train_end_excl == date(2026, 10, 5)
    assert test_start == date(2026, 10, 5)
    assert test_end_incl == date(2026, 10, 26)


def test_ml_split_masks_and_first_future_date() -> None:
    as_of = date(2026, 10, 26)
    train_start, train_end_excl, test_start, test_end_incl = ml_split_bounds(as_of)

    assert _mask_train(train_start) is True
    assert _mask_train(train_end_excl) is False
    assert _mask_test(test_start) is True
    assert _mask_test(test_end_incl) is True
    assert _mask_test(as_of + timedelta(days=1)) is False
    assert as_of + timedelta(days=1) > as_of


def test_parse_region_from_full_name_local() -> None:
    assert _parse_region("Казань, Республика Татарстан") == ("Казань", "Республика Татарстан")
    assert _parse_region("Москва") == ("Москва", "")
    assert _parse_region(None) == ("", "")
    assert _parse_region("  ") == ("", "")


def test_cyclic_feature_math_for_known_date() -> None:
    feats = cyclic_date_features(date(2026, 10, 26))
    assert feats["doy"] == 299
    assert feats["month"] == 10
    assert feats["dow"] == 0
    assert math.isclose(feats["sin_doy"], math.sin(2 * math.pi * 299 / 365.25))
    assert math.isclose(feats["cos_doy"], math.cos(2 * math.pi * 299 / 365.25))
    assert math.isclose(feats["sin_month"], math.sin(2 * math.pi * 9 / 12.0))
    assert math.isclose(feats["cos_month"], math.cos(2 * math.pi * 9 / 12.0))
    assert math.isclose(feats["sin_dow"], 0.0, abs_tol=1e-12)
    assert math.isclose(feats["cos_dow"], 1.0, abs_tol=1e-12)


def test_feature_columns_exclude_forbidden_target_fields() -> None:
    forbidden = {
        "price_with_vat",
        "price_km",
        "price_per_km",
        "price_ton",
        "avg_price",
        "min_km_price",
        "max_km_price",
        "price_group_id",
        "price_group_stats",
        "outlier_mark",
        "carrier_cost",
        "customer_cost",
        "profit",
    }
    feature_names = set(CAT_COLS) | set(NUM_COLS)
    assert feature_names.isdisjoint(forbidden)

    src = inspect.getsource(load_training_table)
    for name in (
        "price_with_vat",
        "price_group_stats",
        "avg_price",
        "min_km_price",
        "max_km_price",
    ):
        assert name not in src


def test_future_scoring_null_rule() -> None:
    as_of = date(2026, 10, 26)
    assert score_actuals(as_of, 100_000.0, 90_000.0, as_of) == (100_000.0, 10_000.0, 10.0)
    assert score_actuals(as_of + timedelta(days=1), 100_000.0, 90_000.0, as_of) == (None, None, None)
    assert "scored_at" not in INSERT_PRED_SQL
    src = inspect.getsource(main)
    assert "model.fit(train_pool)" in src
    assert "eval_set=" not in src
    assert "shuffle=" not in src
    assert "score_actuals(" in src


def test_repeat_fit_is_deterministic() -> None:
    rng = np.random.default_rng(1)
    features = rng.normal(size=(24, 6))
    target = features[:, 0] * 5.0 + 1.0
    fitted = []
    for _ in range(2):
        model = build_model(iterations=20)
        model.fit(features, target)
        fitted.append(model.predict(features))
    assert model.get_params()["random_seed"] == 42
    assert model.get_params()["thread_count"] == 1
    assert np.allclose(fitted[0], fitted[1])
