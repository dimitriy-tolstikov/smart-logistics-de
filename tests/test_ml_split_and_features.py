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
    _mask_fit,
    _mask_test,
    _mask_train,
    _mask_val,
    _parse_region,
    build_model,
    cyclic_date_features,
    load_training_table,
    main,
    ml_fit_val_bounds,
    ml_split_bounds,
    rare_body_group_mask,
    score_actuals,
    segment_sample_weights,
    select_and_fit_model,
)


def test_ml_split_bounds_for_default_as_of() -> None:
    train_start, train_end_excl, test_start, test_end_incl = ml_split_bounds(date(2026, 10, 26))
    assert train_start == date(2026, 7, 26)
    assert train_end_excl == date(2026, 10, 5)
    assert test_start == date(2026, 10, 5)
    assert test_end_incl == date(2026, 10, 26)


def test_ml_fit_val_bounds_inside_train() -> None:
    fit_start, val_start, val_end = ml_fit_val_bounds(date(2026, 7, 26), date(2026, 10, 5))
    assert fit_start == date(2026, 7, 26)
    assert val_start == date(2026, 9, 21)
    assert val_end == date(2026, 10, 5)
    assert _mask_fit(date(2026, 9, 20)) is True
    assert _mask_fit(val_start) is False
    assert _mask_val(val_start) is True
    assert _mask_val(date(2026, 10, 4)) is True
    assert _mask_val(val_end) is False


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
    main_src = inspect.getsource(main)
    fit_src = inspect.getsource(select_and_fit_model)
    assert "select_and_fit_model(" in main_src
    assert "score_actuals(" in main_src
    assert "shuffle=" not in main_src
    assert "eval_set=" not in fit_src
    assert "X_test" not in fit_src
    assert "y_test" not in fit_src
    assert "FINAL_ITERATIONS" in fit_src


def test_segment_sample_weights_boost_rare_categories() -> None:
    # Columns: weight_category, volume, load_region, unload_region, body_group
    X = np.array(
        [
            ["light", "v", "r1", "r2", "common"],
            ["light", "v", "r1", "r2", "common"],
            ["light", "v", "r1", "r2", "common"],
            ["heavy", "v", "r1", "r2", "rare"],
        ],
        dtype=object,
    )
    weights = segment_sample_weights(X)
    assert weights.shape == (4,)
    assert 0.9 <= float(weights.mean()) <= 1.1
    assert weights[3] > weights[0]

    rare_mask = rare_body_group_mask(X[:, 4], np.array(["rare", "common"], dtype=object))
    assert rare_mask.tolist() == [True, False]


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
