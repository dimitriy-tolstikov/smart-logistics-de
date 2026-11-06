"""Unit tests for ML split bounds, feature hygiene, and scoring NULL rules."""

from __future__ import annotations

import inspect
import math
from datetime import date, timedelta

from ml.train_predict import (
    CAT_COLS,
    NUM_COLS,
    _mask_test,
    _mask_train,
    _parse_region,
    load_training_table,
    ml_split_bounds,
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
    ld = date(2026, 10, 26)
    doy = ld.timetuple().tm_yday
    month = ld.month
    dow = ld.weekday()
    assert math.isclose(math.sin(2 * math.pi * doy / 365.25), math.sin(2 * math.pi * 299 / 365.25))
    assert math.isclose(
        math.cos(2 * math.pi * (month - 1) / 12.0),
        math.cos(2 * math.pi * 9 / 12.0),
    )
    assert math.isclose(math.sin(2 * math.pi * dow / 7.0), math.sin(2 * math.pi * 0 / 7.0))


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
    actual_price = 100_000.0
    yhat = 90_000.0

    def score_fields(loading_date: date) -> tuple[float | None, float | None, float | None]:
        is_future = loading_date > as_of
        actual = None if is_future else actual_price
        abs_err = None if actual is None else abs(actual - yhat)
        ape = None if actual is None or actual == 0 else abs((actual - yhat) / actual) * 100.0
        return actual, abs_err, ape

    assert score_fields(as_of) == (actual_price, 10_000.0, 10.0)
    assert score_fields(as_of + timedelta(days=1)) == (None, None, None)
