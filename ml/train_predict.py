"""
Feature engineering, time-based train/test split, CatBoost training, batch scoring.
Uses NumPy + psycopg (no pandas) for fewer heavy native build deps.

Validation and segment handling:
- Holdout ``test`` stays a pure time-based exam and is never used as ``eval_set``.
- Inside ``train``, the last ``VAL_WEEKS`` form a chronological validation fold for a
  small hyper-parameter search; the winner is refit on the full official train window.
- Mild inverse-frequency sample weights on ``body_group`` × ``weight_category`` lift
  rare segments without changing the official train/test calendar windows.
- The target is the carrier rate (``price_type`` 3 and 9). The market corridor still
  uses every factual type. A deal contributes one canonical body group, the minimum
  ``std_id``, so the score joins the route card on that group and ``weight_category``.
- ``lag_median_trip`` is the previous calendar month median of the same cell. The
  current month is excluded. ``ML_TUNE=1`` runs the small grid; otherwise the fit
  uses the already selected depth and learning rate.
"""

from __future__ import annotations

import calendar
import logging
import math
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence

import numpy as np
import psycopg
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, r2_score

from etl.config import AS_OF, ML_RANDOM_SEED, ml_tune_enabled, pg_conn_kwargs

LOG = logging.getLogger("ml.train_predict")

MODEL_NAME = "catboost_regressor_v3"
DEFAULT_DEPTH = 8
DEFAULT_LEARNING_RATE = 0.08
VAL_WEEKS = 2
FINAL_ITERATIONS = 400
# Mild inverse-frequency clip: stronger boosts overfit noisy rare segments on this data.
WEIGHT_CLIP = (0.7, 2.0)
# Prefer overall val quality, but keep a penalty for rare body_group MAPE.
VAL_RARE_MIX = 0.35
# Small time-safe grid; scored on the inner validation fold only.
PARAM_GRID: Sequence[dict[str, float | int]] = (
    {"depth": 6, "learning_rate": 0.08},
    {"depth": 8, "learning_rate": 0.08},
    {"depth": 8, "learning_rate": 0.05},
    {"depth": 10, "learning_rate": 0.05},
)


def _add_months(d: date, months: int) -> date:
    """Shift calendar months, clamping the day to the last valid day of the target month."""
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def ml_split_bounds(as_of: date) -> tuple[date, date, date, date]:
    """
    Time-based split relative to the business snapshot date.

    train: [as_of - 3 months, as_of - 3 weeks)
    test:  [as_of - 3 weeks, as_of]
    """
    test_end_incl = as_of
    test_start = as_of - timedelta(weeks=3)
    train_end_excl = test_start
    train_start = _add_months(as_of, -3)
    return train_start, train_end_excl, test_start, test_end_incl


def ml_fit_val_bounds(
    train_start: date,
    train_end_excl: date,
    *,
    val_weeks: int = VAL_WEEKS,
) -> tuple[date, date, date]:
    """
    Chronological fit/val split inside the official train window.

    fit: [train_start, val_start)
    val: [val_start, train_end_excl)
    """
    if val_weeks < 1:
        raise ValueError("val_weeks must be >= 1")
    val_start = train_end_excl - timedelta(weeks=val_weeks)
    if val_start <= train_start:
        # Degenerate window: keep at least one day for fit when possible.
        val_start = train_start + timedelta(days=1)
    if val_start >= train_end_excl:
        val_start = train_start
    return train_start, val_start, train_end_excl


TRAIN_START, TRAIN_END_EXCL, TEST_START, TEST_END_INCL = ml_split_bounds(AS_OF)
FIT_START, VAL_START, VAL_END_EXCL = ml_fit_val_bounds(TRAIN_START, TRAIN_END_EXCL)

INSERT_PRED_SQL = """
INSERT INTO mart.ml_price_predictions (
    price_id, loading_date, loading_city_gcid, unloading_city_gcid, body_group,
    weight_category, body_group_std_id,
    actual_price_rub, predicted_price_rub, abs_error_rub, ape_pct, model_name
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
BATCH_INSERT_SIZE = 5000


def _parse_region(full_name_local: object) -> tuple[str, str]:
    if full_name_local is None:
        return ("", "")
    s = str(full_name_local).strip()
    if not s:
        return ("", "")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    city = parts[0] if parts else ""
    region = parts[1] if len(parts) > 1 else ""
    return city, region


def _body_group(row_groups: object) -> str:
    if row_groups is None:
        return "-1"
    return str(row_groups)


def _optional_float(x: object) -> float:
    """Missing numbers stay NaN. CatBoost treats that as a gap, not as coordinate 0."""
    if x is None:
        return float("nan")
    return float(x)


def cyclic_date_features(ld: date) -> dict[str, float]:
    """Calendar parts and sin/cos encodings used as model features."""
    year = ld.year
    month = ld.month
    dow = ld.weekday()
    doy = ld.timetuple().tm_yday
    return {
        "year": float(year),
        "month": float(month),
        "dow": float(dow),
        "doy": float(doy),
        "sin_doy": math.sin(2 * math.pi * doy / 365.25),
        "cos_doy": math.cos(2 * math.pi * doy / 365.25),
        "sin_month": math.sin(2 * math.pi * (month - 1) / 12.0),
        "cos_month": math.cos(2 * math.pi * (month - 1) / 12.0),
        "sin_dow": math.sin(2 * math.pi * dow / 7.0),
        "cos_dow": math.cos(2 * math.pi * dow / 7.0),
    }


def score_actuals(
    loading_date: date,
    price_rub: float,
    yhat: float,
    as_of: date,
) -> tuple[float | None, float | None, float | None]:
    """Future deals keep actual, absolute error and APE empty."""
    if loading_date > as_of:
        return None, None, None
    actual = float(price_rub)
    abs_err = abs(actual - float(yhat))
    ape = None if actual == 0 else abs((actual - float(yhat)) / actual) * 100.0
    return actual, abs_err, ape


def build_model(
    *,
    iterations: int = FINAL_ITERATIONS,
    depth: int = 8,
    learning_rate: float = 0.08,
) -> CatBoostRegressor:
    return CatBoostRegressor(
        loss_function="RMSE",
        depth=depth,
        learning_rate=learning_rate,
        iterations=iterations,
        random_seed=ML_RANDOM_SEED,
        thread_count=1,
        allow_writing_files=False,
        verbose=False,
    )


CAT_COLS: Sequence[str] = (
    "weight_category",
    "volume_category",
    "loading_region",
    "unloading_region",
    "body_group",
)

WEIGHT_CATEGORY_COL = 0
BODY_GROUP_COL = 4

NUM_COLS: Sequence[str] = (
    "distance",
    "total_weight",
    "total_volume",
    "load_lat",
    "load_lon",
    "unload_lat",
    "unload_lon",
    "year",
    "month",
    "dow",
    "doy",
    "sin_doy",
    "cos_doy",
    "sin_month",
    "cos_month",
    "sin_dow",
    "cos_dow",
    "lag_median_trip",
)


@dataclass
class TrainingTable:
    price_id: np.ndarray
    loading_date: list[date]
    price_rub: np.ndarray
    loading_city_gcid: np.ndarray
    unloading_city_gcid: np.ndarray
    body_group: np.ndarray
    weight_category: np.ndarray
    body_group_std_id: np.ndarray
    feature_matrix: np.ndarray  # shape (n, n_features), object dtype for mixed str/float


def load_training_table(conn: psycopg.Connection) -> TrainingTable:
    q = """
    WITH dedup AS (
        SELECT DISTINCT ON (gc_id)
            gc_id,
            full_name_local,
            latitude,
            longitude
        FROM public.cities
        ORDER BY gc_id, id ASC
    ),
    body_std AS (
        SELECT
            price_id,
            MIN(body_group_std_id) AS body_group_std_id
        FROM core.price_body_group
        GROUP BY price_id
    ),
    market AS (
        SELECT
            date_trunc('month', f.loading_date)::date AS month,
            f.loading_city_gcid,
            f.unloading_city_gcid,
            bs.body_group_std_id,
            f.weight_category,
            f.price_rub
        FROM core.fact_prices f
        JOIN body_std bs ON bs.price_id = f.price_id
        WHERE f.is_fact
          AND NOT f.complex
          AND COALESCE(f.outlier_mark, false) IS NOT TRUE
          AND f.price_rub > 0
          AND f.distance > 0
          AND f.loading_date <= %(as_of)s::date
    ),
    cell_month AS (
        SELECT
            month,
            loading_city_gcid,
            unloading_city_gcid,
            body_group_std_id,
            weight_category,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS median_trip
        FROM market
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT
        f.price_id,
        f.loading_date,
        f.price_rub,
        f.distance,
        f.total_weight,
        f.total_volume,
        f.weight_category,
        f.volume_category,
        f.loading_city_gcid,
        f.unloading_city_gcid,
        lc.full_name_local AS load_full,
        uc.full_name_local AS unload_full,
        lc.latitude AS load_lat,
        lc.longitude AS load_lon,
        uc.latitude AS unload_lat,
        uc.longitude AS unload_lon,
        bs.body_group_std_id,
        lag.median_trip AS lag_median_trip
    FROM core.fact_prices f
    JOIN public.prices p ON p.id = f.price_id
    JOIN dedup lc ON lc.gc_id = f.loading_city_gcid
    JOIN dedup uc ON uc.gc_id = f.unloading_city_gcid
    JOIN body_std bs ON bs.price_id = f.price_id
    LEFT JOIN cell_month lag
      ON lag.loading_city_gcid = f.loading_city_gcid
     AND lag.unloading_city_gcid = f.unloading_city_gcid
     AND lag.body_group_std_id = bs.body_group_std_id
     AND lag.weight_category = f.weight_category
     AND lag.month = (date_trunc('month', f.loading_date)::date - INTERVAL '1 month')::date
    WHERE f.price_rub > 0
      AND f.distance > 0
      AND (
          (
              f.is_fact
              AND NOT f.complex
              AND COALESCE(f.outlier_mark, false) IS NOT TRUE
              AND p.price_type IN (3, 9)
              AND f.loading_date <= %(as_of)s::date
          )
          OR f.loading_date > %(as_of)s::date
      )
    ORDER BY f.price_id
    """
    with conn.cursor() as cur:
        cur.execute(q, {"as_of": AS_OF})
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description or []]

    if not rows:
        raise RuntimeError("No rows from load_training_table — run ETL first")

    col_idx = {name: i for i, name in enumerate(colnames)}
    n = len(rows)

    price_ids = np.empty(n, dtype=np.int64)
    loading_dates: list[date] = []
    price_rub = np.empty(n, dtype=np.float64)
    load_gc = np.empty(n, dtype=np.int64)
    unload_gc = np.empty(n, dtype=np.int64)
    body_group = np.empty(n, dtype=object)
    weight_category = np.empty(n, dtype=object)
    body_group_std_id = np.empty(n, dtype=np.int64)

    n_features = len(CAT_COLS) + len(NUM_COLS)
    X = np.empty((n, n_features), dtype=object)

    for i, row in enumerate(rows):
        pid = int(row[col_idx["price_id"]])
        ld = row[col_idx["loading_date"]]
        if not isinstance(ld, date):
            raise TypeError("loading_date must be datetime.date")

        wc = row[col_idx["weight_category"]] or ""
        vc = row[col_idx["volume_category"]] or ""
        _lcity, lregion = _parse_region(row[col_idx["load_full"]])
        _ucity, uregion = _parse_region(row[col_idx["unload_full"]])
        std_id = int(row[col_idx["body_group_std_id"]])
        bg = _body_group(std_id)

        cal = cyclic_date_features(ld)

        nums = np.array(
            [
                _optional_float(row[col_idx["distance"]]),
                _optional_float(row[col_idx["total_weight"]]),
                _optional_float(row[col_idx["total_volume"]]),
                _optional_float(row[col_idx["load_lat"]]),
                _optional_float(row[col_idx["load_lon"]]),
                _optional_float(row[col_idx["unload_lat"]]),
                _optional_float(row[col_idx["unload_lon"]]),
                cal["year"],
                cal["month"],
                cal["dow"],
                cal["doy"],
                cal["sin_doy"],
                cal["cos_doy"],
                cal["sin_month"],
                cal["cos_month"],
                cal["sin_dow"],
                cal["cos_dow"],
                _optional_float(row[col_idx["lag_median_trip"]]),
            ],
            dtype=np.float64,
        )

        cats = [str(wc), str(vc), str(lregion), str(uregion), str(bg)]
        X[i, : len(CAT_COLS)] = cats
        X[i, len(CAT_COLS) :] = nums

        price_ids[i] = pid
        loading_dates.append(ld)
        price_rub[i] = float(row[col_idx["price_rub"]])
        load_gc[i] = int(row[col_idx["loading_city_gcid"]])
        unload_gc[i] = int(row[col_idx["unloading_city_gcid"]])
        body_group[i] = bg
        weight_category[i] = str(wc)
        body_group_std_id[i] = std_id

    return TrainingTable(
        price_id=price_ids,
        loading_date=loading_dates,
        price_rub=price_rub,
        loading_city_gcid=load_gc,
        unloading_city_gcid=unload_gc,
        body_group=body_group,
        weight_category=weight_category,
        body_group_std_id=body_group_std_id,
        feature_matrix=X,
    )


def _mask_train(d: date) -> bool:
    return TRAIN_START <= d < TRAIN_END_EXCL


def _mask_fit(d: date) -> bool:
    return FIT_START <= d < VAL_START


def _mask_val(d: date) -> bool:
    return VAL_START <= d < VAL_END_EXCL


def _mask_test(d: date) -> bool:
    return TEST_START <= d <= TEST_END_INCL


def segment_sample_weights(
    feature_matrix: np.ndarray,
    *,
    reference_matrix: np.ndarray | None = None,
    weight_clip: tuple[float, float] = WEIGHT_CLIP,
) -> np.ndarray:
    """
    Inverse-frequency weights for body_group × weight_category.

    Frequencies are estimated on ``reference_matrix`` (defaults to ``feature_matrix``)
    so a validation fold can reuse fit-period rarity. Mean weight is normalized to
    1.0; values are clipped for stability.
    """
    n = feature_matrix.shape[0]
    if n == 0:
        return np.empty(0, dtype=np.float64)

    ref = feature_matrix if reference_matrix is None else reference_matrix
    if ref.shape[0] == 0:
        return np.ones(n, dtype=np.float64)

    ref_wc = ref[:, WEIGHT_CATEGORY_COL].astype(str)
    ref_bg = ref[:, BODY_GROUP_COL].astype(str)
    wc_counts = Counter(ref_wc.tolist())
    bg_counts = Counter(ref_bg.tolist())
    n_ref = ref.shape[0]

    wc = feature_matrix[:, WEIGHT_CATEGORY_COL].astype(str)
    bg = feature_matrix[:, BODY_GROUP_COL].astype(str)

    lo, hi = weight_clip
    raw = np.empty(n, dtype=np.float64)
    for i in range(n):
        # Unseen-in-ref categories get count=1 → treated as rare, which is conservative.
        inv = math.sqrt(
            (n_ref / max(wc_counts.get(wc[i], 1), 1))
            * (n_ref / max(bg_counts.get(bg[i], 1), 1))
        )
        raw[i] = inv

    mean_w = float(raw.mean())
    if mean_w <= 0:
        return np.ones(n, dtype=np.float64)
    # Clip after mean-normalization; mean may drift slightly above/below 1.0.
    return np.clip(raw / mean_w, lo, hi).astype(np.float64)


def rare_body_group_mask(train_body_groups: np.ndarray, eval_body_groups: np.ndarray) -> np.ndarray:
    """Mark eval rows whose body_group frequency in train is at/below the train median."""
    counts = Counter(train_body_groups.astype(str).tolist())
    if not counts:
        return np.zeros(len(eval_body_groups), dtype=bool)
    median_freq = float(np.median(np.array(list(counts.values()), dtype=np.float64)))
    rare = {k for k, v in counts.items() if v <= median_freq}
    return np.array([str(g) in rare for g in eval_body_groups], dtype=bool)


def write_ml_run(
    dsn: str,
    *,
    as_of: date,
    depth: int,
    learning_rate: float,
    tuned: bool,
    train_rows: int,
    test_rows: int,
    rmse: float,
    mae: float,
    r2: float,
    mape: float,
    scored_rows: int,
) -> None:
    """Persist holdout metrics. NaN becomes SQL NULL."""

    def _num(value: float) -> float | None:
        if math.isnan(value):
            return None
        return float(value)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_meta.ml_runs (
                    as_of, model_name, depth, learning_rate, iterations, tuned,
                    train_rows, test_rows, r2, mape_pct, rmse, mae, scored_rows
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    as_of,
                    MODEL_NAME,
                    depth,
                    learning_rate,
                    FINAL_ITERATIONS,
                    tuned,
                    train_rows,
                    test_rows,
                    _num(r2),
                    _num(mape),
                    _num(rmse),
                    _num(mae),
                    scored_rows,
                ),
            )
        conn.commit()


def write_predictions(dsn: str, out_rows: Sequence[tuple]) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE mart.ml_price_predictions;")
            for i in range(0, len(out_rows), BATCH_INSERT_SIZE):
                chunk = out_rows[i : i + BATCH_INSERT_SIZE]
                cur.executemany(INSERT_PRED_SQL, chunk)


def _metrics_from_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> tuple[float, float, float, float]:
    if y_true.shape[0] == 0:
        return float("nan"), float("nan"), float("nan"), float("nan")
    rmse = float(mean_squared_error(y_true, y_pred) ** 0.5)
    mae = float(mean_absolute_error(y_true, y_pred))
    r2 = float(r2_score(y_true, y_pred))
    mape = float(mean_absolute_percentage_error(y_true, y_pred) * 100.0)
    return rmse, mae, r2, mape


def _evaluate_test_split(
    model: CatBoostRegressor,
    X_test: np.ndarray,
    y_test: np.ndarray,
    cat_feat_idx: Sequence[int],
) -> tuple[float, float, float, float]:
    if X_test.shape[0] == 0:
        LOG.warning("Test split is empty — metrics will be NaN")
        return float("nan"), float("nan"), float("nan"), float("nan")

    test_pool = Pool(X_test, label=y_test, cat_features=cat_feat_idx)
    pred_test = model.predict(test_pool)
    return _metrics_from_predictions(y_test, pred_test)


def _log_segment_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    rare_mask: np.ndarray,
    *,
    label: str,
) -> None:
    if y_true.shape[0] == 0:
        return
    rare = rare_mask.astype(bool)
    common = ~rare
    for name, mask in (("rare_body", rare), ("common_body", common)):
        if not mask.any():
            continue
        _rmse, mae, _r2, mape = _metrics_from_predictions(y_true[mask], y_pred[mask])
        LOG.info(
            "%s segment %s — n=%s MAPE=%.2f%% MAE=%.2f",
            label,
            name,
            int(mask.sum()),
            mape,
            mae,
        )


def select_and_fit_model(
    X_fit: np.ndarray,
    y_fit: np.ndarray,
    w_fit: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    w_val: np.ndarray,
    X_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    cat_feat_idx: Sequence[int],
    *,
    fit_body_groups: np.ndarray | None = None,
    val_body_groups: np.ndarray | None = None,
    tune: bool | None = None,
) -> CatBoostRegressor:
    """
    Tune on the inner time-based val fold, then refit on the full official train window.

    The holdout test set is intentionally never passed here. With tuning disabled the
    fit uses the fixed depth and learning rate chosen on the snapshot.
    """
    if tune is None:
        tune = ml_tune_enabled()
    if not tune:
        LOG.info(
            "ML_TUNE off — fixed params depth=%s lr=%.3f iterations=%s",
            DEFAULT_DEPTH,
            DEFAULT_LEARNING_RATE,
            FINAL_ITERATIONS,
        )
        model = build_model(
            iterations=FINAL_ITERATIONS,
            depth=DEFAULT_DEPTH,
            learning_rate=DEFAULT_LEARNING_RATE,
        )
        train_pool = Pool(X_train, label=y_train, weight=w_train, cat_features=cat_feat_idx)
        model.fit(train_pool)
        return model

    fit_pool = Pool(X_fit, label=y_fit, weight=w_fit, cat_features=cat_feat_idx)
    # Val weights keep the same segment emphasis while choosing early-stop / params.
    val_pool = Pool(X_val, label=y_val, weight=w_val, cat_features=cat_feat_idx)

    best_score = float("inf")
    best_mape = float("inf")
    best_params: dict[str, float | int] = dict(PARAM_GRID[0])

    if X_val.shape[0] == 0:
        LOG.warning("Inner validation fold is empty — falling back to default params")
        model = build_model(
            iterations=FINAL_ITERATIONS,
            depth=int(best_params["depth"]),
            learning_rate=float(best_params["learning_rate"]),
        )
        train_pool = Pool(X_train, label=y_train, weight=w_train, cat_features=cat_feat_idx)
        model.fit(train_pool)
        return model

    rare_val = None
    if fit_body_groups is not None and val_body_groups is not None and len(val_body_groups):
        rare_val = rare_body_group_mask(fit_body_groups, val_body_groups)

    for params in PARAM_GRID:
        depth = int(params["depth"])
        lr = float(params["learning_rate"])
        # Score the same iteration budget as the final refit; early stopping here
        # made val ranks diverge from the full-train model quality.
        candidate = build_model(
            iterations=FINAL_ITERATIONS,
            depth=depth,
            learning_rate=lr,
        )
        candidate.fit(fit_pool)
        pred_val = candidate.predict(val_pool)
        _rmse, _mae, _r2, mape = _metrics_from_predictions(y_val, pred_val)
        rare_mape = mape
        if rare_val is not None and rare_val.any():
            rare_mape = _metrics_from_predictions(y_val[rare_val], pred_val[rare_val])[3]
        score = (1.0 - VAL_RARE_MIX) * mape + VAL_RARE_MIX * rare_mape
        used_iter = int(candidate.tree_count_)
        LOG.info(
            "Val candidate depth=%s lr=%.3f trees=%s MAPE=%.2f%% rare_MAPE=%.2f%% score=%.2f",
            depth,
            lr,
            used_iter,
            mape,
            rare_mape,
            score,
        )
        if score < best_score:
            best_score = score
            best_mape = mape
            best_params = {"depth": depth, "learning_rate": lr}

    LOG.info(
        "Selected params depth=%s lr=%.3f final_iterations=%s (val MAPE=%.2f%% score=%.2f)",
        best_params["depth"],
        best_params["learning_rate"],
        FINAL_ITERATIONS,
        best_mape,
        best_score,
    )

    # Refit on the full official train window with a fixed budget — do not carry the
    # shortened early-stopped tree count from the fit fold (it underfits full train).
    final_model = build_model(
        iterations=FINAL_ITERATIONS,
        depth=int(best_params["depth"]),
        learning_rate=float(best_params["learning_rate"]),
    )
    train_pool = Pool(X_train, label=y_train, weight=w_train, cat_features=cat_feat_idx)
    final_model.fit(train_pool)
    return final_model


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    k = pg_conn_kwargs()
    dsn = (
        f"host={k['host']} port={k['port']} user={k['user']} "
        f"password={k.get('password') or ''} dbname={k['dbname']}"
    )

    as_of = AS_OF

    with psycopg.connect(dsn) as conn:
        tbl = load_training_table(conn)

    n = len(tbl.loading_date)
    idx_all = np.arange(n, dtype=np.int64)
    train_idx = np.array([i for i in idx_all if _mask_train(tbl.loading_date[i])], dtype=np.int64)
    fit_idx = np.array([i for i in idx_all if _mask_fit(tbl.loading_date[i])], dtype=np.int64)
    val_idx = np.array([i for i in idx_all if _mask_val(tbl.loading_date[i])], dtype=np.int64)
    test_idx = np.array([i for i in idx_all if _mask_test(tbl.loading_date[i])], dtype=np.int64)

    cat_feat_idx = list(range(len(CAT_COLS)))

    X_train = tbl.feature_matrix[train_idx]
    y_train = tbl.price_rub[train_idx]
    X_fit = tbl.feature_matrix[fit_idx]
    y_fit = tbl.price_rub[fit_idx]
    X_val = tbl.feature_matrix[val_idx]
    y_val = tbl.price_rub[val_idx]
    X_test = tbl.feature_matrix[test_idx]
    y_test = tbl.price_rub[test_idx]
    if X_train.shape[0] == 0:
        raise RuntimeError("Train split is empty — check the ETL window and training dates")
    if X_fit.shape[0] == 0:
        raise RuntimeError("Fit split is empty — check VAL_WEEKS against the train window")

    # Fit-period rarity for the search stage; full-train rarity for the final refit.
    w_fit = segment_sample_weights(X_fit)
    w_val = (
        segment_sample_weights(X_val, reference_matrix=X_fit)
        if X_val.shape[0]
        else np.empty(0, dtype=np.float64)
    )
    w_train = segment_sample_weights(X_train)

    LOG.info(
        "Training CatBoost: fit=%s val=%s train=%s test=%s",
        X_fit.shape[0],
        X_val.shape[0],
        X_train.shape[0],
        X_test.shape[0],
    )
    model = select_and_fit_model(
        X_fit,
        y_fit,
        w_fit,
        X_val,
        y_val,
        w_val,
        X_train,
        y_train,
        w_train,
        cat_feat_idx,
        fit_body_groups=tbl.body_group[fit_idx],
        val_body_groups=tbl.body_group[val_idx] if len(val_idx) else None,
    )

    rmse, mae, r2, mape = _evaluate_test_split(model, X_test, y_test, cat_feat_idx)
    tuned = ml_tune_enabled()
    fitted = model.get_all_params()
    depth = int(fitted["depth"])
    learning_rate = float(fitted["learning_rate"])
    LOG.info(
        "Test metrics (price_type 3 and 9) — R2=%.4f MAPE=%.2f%% RMSE=%.2f MAE=%.2f depth=%s lr=%.3f tuned=%s",
        r2,
        mape,
        rmse,
        mae,
        depth,
        learning_rate,
        tuned,
    )

    if X_test.shape[0]:
        test_pool = Pool(X_test, cat_features=cat_feat_idx)
        pred_test = model.predict(test_pool)
        rare_mask = rare_body_group_mask(tbl.body_group[train_idx], tbl.body_group[test_idx])
        _log_segment_metrics(y_test, pred_test, rare_mask, label="Test")

    score_idx = np.array(
        [i for i in idx_all if _mask_test(tbl.loading_date[i]) or tbl.loading_date[i] > as_of],
        dtype=np.int64,
    )
    X_score = tbl.feature_matrix[score_idx]
    if X_score.shape[0] == 0:
        pred_all = np.empty(0, dtype=np.float64)
    else:
        score_pool = Pool(X_score, cat_features=cat_feat_idx)
        pred_all = model.predict(score_pool)

    out_rows: list[tuple] = []
    for row_i, yhat in zip(score_idx.tolist(), pred_all.tolist()):
        ld = tbl.loading_date[row_i]
        actual, abs_err, ape = score_actuals(ld, float(tbl.price_rub[row_i]), float(yhat), as_of)
        out_rows.append(
            (
                int(tbl.price_id[row_i]),
                ld,
                int(tbl.loading_city_gcid[row_i]),
                int(tbl.unloading_city_gcid[row_i]),
                str(tbl.body_group[row_i]),
                str(tbl.weight_category[row_i]) or None,
                int(tbl.body_group_std_id[row_i]),
                actual,
                float(yhat),
                abs_err,
                ape,
                MODEL_NAME,
            )
        )

    write_predictions(dsn, out_rows)
    write_ml_run(
        dsn,
        as_of=as_of,
        depth=depth,
        learning_rate=learning_rate,
        tuned=tuned,
        train_rows=int(X_train.shape[0]),
        test_rows=int(X_test.shape[0]),
        rmse=rmse,
        mae=mae,
        r2=r2,
        mape=mape,
        scored_rows=len(out_rows),
    )

    LOG.info("Wrote %s scoring rows to mart.ml_price_predictions", len(out_rows))


if __name__ == "__main__":
    main()
