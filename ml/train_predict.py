"""
Feature engineering, time-based train/test split, CatBoost training, batch scoring.
Uses NumPy + psycopg (no pandas) for fewer heavy native build deps.
"""

from __future__ import annotations

import calendar
import logging
import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence

import numpy as np
import psycopg
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, r2_score

from etl.config import AS_OF, ML_RANDOM_SEED, pg_conn_kwargs

LOG = logging.getLogger("ml.train_predict")

MODEL_NAME = "catboost_regressor_v1"


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


TRAIN_START, TRAIN_END_EXCL, TEST_START, TEST_END_INCL = ml_split_bounds(AS_OF)

INSERT_PRED_SQL = """
INSERT INTO mart.ml_price_predictions (
    price_id, loading_date, loading_city_gcid, unloading_city_gcid, body_group,
    actual_price_rub, predicted_price_rub, abs_error_rub, ape_pct, model_name
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def _safe_float(x: object) -> float:
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


def build_model(*, iterations: int = 400) -> CatBoostRegressor:
    return CatBoostRegressor(
        loss_function="RMSE",
        depth=8,
        learning_rate=0.08,
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
)


@dataclass
class TrainingTable:
    price_id: np.ndarray
    loading_date: list[date]
    price_rub: np.ndarray
    loading_city_gcid: np.ndarray
    unloading_city_gcid: np.ndarray
    body_group: np.ndarray
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
            string_agg(body_group_std_id::text, '|' ORDER BY body_group_std_id) AS body_group_key
        FROM core.price_body_group
        GROUP BY price_id
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
        bs.body_group_key AS body_std_raw
    FROM core.fact_prices f
    JOIN public.prices p ON p.id = f.price_id
    JOIN dedup lc ON lc.gc_id = f.loading_city_gcid
    JOIN dedup uc ON uc.gc_id = f.unloading_city_gcid
    JOIN body_std bs ON bs.price_id = f.price_id
    WHERE f.price_rub > 0
      AND f.distance > 0
      AND (
          (
              f.is_fact
              AND NOT f.complex
              AND COALESCE(f.outlier_mark, false) IS NOT TRUE
              AND p.price_type IN (1, 2, 3, 4, 5, 9)
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
        bg = _body_group(row[col_idx["body_std_raw"]])

        def nn(x: object) -> float:
            v = _safe_float(x)
            return 0.0 if math.isnan(v) else v

        cal = cyclic_date_features(ld)

        dist = _safe_float(row[col_idx["distance"]])
        tw = _safe_float(row[col_idx["total_weight"]])
        tv = _safe_float(row[col_idx["total_volume"]])
        lla = nn(row[col_idx["load_lat"]])
        llo = nn(row[col_idx["load_lon"]])
        ula = nn(row[col_idx["unload_lat"]])
        ulo = nn(row[col_idx["unload_lon"]])

        nums = np.array(
            [
                dist,
                tw,
                tv,
                lla,
                llo,
                ula,
                ulo,
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

    return TrainingTable(
        price_id=price_ids,
        loading_date=loading_dates,
        price_rub=price_rub,
        loading_city_gcid=load_gc,
        unloading_city_gcid=unload_gc,
        body_group=body_group,
        feature_matrix=X,
    )


def _mask_train(d: date) -> bool:
    return TRAIN_START <= d < TRAIN_END_EXCL


def _mask_test(d: date) -> bool:
    return TEST_START <= d <= TEST_END_INCL


def write_predictions(dsn: str, out_rows: Sequence[tuple]) -> None:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE mart.ml_price_predictions;")
            for i in range(0, len(out_rows), BATCH_INSERT_SIZE):
                chunk = out_rows[i : i + BATCH_INSERT_SIZE]
                cur.executemany(INSERT_PRED_SQL, chunk)


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
    rmse = float(mean_squared_error(y_test, pred_test) ** 0.5)
    mae = float(mean_absolute_error(y_test, pred_test))
    r2 = float(r2_score(y_test, pred_test))
    mape = float(mean_absolute_percentage_error(y_test, pred_test) * 100.0)
    return rmse, mae, r2, mape


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
    test_idx = np.array([i for i in idx_all if _mask_test(tbl.loading_date[i])], dtype=np.int64)

    cat_feat_idx = list(range(len(CAT_COLS)))

    X_train = tbl.feature_matrix[train_idx]
    y_train = tbl.price_rub[train_idx]
    X_test = tbl.feature_matrix[test_idx]
    y_test = tbl.price_rub[test_idx]
    if X_train.shape[0] == 0:
        raise RuntimeError("Train split is empty — check the ETL window and training dates")

    train_pool = Pool(X_train, label=y_train, cat_features=cat_feat_idx)

    model = build_model()
    LOG.info("Training CatBoost: train=%s test=%s", X_train.shape[0], X_test.shape[0])
    # Do not pass test as eval_set — that would leak the holdout into training dynamics.
    model.fit(train_pool)

    rmse, mae, r2, mape = _evaluate_test_split(model, X_test, y_test, cat_feat_idx)
    LOG.info("Test metrics — R2=%.4f MAPE=%.2f%% RMSE=%.2f MAE=%.2f", r2, mape, rmse, mae)

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
                actual,
                float(yhat),
                abs_err,
                ape,
                MODEL_NAME,
            )
        )

    write_predictions(dsn, out_rows)

    LOG.info("Wrote %s scoring rows to mart.ml_price_predictions", len(out_rows))


if __name__ == "__main__":
    main()
