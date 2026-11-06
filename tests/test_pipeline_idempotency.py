"""
Integration tests: ETL idempotency (repeat load = same business state) and
city deduplication for ML feature loading (one row per gc_id, min(id) wins).

Requires PostgreSQL with DWH schemas applied (e.g. after `python -m etl.bootstrap_db`).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from etl.run_etl import run_etl_transaction
from ml.train_predict import CAT_COLS, NUM_COLS, load_training_table

# Rare gc_ids / window so we do not overlap synthetic seed data.
TEST_GC_LOAD = 9_991_001
TEST_GC_UNLOAD = 9_991_002
TEST_GC_DEDUP = 8_888_888
TEST_LOAD_DATE = date(2099, 6, 15)
TEST_MONTH = date(2099, 6, 1)  # date_trunc('month', loading_date) for June 2099
ETL_WINDOW_START = date(2099, 1, 1)
ETL_WINDOW_END = date(2100, 1, 1)
AS_OF = date(2099, 12, 31)


def _fetchone(cur, query: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
    cur.execute(query, params)
    row = cur.fetchone()
    return tuple(row) if row is not None else None


def _cleanup_test_grain(conn, price_id: int | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.cities WHERE gc_id IN (%s, %s, %s)",
            (TEST_GC_LOAD, TEST_GC_UNLOAD, TEST_GC_DEDUP),
        )
        city_ids = [r[0] for r in cur.fetchall()]
        if price_id is not None:
            cur.execute(
                """
                DELETE FROM mart.route_card
                WHERE month = %s
                  AND unloading_city_gcid = %s
                  AND loading_city_gcid IN (%s, %s)
                """,
                (TEST_MONTH, TEST_GC_UNLOAD, TEST_GC_LOAD, TEST_GC_DEDUP),
            )
            cur.execute("DELETE FROM core.fact_prices WHERE price_id = %s", (price_id,))
            cur.execute("DELETE FROM dq.quarantine_prices WHERE price_id = %s", (price_id,))
            cur.execute("DELETE FROM public.price_dates WHERE price_id = %s", (price_id,))
            cur.execute("DELETE FROM public.prices WHERE id = %s", (price_id,))
        for cid in city_ids:
            cur.execute("DELETE FROM public.cities WHERE id = %s", (cid,))
    conn.commit()


@pytest.mark.integration
def test_etl_idempotent_same_core_and_mart_state(conn) -> None:
    """Running the ETL transaction twice for the same window yields identical fact/mart payload."""
    price_id: int | None = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MIN(id), 1) FROM public.body_types")
            body_type_id = int(cur.fetchone()[0])

            cur.execute(
                """
                INSERT INTO public.cities (
                    full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
                ) VALUES
                    (%s, %s, %s, %s, %s, %s, now(), now()),
                    (%s, %s, %s, %s, %s, %s, now(), now())
                """,
                (
                    "Test City A, Region",
                    "Test City A",
                    TEST_GC_LOAD,
                    "Test A",
                    37.5,
                    55.5,
                    "Test City B, Region",
                    "Test City B",
                    TEST_GC_UNLOAD,
                    "Test B",
                    30.3,
                    59.9,
                ),
            )
            cur.execute(
                """
                INSERT INTO public.prices (
                    created_at, loading_date, loading_city_gcid, unloading_city_gcid,
                    distance, total_weight, total_volume, body_type, price, currency_code,
                    complex, price_type, is_fact, nearest_rate_date,
                    outlier_mark, weight_category, volume_category
                )
                VALUES (
                    now(), %s, %s, %s,
                    100.0, 10.0, 50.0, %s, 50000.0, '643',
                    false, 2, true, NULL,
                    false, 'FTL', 'FTL'
                )
                RETURNING id
                """,
                (TEST_LOAD_DATE, TEST_GC_LOAD, TEST_GC_UNLOAD, body_type_id),
            )
            price_id = int(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (price_id, TEST_LOAD_DATE),
            )
        conn.commit()

        run_etl_transaction(conn, ETL_WINDOW_START, ETL_WINDOW_END, AS_OF)
        conn.commit()

        with conn.cursor() as cur:
            core_1 = _fetchone(
                cur,
                """
                SELECT price_id, price_rub, distance, total_weight, loading_city_gcid,
                       unloading_city_gcid, weight_category, volume_category, source_row_hash
                FROM core.fact_prices WHERE price_id = %s
                """,
                (price_id,),
            )
            assert core_1 is not None
            cur.execute(
                """
                SELECT month, loading_city_gcid, unloading_city_gcid, body_group_std_id,
                       weight_category, volume_category, trips_count, median_trip, avg_trip, as_of
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                ORDER BY month, body_group_std_id
                """,
                (TEST_GC_LOAD, TEST_GC_UNLOAD),
            )
            mart_1 = [tuple(r) for r in cur.fetchall()]

        run_etl_transaction(conn, ETL_WINDOW_START, ETL_WINDOW_END, AS_OF)
        conn.commit()

        with conn.cursor() as cur:
            core_2 = _fetchone(
                cur,
                """
                SELECT price_id, price_rub, distance, total_weight, loading_city_gcid,
                       unloading_city_gcid, weight_category, volume_category, source_row_hash
                FROM core.fact_prices WHERE price_id = %s
                """,
                (price_id,),
            )
            cur.execute(
                """
                SELECT month, loading_city_gcid, unloading_city_gcid, body_group_std_id,
                       weight_category, volume_category, trips_count, median_trip, avg_trip, as_of
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                ORDER BY month, body_group_std_id
                """,
                (TEST_GC_LOAD, TEST_GC_UNLOAD),
            )
            mart_2 = [tuple(r) for r in cur.fetchall()]

        assert core_1 == core_2
        assert mart_1 == mart_2
        assert len(mart_1) >= 1
    finally:
        _cleanup_test_grain(conn, price_id)


@pytest.mark.integration
def test_city_dedup_for_ml_features_matches_smallest_city_id(conn) -> None:
    """
    load_training_table uses DISTINCT ON (gc_id) ... ORDER BY gc_id, id ASC on public.cities.
    Coordinates joined to loading_city_gcid must match the row with the smallest id for that gc_id.
    """
    price_id: int | None = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MIN(id), 1) FROM public.body_types")
            body_type_id = int(cur.fetchone()[0])

            cur.execute(
                """
                INSERT INTO public.cities (
                    full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
                ) VALUES
                    (%s, %s, %s, %s, %s, %s, now(), now()),
                    (%s, %s, %s, %s, %s, %s, now(), now()),
                    (%s, %s, %s, %s, %s, %s, now(), now())
                RETURNING id, gc_id, latitude, full_name_local
                """,
                (
                    "Dedup First, Reg Alpha",
                    "Dedup First",
                    TEST_GC_DEDUP,
                    "Dedup",
                    11.11,
                    22.22,
                    "Dedup Second Different Name, Reg",
                    "Dedup Second",
                    TEST_GC_DEDUP,
                    "Dedup2",
                    99.99,
                    88.88,
                    "Unload Solo, Reg",
                    "Unload Solo",
                    TEST_GC_UNLOAD,
                    "Unload",
                    30.0,
                    60.0,
                ),
            )
            rows = cur.fetchall()
            first_id = int(rows[0][0])
            first_lat = float(rows[0][2])

            cur.execute(
                """
                INSERT INTO public.prices (
                    created_at, loading_date, loading_city_gcid, unloading_city_gcid,
                    distance, total_weight, total_volume, body_type, price, currency_code,
                    complex, price_type, is_fact, nearest_rate_date,
                    outlier_mark, weight_category, volume_category
                )
                VALUES (
                    now(), %s, %s, %s,
                    150.0, 12.0, 40.0, %s, 60000.0, '643',
                    false, 2, true, NULL,
                    false, 'FTL', 'FTL'
                )
                RETURNING id
                """,
                (TEST_LOAD_DATE, TEST_GC_DEDUP, TEST_GC_UNLOAD, body_type_id),
            )
            price_id = int(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (price_id, TEST_LOAD_DATE),
            )
        conn.commit()

        run_etl_transaction(conn, ETL_WINDOW_START, ETL_WINDOW_END, AS_OF)
        conn.commit()

        tbl = load_training_table(conn)
        matches = [i for i, pid in enumerate(tbl.price_id) if int(pid) == price_id]
        assert len(matches) == 1, "price_id must appear exactly once in the ML training table"
        i = matches[0]

        load_lat_i = len(CAT_COLS) + list(NUM_COLS).index("load_lat")
        load_lon_i = len(CAT_COLS) + list(NUM_COLS).index("load_lon")
        region_i = list(CAT_COLS).index("loading_region")

        assert abs(float(tbl.feature_matrix[i, load_lat_i]) - first_lat) < 1e-9
        assert abs(float(tbl.feature_matrix[i, load_lon_i]) - 11.11) < 1e-9
        assert str(tbl.feature_matrix[i, region_i]) == "Reg Alpha"

        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM public.cities WHERE gc_id = %s ORDER BY id",
                (TEST_GC_DEDUP,),
            )
            ids = [r[0] for r in cur.fetchall()]
            assert int(ids[0]) == first_id
    finally:
        _cleanup_test_grain(conn, price_id)
