from __future__ import annotations

from datetime import date, timedelta

import psycopg
import pytest

from etl.config import AS_OF
from etl.run_etl import run_etl_transaction
from ml.train_predict import CAT_COLS, NUM_COLS, load_training_table, write_predictions
from tests.db_helpers import integration_dsn


def _insert_cities(cur: psycopg.Cursor, load_gc: int, unload_gc: int) -> None:
    cur.execute(
        """
        INSERT INTO public.cities (
            full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
        ) VALUES
            ('Regression Load, Region', 'Regression Load', %s, 'Load', 37.5, 55.5, now(), now()),
            ('Regression Unload, Region', 'Regression Unload', %s, 'Unload', 30.3, 59.9, now(), now())
        """,
        (load_gc, unload_gc),
    )


def _insert_price(
    cur: psycopg.Cursor,
    loading_date: date,
    load_gc: int,
    unload_gc: int,
    price: float = 50000.0,
    currency_code: str = "643",
    nearest_rate_date: date | None = None,
    total_weight: float = 10.0,
    total_volume: float = 50.0,
    price_type: int = 3,
    is_fact: bool = True,
    complex: bool = False,
    outlier_mark: bool = False,
    organization_id: int | None = 900001,
) -> int:
    cur.execute(
        """
        INSERT INTO public.prices (
            created_at, loading_date, loading_city_gcid, unloading_city_gcid,
            distance, total_weight, total_volume, price, currency_code,
            complex, price_type, is_fact, nearest_rate_date, outlier_mark, organization_id
        ) VALUES (
            now(), %(loading_date)s, %(load_gc)s, %(unload_gc)s,
            100.0, %(total_weight)s, %(total_volume)s, %(price)s, %(currency_code)s,
            %(complex)s, %(price_type)s, %(is_fact)s, %(nearest_rate_date)s,
            %(outlier_mark)s, %(organization_id)s
        )
        RETURNING id
        """,
        locals(),
    )
    return int(cur.fetchone()[0])


def _cleanup_prices(
    conn: psycopg.Connection,
    price_ids: list[int],
    city_ids: list[int],
    route_load_gc: int | None = None,
) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        if route_load_gc is not None:
            cur.execute("DELETE FROM mart.route_card WHERE loading_city_gcid = %s", (route_load_gc,))
        if price_ids:
            cur.execute("DELETE FROM dq.quarantine_prices WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM core.fact_prices WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM public.prices WHERE id = ANY(%s)", (price_ids,))
        if city_ids:
            cur.execute("DELETE FROM public.cities WHERE gc_id = ANY(%s)", (city_ids,))
    conn.commit()


@pytest.mark.integration
def test_route_multibody_date_dedup_and_mom_yoy(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_001, 9_992_002
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            cur.execute(
                "SELECT std_id, MIN(id) FROM public.body_types WHERE std_id IN (15, 56) GROUP BY std_id"
            )
            body_ids = {int(std_id): int(body_id) for std_id, body_id in cur.fetchall()}
            assert set(body_ids) == {15, 56}

            previous_year = _insert_price(cur, date(2025, 6, 10), load_gc, unload_gc, price=10000.0)
            previous_month = _insert_price(cur, date(2026, 5, 10), load_gc, unload_gc, price=12000.0)
            current = _insert_price(cur, date(2026, 6, 10), load_gc, unload_gc, price=15000.0)
            price_ids = [previous_year, previous_month, current]
            for price_id, price_date in (
                (previous_year, date(2025, 6, 10)),
                (previous_month, date(2026, 5, 10)),
                (current, date(2026, 6, 5)),
                (current, date(2026, 6, 20)),
            ):
                cur.execute(
                    "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                    (price_id, price_date),
                )
            for price_id in price_ids:
                for body_id in body_ids.values():
                    cur.execute(
                        "INSERT INTO public.price_body_types (price_id, body_type_id) VALUES (%s, %s)",
                        (price_id, body_id),
                    )
        conn.commit()

        run_etl_transaction(conn, date(2025, 1, 1), date(2027, 1, 1), AS_OF)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT body_group_std_id, trips_count, median_trip, avg_trip, cv_rate, mom_pct, yoy_pct
                FROM mart.route_card
                WHERE month = '2026-06-01' AND loading_city_gcid = %s AND unloading_city_gcid = %s
                ORDER BY body_group_std_id
                """,
                (load_gc, unload_gc),
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        assert {int(row[0]) for row in rows} == {15, 56}
        for row in rows:
            assert int(row[1]) == 1
            assert float(row[2]) == pytest.approx(15000.0)
            assert float(row[3]) == pytest.approx(15000.0)
            assert row[4] is None
            assert float(row[5]) == pytest.approx(25.0)
            assert float(row[6]) == pytest.approx(50.0)
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)


@pytest.mark.integration
def test_backfill_removes_rows_moved_out_of_old_window(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_011, 9_992_012
    old_date = date(2098, 6, 15)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            valid_id = _insert_price(cur, old_date, load_gc, unload_gc)
            invalid_id = _insert_price(cur, old_date, load_gc, unload_gc, price=0.0)
            price_ids = [valid_id, invalid_id]
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (valid_id, old_date),
            )
        conn.commit()
        run_etl_transaction(conn, date(2098, 6, 1), date(2098, 7, 1), date(2098, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.prices SET loading_date = '2098-07-15' WHERE id = ANY(%s)",
                (price_ids,),
            )
        conn.commit()
        run_etl_transaction(conn, date(2098, 6, 1), date(2098, 7, 1), date(2098, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM core.fact_prices WHERE price_id = ANY(%s)", (price_ids,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM dq.quarantine_prices WHERE price_id = ANY(%s)", (price_ids,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (load_gc,))
            assert cur.fetchone()[0] == 0

        run_etl_transaction(conn, date(2098, 7, 1), date(2098, 8, 1), date(2098, 12, 31))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM core.fact_prices WHERE price_id = %s", (valid_id,))
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (load_gc,))
            assert cur.fetchone()[0] == 1
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)


@pytest.mark.integration
def test_backfill_preserves_unrelated_route(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_041, 9_992_042
    other_load_gc, other_unload_gc = 9_992_043, 9_992_044
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            price_id = _insert_price(cur, date(2099, 6, 15), load_gc, unload_gc)
            price_ids = [price_id]
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (price_id, date(2099, 6, 15)),
            )
            cur.execute(
                """
                INSERT INTO mart.route_card (
                    month, loading_city_gcid, unloading_city_gcid, body_group_std_id,
                    weight_category, trips_count, reliability_level, as_of, computed_at
                ) VALUES (
                    '2099-06-01', %s, %s, 56, 'FTL', 7, 'LOW_DATA', '2099-12-31',
                    '2000-01-01 00:00:00+00'
                )
                """,
                (other_load_gc, other_unload_gc),
            )
        conn.commit()

        run_etl_transaction(conn, date(2099, 1, 1), date(2100, 1, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trips_count, computed_at
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                """,
                (other_load_gc, other_unload_gc),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 7
        assert row[1].isoformat().startswith("2000-01-01T00:00:00")
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM mart.route_card WHERE loading_city_gcid = %s AND unloading_city_gcid = %s",
                (other_load_gc, other_unload_gc),
            )
        conn.commit()


@pytest.mark.integration
def test_dq_fx_and_category_rounding(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_021, 9_992_022
    rate_date = date(2099, 6, 14)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            cur.execute(
                "INSERT INTO public.exchange_rates (num_code, value, setting_date) VALUES ('840', 91.5, %s)",
                (rate_date,),
            )
            usd_id = _insert_price(
                cur,
                date(2099, 6, 15),
                load_gc,
                unload_gc,
                price=100.0,
                currency_code="840",
                nearest_rate_date=rate_date,
                total_weight=29.96,
                total_volume=120.0,
            )
            low_rub_id = _insert_price(cur, date(2099, 6, 15), load_gc, unload_gc, price=1000.0)
            missing_fx_id = _insert_price(
                cur, date(2099, 6, 15), load_gc, unload_gc, price=100.0, currency_code="840"
            )
            rounded_id = _insert_price(
                cur,
                date(2099, 6, 15),
                load_gc,
                unload_gc,
                total_weight=2.45,
                total_volume=120.01,
            )
            price_ids = [usd_id, low_rub_id, missing_fx_id, rounded_id]
        conn.commit()
        run_etl_transaction(conn, date(2099, 1, 1), date(2100, 1, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT price_rub, weight_category, volume_category FROM core.fact_prices WHERE price_id = %s",
                (usd_id,),
            )
            usd_row = cur.fetchone()
            assert float(usd_row[0]) == pytest.approx(9150.0)
            assert usd_row[1:] == ("G20T", "FTL")
            cur.execute(
                "SELECT weight_category, volume_category FROM core.fact_prices WHERE price_id = %s",
                (rounded_id,),
            )
            assert cur.fetchone() == ("5T", "OTH")
            cur.execute(
                "SELECT price_id, reason_code FROM dq.quarantine_prices WHERE price_id = ANY(%s) ORDER BY price_id",
                (price_ids,),
            )
            reasons = {int(row[0]): row[1] for row in cur.fetchall()}
        assert reasons[low_rub_id] == "rub_price_extreme"
        assert reasons[missing_fx_id] == "fx_date_missing"
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM public.exchange_rates WHERE num_code = '840' AND setting_date = %s AND value = 91.5",
                (rate_date,),
            )
        conn.commit()


@pytest.mark.integration
def test_lag_median_uses_previous_calendar_month(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_081, 9_992_082
    may_day, june_day = date(2026, 5, 10), date(2026, 6, 10)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            may_id = _insert_price(cur, may_day, load_gc, unload_gc, price=40_000.0)
            june_id = _insert_price(cur, june_day, load_gc, unload_gc, price=80_000.0)
            price_ids = [may_id, june_id]
        conn.commit()
        run_etl_transaction(conn, date(2026, 5, 1), date(2026, 7, 1), AS_OF)
        conn.commit()

        tbl = load_training_table(conn)
        positions = {int(price_id): i for i, price_id in enumerate(tbl.price_id)}
        lag_i = len(CAT_COLS) + list(NUM_COLS).index("lag_median_trip")
        may_lag = float(tbl.feature_matrix[positions[may_id], lag_i])
        june_lag = float(tbl.feature_matrix[positions[june_id], lag_i])
        assert may_lag != may_lag  # NaN: April cell does not exist
        assert june_lag == pytest.approx(40_000.0)
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)


@pytest.mark.integration
def test_future_scoring_set_includes_type6_complex_and_outliers(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_992_031, 9_992_032
    future_date = AS_OF + timedelta(days=10)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            type6_id = _insert_price(
                cur, future_date, load_gc, unload_gc, price_type=6, is_fact=False
            )
            complex_id = _insert_price(cur, future_date, load_gc, unload_gc, complex=True)
            outlier_id = _insert_price(cur, future_date, load_gc, unload_gc, outlier_mark=True)
            price_ids = [type6_id, complex_id, outlier_id]
            cur.execute(
                "SELECT MIN(id) FROM public.body_types WHERE std_id = 56"
            )
            tent_id = int(cur.fetchone()[0])
            cur.execute(
                "SELECT MIN(id) FROM public.body_types WHERE std_id = 15"
            )
            side_id = int(cur.fetchone()[0])
            for body_id in (tent_id, side_id):
                cur.execute(
                    "INSERT INTO public.price_body_types (price_id, body_type_id) VALUES (%s, %s)",
                    (type6_id, body_id),
                )
        conn.commit()
        run_etl_transaction(conn, future_date, future_date + timedelta(days=1), AS_OF)
        conn.commit()

        tbl = load_training_table(conn)
        positions = {int(price_id): i for i, price_id in enumerate(tbl.price_id)}
        assert set(price_ids).issubset(positions)
        assert tbl.body_group[positions[type6_id]] == "15"
        assert int(tbl.body_group_std_id[positions[type6_id]]) == 15
    finally:
        _cleanup_prices(conn, price_ids, [load_gc, unload_gc], load_gc)


@pytest.mark.integration
def test_ml_write_rolls_back_truncate_on_insert_failure(conn: psycopg.Connection) -> None:
    sentinel_id = -9_000_000_001
    replacement_id = -9_000_000_002
    invalid_id = -9_000_000_003
    test_ids = [sentinel_id, replacement_id, invalid_id]

    def row(price_id: int, predicted: float | None) -> tuple:
        return (
            price_id,
            date(2026, 10, 26),
            0,
            0,
            "test",
            None,
            None,
            None,
            predicted,
            None,
            None,
            "atomicity_test",
        )

    with conn.cursor() as cur:
        cur.execute("DELETE FROM mart.ml_price_predictions WHERE price_id = ANY(%s)", (test_ids,))
        cur.execute(
            """
            INSERT INTO mart.ml_price_predictions (
                price_id, loading_date, loading_city_gcid, unloading_city_gcid,
                body_group, predicted_price_rub, model_name
            ) VALUES (%s, '2026-10-26', 0, 0, 'test', 123.0, 'atomicity_test')
            """,
            (sentinel_id,),
        )
    conn.commit()
    try:
        with pytest.raises(psycopg.errors.NotNullViolation):
            write_predictions(
                integration_dsn(), [row(replacement_id, 456.0), row(invalid_id, None)]
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT price_id, predicted_price_rub FROM mart.ml_price_predictions WHERE price_id = ANY(%s)",
                (test_ids,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert int(rows[0][0]) == sentinel_id
        assert float(rows[0][1]) == pytest.approx(123.0)
    finally:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM mart.ml_price_predictions WHERE price_id = ANY(%s)", (test_ids,))
        conn.commit()
