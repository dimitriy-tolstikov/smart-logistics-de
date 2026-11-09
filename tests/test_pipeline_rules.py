"""Integration coverage for DQ edges, mart filters/metrics, and route backfill."""

from __future__ import annotations

from datetime import date, timedelta

import psycopg
import pytest

from etl.run_etl import run_etl_transaction


def _insert_cities(cur: psycopg.Cursor, load_gc: int, unload_gc: int) -> None:
    cur.execute(
        """
        INSERT INTO public.cities (
            full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
        ) VALUES
            ('Rules Load, Region', 'Rules Load', %s, 'Load', 37.5, 55.5, now(), now()),
            ('Rules Unload, Region', 'Rules Unload', %s, 'Unload', 30.3, 59.9, now(), now())
        """,
        (load_gc, unload_gc),
    )


def _insert_price(
    cur: psycopg.Cursor,
    loading_date: date,
    load_gc: int,
    unload_gc: int,
    *,
    price: float = 50000.0,
    currency_code: str = "643",
    nearest_rate_date: date | None = None,
    distance: float = 100.0,
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
            %(distance)s, %(total_weight)s, %(total_volume)s, %(price)s, %(currency_code)s,
            %(complex)s, %(price_type)s, %(is_fact)s, %(nearest_rate_date)s,
            %(outlier_mark)s, %(organization_id)s
        )
        RETURNING id
        """,
        locals(),
    )
    return int(cur.fetchone()[0])


def _cleanup(
    conn: psycopg.Connection,
    price_ids: list[int],
    city_gcs: list[int],
    *,
    rate_dates: list[tuple[str, date]] | None = None,
) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        if price_ids:
            cur.execute("DELETE FROM dq.quarantine_prices WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM core.fact_prices WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM public.price_dates WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM public.price_body_types WHERE price_id = ANY(%s)", (price_ids,))
            cur.execute("DELETE FROM public.prices WHERE id = ANY(%s)", (price_ids,))
        for gc in city_gcs:
            cur.execute("DELETE FROM mart.route_card WHERE loading_city_gcid = %s OR unloading_city_gcid = %s", (gc, gc))
            cur.execute("DELETE FROM public.cities WHERE gc_id = %s", (gc,))
        for num_code, setting_date in rate_dates or []:
            cur.execute(
                "DELETE FROM public.exchange_rates WHERE num_code = %s AND setting_date = %s",
                (num_code, setting_date),
            )
    conn.commit()


@pytest.mark.integration
def test_dq_boundaries_and_currency_rules(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_993_001, 9_993_002
    unknown_gc = 9_993_999
    rate_date = date(2099, 3, 10)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            cur.execute(
                "INSERT INTO public.exchange_rates (num_code, value, setting_date) VALUES ('978', 100.0, %s)",
                (rate_date,),
            )
            cases = {
                "neg_weight": _insert_price(
                    cur, date(2099, 3, 15), load_gc, unload_gc, total_weight=-1.0
                ),
                "neg_volume": _insert_price(
                    cur, date(2099, 3, 15), load_gc, unload_gc, total_volume=-1.0
                ),
                "zero_distance": _insert_price(
                    cur, date(2099, 3, 15), load_gc, unload_gc, distance=0.0
                ),
                "unknown_city": _insert_price(
                    cur, date(2099, 3, 15), unknown_gc, unload_gc
                ),
                "eur_ok": _insert_price(
                    cur,
                    date(2099, 3, 15),
                    load_gc,
                    unload_gc,
                    price=10.0,
                    currency_code="978",
                    nearest_rate_date=rate_date,
                ),
                "eur_missing_rate": _insert_price(
                    cur,
                    date(2099, 3, 15),
                    load_gc,
                    unload_gc,
                    price=10.0,
                    currency_code="978",
                    nearest_rate_date=date(2099, 3, 1),
                ),
                "unsupported_fx": _insert_price(
                    cur,
                    date(2099, 3, 15),
                    load_gc,
                    unload_gc,
                    price=10.0,
                    currency_code="392",
                ),
            }
            price_ids = list(cases.values())
        conn.commit()
        run_etl_transaction(conn, date(2099, 3, 1), date(2099, 4, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT price_id, reason_code FROM dq.quarantine_prices WHERE price_id = ANY(%s)",
                (price_ids,),
            )
            reasons = {int(pid): code for pid, code in cur.fetchall()}
            cur.execute(
                "SELECT price_rub FROM core.fact_prices WHERE price_id = %s",
                (cases["eur_ok"],),
            )
            eur_row = cur.fetchone()

        assert reasons[cases["neg_weight"]] == "weight_negative"
        assert reasons[cases["neg_volume"]] == "volume_negative"
        assert reasons[cases["zero_distance"]] == "distance_non_positive"
        assert reasons[cases["unknown_city"]] == "city_unknown"
        assert reasons[cases["eur_missing_rate"]] == "fx_rate_missing"
        assert reasons[cases["unsupported_fx"]] == "currency_unsupported"
        assert cases["eur_ok"] not in reasons
        assert eur_row is not None
        assert float(eur_row[0]) == pytest.approx(1000.0)
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc], rate_dates=[("978", rate_date)])


@pytest.mark.integration
def test_route_card_filters_outliers_complex_nonfact_and_future_dates(
    conn: psycopg.Connection,
) -> None:
    load_gc, unload_gc = 9_993_011, 9_993_012
    as_of = date(2099, 6, 30)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            ok_id = _insert_price(cur, date(2099, 6, 10), load_gc, unload_gc, price=20000.0)
            outlier_id = _insert_price(
                cur, date(2099, 6, 10), load_gc, unload_gc, price=21000.0, outlier_mark=True
            )
            complex_id = _insert_price(
                cur, date(2099, 6, 10), load_gc, unload_gc, price=22000.0, complex=True
            )
            nonfact_id = _insert_price(
                cur,
                date(2099, 6, 10),
                load_gc,
                unload_gc,
                price=23000.0,
                price_type=6,
                is_fact=False,
            )
            future_id = _insert_price(cur, date(2099, 7, 10), load_gc, unload_gc, price=24000.0)
            price_ids = [ok_id, outlier_id, complex_id, nonfact_id, future_id]
            for pid, pd in (
                (ok_id, date(2099, 6, 10)),
                (outlier_id, date(2099, 6, 10)),
                (complex_id, date(2099, 6, 10)),
                (nonfact_id, date(2099, 6, 10)),
                (future_id, date(2099, 7, 10)),
            ):
                cur.execute(
                    "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                    (pid, pd),
                )
        conn.commit()
        run_etl_transaction(conn, date(2099, 6, 1), date(2099, 8, 1), as_of)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trips_count, median_trip
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                """,
                (load_gc, unload_gc),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert int(rows[0][0]) == 1
        assert float(rows[0][1]) == pytest.approx(20000.0)
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc])


@pytest.mark.integration
def test_route_card_percentiles_carriers_reliability_and_null_mom(
    conn: psycopg.Connection,
) -> None:
    load_gc, unload_gc = 9_993_021, 9_993_022
    as_of = date(2099, 8, 31)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            # Three factual carrier deals in Aug → p25/p50/p75 and carriers_count
            for price, org, ptype in (
                (10000.0, 1, 3),
                (20000.0, 2, 3),
                (30000.0, 3, 1),  # type 1 must not increase carriers_count
            ):
                pid = _insert_price(
                    cur,
                    date(2099, 8, 10),
                    load_gc,
                    unload_gc,
                    price=price,
                    price_type=ptype,
                    organization_id=org,
                    distance=100.0,
                )
                price_ids.append(pid)
                cur.execute(
                    "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                    (pid, date(2099, 8, 10)),
                )
            # Previous month with zero median edge case is skipped → MoM NULL
        conn.commit()
        run_etl_transaction(conn, date(2099, 8, 1), date(2099, 9, 1), as_of)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p25_trip, median_trip, p75_trip, median_km, trips_count,
                       carriers_count, mom_pct, yoy_pct, reliability_level
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s AND month = '2099-08-01'
                """,
                (load_gc, unload_gc),
            )
            row = cur.fetchone()
        assert row is not None
        # percentile_cont over {10000,20000,30000}: p25=15000, p50=20000, p75=25000
        assert float(row[0]) == pytest.approx(15000.0)
        assert float(row[1]) == pytest.approx(20000.0)
        assert float(row[2]) == pytest.approx(25000.0)
        assert float(row[3]) == pytest.approx(200.0)  # median of price/distance
        assert int(row[4]) == 3
        assert int(row[5]) == 2  # only price_type 3 orgs
        assert row[6] is None
        assert row[7] is None
        assert row[8] == "LOW_DATA"
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc])


@pytest.mark.integration
def test_reliability_high_requires_freshness_and_thresholds(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_993_031, 9_993_032
    as_of = date(2099, 9, 30)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            for i in range(30):
                pid = _insert_price(
                    cur,
                    date(2099, 9, 15),
                    load_gc,
                    unload_gc,
                    price=15000.0 + i,
                    organization_id=1000 + (i % 5),
                    price_type=3,
                )
                price_ids.append(pid)
                cur.execute(
                    "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                    (pid, date(2099, 9, 15)),
                )
        conn.commit()
        run_etl_transaction(conn, date(2099, 9, 1), date(2099, 10, 1), as_of)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trips_count, carriers_count, reliability_level
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                """,
                (load_gc, unload_gc),
            )
            row = cur.fetchone()
        assert row is not None
        assert int(row[0]) == 30
        assert int(row[1]) == 5
        assert row[2] == "HIGH"

        # Stale as_of (>30 days) must drop HIGH → MEDIUM
        run_etl_transaction(conn, date(2099, 9, 1), date(2099, 10, 1), as_of + timedelta(days=40))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reliability_level
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                """,
                (load_gc, unload_gc),
            )
            assert cur.fetchone()[0] == "MEDIUM"
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc])


@pytest.mark.integration
def test_backfill_handles_route_city_change(conn: psycopg.Connection) -> None:
    old_load, unload_gc, new_load = 9_993_041, 9_993_042, 9_993_043
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, old_load, unload_gc)
            cur.execute(
                """
                INSERT INTO public.cities (
                    full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
                ) VALUES ('New Load, Region', 'New Load', %s, 'New', 40.0, 50.0, now(), now())
                """,
                (new_load,),
            )
            pid = _insert_price(cur, date(2099, 5, 10), old_load, unload_gc)
            price_ids = [pid]
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (pid, date(2099, 5, 10)),
            )
        conn.commit()
        run_etl_transaction(conn, date(2099, 5, 1), date(2099, 6, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.prices SET loading_city_gcid = %s WHERE id = %s",
                (new_load, pid),
            )
        conn.commit()
        run_etl_transaction(conn, date(2099, 5, 1), date(2099, 6, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT loading_city_gcid FROM core.fact_prices WHERE price_id = %s", (pid,))
            assert int(cur.fetchone()[0]) == new_load
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (old_load,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (new_load,))
            assert cur.fetchone()[0] == 1
    finally:
        _cleanup(conn, price_ids, [old_load, unload_gc, new_load])


@pytest.mark.integration
def test_backfill_refreshes_old_route_when_date_moves_into_window(conn: psycopg.Connection) -> None:
    """Stored date outside the window must still refresh that city pair."""
    old_load, unload_gc, new_load = 9_993_061, 9_993_062, 9_993_063
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, old_load, unload_gc)
            cur.execute(
                """
                INSERT INTO public.cities (
                    full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at
                ) VALUES ('Moved Load, Region', 'Moved Load', %s, 'Moved', 41.0, 51.0, now(), now())
                """,
                (new_load,),
            )
            pid = _insert_price(cur, date(2099, 4, 15), old_load, unload_gc)
            price_ids = [pid]
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (pid, date(2099, 4, 15)),
            )
        conn.commit()
        run_etl_transaction(conn, date(2099, 4, 1), date(2099, 5, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.prices
                SET loading_city_gcid = %s, loading_date = %s
                WHERE id = %s
                """,
                (new_load, date(2099, 5, 10), pid),
            )
            cur.execute(
                "UPDATE public.price_dates SET price_date = %s WHERE price_id = %s",
                (date(2099, 5, 10), pid),
            )
        conn.commit()
        run_etl_transaction(conn, date(2099, 5, 1), date(2099, 6, 1), date(2099, 12, 31))
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT loading_city_gcid, loading_date FROM core.fact_prices WHERE price_id = %s", (pid,))
            city, loaded = cur.fetchone()
            assert int(city) == new_load
            assert loaded == date(2099, 5, 10)
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (old_load,))
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM mart.route_card WHERE loading_city_gcid = %s", (new_load,))
            assert cur.fetchone()[0] == 1
    finally:
        _cleanup(conn, price_ids, [old_load, unload_gc, new_load])


@pytest.mark.integration
def test_weight_volume_boundaries_and_negative_distance(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_993_051, 9_993_052
    price_ids: list[int] = []
    day = date(2099, 4, 10)
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            specs = [
                ("w30", 30.0, 120.0, 50000.0, 100.0),
                ("w10", 10.0, 0.1, 50000.0, 100.0),
                ("w5", 5.0, 50.0, 50000.0, 100.0),
                ("w2_5", 2.5, 50.0, 50000.0, 100.0),
                ("w0_5", 0.5, 0.0, 50000.0, 100.0),
                ("w0_44", 0.44, 120.01, 50000.0, 100.0),
                ("w9_94", 9.94, 50.0, 50000.0, 100.0),
            ]
            created: dict[str, int] = {}
            for name, weight, volume, price, distance in specs:
                created[name] = _insert_price(
                    cur,
                    day,
                    load_gc,
                    unload_gc,
                    price=price,
                    total_weight=weight,
                    total_volume=volume,
                    distance=distance,
                )
            created["neg_distance"] = _insert_price(
                cur, day, load_gc, unload_gc, distance=-5.0
            )
            price_ids = list(created.values())
        conn.commit()
        counts = run_etl_transaction(conn, date(2099, 4, 1), date(2099, 5, 1), date(2099, 12, 31))
        conn.commit()
        assert counts["load_core_insert_fact"] >= 7
        assert counts["load_core_insert_quarantine"] >= 1

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT price_id, weight_category, volume_category
                FROM core.fact_prices
                WHERE price_id = ANY(%s)
                """,
                (price_ids,),
            )
            cats = {int(pid): (wc, vc) for pid, wc, vc in cur.fetchall()}
            cur.execute(
                "SELECT reason_code FROM dq.quarantine_prices WHERE price_id = %s",
                (created["neg_distance"],),
            )
            reason = cur.fetchone()
            cur.execute(
                "SELECT mart.change_pct(15, NULL), mart.change_pct(15, 0), mart.change_pct(15, 12)"
            )
            mom = cur.fetchone()

        assert cats[created["w30"]] == ("G20T", "FTL")
        assert cats[created["w10"]] == ("FTL", "FTL")
        assert cats[created["w5"]] == ("10T", "FTL")
        assert cats[created["w2_5"]] == ("5T", "FTL")
        assert cats[created["w0_5"]] == ("L5T", "OTH")
        assert cats[created["w0_44"]] == ("OTH", "OTH")
        assert cats[created["w9_94"]] == ("10T", "FTL")
        assert created["neg_distance"] not in cats
        assert reason[0] == "distance_non_positive"
        assert mom[0] is None and mom[1] is None
        assert float(mom[2]) == pytest.approx(25.0)
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc])


@pytest.mark.integration
def test_carriers_type_9_and_medium_reliability(conn: psycopg.Connection) -> None:
    load_gc, unload_gc = 9_993_061, 9_993_062
    as_of = date(2099, 10, 20)
    price_ids: list[int] = []
    try:
        with conn.cursor() as cur:
            _insert_cities(cur, load_gc, unload_gc)
            for i in range(10):
                price_type = 9 if i == 0 else 3
                pid = _insert_price(
                    cur,
                    date(2099, 10, 10),
                    load_gc,
                    unload_gc,
                    price=20000.0 + i,
                    price_type=price_type,
                    organization_id=5000 + i,
                )
                price_ids.append(pid)
                cur.execute(
                    "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                    (pid, date(2099, 10, 10)),
                )
            # Type 2 with its own org must not be counted as a carrier.
            extra = _insert_price(
                cur,
                date(2099, 10, 10),
                load_gc,
                unload_gc,
                price=26000.0,
                price_type=2,
                organization_id=5999,
            )
            price_ids.append(extra)
            cur.execute(
                "INSERT INTO public.price_dates (price_id, price_date) VALUES (%s, %s)",
                (extra, date(2099, 10, 10)),
            )
        conn.commit()
        run_etl_transaction(conn, date(2099, 10, 1), date(2099, 11, 1), as_of)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trips_count, carriers_count, reliability_level
                FROM mart.route_card
                WHERE loading_city_gcid = %s AND unloading_city_gcid = %s
                """,
                (load_gc, unload_gc),
            )
            row = cur.fetchone()
        assert int(row[0]) == 11
        assert int(row[1]) == 10  # types 3 and 9 only; the type-2 org is excluded
        assert row[2] == "MEDIUM"
    finally:
        _cleanup(conn, price_ids, [load_gc, unload_gc])
