"""
ETL runner: load core.fact_prices for [start, end) and refresh affected routes.
"""

from __future__ import annotations

import json
import logging
from datetime import date

import psycopg

from etl.config import AS_OF, ETL_END, ETL_START, pg_conn_kwargs
from etl.sql_runner import read_sql

LOG = logging.getLogger("etl.run_etl")

LOAD_CORE_SQL_FILES = (
    "load_core_delete_quarantine.sql",
    "load_core_delete_fact.sql",
    "load_core_prepare_staging.sql",
    "load_core_insert_fact.sql",
    "load_core_insert_quarantine.sql",
)

REFRESH_ROUTE_CARD_SQL_FILES = (
    "refresh_route_card_prepare.sql",
    "refresh_route_card_delete.sql",
    "refresh_route_card_insert.sql",
)


def _dsn() -> str:
    k = pg_conn_kwargs()
    return (
        f"host={k['host']} port={k['port']} user={k['user']} "
        f"password={k.get('password') or ''} dbname={k['dbname']}"
    )


def run_etl_transaction(conn: psycopg.Connection, start: date, end: date, as_of: date) -> None:
    """
    Load window [start, end) into core/dq and rebuild affected routes for as_of.

    Idempotent for a fixed window: delete+reload is encapsulated in the SQL scripts
    (see load_core_delete_* / load_core_insert_*).
    """
    with conn.transaction():
        params = {"start_date": start, "end_date": end, "as_of": as_of}
        with conn.cursor() as cur:
            cur.execute(read_sql(REFRESH_ROUTE_CARD_SQL_FILES[0]), params)
            for name in LOAD_CORE_SQL_FILES:
                cur.execute(read_sql(name), params)
            for name in REFRESH_ROUTE_CARD_SQL_FILES[1:]:
                cur.execute(read_sql(name), params)


def _start_run(start: date, end: date, as_of: date) -> int:
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_meta.runs (start_date, end_date, as_of, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING id
                """,
                (start, end, as_of),
            )
            return int(cur.fetchone()[0])


def _finish_run(
    run_id: int,
    *,
    status: str,
    stats: dict | None = None,
    error_message: str | None = None,
) -> None:
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE etl_meta.runs
                SET finished_at = now(),
                    status = %s,
                    stats = %s,
                    error_message = %s
                WHERE id = %s
                """,
                (status, json.dumps(stats) if stats is not None else None, error_message, run_id),
            )


def _collect_window_stats(
    conn: psycopg.Connection,
    start: date,
    end: date,
    mart_before: int,
    routes_touched: int,
) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM public.prices WHERE loading_date >= %s AND loading_date < %s",
            (start, end),
        )
        read_rows = int(cur.fetchone()[0])
        cur.execute(
            "SELECT COUNT(*) FROM core.fact_prices WHERE loading_date >= %s AND loading_date < %s",
            (start, end),
        )
        core_rows = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT reason_code, COUNT(*)
            FROM dq.quarantine_prices q
            JOIN public.prices p ON p.id = q.price_id
            WHERE p.loading_date >= %s AND p.loading_date < %s
            GROUP BY 1
            ORDER BY 2 DESC
            """,
            (start, end),
        )
        quarantine = {r[0]: int(r[1]) for r in cur.fetchall()}
        cur.execute(
            """
            SELECT COUNT(*)
            FROM mart.route_card rc
            WHERE EXISTS (
                SELECT 1
                FROM core.fact_prices f
                WHERE f.loading_city_gcid = rc.loading_city_gcid
                  AND f.unloading_city_gcid = rc.unloading_city_gcid
                  AND f.loading_date >= %s
                  AND f.loading_date < %s
            )
            """,
            (start, end),
        )
        mart_window_rows = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM mart.route_card")
        mart_total = int(cur.fetchone()[0])

    return {
        "read_rows_in_window": read_rows,
        "core_rows_in_window": core_rows,
        "quarantine_by_reason": quarantine,
        "quarantine_rows_in_window": sum(quarantine.values()),
        "routes_touched": routes_touched,
        "mart_rows_for_touched_routes": mart_window_rows,
        "mart_route_card_rows_before": mart_before,
        "mart_route_card_rows_after": mart_total,
        "mart_route_card_delta": mart_total - mart_before,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    start: date = ETL_START
    end: date = ETL_END
    as_of: date = AS_OF

    LOG.info("ETL window: [%s, %s), as_of=%s", start, end, as_of)

    run_id = _start_run(start, end, as_of)
    try:
        with psycopg.connect(_dsn(), autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM mart.route_card")
                mart_before = int(cur.fetchone()[0])
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT loading_city_gcid, unloading_city_gcid
                        FROM core.fact_prices
                        WHERE loading_date >= %s AND loading_date < %s
                        UNION
                        SELECT loading_city_gcid, unloading_city_gcid
                        FROM public.prices
                        WHERE loading_date >= %s AND loading_date < %s
                    ) routes
                    """,
                    (start, end, start, end),
                )
                routes_touched = int(cur.fetchone()[0])

            run_etl_transaction(conn, start, end, as_of)
            stats = _collect_window_stats(conn, start, end, mart_before, routes_touched)
            conn.commit()

        _finish_run(run_id, status="success", stats=stats)
        LOG.info("ETL complete: %s", stats)
    except Exception as exc:
        _finish_run(run_id, status="failed", error_message=str(exc))
        LOG.exception("ETL failed")
        raise


if __name__ == "__main__":
    main()
