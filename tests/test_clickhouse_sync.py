"""Live ClickHouse sync. Runs in CI, or locally when CLICKHOUSE_TEST=1."""

from __future__ import annotations

import os
from datetime import date

import psycopg
import pytest

from etl.sync_clickhouse import (
    ML_PRED_COLS,
    ROUTE_CARD_COLS,
    _apply_ddl,
    _ch_client,
    _sync_table,
)


def _client_or_skip():
    enabled = os.getenv("CI") == "1" or os.getenv("CLICKHOUSE_TEST", "").lower() in {"1", "true", "yes"}
    if not enabled:
        pytest.skip("live ClickHouse sync runs in CI or with CLICKHOUSE_TEST=1")
    try:
        client = _ch_client()
        client.command("SELECT 1")
        return client
    except Exception as exc:  # noqa: BLE001 — connection errors skip locally and fail in CI
        if os.getenv("CI") == "1":
            pytest.fail(f"ClickHouse required in CI: {exc}")
        pytest.skip(f"ClickHouse not available: {exc}")


@pytest.mark.integration
def test_clickhouse_exchange_sync_matches_postgres(conn: psycopg.Connection) -> None:
    client = _client_or_skip()
    load_gc, unload_gc, price_id = 9_994_001, 9_994_002, 9_994_003
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mart.route_card (
                    month, loading_city_gcid, unloading_city_gcid, body_group_std_id,
                    weight_category, trips_count, reliability_level, as_of
                ) VALUES (%s, %s, %s, 56, 'FTL', 1, 'LOW_DATA', %s)
                """,
                (date(2026, 10, 1), load_gc, unload_gc, date(2026, 10, 26)),
            )
            cur.execute(
                """
                INSERT INTO mart.ml_price_predictions (
                    price_id, loading_date, loading_city_gcid, unloading_city_gcid,
                    body_group, weight_category, body_group_std_id,
                    predicted_price_rub, model_name
                ) VALUES (%s, %s, %s, %s, '56', 'FTL', 56, 1000, 'sync_test')
                """,
                (price_id, date(2026, 10, 26), load_gc, unload_gc),
            )
        _apply_ddl(client)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mart.route_card")
            pg_routes = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM mart.ml_price_predictions")
            pg_preds = int(cur.fetchone()[0])
        route_n = _sync_table(
            conn,
            client,
            pg_sql=f"SELECT {', '.join(ROUTE_CARD_COLS)} FROM mart.route_card",
            ch_table="mart.route_card",
            cols=ROUTE_CARD_COLS,
        )
        pred_n = _sync_table(
            conn,
            client,
            pg_sql=f"SELECT {', '.join(ML_PRED_COLS)} FROM mart.ml_price_predictions",
            ch_table="mart.ml_price_predictions",
            cols=ML_PRED_COLS,
        )
        ch_routes = int(client.query("SELECT count() FROM mart.route_card").result_rows[0][0])
        ch_preds = int(client.query("SELECT count() FROM mart.ml_price_predictions").result_rows[0][0])
        assert route_n == pg_routes == ch_routes
        assert pred_n == pg_preds == ch_preds
        assert ch_routes >= 1
    finally:
        conn.rollback()
