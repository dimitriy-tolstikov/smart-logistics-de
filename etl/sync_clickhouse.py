"""
Idempotent sync of PostgreSQL mart tables into ClickHouse (bonus ★).

Load into an empty staging table, then EXCHANGE TABLES. A failed batch leaves the
published mart untouched. CLICKHOUSE_SYNC_ENABLED is honored only by the app
entrypoint; this module always syncs when invoked directly or from Airflow.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from etl.config import clickhouse_settings, pg_conn_kwargs

LOG = logging.getLogger("etl.sync_clickhouse")

CH_DDL = Path(__file__).resolve().parents[1] / "sql" / "clickhouse" / "01_mart_route_card.sql"

ROUTE_CARD_COLS = (
    "month",
    "loading_city_gcid",
    "unloading_city_gcid",
    "body_group_std_id",
    "weight_category",
    "volume_category",
    "p25_trip",
    "median_trip",
    "p75_trip",
    "avg_trip",
    "p25_km",
    "median_km",
    "p75_km",
    "trips_count",
    "carriers_count",
    "cv_rate",
    "mom_pct",
    "yoy_pct",
    "reliability_level",
    "as_of",
    "computed_at",
)

ML_PRED_COLS = (
    "price_id",
    "loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "body_group",
    "weight_category",
    "body_group_std_id",
    "actual_price_rub",
    "predicted_price_rub",
    "abs_error_rub",
    "ape_pct",
    "model_name",
    "scored_at",
)

BATCH_SIZE = 50_000


def _pg_dsn() -> str:
    k = pg_conn_kwargs()
    return (
        f"host={k['host']} port={k['port']} user={k['user']} "
        f"password={k.get('password') or ''} dbname={k['dbname']}"
    )


def _ch_client():
    import clickhouse_connect

    cfg = clickhouse_settings()
    return clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
    )


def _apply_ddl(client) -> None:
    raw = CH_DDL.read_text(encoding="utf-8")
    # Strip line comments; split on semicolons.
    statements: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    if buf:
        stmt = "\n".join(buf).strip().rstrip(";").strip()
        if stmt:
            statements.append(stmt)
    for stmt in statements:
        client.command(stmt)


def _fetch_batches(conn: psycopg.Connection, sql: str, cols: tuple[str, ...]):
    with conn.cursor(name="ch_sync_cursor") as cur:
        cur.itersize = BATCH_SIZE
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            if isinstance(rows[0], dict):
                yield [[r[c] for c in cols] for r in rows]
            else:
                yield [list(r) for r in rows]


def _sync_table(
    pg: psycopg.Connection,
    ch,
    *,
    pg_sql: str,
    ch_table: str,
    cols: tuple[str, ...],
) -> int:
    staging = f"{ch_table}__staging"
    ch.command(f"DROP TABLE IF EXISTS {staging}")
    ch.command(f"CREATE TABLE {staging} EMPTY AS {ch_table}")
    total = 0
    try:
        for batch in _fetch_batches(pg, pg_sql, cols):
            ch.insert(staging, batch, column_names=list(cols))
            total += len(batch)
            LOG.info("%s: staged %s rows (running total %s)", ch_table, len(batch), total)
        ch.command(f"EXCHANGE TABLES {ch_table} AND {staging}")
    except Exception:
        ch.command(f"DROP TABLE IF EXISTS {staging}")
        raise
    return total


def sync_marts() -> dict[str, int]:
    cfg = clickhouse_settings()
    LOG.info(
        "Sync Postgres mart -> ClickHouse %s:%s/%s",
        cfg["host"],
        cfg["port"],
        cfg["database"],
    )
    ch = _ch_client()
    _apply_ddl(ch)

    with psycopg.connect(_pg_dsn(), row_factory=dict_row) as pg:
        route_n = _sync_table(
            pg,
            ch,
            pg_sql=f"SELECT {', '.join(ROUTE_CARD_COLS)} FROM mart.route_card",
            ch_table="mart.route_card",
            cols=ROUTE_CARD_COLS,
        )
        ml_n = _sync_table(
            pg,
            ch,
            pg_sql=f"SELECT {', '.join(ML_PRED_COLS)} FROM mart.ml_price_predictions",
            ch_table="mart.ml_price_predictions",
            cols=ML_PRED_COLS,
        )

    stats = {"route_card_rows": route_n, "ml_price_predictions_rows": ml_n}
    LOG.info("ClickHouse sync complete: %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sync_marts()


if __name__ == "__main__":
    main()
