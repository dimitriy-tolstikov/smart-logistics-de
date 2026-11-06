"""
Idempotent seed: Postgres + ClickHouse databases, datasets, demo charts/dashboard.
Runs inside the Superset container after `superset init`.
"""

from __future__ import annotations

import os
import textwrap

from superset.app import create_app


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def main() -> None:
    app = create_app()
    with app.app_context():
        from flask_appbuilder.security.sqla.models import User
        from superset import db, security_manager
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin = (
            db.session.query(User)
            .filter_by(username=_env("SUPERSET_ADMIN_USERNAME", "admin"))
            .one_or_none()
        )
        if admin is None:
            raise RuntimeError("Superset admin user not found; run fab create-admin first")

        pg_uri = _env(
            "SUPERSET_POSTGRES_URI",
            "postgresql+psycopg2://postgres:postgres@db:5432/deals_bigdata",
        )
        ch_uri = _env(
            "SUPERSET_CLICKHOUSE_URI",
            "clickhousedb://default:clickhouse@clickhouse:8123/default",
        )

        def upsert_database(name: str, uri: str, extra: str = "{}") -> Database:
            existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
            if existing is None:
                existing = Database(database_name=name)
                db.session.add(existing)
            existing.set_sqlalchemy_uri(uri)
            existing.extra = extra
            existing.expose_in_sqllab = True
            db.session.commit()
            return existing

        pg_db = upsert_database("PostgreSQL DWH", pg_uri)
        ch_db = upsert_database("ClickHouse Mart", ch_uri)

        def upsert_dataset(database: Database, schema: str, table_name: str) -> SqlaTable:
            existing = (
                db.session.query(SqlaTable)
                .filter_by(database_id=database.id, schema=schema, table_name=table_name)
                .one_or_none()
            )
            if existing is None:
                # Some Superset versions uniquely index table_name alone; also match by name.
                existing = (
                    db.session.query(SqlaTable).filter_by(table_name=table_name, schema=schema).one_or_none()
                )
            if existing is None:
                existing = SqlaTable(
                    table_name=table_name,
                    schema=schema,
                    database=database,
                    owners=[admin],
                )
                db.session.add(existing)
                db.session.commit()
            else:
                existing.database = database
                existing.schema = schema
                db.session.commit()
            try:
                existing.fetch_metadata()
                db.session.commit()
            except Exception as exc:  # noqa: BLE001 — seed must continue if CH is empty
                print(f"  warn: fetch_metadata {schema}.{table_name}: {exc}")
                db.session.rollback()
            return (
                db.session.query(SqlaTable)
                .filter_by(database_id=database.id, schema=schema, table_name=table_name)
                .one()
            )

        route_pg = upsert_dataset(pg_db, "mart", "route_card")
        pred_pg = upsert_dataset(pg_db, "mart", "ml_price_predictions")

        # Virtual dataset name avoids UNIQUE(table_name) clash with Postgres route_card.
        try:
            ch_name = "route_card_clickhouse"
            existing_ch = db.session.query(SqlaTable).filter_by(table_name=ch_name).one_or_none()
            if existing_ch is None:
                existing_ch = SqlaTable(
                    table_name=ch_name,
                    schema="mart",
                    database=ch_db,
                    sql="SELECT * FROM mart.route_card FINAL",
                    owners=[admin],
                )
                db.session.add(existing_ch)
            else:
                existing_ch.database = ch_db
                existing_ch.sql = "SELECT * FROM mart.route_card FINAL"
            db.session.commit()
            try:
                existing_ch.fetch_metadata()
                db.session.commit()
            except Exception as meta_exc:  # noqa: BLE001
                print(f"  warn: ClickHouse fetch_metadata: {meta_exc}")
                db.session.rollback()
        except Exception as exc:  # noqa: BLE001
            print(f"  warn: ClickHouse dataset: {exc}")
            db.session.rollback()

        def upsert_chart(
            *,
            slice_name: str,
            datasource: SqlaTable,
            viz_type: str,
            params: str,
        ) -> Slice:
            existing = db.session.query(Slice).filter_by(slice_name=slice_name).one_or_none()
            if existing is None:
                existing = Slice(slice_name=slice_name, owners=[admin])
                db.session.add(existing)
            existing.datasource_type = "table"
            existing.datasource_id = datasource.id
            existing.viz_type = viz_type
            existing.params = params
            db.session.commit()
            return existing

        corridor = upsert_chart(
            slice_name="Route corridor P25/P50/P75",
            datasource=route_pg,
            viz_type="table",
            params=textwrap.dedent(
                """\
                {
                  "adhoc_filters": [],
                  "all_columns": [
                    "month", "loading_city_gcid", "unloading_city_gcid",
                    "body_group_std_id", "weight_category",
                    "p25_trip", "median_trip", "p75_trip",
                    "trips_count", "carriers_count", "reliability_level"
                  ],
                  "row_limit": 100,
                  "order_desc": true,
                  "order_by_cols": ["[\\"month\\", false]"]
                }
                """
            ).strip(),
        )

        reliability = upsert_chart(
            slice_name="Trips by reliability",
            datasource=route_pg,
            viz_type="pie",
            params=textwrap.dedent(
                """\
                {
                  "metric": {
                    "aggregate": "SUM",
                    "column": {"column_name": "trips_count", "type": "BIGINT"},
                    "expressionType": "SIMPLE",
                    "label": "SUM(trips_count)"
                  },
                  "groupby": ["reliability_level"],
                  "row_limit": 10
                }
                """
            ).strip(),
        )

        fact_vs_pred = upsert_chart(
            slice_name="Fact vs forecast sample",
            datasource=pred_pg,
            viz_type="table",
            params=textwrap.dedent(
                """\
                {
                  "adhoc_filters": [],
                  "all_columns": [
                    "loading_date", "loading_city_gcid", "unloading_city_gcid",
                    "body_group", "actual_price_rub", "predicted_price_rub",
                    "ape_pct", "model_name"
                  ],
                  "row_limit": 100,
                  "order_desc": true,
                  "order_by_cols": ["[\\"loading_date\\", false]"]
                }
                """
            ).strip(),
        )

        dash_title = "Smart Logistics — Market Corridor"
        dash = db.session.query(Dashboard).filter_by(dashboard_title=dash_title).one_or_none()
        if dash is None:
            dash = Dashboard(dashboard_title=dash_title, owners=[admin], published=True)
            db.session.add(dash)
        dash.slug = "smart-logistics-corridor"
        dash.slices = [corridor, reliability, fact_vs_pred]
        # Simple vertical layout positions (Superset 4 position JSON).
        dash.position_json = textwrap.dedent(
            f"""\
            {{
              "DASHBOARD_VERSION_KEY": "v2",
              "ROOT_ID": {{"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"}},
              "GRID_ID": {{
                "children": ["ROW-1", "ROW-2", "ROW-3"],
                "id": "GRID_ID",
                "type": "GRID"
              }},
              "ROW-1": {{
                "children": ["CHART-{corridor.id}"],
                "id": "ROW-1",
                "type": "ROW",
                "meta": {{"background": "BACKGROUND_TRANSPARENT"}}
              }},
              "ROW-2": {{
                "children": ["CHART-{reliability.id}"],
                "id": "ROW-2",
                "type": "ROW",
                "meta": {{"background": "BACKGROUND_TRANSPARENT"}}
              }},
              "ROW-3": {{
                "children": ["CHART-{fact_vs_pred.id}"],
                "id": "ROW-3",
                "type": "ROW",
                "meta": {{"background": "BACKGROUND_TRANSPARENT"}}
              }},
              "CHART-{corridor.id}": {{
                "children": [],
                "id": "CHART-{corridor.id}",
                "meta": {{
                  "chartId": {corridor.id},
                  "height": 50,
                  "sliceName": "{corridor.slice_name}",
                  "uuid": "corridor-chart",
                  "width": 12
                }},
                "type": "CHART"
              }},
              "CHART-{reliability.id}": {{
                "children": [],
                "id": "CHART-{reliability.id}",
                "meta": {{
                  "chartId": {reliability.id},
                  "height": 50,
                  "sliceName": "{reliability.slice_name}",
                  "uuid": "reliability-chart",
                  "width": 12
                }},
                "type": "CHART"
              }},
              "CHART-{fact_vs_pred.id}": {{
                "children": [],
                "id": "CHART-{fact_vs_pred.id}",
                "meta": {{
                  "chartId": {fact_vs_pred.id},
                  "height": 50,
                  "sliceName": "{fact_vs_pred.slice_name}",
                  "uuid": "pred-chart",
                  "width": 12
                }},
                "type": "CHART"
              }}
            }}
            """
        ).strip()
        db.session.commit()

        try:
            security_manager.sync_role_definitions()
        except Exception as exc:  # noqa: BLE001
            print(f"  warn: sync_role_definitions: {exc}")

        print(
            f"Dashboard ready: slug={dash.slug} charts="
            f"{[s.slice_name for s in dash.slices]}"
        )


if __name__ == "__main__":
    main()
