"""
Bonus ★: Apache Airflow DAG wrapping the same ETL → ML → ClickHouse sync path.

Trigger with conf, e.g.:
  {"start_date": "2026-10-01", "end_date": "2026-11-01", "as_of": "2026-10-26"}

Falls back to container env (ETL_START_DATE, ETL_END_DATE, AS_OF_DATE).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.environ.get("UL_PROJECT_ROOT", "/opt/airflow/project")

DEFAULT_ARGS = {
    "owner": "ul-de",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COMMON_ENV = {
    "UL_PROJECT_ROOT": PROJECT_ROOT,
    "PYTHONPATH": PROJECT_ROOT,
    "DATABASE_URL": os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@db:5432/deals_bigdata",
    ),
    "PGHOST": os.environ.get("PGHOST", "db"),
    "PGPORT": os.environ.get("PGPORT", "5432"),
    "PGUSER": os.environ.get("PGUSER", "postgres"),
    "PGPASSWORD": os.environ.get("PGPASSWORD", "postgres"),
    "PGDATABASE": os.environ.get("PGDATABASE", "deals_bigdata"),
    "ETL_START_DATE": os.environ.get("ETL_START_DATE", "2019-01-01"),
    "ETL_END_DATE": os.environ.get("ETL_END_DATE", "2027-12-31"),
    "AS_OF_DATE": os.environ.get("AS_OF_DATE", "2026-10-26"),
    "ML_RANDOM_SEED": os.environ.get("ML_RANDOM_SEED", "42"),
    "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
    "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
    "CLICKHOUSE_DB": os.environ.get("CLICKHOUSE_DB", "default"),
}

# Jinja in bash_command (not an f-string — braces must stay for Airflow templating).
WINDOW_EXPORT = """
set -euo pipefail
export PYTHONPATH=\"""" + PROJECT_ROOT + """:${PYTHONPATH:-}\"
cd \"""" + PROJECT_ROOT + """\"
{% if dag_run and dag_run.conf.get('start_date') -%}
export ETL_START_DATE="{{ dag_run.conf['start_date'] }}"
{%- endif %}
{% if dag_run and dag_run.conf.get('end_date') -%}
export ETL_END_DATE="{{ dag_run.conf['end_date'] }}"
{%- endif %}
{% if dag_run and dag_run.conf.get('as_of') -%}
export AS_OF_DATE="{{ dag_run.conf['as_of'] }}"
{%- endif %}
echo "window=[$ETL_START_DATE, $ETL_END_DATE) as_of=$AS_OF_DATE"
"""

with DAG(
    dag_id="ul_etl_ml_clickhouse",
    default_args=DEFAULT_ARGS,
    description="Smart Logistics: ETL → ML batch scoring → ClickHouse mart sync",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ul", "etl", "ml", "clickhouse"],
) as dag:
    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=WINDOW_EXPORT + "\npython -m etl.run_etl\n",
        env=COMMON_ENV,
        append_env=True,
    )

    train_predict = BashOperator(
        task_id="train_predict",
        bash_command=WINDOW_EXPORT + "\npython -m ml.train_predict\n",
        env=COMMON_ENV,
        append_env=True,
    )

    sync_clickhouse = BashOperator(
        task_id="sync_clickhouse",
        bash_command=WINDOW_EXPORT + "\npython -m etl.sync_clickhouse\n",
        env=COMMON_ENV,
        append_env=True,
    )

    run_etl >> train_predict >> sync_clickhouse
