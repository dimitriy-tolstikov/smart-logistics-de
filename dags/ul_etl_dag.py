"""
Bonus ★: Airflow around the same ETL, ML and ClickHouse path.

ul_etl_daily
    Every day loads only that data interval into core/mart and syncs ClickHouse.
    It does not retrain the model.

ul_etl_ml_clickhouse
    Manual backfill. Trigger with conf, for example:
    {"start_date": "2026-10-01", "end_date": "2026-11-01", "as_of": "2026-10-26"}
    Without conf the window comes from ETL_START_DATE / ETL_END_DATE / AS_OF_DATE.
    Runs ETL, then training, then ClickHouse sync.
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

# DATABASE_URL is built inside the Python entrypoints from PG*, with quoting.
COMMON_ENV = {
    "UL_PROJECT_ROOT": PROJECT_ROOT,
    "PYTHONPATH": PROJECT_ROOT,
    "PGHOST": os.environ.get("PGHOST", "db"),
    "PGPORT": os.environ.get("PGPORT", "5432"),
    "PGUSER": os.environ.get("PGUSER", "postgres"),
    "PGPASSWORD": os.environ.get("PGPASSWORD", "postgres"),
    "PGDATABASE": os.environ.get("PGDATABASE", "deals_bigdata"),
    "ETL_START_DATE": os.environ.get("ETL_START_DATE", "2019-01-01"),
    "ETL_END_DATE": os.environ.get("ETL_END_DATE", "2027-12-31"),
    "AS_OF_DATE": os.environ.get("AS_OF_DATE", "2026-10-26"),
    "ML_RANDOM_SEED": os.environ.get("ML_RANDOM_SEED", "42"),
    "ML_TUNE": os.environ.get("ML_TUNE", "0"),
    "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
    "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
    "CLICKHOUSE_DB": os.environ.get("CLICKHOUSE_DB", "default"),
}

SHELL_PREFIX = """
set -euo pipefail
export PYTHONPATH=\"""" + PROJECT_ROOT + """:${PYTHONPATH:-}\"
cd \"""" + PROJECT_ROOT + """\"
"""

# Conf overrides win. Otherwise keep the container window (full backfill).
BACKFILL_WINDOW = SHELL_PREFIX + """
{% if dag_run and dag_run.conf.get('start_date') -%}
export ETL_START_DATE="{{ dag_run.conf['start_date'] }}"
{%- endif %}
{% if dag_run and dag_run.conf.get('end_date') -%}
export ETL_END_DATE="{{ dag_run.conf['end_date'] }}"
{%- endif %}
{% if dag_run and dag_run.conf.get('as_of') -%}
export AS_OF_DATE="{{ dag_run.conf['as_of'] }}"
{%- endif %}
echo "backfill window=[$ETL_START_DATE, $ETL_END_DATE) as_of=$AS_OF_DATE"
"""

# One logical day. AS_OF stays the business snapshot from the environment.
DAILY_WINDOW = SHELL_PREFIX + """
export ETL_START_DATE="{{ data_interval_start.strftime('%Y-%m-%d') }}"
export ETL_END_DATE="{{ data_interval_end.strftime('%Y-%m-%d') }}"
echo "daily window=[$ETL_START_DATE, $ETL_END_DATE) as_of=$AS_OF_DATE"
"""


def _bash(task_id: str, window: str, module: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=window + f"\npython -m {module}\n",
        env=COMMON_ENV,
        append_env=True,
    )


with DAG(
    dag_id="ul_etl_daily",
    default_args=DEFAULT_ARGS,
    description="Smart Logistics: one-day ETL and ClickHouse sync, no model retrain",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ul", "etl", "clickhouse"],
) as daily_dag:
    daily_etl = _bash("run_etl", DAILY_WINDOW, "etl.run_etl")
    daily_sync = _bash("sync_clickhouse", DAILY_WINDOW, "etl.sync_clickhouse")
    daily_etl >> daily_sync

with DAG(
    dag_id="ul_etl_ml_clickhouse",
    default_args=DEFAULT_ARGS,
    description="Smart Logistics backfill: ETL → ML batch scoring → ClickHouse mart sync",
    schedule_interval=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ul", "etl", "ml", "clickhouse"],
) as backfill_dag:
    run_etl = _bash("run_etl", BACKFILL_WINDOW, "etl.run_etl")
    train_predict = _bash("train_predict", BACKFILL_WINDOW, "ml.train_predict")
    sync_clickhouse = _bash("sync_clickhouse", BACKFILL_WINDOW, "etl.sync_clickhouse")
    run_etl >> train_predict >> sync_clickhouse
