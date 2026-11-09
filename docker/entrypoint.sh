#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-db}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-${POSTGRES_USER:-postgres}}"
export PGPASSWORD="${PGPASSWORD:-${POSTGRES_PASSWORD:-postgres}}"
export PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-deals_bigdata}}"

# Rebuild the DSN from PG* so a password that contains @ or : survives URL parsing.
export DATABASE_URL="$(python - <<'PY'
from urllib.parse import quote
import os
user = os.environ.get("PGUSER") or os.environ.get("POSTGRES_USER") or "postgres"
password = os.environ.get("PGPASSWORD")
if password is None:
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
host = os.environ.get("PGHOST") or "localhost"
port = os.environ.get("PGPORT") or os.environ.get("POSTGRES_PORT") or "5432"
db = os.environ.get("PGDATABASE") or os.environ.get("POSTGRES_DB") or "deals_bigdata"
print(
    "postgresql://{user}:{password}@{host}:{port}/{db}".format(
        user=quote(user, safe=""),
        password=quote(password, safe=""),
        host=host,
        port=port,
        db=quote(db, safe=""),
    )
)
PY
)"

echo "Waiting for Postgres..."
until pg_isready -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do sleep 1; done

echo "Ensuring source schema + synthetic data (idempotent checks)..."
python -m etl.bootstrap_db

echo "Running ETL + mart refresh..."
python -m etl.run_etl

echo "Training model + batch scoring..."
python -m ml.train_predict

# Bonus ★: optional mart sync when ClickHouse is up (profile bonus + CLICKHOUSE_SYNC_ENABLED=1).
if [[ "${CLICKHOUSE_SYNC_ENABLED:-0}" =~ ^(1|true|yes)$ ]]; then
  echo "Syncing mart tables to ClickHouse..."
  python -m etl.sync_clickhouse
fi

echo "Done. Postgres: host=$PGHOST db=$PGDATABASE"
