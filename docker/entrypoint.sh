#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-db}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-${POSTGRES_USER:-postgres}}"
export PGPASSWORD="${PGPASSWORD:-${POSTGRES_PASSWORD:-postgres}}"
export PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-deals_bigdata}}"

# Keep DATABASE_URL aligned with the same credentials when it was not set explicitly
# by the orchestrator (Compose already injects a constructed DSN).
if [[ -z "${DATABASE_URL:-}" ]]; then
  export DATABASE_URL="postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}"
fi

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
  python -m etl.sync_clickhouse || echo "ClickHouse sync skipped/failed (non-fatal for base pipeline)"
fi

echo "Done. Postgres: host=$PGHOST db=$PGDATABASE"
