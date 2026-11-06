#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Superset metadata DB..."
python - <<'PY'
import os, time
import psycopg2
url = os.environ["DATABASE_URL"]
# postgresql+psycopg2://... → postgresql://...
dsn = url.replace("postgresql+psycopg2://", "postgresql://")
for i in range(60):
    try:
        psycopg2.connect(dsn).close()
        break
    except Exception as exc:
        print(f"  wait meta db ({i}): {exc}")
        time.sleep(2)
else:
    raise SystemExit("Superset metadata DB not ready")
PY

echo "superset db upgrade..."
superset db upgrade

echo "Create admin (idempotent)..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname Admin \
  --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
  || true

echo "superset init..."
superset init

echo "Seed datasources + demo dashboard..."
python /app/docker/create_dashboard.py || {
  echo "WARN: dashboard seed failed (datasources may still be added manually in UI)"
  exit 0
}

echo "Superset bootstrap complete."
