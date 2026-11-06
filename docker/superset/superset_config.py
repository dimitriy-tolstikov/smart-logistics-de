"""Superset config for local bonus ★ stack."""

from __future__ import annotations

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "ul_local_superset_secret_change_me")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
    ),
)

# Local demo defaults
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
CONTENT_SECURITY_POLICY_WARNING = False
PREVENT_UNSAFE_DB_CONNECTIONS = False
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}
