"""Unit smoke: DATABASE_URL / POSTGRES_* consistency helpers."""

from __future__ import annotations

import etl.config as config


def test_pg_conn_kwargs_from_postgres_env_without_database_url(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "custom_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "custom_password")
    monkeypatch.setenv("POSTGRES_DB", "custom_db")
    monkeypatch.setenv("PGHOST", "db")
    monkeypatch.setenv("PGPORT", "5432")

    kwargs = config.pg_conn_kwargs()
    assert kwargs == {
        "host": "db",
        "port": 5432,
        "user": "custom_user",
        "password": "custom_password",
        "dbname": "custom_db",
    }
    assert (
        config.database_url()
        == "postgresql://custom_user:custom_password@db:5432/custom_db"
    )


def test_pg_conn_kwargs_prefers_database_url(monkeypatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://custom_user:custom_password@db:5432/custom_db",
    )
    monkeypatch.setenv("POSTGRES_USER", "postgres")
    monkeypatch.setenv("POSTGRES_PASSWORD", "postgres")
    monkeypatch.setenv("POSTGRES_DB", "deals_bigdata")

    kwargs = config.pg_conn_kwargs()
    assert kwargs["user"] == "custom_user"
    assert kwargs["password"] == "custom_password"
    assert kwargs["dbname"] == "custom_db"
    assert kwargs["host"] == "db"
