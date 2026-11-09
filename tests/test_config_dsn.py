"""Unit smoke: DATABASE_URL / POSTGRES_* consistency helpers."""

from __future__ import annotations

from pathlib import Path

import etl.config as config

ROOT = Path(__file__).resolve().parents[1]


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


def test_database_url_quotes_reserved_characters(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("PGUSER", "user")
    monkeypatch.setenv("PGPASSWORD", "p@ss:word")
    monkeypatch.setenv("PGHOST", "db")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "deals")

    assert config.database_url() == "postgresql://user:p%40ss%3Aword@db:5432/deals"

    monkeypatch.setenv("DATABASE_URL", config.database_url())
    kwargs = config.pg_conn_kwargs()
    assert kwargs["password"] == "p@ss:word"
    assert kwargs["user"] == "user"


def test_compose_app_dsn_uses_the_same_postgres_variables() -> None:
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    app_block = text.split("  app:", 1)[1].split("\n  # --- bonus", 1)[0]
    assert "DATABASE_URL: postgresql://${POSTGRES_USER:-postgres}:${POSTGRES_PASSWORD:-postgres}@db:5432/${POSTGRES_DB:-deals_bigdata}" in app_block
    assert "postgresql://postgres:postgres@db:5432/deals_bigdata" not in app_block
