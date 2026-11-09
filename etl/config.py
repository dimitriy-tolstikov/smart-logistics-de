from __future__ import annotations

import os
from datetime import date
from urllib.parse import quote, unquote, urlparse

from dotenv import load_dotenv

load_dotenv()


def _parse_date(env_name: str, default: date) -> date:
    raw = os.getenv(env_name)
    if not raw:
        return default
    return date.fromisoformat(raw)


AS_OF: date = _parse_date("AS_OF_DATE", date(2026, 10, 26))
ETL_START: date = _parse_date("ETL_START_DATE", date(2019, 1, 1))
ETL_END: date = _parse_date("ETL_END_DATE", date(2027, 12, 31))
N_PRICES: int = int(os.getenv("N_PRICES", "300000"))
ML_RANDOM_SEED: int = int(os.getenv("ML_RANDOM_SEED", "42"))


def _quoted_database_url(user: str, password: str, host: str, port: int | str, db: str) -> str:
    """Percent-encode user and password so @ and : inside them do not break the DSN."""
    return (
        f"postgresql://{quote(user, safe='')}:"
        f"{quote(password, safe='')}@{host}:{port}/{quote(db, safe='')}"
    )


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        parsed = urlparse(url)
        if parsed.hostname and parsed.username is not None:
            return _quoted_database_url(
                unquote(parsed.username),
                unquote(parsed.password or ""),
                parsed.hostname,
                parsed.port or 5432,
                unquote((parsed.path or "/").lstrip("/")),
            )
        return url
    host = os.getenv("PGHOST", "localhost")
    user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres"))
    password = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres"))
    db = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "deals_bigdata"))
    port = os.getenv("PGPORT", os.getenv("POSTGRES_PORT", "5432"))
    return _quoted_database_url(user, password, host, port, db)


def ml_tune_enabled() -> bool:
    """Hyper-parameter search is off unless ML_TUNE=1. Default fit uses the chosen params."""
    return os.getenv("ML_TUNE", "0").lower() in {"1", "true", "yes"}


def pg_conn_kwargs() -> dict:
    """psycopg conninfo from DATABASE_URL or PG* env (docker-friendly)."""
    url = os.getenv("DATABASE_URL")
    if url:
        p = urlparse(url)
        return {
            "host": p.hostname or "localhost",
            "port": p.port or 5432,
            "user": unquote(p.username) if p.username else p.username,
            "password": unquote(p.password) if p.password else p.password,
            "dbname": unquote((p.path or "/").lstrip("/")),
        }
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", os.getenv("POSTGRES_PORT", "5432"))),
        "user": os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres")),
        "password": os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
        "dbname": os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "deals_bigdata")),
    }


def clickhouse_settings() -> dict:
    """ClickHouse connection settings for bonus mart sync."""
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        "database": os.getenv("CLICKHOUSE_DB", "default"),
    }
