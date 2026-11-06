"""Shared DB helpers for integration tests."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator
from urllib.parse import urlparse

import psycopg
import pytest


def require_db() -> bool:
    return os.getenv("PYTEST_REQUIRE_DB", "").lower() in {"1", "true", "yes"} or os.getenv(
        "CI", ""
    ).lower() in {"1", "true", "yes"}


def integration_dsn() -> str:
    """Prefer TEST_DATABASE_URL so integration tests do not hit an arbitrary production-like DSN."""
    url = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if url:
        p = urlparse(url)
        return (
            f"host={p.hostname or 'localhost'} port={p.port or 5432} "
            f"user={p.username or 'postgres'} password={p.password or ''} "
            f"dbname={(p.path or '/').lstrip('/') or 'deals_bigdata'}"
        )

    from etl.config import pg_conn_kwargs

    k = pg_conn_kwargs()
    return (
        f"host={k['host']} port={k['port']} user={k['user']} "
        f"password={k.get('password') or ''} dbname={k['dbname']}"
    )


@contextmanager
def connect_integration() -> Generator[psycopg.Connection, None, None]:
    try:
        conn = psycopg.connect(integration_dsn(), autocommit=False)
    except (psycopg.Error, OSError) as e:
        if require_db():
            pytest.fail(f"PostgreSQL required but unavailable: {e}")
        pytest.skip(f"PostgreSQL not available: {e}")
    try:
        yield conn
    finally:
        conn.close()
