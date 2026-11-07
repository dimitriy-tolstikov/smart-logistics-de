"""Shared DB helpers for integration tests.

Integration tests never write into the application database. They create a
throwaway database on a local (or explicitly named) Postgres server, apply
schema + DWH DDL, and drop it when the session ends.
"""

from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

import psycopg
import pytest
from psycopg import sql

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "db", "postgres"}
_STATE: dict[str, str | None] = {"name": None, "dsn": None}
_INTEGRATION_SKIPS = 0

_BODY_TYPE_SEED = """
INSERT INTO public.body_types (name, "id_from_1C", std_id) VALUES
    ('тент', 56, 56),
    ('тент-дочерний', 57, 57),
    ('рефрижератор', 4, 4),
    ('изотерм', 46, 46),
    ('контейнер', 2, 2),
    ('борт', 1, 1),
    ('борт-группа', 15, 15);
"""


def require_db() -> bool:
    return os.getenv("PYTEST_REQUIRE_DB", "").lower() in {"1", "true", "yes"} or os.getenv(
        "CI", ""
    ).lower() in {"1", "true", "yes"}


def integration_skips() -> int:
    return _INTEGRATION_SKIPS


def _unavailable(message: str) -> None:
    global _INTEGRATION_SKIPS
    if require_db():
        pytest.fail(message)
    _INTEGRATION_SKIPS += 1
    pytest.skip(message)


def _server_kwargs() -> dict[str, object]:
    url = os.getenv("TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if url:
        parsed = urlparse(url)
        return {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
        }
    from etl.config import pg_conn_kwargs

    current = pg_conn_kwargs()
    return {
        "host": current["host"],
        "port": current["port"],
        "user": current["user"],
        "password": current.get("password") or "",
    }


def _assert_disposable_server(host: str) -> None:
    if os.getenv("TEST_DATABASE_URL"):
        return
    if host in _LOCAL_HOSTS:
        return
    if os.getenv("PYTEST_ALLOW_REMOTE_DB", "").lower() in {"1", "true", "yes"}:
        return
    _unavailable(
        f"Refusing to create a test database on non-local host {host!r}. "
        "Point TEST_DATABASE_URL at a disposable Postgres."
    )


def _admin_connect(server: dict[str, object]) -> psycopg.Connection:
    last_error: Exception | None = None
    for dbname in ("postgres", "template1"):
        try:
            conn = psycopg.connect(
                host=server["host"],
                port=int(server["port"]),
                user=server["user"],
                password=server["password"],
                dbname=dbname,
                autocommit=True,
            )
            return conn
        except (psycopg.Error, OSError) as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def _apply_sql_file(server: dict[str, object], dbname: str, path: Path) -> None:
    env = {**os.environ, "PGPASSWORD": str(server["password"] or "")}
    cmd = [
        "psql",
        "-h",
        str(server["host"]),
        "-p",
        str(server["port"]),
        "-U",
        str(server["user"]),
        "-d",
        dbname,
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        str(path),
    ]
    subprocess.run(cmd, check=True, env=env, capture_output=True, text=True)


def _bootstrap(server: dict[str, object], dbname: str) -> None:
    files = [SQL_DIR / "schema.sql", *sorted((SQL_DIR / "dwh").glob("*.sql"))]
    for path in files:
        _apply_sql_file(server, dbname, path)
    with psycopg.connect(
        host=server["host"],
        port=int(server["port"]),
        user=server["user"],
        password=server["password"],
        dbname=dbname,
        autocommit=True,
    ) as conn:
        conn.execute(_BODY_TYPE_SEED)


def drop_test_database() -> None:
    name = _STATE.get("name")
    if not name:
        return
    server = _server_kwargs()
    try:
        with _admin_connect(server) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )
    except (psycopg.Error, OSError):
        return
    _STATE["name"] = None
    _STATE["dsn"] = None


def ensure_test_database() -> str:
    """Create the session database once and return a libpq conninfo string."""
    if _STATE["dsn"]:
        return str(_STATE["dsn"])

    server = _server_kwargs()
    host = str(server["host"])
    _assert_disposable_server(host)

    name = f"ul_pytest_{os.getpid()}"
    try:
        admin = _admin_connect(server)
    except (psycopg.Error, OSError) as exc:
        _unavailable(f"PostgreSQL not available: {exc}")

    try:
        with admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
            )
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        _bootstrap(server, name)
    except FileNotFoundError:
        _unavailable("psql is required to bootstrap the integration database")
    except subprocess.CalledProcessError as exc:
        drop_tail = (exc.stderr or exc.stdout or "").strip()
        _unavailable(f"Failed to bootstrap test database: {drop_tail or exc}")
    except (psycopg.Error, OSError) as exc:
        _unavailable(f"Failed to create test database: {exc}")

    _STATE["name"] = name
    _STATE["dsn"] = (
        f"host={host} port={server['port']} user={server['user']} "
        f"password={server['password'] or ''} dbname={name}"
    )
    return str(_STATE["dsn"])


def integration_dsn() -> str:
    return ensure_test_database()


@contextmanager
def connect_integration() -> Generator[psycopg.Connection, None, None]:
    dsn = ensure_test_database()
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        yield conn
    finally:
        conn.close()
