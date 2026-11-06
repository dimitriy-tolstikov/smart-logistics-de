"""
Idempotent source + DWH bootstrap.
Uses psql for sql/schema.sql, sql/synthetic.sql (psql \\if meta-commands), and sql/dwh/*.sql.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg

from etl.config import N_PRICES, pg_conn_kwargs

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"


def _run_psql_file(path: Path, extra_vars: list[str] | None = None) -> None:
    k = pg_conn_kwargs()
    env = {**os.environ, "PGPASSWORD": k.get("password") or ""}
    cmd = [
        "psql",
        f"-h{k['host']}",
        f"-p{str(k['port'])}",
        f"-U{k['user']}",
        f"-d{k['dbname']}",
        "-v",
        "ON_ERROR_STOP=1",
    ]
    if extra_vars:
        for v in extra_vars:
            cmd.extend(["-v", v])
    cmd.extend(["-f", str(path)])
    subprocess.run(cmd, check=True, env=env)


def main() -> None:
    k = pg_conn_kwargs()
    dsn = (
        f"host={k['host']} port={k['port']} user={k['user']} "
        f"password={k.get('password') or ''} dbname={k['dbname']}"
    )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = 'public' AND table_name = 'prices'
                )
                """
            )
            has_prices_table = cur.fetchone()[0]

    if not has_prices_table:
        _run_psql_file(SQL_DIR / "schema.sql")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.prices")
            n = cur.fetchone()[0]

    if n == 0:
        _run_psql_file(SQL_DIR / "synthetic.sql", extra_vars=[f"n_prices={N_PRICES}"])

    dwh_dir = SQL_DIR / "dwh"
    if dwh_dir.is_dir():
        for p in sorted(dwh_dir.glob("*.sql")):
            _run_psql_file(p)


if __name__ == "__main__":
    main()
