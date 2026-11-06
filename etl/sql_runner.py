from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "etl" / "sql"


def read_sql(name: str) -> str:
    path = SQL_DIR / name
    if not path.is_file():
        raise FileNotFoundError(str(path))
    return path.read_text(encoding="utf-8")
