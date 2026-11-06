from __future__ import annotations

from pathlib import Path

from etl.sql_runner import read_sql


def test_sql_templates_exist() -> None:
    root = Path(__file__).resolve().parents[1]
    assert (root / "etl/sql/load_core.sql").exists()
    for name in (
        "load_core_delete_quarantine.sql",
        "load_core_delete_fact.sql",
        "load_core_prepare_staging.sql",
        "load_core_insert_fact.sql",
        "load_core_insert_quarantine.sql",
        "refresh_route_card.sql",
        "refresh_route_card_prepare.sql",
        "refresh_route_card_delete.sql",
        "refresh_route_card_insert.sql",
    ):
        assert (root / "etl/sql" / name).exists()

    assert (root / ".dockerignore").exists()
    assert (root / "sql/dwh/07_body_group_map.sql").exists()


def test_read_sql_nonexistent() -> None:
    try:
        read_sql("does_not_exist.sql")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("expected FileNotFoundError")
