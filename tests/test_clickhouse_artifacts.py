"""Smoke checks for bonus ★ ClickHouse artifacts (no live CH required)."""

from __future__ import annotations

from pathlib import Path

from etl.config import clickhouse_settings


ROOT = Path(__file__).resolve().parents[1]


def test_clickhouse_ddl_present_and_uses_replacing_mergetree() -> None:
    ddl = (ROOT / "sql" / "clickhouse" / "01_mart_route_card.sql").read_text(encoding="utf-8")
    assert "ReplacingMergeTree" in ddl
    assert "mart.route_card" in ddl
    assert "mart.ml_price_predictions" in ddl


def test_clickhouse_settings_defaults() -> None:
    cfg = clickhouse_settings()
    assert cfg["host"]
    assert cfg["port"] == 8123
    assert cfg["user"]
    assert "password" in cfg


def test_airflow_dag_file_exists() -> None:
    dag = ROOT / "dags" / "ul_etl_dag.py"
    text = dag.read_text(encoding="utf-8")
    assert "ul_etl_ml_clickhouse" in text
    assert "ul_etl_daily" in text
    assert "sync_clickhouse" in text
    assert "schedule_interval=None" in text
    assert "data_interval_start" in text
    assert "EXCHANGE TABLES" in (ROOT / "etl" / "sync_clickhouse.py").read_text(encoding="utf-8")


def test_sync_main_runs_when_flag_is_disabled(monkeypatch) -> None:
    monkeypatch.setenv("CLICKHOUSE_SYNC_ENABLED", "0")
    calls: list[int] = []
    monkeypatch.setattr(
        "etl.sync_clickhouse.sync_marts",
        lambda: calls.append(1) or {"route_card_rows": 0, "ml_price_predictions_rows": 0},
    )
    from etl.sync_clickhouse import main

    main()
    assert calls == [1]


def test_superset_uses_http_clickhouse_dialect() -> None:
    text = (ROOT / "docker" / "superset" / "create_dashboard.py").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    bootstrap = (ROOT / "docker" / "superset" / "bootstrap.sh").read_text(encoding="utf-8")
    assert "clickhousedb+connect://" in text
    assert "clickhousedb+connect://" in compose
    assert "registry.load" in text
    assert "exit 0" not in bootstrap
    assert "native_filter_configuration" in text
