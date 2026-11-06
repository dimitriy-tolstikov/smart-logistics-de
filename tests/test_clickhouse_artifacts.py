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
    assert "sync_clickhouse" in text
