"""Shared fixtures for unit and integration tests."""

from __future__ import annotations

from typing import Generator

import psycopg
import pytest

from tests.db_helpers import connect_integration, drop_test_database, integration_skips, require_db


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: tests that require a live PostgreSQL database")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    drop_test_database()
    if require_db() and integration_skips() and exitstatus == 0:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture
def conn() -> Generator[psycopg.Connection, None, None]:
    with connect_integration() as c:
        yield c
