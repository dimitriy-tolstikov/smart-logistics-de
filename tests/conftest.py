"""Shared fixtures for unit and integration tests."""

from __future__ import annotations

from typing import Generator

import psycopg
import pytest

from tests.db_helpers import connect_integration


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: tests that require a live PostgreSQL database")


@pytest.fixture
def conn() -> Generator[psycopg.Connection, None, None]:
    with connect_integration() as c:
        yield c
