"""Pytest configuration and fixtures."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_config_path(temp_dir: Path) -> Path:
    """Create a sample config file."""
    import yaml

    config = {
        "defaults": {
            "seed": 42,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
        "tables": [
            {
                "name": "USERS",
                "rows": 100,
                "columns": {
                    "ID": {"generator": "seq", "start": 1},
                    "NAME": {"generator": "faker", "provider": "name"},
                    "EMAIL": {"generator": "faker", "provider": "email", "unique": True},
                },
            },
            {
                "name": "ORDERS",
                "rows": 500,
                "columns": {
                    "ID": {"generator": "seq", "start": 1},
                    "AMOUNT": {"generator": "uniform", "min_value": 10, "max_value": 1000},
                },
                "relationships": [
                    {"column": "USER_ID", "references": "USERS.ID"},
                ],
            },
        ],
    }

    config_path = temp_dir / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return config_path


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests as integration tests requiring Snowflake credentials",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip integration tests if not explicitly requested."""
    if os.environ.get("SF_SYNTH_INTEGRATION_TESTS") != "1":
        skip_integration = pytest.mark.skip(
            reason="Integration tests disabled. Set SF_SYNTH_INTEGRATION_TESTS=1 to run."
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
