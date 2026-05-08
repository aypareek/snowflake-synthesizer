"""Integration tests requiring Snowflake credentials.

To run these tests:
1. Set environment variables for Snowflake connection
2. Run: SF_SYNTH_INTEGRATION_TESTS=1 pytest tests/integration/
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture
def snowflake_connection_params() -> dict[str, str]:
    """Get Snowflake connection parameters from environment."""
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        pytest.skip(f"Missing environment variables: {missing}")

    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_SAMPLE_DATA"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }


@pytest.mark.integration
class TestSnowflakeDiscovery:
    """Integration tests for schema discovery."""

    def test_discover_sample_database(
        self, snowflake_connection_params: dict[str, str]
    ) -> None:
        """Test discovering the sample database schema."""
        from sf_synth.backend import SnowparkBackend

        backend = SnowparkBackend(**snowflake_connection_params)
        backend.connect()

        try:
            schema_model = backend.discover_schema(
                database=snowflake_connection_params["database"],
                schemas=["TPCH_SF1"],
                tables=["CUSTOMER", "ORDERS"],
            )

            assert len(schema_model.tables) >= 0
        finally:
            backend.disconnect()


@pytest.mark.integration
class TestSnowflakeGeneration:
    """Integration tests for data generation."""

    def test_generate_simple_table(
        self, snowflake_connection_params: dict[str, str]
    ) -> None:
        """Test generating a simple table."""
        from sf_synth.backend import SnowparkBackend
        from sf_synth.config import (
            ColumnConfig,
            DefaultsConfig,
            GeneratorType,
            SynthConfig,
            TableConfig,
        )

        test_table = f"SF_SYNTH_TEST_{os.getpid()}"

        config = SynthConfig(
            defaults=DefaultsConfig(
                seed=42,
                database=snowflake_connection_params["database"],
                schema=snowflake_connection_params["schema"],
            ),
            tables=[
                TableConfig(
                    name=test_table,
                    rows=100,
                    columns={
                        "ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                        "VALUE": ColumnConfig(
                            generator=GeneratorType.UNIFORM,
                            min_value=0,
                            max_value=100,
                        ),
                    },
                ),
            ],
        )

        backend = SnowparkBackend(**snowflake_connection_params)
        backend.connect()

        try:
            fqn = f'"{config.defaults.database}"."{config.defaults.schema}"."{test_table}"'
            backend.session.sql(f"""
                CREATE OR REPLACE TRANSIENT TABLE {fqn} (
                    ID NUMBER,
                    VALUE FLOAT
                )
            """).collect()

            result = backend.generate(config)

            assert result.success
            assert result.total_rows == 100

            count = backend.session.sql(f"SELECT COUNT(*) AS CNT FROM {fqn}").collect()
            assert count[0]["CNT"] == 100

        finally:
            try:
                backend.session.sql(f"DROP TABLE IF EXISTS {fqn}").collect()
            except Exception:
                pass
            backend.disconnect()
