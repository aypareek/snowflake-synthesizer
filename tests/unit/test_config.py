"""Unit tests for configuration module."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from sf_synth.config import (
    ColumnConfig,
    DefaultsConfig,
    GeneratorType,
    RelationshipConfig,
    SkewType,
    SynthConfig,
    TableConfig,
    load_config,
    save_config,
)


class TestColumnConfig:
    """Tests for ColumnConfig."""

    def test_faker_requires_provider(self) -> None:
        with pytest.raises(ValueError, match="requires 'provider'"):
            ColumnConfig(generator=GeneratorType.FAKER)

    def test_faker_with_provider(self) -> None:
        config = ColumnConfig(generator=GeneratorType.FAKER, provider="email")
        assert config.provider == "email"

    def test_choice_requires_values(self) -> None:
        with pytest.raises(ValueError, match="requires 'values'"):
            ColumnConfig(generator=GeneratorType.CHOICE)

    def test_choice_with_values(self) -> None:
        config = ColumnConfig(generator=GeneratorType.CHOICE, values=["A", "B", "C"])
        assert config.values == ["A", "B", "C"]

    def test_choice_weights_mismatch(self) -> None:
        with pytest.raises(ValueError, match="weights length must match"):
            ColumnConfig(
                generator=GeneratorType.CHOICE,
                values=["A", "B"],
                weights=[0.5, 0.3, 0.2],
            )

    def test_distribution_requires_source(self) -> None:
        with pytest.raises(ValueError, match="requires 'source'"):
            ColumnConfig(generator=GeneratorType.DISTRIBUTION)

    def test_range_requires_min_max(self) -> None:
        with pytest.raises(ValueError, match="requires 'min_value' and 'max_value'"):
            ColumnConfig(generator=GeneratorType.RANGE, min_value=0)

    def test_null_ratio_validation(self) -> None:
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            ColumnConfig(
                generator=GeneratorType.UNIFORM, null_ratio=1.5
            )


class TestRelationshipConfig:
    """Tests for RelationshipConfig."""

    def test_valid_reference(self) -> None:
        rel = RelationshipConfig(
            column="customer_id",
            references="DB.SCHEMA.CUSTOMERS.ID",
        )
        assert rel.column == "customer_id"
        assert rel.references == "DB.SCHEMA.CUSTOMERS.ID"

    def test_simple_reference(self) -> None:
        rel = RelationshipConfig(
            column="customer_id",
            references="CUSTOMERS.ID",
        )
        assert rel.references == "CUSTOMERS.ID"

    def test_invalid_reference(self) -> None:
        with pytest.raises(ValueError, match="at least TABLE.COLUMN"):
            RelationshipConfig(
                column="customer_id",
                references="CUSTOMERS",
            )

    def test_skew_types(self) -> None:
        rel = RelationshipConfig(
            column="customer_id",
            references="CUSTOMERS.ID",
            skew=SkewType.ZIPF,
            skew_param=1.2,
        )
        assert rel.skew == SkewType.ZIPF
        assert rel.skew_param == 1.2


class TestTableConfig:
    """Tests for TableConfig."""

    def test_basic_table(self) -> None:
        table = TableConfig(name="USERS", rows=1000)
        assert table.name == "USERS"
        assert table.rows == 1000
        assert table.columns == {}
        assert table.relationships == []

    def test_rows_must_be_positive(self) -> None:
        with pytest.raises(ValueError):
            TableConfig(name="USERS", rows=0)

    def test_get_fqn_simple(self) -> None:
        table = TableConfig(name="USERS", rows=100)
        fqn = table.get_fqn(default_database="DB", default_schema="SCHEMA")
        assert fqn == "DB.SCHEMA.USERS"

    def test_get_fqn_with_schema(self) -> None:
        table = TableConfig(name="SCHEMA.USERS", rows=100)
        fqn = table.get_fqn(default_database="DB")
        assert fqn == "DB.SCHEMA.USERS"

    def test_get_fqn_fully_qualified(self) -> None:
        table = TableConfig(name="DB.SCHEMA.USERS", rows=100)
        fqn = table.get_fqn()
        assert fqn == "DB.SCHEMA.USERS"


class TestSynthConfig:
    """Tests for SynthConfig."""

    def test_empty_config(self) -> None:
        config = SynthConfig()
        assert config.tables == []
        assert config.defaults.seed is None

    def test_duplicate_tables(self) -> None:
        with pytest.raises(ValueError, match="Duplicate table names"):
            SynthConfig(
                tables=[
                    TableConfig(name="USERS", rows=100),
                    TableConfig(name="USERS", rows=200),
                ]
            )

    def test_get_table(self) -> None:
        config = SynthConfig(
            tables=[
                TableConfig(name="USERS", rows=100),
                TableConfig(name="ORDERS", rows=500),
            ]
        )
        assert config.get_table("USERS") is not None
        assert config.get_table("USERS").rows == 100
        assert config.get_table("PRODUCTS") is None

    def test_get_total_rows(self) -> None:
        config = SynthConfig(
            tables=[
                TableConfig(name="USERS", rows=100),
                TableConfig(name="ORDERS", rows=500),
            ]
        )
        assert config.get_total_rows() == 600


class TestLoadSaveConfig:
    """Tests for config file I/O."""

    def test_load_minimal_config(self) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(
                {
                    "tables": [
                        {"name": "USERS", "rows": 100},
                    ]
                },
                f,
            )
            f.flush()

            config = load_config(f.name)
            assert len(config.tables) == 1
            assert config.tables[0].name == "USERS"

    def test_load_full_config(self) -> None:
        data = {
            "defaults": {
                "seed": 42,
                "locale": "en_US",
                "database": "MYDB",
            },
            "tables": [
                {
                    "name": "CUSTOMERS",
                    "rows": 1000,
                    "columns": {
                        "EMAIL": {
                            "generator": "faker",
                            "provider": "email",
                            "unique": True,
                        },
                    },
                },
                {
                    "name": "ORDERS",
                    "rows": 5000,
                    "relationships": [
                        {
                            "column": "CUST_ID",
                            "references": "CUSTOMERS.ID",
                            "null_ratio": 0.05,
                            "skew": "zipf",
                        },
                    ],
                },
            ],
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(data, f)
            f.flush()

            config = load_config(f.name)

        assert config.defaults.seed == 42
        assert config.defaults.database == "MYDB"
        assert len(config.tables) == 2
        assert "EMAIL" in config.tables[0].columns
        assert config.tables[0].columns["EMAIL"].unique is True
        assert len(config.tables[1].relationships) == 1
        assert config.tables[1].relationships[0].skew == SkewType.ZIPF

    def test_save_and_load_roundtrip(self) -> None:
        config = SynthConfig(
            defaults=DefaultsConfig(seed=123, database="TEST"),
            tables=[
                TableConfig(
                    name="USERS",
                    rows=500,
                    columns={
                        "NAME": ColumnConfig(
                            generator=GeneratorType.FAKER, provider="name"
                        ),
                    },
                ),
            ],
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            save_config(config, f.name)
            loaded = load_config(f.name)

        assert loaded.defaults.seed == 123
        assert loaded.tables[0].rows == 500

    def test_load_nonexistent_file(self) -> None:
        from sf_synth.errors import ConfigError

        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/path.yaml")

    def test_invalid_yaml(self) -> None:
        from sf_synth.errors import ConfigError

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("invalid: yaml: content: [")
            f.flush()

            with pytest.raises(ConfigError, match="Failed to parse"):
                load_config(f.name)
