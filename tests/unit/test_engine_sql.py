"""Tests for SynthEngine SQL-generation helpers (no Snowflake required)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sf_synth.config import (
    ColumnConfig,
    DefaultsConfig,
    GeneratorType,
    SynthConfig,
    TableConfig,
)
from sf_synth.engine import SynthEngine


@pytest.fixture
def mock_session() -> MagicMock:
    session = MagicMock()
    session.get_current_database.return_value = "DB"
    session.get_current_schema.return_value = "SCHEMA"
    return session


@pytest.fixture
def engine(mock_session: MagicMock) -> SynthEngine:
    cfg = SynthConfig(defaults=DefaultsConfig(seed=42, database="DB", schema="SCHEMA"))
    return SynthEngine(mock_session, cfg)


class TestSqlLiteral:
    """Tests for SQL literal escaping."""

    def test_string_literal(self, engine: SynthEngine) -> None:
        assert engine._sql_literal("hello") == "'hello'"

    def test_string_with_quote(self, engine: SynthEngine) -> None:
        assert engine._sql_literal("O'Brien") == "'O''Brien'"

    def test_int_literal(self, engine: SynthEngine) -> None:
        assert engine._sql_literal(42) == "42"

    def test_float_literal(self, engine: SynthEngine) -> None:
        assert engine._sql_literal(3.14) == "3.14"

    def test_bool_literal(self, engine: SynthEngine) -> None:
        assert engine._sql_literal(True) == "TRUE"
        assert engine._sql_literal(False) == "FALSE"

    def test_none_literal(self, engine: SynthEngine) -> None:
        assert engine._sql_literal(None) == "NULL"


class TestExpressionGenerator:
    """Tests for the EXPRESSION generator type."""

    def test_expression_uses_raw_sql(self, engine: SynthEngine) -> None:
        cfg = ColumnConfig(generator=GeneratorType.EXPRESSION, sql="A + B")
        expr = engine._config_to_sql_expr("col", cfg, 100, 42)
        assert "A + B" in expr


class TestChoiceExpr:
    """Tests for choice expression building."""

    def test_choice_with_strings(self, engine: SynthEngine) -> None:
        result = engine._build_choice_expr(["a", "b", "c"], None, 42)
        assert "CASE" in result
        assert "'a'" in result
        assert "'b'" in result

    def test_choice_with_ints(self, engine: SynthEngine) -> None:
        result = engine._build_choice_expr([1, 2, 3], None, 42)
        assert "CASE" in result
        assert " 1" in result or "1 " in result

    def test_choice_empty(self, engine: SynthEngine) -> None:
        assert engine._build_choice_expr([], None, 42) == "NULL"


class TestJsonTemplate:
    """Tests for json_template rendering."""

    def test_simple_template(self, engine: SynthEngine) -> None:
        sql = engine._render_json_template('{"x": 1}', 42)
        assert "TRY_PARSE_JSON" in sql
        assert "{" in sql

    def test_template_with_uniform(self, engine: SynthEngine) -> None:
        sql = engine._render_json_template('{"v": {{uniform(1,5)}}}', 42)
        assert "UNIFORM" in sql

    def test_template_with_seq(self, engine: SynthEngine) -> None:
        sql = engine._render_json_template('{"id": {{seq}}}', 42)
        assert "SEQ8()" in sql


class TestArrayExpr:
    """Tests for array generator SQL building."""

    def test_array_with_uniform_elements(self, engine: SynthEngine) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.ARRAY,
            element_generator=GeneratorType.UNIFORM,
            element_min=1,
            element_max=10,
            length=3,
        )
        sql = engine._build_array_expr(cfg, 42, 42)
        assert sql.startswith("ARRAY_CONSTRUCT(")
        assert sql.count("UNIFORM") == 3

    def test_array_with_random_length(self, engine: SynthEngine) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.ARRAY,
            element_generator=GeneratorType.UNIFORM,
            element_min=1,
            element_max=10,
            length=[1, 3],
        )
        sql = engine._build_array_expr(cfg, 42, 42)
        assert "ARRAY_SLICE" in sql


class TestObjectExpr:
    """Tests for object generator SQL building."""

    def test_object_with_fields(self, engine: SynthEngine) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.OBJECT,
            fields={
                "x": ColumnConfig(generator=GeneratorType.UNIFORM, min_value=1, max_value=10),
                "y": ColumnConfig(generator=GeneratorType.SEQ, start=1, step=1),
            },
        )
        sql = engine._build_object_expr("col", cfg, 42, 42, 100)
        assert sql.startswith("OBJECT_CONSTRUCT(")
        assert "'x'" in sql
        assert "'y'" in sql


class TestErrorColumnExtraction:
    """Test the _extract_offending_column heuristic."""

    def test_extract_from_column_quotes(self) -> None:
        col = SynthEngine._extract_offending_column("Bad value in column 'EMAIL' too long")
        assert col == "EMAIL"

    def test_extract_returns_none(self) -> None:
        col = SynthEngine._extract_offending_column("totally unrelated error")
        assert col is None


class TestDepthLevels:
    """Test DAG depth computation for parallel execution."""

    def test_compute_depth_levels(self, mock_session: MagicMock) -> None:
        cfg = SynthConfig(
            defaults=DefaultsConfig(seed=42, database="DB", schema="SCHEMA"),
            tables=[
                TableConfig(name="A", rows=10),
                TableConfig(name="B", rows=10),
                TableConfig(
                    name="C",
                    rows=10,
                    relationships=[
                        {"column": "a_id", "references": "A.ID"},
                        {"column": "b_id", "references": "B.ID"},
                    ],
                ),
            ],
        )
        engine = SynthEngine(mock_session, cfg)
        engine.plan()
        depths = engine._compute_depth_levels()
        assert depths["DB.SCHEMA.A"] == 0
        assert depths["DB.SCHEMA.B"] == 0
        assert depths["DB.SCHEMA.C"] == 1
