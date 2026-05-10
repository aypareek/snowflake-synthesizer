"""Tests for v0.4.0 enhancements: new generator types, write modes, validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from sf_synth.config import (
    ColumnConfig,
    GeneratorType,
    SynthConfig,
    TableConfig,
    WriteMode,
)


class TestNewGeneratorTypes:
    """Tests for new generator types (expression, json_template, array, object)."""

    def test_expression_requires_sql(self) -> None:
        with pytest.raises(ValidationError, match="requires 'sql'"):
            ColumnConfig(generator=GeneratorType.EXPRESSION)

    def test_expression_with_sql(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.EXPRESSION,
            sql="FIRST_NAME || ' ' || LAST_NAME",
        )
        assert cfg.sql == "FIRST_NAME || ' ' || LAST_NAME"

    def test_json_template_requires_template(self) -> None:
        with pytest.raises(ValidationError, match="requires 'template'"):
            ColumnConfig(generator=GeneratorType.JSON_TEMPLATE)

    def test_json_template_with_template(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.JSON_TEMPLATE,
            template='{"foo": "{{faker.name}}"}',
        )
        assert "faker.name" in cfg.template

    def test_array_requires_element_generator(self) -> None:
        with pytest.raises(ValidationError, match="requires 'element_generator'"):
            ColumnConfig(generator=GeneratorType.ARRAY)

    def test_array_with_element_generator(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.ARRAY,
            element_generator=GeneratorType.UNIFORM,
            element_min=1,
            element_max=100,
            length=5,
        )
        assert cfg.element_generator == GeneratorType.UNIFORM
        assert cfg.length == 5

    def test_array_length_range(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.ARRAY,
            element_generator=GeneratorType.FAKER,
            element_provider="word",
            length=[1, 5],
        )
        assert cfg.length == [1, 5]

    def test_array_invalid_length_range(self) -> None:
        with pytest.raises(ValidationError, match="\\[min, max\\]"):
            ColumnConfig(
                generator=GeneratorType.ARRAY,
                element_generator=GeneratorType.UNIFORM,
                length=[5, 1],
            )

    def test_object_requires_fields(self) -> None:
        with pytest.raises(ValidationError, match="requires 'fields'"):
            ColumnConfig(generator=GeneratorType.OBJECT)

    def test_object_with_fields(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.OBJECT,
            fields={
                "name": ColumnConfig(generator=GeneratorType.FAKER, provider="name"),
                "age": ColumnConfig(
                    generator=GeneratorType.UNIFORM, min_value=18, max_value=80
                ),
            },
        )
        assert "name" in cfg.fields
        assert cfg.fields["name"].provider == "name"


class TestCorrelationGroup:
    """Tests for correlation_group field on ColumnConfig."""

    def test_correlation_group_default_none(self) -> None:
        cfg = ColumnConfig(generator=GeneratorType.FAKER, provider="city")
        assert cfg.correlation_group is None

    def test_correlation_group_set(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.FAKER,
            provider="city",
            correlation_group="address",
        )
        assert cfg.correlation_group == "address"


class TestTemporalAfter:
    """Tests for `after` temporal ordering field."""

    def test_after_default(self) -> None:
        cfg = ColumnConfig(generator=GeneratorType.FAKER, provider="date_time")
        assert cfg.after is None
        assert cfg.after_offset_unit == "day"
        assert cfg.after_offset_min == 1
        assert cfg.after_offset_max == 365

    def test_after_set(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.FAKER,
            provider="date_time",
            after="CREATED_AT",
            after_offset_unit="hour",
            after_offset_min=1,
            after_offset_max=24,
        )
        assert cfg.after == "CREATED_AT"
        assert cfg.after_offset_unit == "hour"


class TestWriteMode:
    """Tests for write_mode field."""

    def test_default_write_mode(self) -> None:
        tbl = TableConfig(name="USERS", rows=100)
        assert tbl.write_mode == WriteMode.REPLACE

    def test_write_mode_values(self) -> None:
        for mode_value in ["replace", "append", "upsert", "fill_to"]:
            tbl = TableConfig(name="T", rows=10, write_mode=mode_value)
            assert tbl.write_mode.value == mode_value

    def test_upsert_keys(self) -> None:
        tbl = TableConfig(
            name="T",
            rows=10,
            write_mode=WriteMode.UPSERT,
            upsert_keys=["id", "version"],
        )
        assert tbl.upsert_keys == ["id", "version"]


class TestConditionalGeneration:
    """Tests for `condition` and `else_value` fields."""

    def test_condition_set(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.FAKER,
            provider="date_time",
            condition="STATUS = 'active'",
            else_value=None,
        )
        assert cfg.condition == "STATUS = 'active'"

    def test_condition_with_else_value(self) -> None:
        cfg = ColumnConfig(
            generator=GeneratorType.UNIFORM,
            min_value=0,
            max_value=100,
            condition="TYPE = 'premium'",
            else_value=0,
        )
        assert cfg.else_value == 0


class TestValidationModule:
    """Tests for sf_synth.validation."""

    def test_resolve_fk_target_full(self) -> None:
        from sf_synth.validation import _resolve_fk_target
        result = _resolve_fk_target("DB.SCHEMA.TABLE.COL", "DEFAULT_DB", "DEFAULT_SCHEMA")
        assert result == ("DB.SCHEMA.TABLE", "COL")

    def test_resolve_fk_target_short(self) -> None:
        from sf_synth.validation import _resolve_fk_target
        result = _resolve_fk_target("TABLE.COL", "DB", "SCHEMA")
        assert result == ("DB.SCHEMA.TABLE", "COL")

    def test_resolve_fk_target_invalid(self) -> None:
        from sf_synth.validation import _resolve_fk_target
        result = _resolve_fk_target("OnlyTable", None, None)
        assert result is None
