"""Unit tests for semantic type inference."""

from __future__ import annotations

import pytest

from sf_synth.semantic import (
    SemanticMatch,
    SemanticType,
    batch_infer,
    infer_semantic_type,
    suggest_generator_for_column,
)


class TestInferSemanticType:
    """Tests for semantic type inference."""

    def test_email_patterns(self) -> None:
        assert infer_semantic_type("email").semantic_type == SemanticType.EMAIL
        assert infer_semantic_type("EMAIL").semantic_type == SemanticType.EMAIL
        assert infer_semantic_type("e_mail").semantic_type == SemanticType.EMAIL
        assert infer_semantic_type("email_address").semantic_type == SemanticType.EMAIL

    def test_phone_patterns(self) -> None:
        assert infer_semantic_type("phone").semantic_type == SemanticType.PHONE
        assert infer_semantic_type("phone_number").semantic_type == SemanticType.PHONE
        assert infer_semantic_type("tel").semantic_type == SemanticType.PHONE
        assert infer_semantic_type("mobile").semantic_type == SemanticType.PHONE

    def test_name_patterns(self) -> None:
        assert infer_semantic_type("first_name").semantic_type == SemanticType.FIRST_NAME
        assert infer_semantic_type("fname").semantic_type == SemanticType.FIRST_NAME
        assert infer_semantic_type("last_name").semantic_type == SemanticType.LAST_NAME
        assert infer_semantic_type("surname").semantic_type == SemanticType.LAST_NAME
        assert infer_semantic_type("name").semantic_type == SemanticType.FULL_NAME

    def test_address_patterns(self) -> None:
        assert infer_semantic_type("address").semantic_type == SemanticType.ADDRESS
        assert infer_semantic_type("street_address").semantic_type == SemanticType.STREET_ADDRESS
        assert infer_semantic_type("city").semantic_type == SemanticType.CITY
        assert infer_semantic_type("state").semantic_type == SemanticType.STATE
        assert infer_semantic_type("country").semantic_type == SemanticType.COUNTRY
        assert infer_semantic_type("zip_code").semantic_type == SemanticType.ZIPCODE
        assert infer_semantic_type("postal_code").semantic_type == SemanticType.ZIPCODE

    def test_date_patterns(self) -> None:
        assert infer_semantic_type("dob").semantic_type == SemanticType.DATE_OF_BIRTH
        assert infer_semantic_type("birth_date").semantic_type == SemanticType.DATE_OF_BIRTH
        assert infer_semantic_type("created_at").semantic_type == SemanticType.CREATED_AT
        assert infer_semantic_type("updated_at").semantic_type == SemanticType.UPDATED_AT

    def test_boolean_patterns(self) -> None:
        assert infer_semantic_type("is_active").semantic_type == SemanticType.BOOLEAN
        assert infer_semantic_type("has_subscription").semantic_type == SemanticType.BOOLEAN
        assert infer_semantic_type("can_edit").semantic_type == SemanticType.BOOLEAN
        assert infer_semantic_type("active_flag").semantic_type == SemanticType.BOOLEAN

    def test_id_patterns(self) -> None:
        assert infer_semantic_type("id").semantic_type == SemanticType.ID
        assert infer_semantic_type("customer_id").semantic_type == SemanticType.FOREIGN_KEY
        assert infer_semantic_type("fk_customer").semantic_type == SemanticType.FOREIGN_KEY

    def test_data_type_fallback(self) -> None:
        result = infer_semantic_type("random_column", "NUMBER")
        assert result.suggested_generator == "uniform"

        result = infer_semantic_type("random_column", "VARCHAR")
        assert result.suggested_generator == "faker"

        result = infer_semantic_type("random_column", "BOOLEAN")
        assert result.suggested_generator == "choice"

        result = infer_semantic_type("random_column", "TIMESTAMP_NTZ")
        assert result.suggested_generator == "faker"
        assert result.generator_params.get("provider") == "date_time"

    def test_column_name_takes_precedence(self) -> None:
        result = infer_semantic_type("email", "VARCHAR")
        assert result.semantic_type == SemanticType.EMAIL
        assert result.suggested_generator == "faker"
        assert result.generator_params.get("provider") == "email"

    def test_confidence_levels(self) -> None:
        email_result = infer_semantic_type("email")
        assert email_result.confidence > 0.8

        fallback_result = infer_semantic_type("xyz123")
        assert fallback_result.confidence < 0.5

    def test_unknown_type(self) -> None:
        result = infer_semantic_type("xyz123abc")
        assert result.semantic_type == SemanticType.UNKNOWN


class TestSuggestGenerator:
    """Tests for generator suggestion."""

    def test_primary_key_suggestion(self) -> None:
        result = suggest_generator_for_column(
            "ID", "NUMBER", is_primary_key=True
        )
        assert result["generator"] == "seq"
        assert result["unique"] is True

    def test_unique_column(self) -> None:
        result = suggest_generator_for_column(
            "email", "VARCHAR", is_unique=True
        )
        assert result["unique"] is True

    def test_semantic_match(self) -> None:
        result = suggest_generator_for_column("email", "VARCHAR")
        assert result["generator"] == "faker"
        assert result["provider"] == "email"


class TestBatchInfer:
    """Tests for batch inference."""

    def test_batch_multiple_columns(self) -> None:
        columns = [
            ("email", "VARCHAR"),
            ("phone", "VARCHAR"),
            ("amount", "NUMBER"),
            ("created_at", "TIMESTAMP_NTZ"),
        ]

        results = batch_infer(columns)

        assert len(results) == 4
        assert results["email"].semantic_type == SemanticType.EMAIL
        assert results["phone"].semantic_type == SemanticType.PHONE
        assert results["amount"].semantic_type == SemanticType.AMOUNT
        assert results["created_at"].semantic_type == SemanticType.CREATED_AT
