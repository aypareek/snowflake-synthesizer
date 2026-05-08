"""Semantic type inference from column names.

This module infers the semantic type of a column based on its name,
allowing automatic selection of appropriate Faker providers or generators.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


class SemanticType(Enum):
    """Semantic types for column data."""

    EMAIL = auto()
    PHONE = auto()
    FIRST_NAME = auto()
    LAST_NAME = auto()
    FULL_NAME = auto()
    ADDRESS = auto()
    STREET_ADDRESS = auto()
    CITY = auto()
    STATE = auto()
    COUNTRY = auto()
    ZIPCODE = auto()
    DATE_OF_BIRTH = auto()
    CREATED_AT = auto()
    UPDATED_AT = auto()
    DELETED_AT = auto()
    DATE = auto()
    TIMESTAMP = auto()
    UUID = auto()
    URL = auto()
    IP_ADDRESS = auto()
    MAC_ADDRESS = auto()
    SSN = auto()
    CREDIT_CARD = auto()
    COMPANY = auto()
    JOB_TITLE = auto()
    USERNAME = auto()
    PASSWORD = auto()
    AMOUNT = auto()
    PRICE = auto()
    QUANTITY = auto()
    PERCENTAGE = auto()
    BOOLEAN = auto()
    ID = auto()
    FOREIGN_KEY = auto()
    TEXT = auto()
    UNKNOWN = auto()


@dataclass
class SemanticMatch:
    """Result of semantic type inference."""

    semantic_type: SemanticType
    confidence: float
    suggested_generator: str
    generator_params: dict[str, Any]


COLUMN_PATTERNS: list[tuple[re.Pattern[str], SemanticType, str, dict[str, Any]]] = [
    (re.compile(r"^e[-_]?mail$", re.I), SemanticType.EMAIL, "faker", {"provider": "email"}),
    (re.compile(r"email[-_]?addr", re.I), SemanticType.EMAIL, "faker", {"provider": "email"}),
    (re.compile(r"^phone$", re.I), SemanticType.PHONE, "faker", {"provider": "phone_number"}),
    (re.compile(r"phone[-_]?(num|number)", re.I), SemanticType.PHONE, "faker", {"provider": "phone_number"}),
    (re.compile(r"^tel$", re.I), SemanticType.PHONE, "faker", {"provider": "phone_number"}),
    (re.compile(r"^mobile$", re.I), SemanticType.PHONE, "faker", {"provider": "phone_number"}),
    (re.compile(r"^cell[-_]?(phone)?$", re.I), SemanticType.PHONE, "faker", {"provider": "phone_number"}),
    (re.compile(r"^first[-_]?name$", re.I), SemanticType.FIRST_NAME, "faker", {"provider": "first_name"}),
    (re.compile(r"^fname$", re.I), SemanticType.FIRST_NAME, "faker", {"provider": "first_name"}),
    (re.compile(r"^given[-_]?name$", re.I), SemanticType.FIRST_NAME, "faker", {"provider": "first_name"}),
    (re.compile(r"^last[-_]?name$", re.I), SemanticType.LAST_NAME, "faker", {"provider": "last_name"}),
    (re.compile(r"^lname$", re.I), SemanticType.LAST_NAME, "faker", {"provider": "last_name"}),
    (re.compile(r"^surname$", re.I), SemanticType.LAST_NAME, "faker", {"provider": "last_name"}),
    (re.compile(r"^family[-_]?name$", re.I), SemanticType.LAST_NAME, "faker", {"provider": "last_name"}),
    (re.compile(r"^(full[-_]?)?name$", re.I), SemanticType.FULL_NAME, "faker", {"provider": "name"}),
    (re.compile(r"^customer[-_]?name$", re.I), SemanticType.FULL_NAME, "faker", {"provider": "name"}),
    (re.compile(r"^user[-_]?name$", re.I), SemanticType.USERNAME, "faker", {"provider": "user_name"}),
    (re.compile(r"^username$", re.I), SemanticType.USERNAME, "faker", {"provider": "user_name"}),
    (re.compile(r"^login$", re.I), SemanticType.USERNAME, "faker", {"provider": "user_name"}),
    (re.compile(r"^addr(ess)?$", re.I), SemanticType.ADDRESS, "faker", {"provider": "address"}),
    (re.compile(r"^street[-_]?addr", re.I), SemanticType.STREET_ADDRESS, "faker", {"provider": "street_address"}),
    (re.compile(r"^street$", re.I), SemanticType.STREET_ADDRESS, "faker", {"provider": "street_address"}),
    (re.compile(r"^address[-_]?line", re.I), SemanticType.STREET_ADDRESS, "faker", {"provider": "street_address"}),
    (re.compile(r"^city$", re.I), SemanticType.CITY, "faker", {"provider": "city"}),
    (re.compile(r"^state$", re.I), SemanticType.STATE, "faker", {"provider": "state"}),
    (re.compile(r"^state[-_]?code$", re.I), SemanticType.STATE, "faker", {"provider": "state_abbr"}),
    (re.compile(r"^province$", re.I), SemanticType.STATE, "faker", {"provider": "state"}),
    (re.compile(r"^region$", re.I), SemanticType.STATE, "faker", {"provider": "state"}),
    (re.compile(r"^country$", re.I), SemanticType.COUNTRY, "faker", {"provider": "country"}),
    (re.compile(r"^country[-_]?code$", re.I), SemanticType.COUNTRY, "faker", {"provider": "country_code"}),
    (re.compile(r"^zip$", re.I), SemanticType.ZIPCODE, "faker", {"provider": "zipcode"}),
    (re.compile(r"^zip[-_]?code$", re.I), SemanticType.ZIPCODE, "faker", {"provider": "zipcode"}),
    (re.compile(r"^postal[-_]?code$", re.I), SemanticType.ZIPCODE, "faker", {"provider": "postcode"}),
    (re.compile(r"^postcode$", re.I), SemanticType.ZIPCODE, "faker", {"provider": "postcode"}),
    (re.compile(r"^dob$", re.I), SemanticType.DATE_OF_BIRTH, "faker", {"provider": "date_of_birth"}),
    (re.compile(r"^birth[-_]?date$", re.I), SemanticType.DATE_OF_BIRTH, "faker", {"provider": "date_of_birth"}),
    (re.compile(r"^date[-_]?of[-_]?birth$", re.I), SemanticType.DATE_OF_BIRTH, "faker", {"provider": "date_of_birth"}),
    (re.compile(r"^birthday$", re.I), SemanticType.DATE_OF_BIRTH, "faker", {"provider": "date_of_birth"}),
    (re.compile(r"^created[-_]?(at|on|date|time)?$", re.I), SemanticType.CREATED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^create[-_]?time$", re.I), SemanticType.CREATED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^updated[-_]?(at|on|date|time)?$", re.I), SemanticType.UPDATED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^modified[-_]?(at|on|date|time)?$", re.I), SemanticType.UPDATED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^last[-_]?modified$", re.I), SemanticType.UPDATED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^deleted[-_]?(at|on|date|time)?$", re.I), SemanticType.DELETED_AT, "faker", {"provider": "date_time"}),
    (re.compile(r"^uuid$", re.I), SemanticType.UUID, "faker", {"provider": "uuid4"}),
    (re.compile(r"^guid$", re.I), SemanticType.UUID, "faker", {"provider": "uuid4"}),
    (re.compile(r"^url$", re.I), SemanticType.URL, "faker", {"provider": "url"}),
    (re.compile(r"^website$", re.I), SemanticType.URL, "faker", {"provider": "url"}),
    (re.compile(r"^link$", re.I), SemanticType.URL, "faker", {"provider": "url"}),
    (re.compile(r"^ip[-_]?addr", re.I), SemanticType.IP_ADDRESS, "faker", {"provider": "ipv4"}),
    (re.compile(r"^ip$", re.I), SemanticType.IP_ADDRESS, "faker", {"provider": "ipv4"}),
    (re.compile(r"^ipv4$", re.I), SemanticType.IP_ADDRESS, "faker", {"provider": "ipv4"}),
    (re.compile(r"^ipv6$", re.I), SemanticType.IP_ADDRESS, "faker", {"provider": "ipv6"}),
    (re.compile(r"^mac[-_]?addr", re.I), SemanticType.MAC_ADDRESS, "faker", {"provider": "mac_address"}),
    (re.compile(r"^ssn$", re.I), SemanticType.SSN, "faker", {"provider": "ssn"}),
    (re.compile(r"^social[-_]?security", re.I), SemanticType.SSN, "faker", {"provider": "ssn"}),
    (re.compile(r"^credit[-_]?card", re.I), SemanticType.CREDIT_CARD, "faker", {"provider": "credit_card_number"}),
    (re.compile(r"^cc[-_]?num", re.I), SemanticType.CREDIT_CARD, "faker", {"provider": "credit_card_number"}),
    (re.compile(r"^card[-_]?number$", re.I), SemanticType.CREDIT_CARD, "faker", {"provider": "credit_card_number"}),
    (re.compile(r"^company$", re.I), SemanticType.COMPANY, "faker", {"provider": "company"}),
    (re.compile(r"^company[-_]?name$", re.I), SemanticType.COMPANY, "faker", {"provider": "company"}),
    (re.compile(r"^organization$", re.I), SemanticType.COMPANY, "faker", {"provider": "company"}),
    (re.compile(r"^employer$", re.I), SemanticType.COMPANY, "faker", {"provider": "company"}),
    (re.compile(r"^job[-_]?title$", re.I), SemanticType.JOB_TITLE, "faker", {"provider": "job"}),
    (re.compile(r"^title$", re.I), SemanticType.JOB_TITLE, "faker", {"provider": "job"}),
    (re.compile(r"^position$", re.I), SemanticType.JOB_TITLE, "faker", {"provider": "job"}),
    (re.compile(r"^occupation$", re.I), SemanticType.JOB_TITLE, "faker", {"provider": "job"}),
    (re.compile(r"^password$", re.I), SemanticType.PASSWORD, "faker", {"provider": "password"}),
    (re.compile(r"^pwd$", re.I), SemanticType.PASSWORD, "faker", {"provider": "password"}),
    (re.compile(r"^pass[-_]?hash$", re.I), SemanticType.PASSWORD, "faker", {"provider": "password"}),
    (re.compile(r"^amount$", re.I), SemanticType.AMOUNT, "uniform", {"min_value": 0, "max_value": 10000}),
    (re.compile(r"^total[-_]?amount$", re.I), SemanticType.AMOUNT, "uniform", {"min_value": 0, "max_value": 10000}),
    (re.compile(r"^price$", re.I), SemanticType.PRICE, "uniform", {"min_value": 0.01, "max_value": 999.99}),
    (re.compile(r"^unit[-_]?price$", re.I), SemanticType.PRICE, "uniform", {"min_value": 0.01, "max_value": 999.99}),
    (re.compile(r"^cost$", re.I), SemanticType.PRICE, "uniform", {"min_value": 0.01, "max_value": 999.99}),
    (re.compile(r"^qty$", re.I), SemanticType.QUANTITY, "uniform", {"min_value": 1, "max_value": 100}),
    (re.compile(r"^quantity$", re.I), SemanticType.QUANTITY, "uniform", {"min_value": 1, "max_value": 100}),
    (re.compile(r"^count$", re.I), SemanticType.QUANTITY, "uniform", {"min_value": 0, "max_value": 1000}),
    (re.compile(r"^percent", re.I), SemanticType.PERCENTAGE, "uniform", {"min_value": 0, "max_value": 100}),
    (re.compile(r"^rate$", re.I), SemanticType.PERCENTAGE, "uniform", {"min_value": 0, "max_value": 100}),
    (re.compile(r"^is[-_]", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"^has[-_]", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"^can[-_]", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"[-_]flag$", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"^active$", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"^enabled$", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False]}),
    (re.compile(r"^deleted$", re.I), SemanticType.BOOLEAN, "choice", {"values": [True, False], "weights": [0.1, 0.9]}),
    (re.compile(r"^id$", re.I), SemanticType.ID, "seq", {"start": 1, "step": 1}),
    (re.compile(r"[-_]id$", re.I), SemanticType.FOREIGN_KEY, "uniform", {"min_value": 1, "max_value": 10000}),
    (re.compile(r"^fk[-_]", re.I), SemanticType.FOREIGN_KEY, "uniform", {"min_value": 1, "max_value": 10000}),
]

DATA_TYPE_DEFAULTS: dict[str, tuple[str, dict[str, Any]]] = {
    "NUMBER": ("uniform", {"min_value": 0, "max_value": 10000}),
    "DECIMAL": ("uniform", {"min_value": 0, "max_value": 10000}),
    "NUMERIC": ("uniform", {"min_value": 0, "max_value": 10000}),
    "INT": ("uniform", {"min_value": 0, "max_value": 10000}),
    "INTEGER": ("uniform", {"min_value": 0, "max_value": 10000}),
    "BIGINT": ("uniform", {"min_value": 0, "max_value": 10000}),
    "SMALLINT": ("uniform", {"min_value": 0, "max_value": 1000}),
    "TINYINT": ("uniform", {"min_value": 0, "max_value": 255}),
    "BYTEINT": ("uniform", {"min_value": 0, "max_value": 127}),
    "FLOAT": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "FLOAT4": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "FLOAT8": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "DOUBLE": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "DOUBLE PRECISION": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "REAL": ("uniform", {"min_value": 0.0, "max_value": 1000.0}),
    "VARCHAR": ("faker", {"provider": "text"}),
    "CHAR": ("faker", {"provider": "text"}),
    "CHARACTER": ("faker", {"provider": "text"}),
    "STRING": ("faker", {"provider": "text"}),
    "TEXT": ("faker", {"provider": "text"}),
    "BINARY": ("faker", {"provider": "text"}),
    "VARBINARY": ("faker", {"provider": "text"}),
    "BOOLEAN": ("choice", {"values": [True, False]}),
    "DATE": ("faker", {"provider": "date"}),
    "DATETIME": ("faker", {"provider": "date_time"}),
    "TIME": ("faker", {"provider": "time"}),
    "TIMESTAMP": ("faker", {"provider": "date_time"}),
    "TIMESTAMP_LTZ": ("faker", {"provider": "date_time"}),
    "TIMESTAMP_NTZ": ("faker", {"provider": "date_time"}),
    "TIMESTAMP_TZ": ("faker", {"provider": "date_time"}),
}


def infer_semantic_type(
    column_name: str,
    data_type: str | None = None,
) -> SemanticMatch:
    """Infer semantic type from column name and optionally data type.

    Column name patterns take precedence over data type defaults.

    Args:
        column_name: The column name to analyze.
        data_type: Optional Snowflake data type.

    Returns:
        SemanticMatch with inferred type and suggested generator.
    """
    column_name_clean = column_name.strip().strip('"').strip("'")

    for pattern, sem_type, generator, params in COLUMN_PATTERNS:
        if pattern.search(column_name_clean):
            return SemanticMatch(
                semantic_type=sem_type,
                confidence=0.9,
                suggested_generator=generator,
                generator_params=params.copy(),
            )

    if data_type:
        base_type = data_type.split("(")[0].upper().strip()
        if base_type in DATA_TYPE_DEFAULTS:
            generator, params = DATA_TYPE_DEFAULTS[base_type]
            return SemanticMatch(
                semantic_type=SemanticType.UNKNOWN,
                confidence=0.5,
                suggested_generator=generator,
                generator_params=params.copy(),
            )

    return SemanticMatch(
        semantic_type=SemanticType.UNKNOWN,
        confidence=0.1,
        suggested_generator="faker",
        generator_params={"provider": "text"},
    )


def suggest_generator_for_column(
    column_name: str,
    data_type: str,
    is_nullable: bool = True,
    is_unique: bool = False,
    is_primary_key: bool = False,
) -> dict[str, Any]:
    """Suggest a generator configuration for a column.

    Args:
        column_name: The column name.
        data_type: The Snowflake data type.
        is_nullable: Whether the column allows nulls.
        is_unique: Whether the column has a unique constraint.
        is_primary_key: Whether the column is a primary key.

    Returns:
        Dictionary with generator configuration suitable for YAML.
    """
    if is_primary_key:
        return {
            "generator": "seq",
            "start": 1,
            "step": 1,
            "unique": True,
        }

    match = infer_semantic_type(column_name, data_type)

    config: dict[str, Any] = {
        "generator": match.suggested_generator,
        **match.generator_params,
    }

    if is_unique:
        config["unique"] = True

    return config


def batch_infer(
    columns: list[tuple[str, str]],
) -> dict[str, SemanticMatch]:
    """Batch infer semantic types for multiple columns.

    Args:
        columns: List of (column_name, data_type) tuples.

    Returns:
        Dictionary mapping column names to SemanticMatch results.
    """
    return {name: infer_semantic_type(name, dtype) for name, dtype in columns}
