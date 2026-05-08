"""Custom exceptions for sf-synth."""

from __future__ import annotations


class SynthError(Exception):
    """Base exception for sf-synth."""


class ConfigError(SynthError):
    """Raised when configuration is invalid."""


class DiscoveryError(SynthError):
    """Raised when schema discovery fails."""


class DAGError(SynthError):
    """Raised when DAG construction or traversal fails."""


class CycleError(DAGError):
    """Raised when a non-self-referential cycle is detected in the DAG."""


class GeneratorError(SynthError):
    """Raised when a generator fails to produce data."""


class UnsupportedTypeError(GeneratorError):
    """Raised when a column type is not supported."""

    def __init__(self, column: str, data_type: str) -> None:
        self.column = column
        self.data_type = data_type
        super().__init__(
            f"Unsupported data type '{data_type}' for column '{column}'. "
            f"Types VARIANT, OBJECT, ARRAY, GEOGRAPHY, GEOMETRY, VECTOR are not supported in v1."
        )


class ReferentialIntegrityError(SynthError):
    """Raised when referential integrity cannot be maintained."""


class SnowparkError(SynthError):
    """Raised when Snowpark operations fail."""


class FakerUnavailableError(SnowparkError):
    """Raised when Faker package is not available in Snowpark runtime."""

    def __init__(self) -> None:
        super().__init__(
            "Faker package is not available in the Snowpark runtime. "
            "Ensure your Snowflake account has access to the Anaconda channel "
            "and the Faker package is whitelisted."
        )
