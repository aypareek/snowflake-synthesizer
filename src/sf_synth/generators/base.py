"""Base classes for column generators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Column, Session


class ColumnGenerator(ABC):
    """Abstract base class for column data generators.

    Generators produce Snowpark Column expressions that can be used
    to generate synthetic data for a specific column.
    """

    def __init__(
        self,
        column_name: str,
        data_type: str,
        nullable: bool = True,
        unique: bool = False,
        seed: int | None = None,
    ) -> None:
        self.column_name = column_name
        self.data_type = data_type
        self.nullable = nullable
        self.unique = unique
        self.seed = seed

    @abstractmethod
    def generate(self, session: Session, row_count: int) -> Column:
        """Generate a Snowpark Column expression for this generator.

        Args:
            session: Active Snowpark session.
            row_count: Number of rows being generated.

        Returns:
            A Snowpark Column expression that generates values.
        """

    @property
    @abstractmethod
    def is_sql_native(self) -> bool:
        """Whether this generator uses pure SQL (fast path) or UDFs (slow path)."""

    def with_null_ratio(self, null_ratio: float, column: Column) -> Column:
        """Apply null ratio to a column if nullable.

        Args:
            null_ratio: Ratio of nulls to generate (0.0 to 1.0).
            column: The column expression to potentially nullify.

        Returns:
            Column with nulls applied.
        """
        if not self.nullable or null_ratio <= 0:
            return column

        from snowflake.snowpark.functions import iff, random

        return iff(random() < null_ratio, None, column)


class GeneratorRegistry:
    """Registry for mapping generator types to implementations."""

    _generators: dict[str, type[ColumnGenerator]] = {}

    @classmethod
    def register(cls, name: str) -> Any:
        """Decorator to register a generator class.

        Args:
            name: The generator type name (e.g., 'faker', 'choice', 'distribution').
        """

        def decorator(generator_cls: type[ColumnGenerator]) -> type[ColumnGenerator]:
            cls._generators[name] = generator_cls
            return generator_cls

        return decorator

    @classmethod
    def get(cls, name: str) -> type[ColumnGenerator] | None:
        """Get a generator class by name."""
        return cls._generators.get(name)

    @classmethod
    def list_generators(cls) -> list[str]:
        """List all registered generator names."""
        return list(cls._generators.keys())
