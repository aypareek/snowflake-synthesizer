"""SQL-native generators for fast data generation.

These generators produce pure SQL expressions without UDFs,
which is significantly faster for large volumes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sf_synth.generators.base import ColumnGenerator, GeneratorRegistry

if TYPE_CHECKING:
    from snowflake.snowpark import Column, Session


@GeneratorRegistry.register("uniform")
class UniformGenerator(ColumnGenerator):
    """Generate uniformly distributed random numbers."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        min_value: float | int = 0,
        max_value: float | int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.min_value = min_value
        self.max_value = max_value

    @property
    def is_sql_native(self) -> bool:
        return True

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import uniform

        seed_val = self.seed if self.seed is not None else 0
        return uniform(self.min_value, self.max_value, seed_val).alias(self.column_name)


@GeneratorRegistry.register("seq")
class SeqGenerator(ColumnGenerator):
    """Generate sequential integer values using SEQ8()."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        start: int = 1,
        step: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.start = start
        self.step = step

    @property
    def is_sql_native(self) -> bool:
        return True

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import col, lit

        if self.start == 0 and self.step == 1:
            return col("_rownum").alias(self.column_name)
        return (col("_rownum") * lit(self.step) + lit(self.start)).alias(self.column_name)


@GeneratorRegistry.register("choice")
class ChoiceGenerator(ColumnGenerator):
    """Generate values from a fixed set of choices with optional weights."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        values: list[Any],
        weights: list[float] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.values = values
        self.weights = weights or [1.0 / len(values)] * len(values)
        self._validate_weights()

    def _validate_weights(self) -> None:
        if len(self.weights) != len(self.values):
            raise ValueError("Number of weights must match number of values")
        if abs(sum(self.weights) - 1.0) > 0.001:
            total = sum(self.weights)
            self.weights = [w / total for w in self.weights]

    @property
    def is_sql_native(self) -> bool:
        return True

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import lit, random, when

        seed_val = self.seed if self.seed is not None else 0
        rand_col = random(seed_val)

        cumulative = 0.0
        result: Column | None = None

        for i, (value, weight) in enumerate(zip(self.values, self.weights)):
            cumulative += weight
            if result is None:
                result = when(rand_col < cumulative, lit(value))
            elif i == len(self.values) - 1:
                result = result.otherwise(lit(value))
            else:
                result = result.when(rand_col < cumulative, lit(value))

        if result is None:
            raise ValueError("No values provided for choice generator")

        return result.alias(self.column_name)


@GeneratorRegistry.register("range")
class RangeGenerator(ColumnGenerator):
    """Generate values within a numeric or date range."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        min_value: Any,
        max_value: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.min_value = min_value
        self.max_value = max_value

    @property
    def is_sql_native(self) -> bool:
        return True

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import dateadd, lit, to_date, uniform

        seed_val = self.seed if self.seed is not None else 0
        data_type_upper = self.data_type.upper()

        if "DATE" in data_type_upper or "TIMESTAMP" in data_type_upper:
            min_date = self.min_value
            max_date = self.max_value
            if isinstance(min_date, str):
                min_date_expr = to_date(lit(min_date))
                max_date_expr = to_date(lit(max_date))
            else:
                min_date_expr = lit(min_date)
                max_date_expr = lit(max_date)

            days_diff = session.sql(
                f"SELECT DATEDIFF('day', '{self.min_value}', '{self.max_value}')"
            ).collect()[0][0]

            random_days = uniform(0, days_diff, seed_val)
            return dateadd("day", random_days, min_date_expr).alias(self.column_name)
        else:
            return uniform(self.min_value, self.max_value, seed_val).alias(self.column_name)


@GeneratorRegistry.register("regex")
class RegexGenerator(ColumnGenerator):
    """Generate strings matching a regex pattern.

    Note: This falls back to UDF for complex patterns.
    Simple patterns like [A-Z]{3}-[0-9]{4} can be done in SQL.
    """

    def __init__(
        self,
        column_name: str,
        data_type: str,
        pattern: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.pattern = pattern
        self._is_simple = self._check_simple_pattern()

    def _check_simple_pattern(self) -> bool:
        import re

        simple_pattern = r"^[\[\]A-Za-z0-9\-\{\}]+$"
        return bool(re.match(simple_pattern, self.pattern))

    @property
    def is_sql_native(self) -> bool:
        return self._is_simple

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import call_udf, lit

        if self._is_simple:
            return self._generate_simple_sql(session)
        else:
            return call_udf("sf_synth_regex_generate", lit(self.pattern)).alias(self.column_name)

    def _generate_simple_sql(self, session: Session) -> Column:
        """Generate simple regex patterns using SQL string functions."""
        import re

        from snowflake.snowpark.functions import concat, lit, substring, uniform

        parts: list[Column] = []
        chars_upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        chars_lower = "abcdefghijklmnopqrstuvwxyz"
        chars_digit = "0123456789"

        pattern = self.pattern
        i = 0
        seed_val = self.seed if self.seed is not None else 0

        while i < len(pattern):
            if pattern[i] == "[":
                end_bracket = pattern.index("]", i)
                char_class = pattern[i + 1 : end_bracket]

                repeat = 1
                if end_bracket + 1 < len(pattern) and pattern[end_bracket + 1] == "{":
                    repeat_end = pattern.index("}", end_bracket + 1)
                    repeat = int(pattern[end_bracket + 2 : repeat_end])
                    i = repeat_end + 1
                else:
                    i = end_bracket + 1

                if char_class == "A-Z":
                    chars = chars_upper
                elif char_class == "a-z":
                    chars = chars_lower
                elif char_class == "0-9":
                    chars = chars_digit
                elif char_class == "A-Za-z":
                    chars = chars_upper + chars_lower
                else:
                    chars = char_class.replace("-", "")

                for _ in range(repeat):
                    idx = uniform(1, len(chars), seed_val)
                    parts.append(substring(lit(chars), idx, lit(1)))
            else:
                parts.append(lit(pattern[i]))
                i += 1

        if len(parts) == 1:
            return parts[0].alias(self.column_name)
        return concat(*parts).alias(self.column_name)
