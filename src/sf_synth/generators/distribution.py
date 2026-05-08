"""Distribution-preserving generators.

These generators sample from the statistical distribution of existing
production data without exposing actual PII values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sf_synth.generators.base import ColumnGenerator, GeneratorRegistry

if TYPE_CHECKING:
    from snowflake.snowpark import Column, Session


@dataclass
class ColumnStats:
    """Statistics for a single column."""

    column_name: str
    data_type: str
    row_count: int
    null_count: int
    distinct_count: int
    min_value: Any | None = None
    max_value: Any | None = None
    percentiles: list[float] | None = None
    percentile_values: list[Any] | None = None
    top_k_values: list[tuple[Any, int]] | None = None
    is_low_cardinality: bool = False


def compute_column_stats(
    session: Session,
    database: str,
    schema: str,
    table: str,
    column: str,
) -> ColumnStats:
    """Compute statistics for a column using approximate functions.

    Uses APPROX_TOP_K, APPROX_PERCENTILE, and HLL for efficient computation.
    """
    fqn = f'"{database}"."{schema}"."{table}"'
    col_quoted = f'"{column}"'

    basic_stats_sql = f"""
    SELECT
        COUNT(*) AS row_count,
        COUNT(*) - COUNT({col_quoted}) AS null_count,
        APPROX_COUNT_DISTINCT({col_quoted}) AS distinct_count,
        MIN({col_quoted}) AS min_value,
        MAX({col_quoted}) AS max_value,
        TYPEOF({col_quoted}) AS data_type
    FROM {fqn}
    """
    basic_result = session.sql(basic_stats_sql).collect()[0]

    stats = ColumnStats(
        column_name=column,
        data_type=str(basic_result["DATA_TYPE"]),
        row_count=int(basic_result["ROW_COUNT"]),
        null_count=int(basic_result["NULL_COUNT"]),
        distinct_count=int(basic_result["DISTINCT_COUNT"]),
        min_value=basic_result["MIN_VALUE"],
        max_value=basic_result["MAX_VALUE"],
    )

    cardinality_ratio = stats.distinct_count / max(stats.row_count, 1)
    stats.is_low_cardinality = cardinality_ratio < 0.01 or stats.distinct_count <= 100

    if stats.is_low_cardinality:
        top_k_sql = f"""
        SELECT VALUE, COUNT AS CNT
        FROM TABLE(APPROX_TOP_K({col_quoted}, 100))
        OVER (SELECT {col_quoted} FROM {fqn} WHERE {col_quoted} IS NOT NULL)
        ORDER BY CNT DESC
        """
        try:
            top_k_result = session.sql(top_k_sql).collect()
            stats.top_k_values = [(row["VALUE"], int(row["CNT"])) for row in top_k_result]
        except Exception:
            top_k_alt_sql = f"""
            SELECT {col_quoted} AS VALUE, COUNT(*) AS CNT
            FROM {fqn}
            WHERE {col_quoted} IS NOT NULL
            GROUP BY {col_quoted}
            ORDER BY CNT DESC
            LIMIT 100
            """
            top_k_result = session.sql(top_k_alt_sql).collect()
            stats.top_k_values = [(row["VALUE"], int(row["CNT"])) for row in top_k_result]
    else:
        data_type_upper = stats.data_type.upper()
        if any(t in data_type_upper for t in ["NUMBER", "FLOAT", "INT", "DECIMAL", "DATE", "TIME"]):
            percentiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
            percentile_exprs = ", ".join(
                f"APPROX_PERCENTILE({col_quoted}, {p}) AS p{int(p*100)}" for p in percentiles
            )
            percentile_sql = f"""
            SELECT {percentile_exprs}
            FROM {fqn}
            WHERE {col_quoted} IS NOT NULL
            """
            try:
                perc_result = session.sql(percentile_sql).collect()[0]
                stats.percentiles = percentiles
                stats.percentile_values = [perc_result[f"P{int(p*100)}"] for p in percentiles]
            except Exception:
                pass

    return stats


@GeneratorRegistry.register("distribution")
class DistributionGenerator(ColumnGenerator):
    """Generate values that match the distribution of source data."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        source: str,
        **kwargs: Any,
    ) -> None:
        """Initialize distribution generator.

        Args:
            column_name: Target column name.
            data_type: Target data type.
            source: Fully qualified source column (DB.SCHEMA.TABLE.COLUMN).
            **kwargs: Additional generator options.
        """
        super().__init__(column_name, data_type, **kwargs)
        self.source = source
        self._stats: ColumnStats | None = None
        self._parse_source()

    def _parse_source(self) -> None:
        parts = self.source.split(".")
        if len(parts) != 4:
            raise ValueError(
                f"Source must be fully qualified as DB.SCHEMA.TABLE.COLUMN, got: {self.source}"
            )
        self.source_database = parts[0]
        self.source_schema = parts[1]
        self.source_table = parts[2]
        self.source_column = parts[3]

    @property
    def is_sql_native(self) -> bool:
        return True

    def _compute_stats(self, session: Session) -> ColumnStats:
        if self._stats is None:
            self._stats = compute_column_stats(
                session,
                self.source_database,
                self.source_schema,
                self.source_table,
                self.source_column,
            )
        return self._stats

    def generate(self, session: Session, row_count: int) -> Column:
        stats = self._compute_stats(session)

        if stats.is_low_cardinality and stats.top_k_values:
            return self._generate_from_top_k(session, stats)
        elif stats.percentiles and stats.percentile_values:
            return self._generate_from_percentiles(session, stats)
        else:
            return self._generate_uniform(session, stats)

    def _generate_from_top_k(self, session: Session, stats: ColumnStats) -> Column:
        """Generate values based on top-k frequency distribution."""
        from snowflake.snowpark.functions import lit, random, when

        if not stats.top_k_values:
            raise ValueError("No top-k values available")

        total_count = sum(count for _, count in stats.top_k_values)
        weights = [count / total_count for _, count in stats.top_k_values]

        seed_val = self.seed if self.seed is not None else 0
        rand_col = random(seed_val)

        cumulative = 0.0
        result: Column | None = None

        for i, ((value, _), weight) in enumerate(zip(stats.top_k_values, weights)):
            cumulative += weight
            if result is None:
                result = when(rand_col < cumulative, lit(value))
            elif i == len(stats.top_k_values) - 1:
                result = result.otherwise(lit(value))
            else:
                result = result.when(rand_col < cumulative, lit(value))

        if result is None:
            raise ValueError("Failed to build distribution expression")

        null_ratio = stats.null_count / max(stats.row_count, 1)
        if null_ratio > 0 and self.nullable:
            result = self.with_null_ratio(null_ratio, result)

        return result.alias(self.column_name)

    def _generate_from_percentiles(self, session: Session, stats: ColumnStats) -> Column:
        """Generate values using piecewise-uniform sampling from percentiles."""
        from snowflake.snowpark.functions import lit, random, uniform, when

        if not stats.percentiles or not stats.percentile_values:
            return self._generate_uniform(session, stats)

        seed_val = self.seed if self.seed is not None else 0
        rand_col = random(seed_val)

        all_values = [stats.min_value] + stats.percentile_values + [stats.max_value]
        all_percentiles = [0.0] + stats.percentiles + [1.0]

        result: Column | None = None

        for i in range(len(all_percentiles) - 1):
            lower_p = all_percentiles[i]
            upper_p = all_percentiles[i + 1]
            lower_v = all_values[i]
            upper_v = all_values[i + 1]

            if lower_v is None or upper_v is None:
                continue

            bucket_value = uniform(float(lower_v), float(upper_v), seed_val)

            if result is None:
                result = when(rand_col < upper_p, bucket_value)
            elif i == len(all_percentiles) - 2:
                result = result.otherwise(bucket_value)
            else:
                result = result.when(rand_col < upper_p, bucket_value)

        if result is None:
            return self._generate_uniform(session, stats)

        null_ratio = stats.null_count / max(stats.row_count, 1)
        if null_ratio > 0 and self.nullable:
            result = self.with_null_ratio(null_ratio, result)

        return result.alias(self.column_name)

    def _generate_uniform(self, session: Session, stats: ColumnStats) -> Column:
        """Fallback: generate uniform distribution between min and max."""
        from snowflake.snowpark.functions import uniform

        seed_val = self.seed if self.seed is not None else 0

        min_val = stats.min_value if stats.min_value is not None else 0
        max_val = stats.max_value if stats.max_value is not None else 100

        try:
            result = uniform(float(min_val), float(max_val), seed_val)
        except (TypeError, ValueError):
            from snowflake.snowpark.functions import lit

            result = lit(min_val)

        null_ratio = stats.null_count / max(stats.row_count, 1)
        if null_ratio > 0 and self.nullable:
            result = self.with_null_ratio(null_ratio, result)

        return result.alias(self.column_name)
