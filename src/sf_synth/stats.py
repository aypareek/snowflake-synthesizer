"""Distribution statistics sampling for high-fidelity synthesis.

Uses Snowflake's approximate aggregation functions to efficiently
compute column statistics without exposing actual PII values.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Session


@dataclass
class NumericStats:
    """Statistics for numeric columns."""

    min_value: float | int
    max_value: float | int
    mean: float | None = None
    stddev: float | None = None
    percentiles: dict[float, float] = field(default_factory=dict)


@dataclass
class CategoricalStats:
    """Statistics for categorical/low-cardinality columns."""

    values: list[Any]
    counts: list[int]
    total_count: int

    @property
    def weights(self) -> list[float]:
        """Calculate normalized weights from counts."""
        return [c / self.total_count for c in self.counts]


@dataclass
class ColumnStatistics:
    """Complete statistics for a column."""

    column_name: str
    table_fqn: str
    data_type: str
    row_count: int
    null_count: int
    distinct_count: int
    is_low_cardinality: bool = False
    numeric_stats: NumericStats | None = None
    categorical_stats: CategoricalStats | None = None

    @property
    def null_ratio(self) -> float:
        """Calculate null ratio."""
        return self.null_count / max(self.row_count, 1)


class StatsSampler:
    """Samples distribution statistics from source columns."""

    CARDINALITY_THRESHOLD = 100
    CARDINALITY_RATIO_THRESHOLD = 0.01
    PERCENTILE_POINTS = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]
    TOP_K_LIMIT = 100

    def __init__(self, session: Session) -> None:
        self.session = session
        self._cache: dict[str, ColumnStatistics] = {}
        self._temp_tables: list[str] = []

    def sample_column(
        self,
        database: str,
        schema: str,
        table: str,
        column: str,
        force_refresh: bool = False,
    ) -> ColumnStatistics:
        """Sample statistics for a single column.

        Args:
            database: Database name.
            schema: Schema name.
            table: Table name.
            column: Column name.
            force_refresh: Force re-sampling even if cached.

        Returns:
            ColumnStatistics with sampled distribution info.
        """
        cache_key = f"{database}.{schema}.{table}.{column}"

        if not force_refresh and cache_key in self._cache:
            return self._cache[cache_key]

        fqn = f'"{database}"."{schema}"."{table}"'
        col_quoted = f'"{column}"'

        basic_sql = f"""
        SELECT
            COUNT(*) AS row_count,
            COUNT(*) - COUNT({col_quoted}) AS null_count,
            APPROX_COUNT_DISTINCT({col_quoted}) AS distinct_count,
            TYPEOF({col_quoted}) AS data_type,
            MIN({col_quoted}) AS min_value,
            MAX({col_quoted}) AS max_value,
            AVG(TRY_CAST({col_quoted} AS DOUBLE)) AS mean_value,
            STDDEV(TRY_CAST({col_quoted} AS DOUBLE)) AS stddev_value
        FROM {fqn}
        """

        basic_result = self.session.sql(basic_sql).collect()[0]

        stats = ColumnStatistics(
            column_name=column,
            table_fqn=f"{database}.{schema}.{table}",
            data_type=str(basic_result["DATA_TYPE"]),
            row_count=int(basic_result["ROW_COUNT"]),
            null_count=int(basic_result["NULL_COUNT"]),
            distinct_count=int(basic_result["DISTINCT_COUNT"]),
        )

        cardinality_ratio = stats.distinct_count / max(stats.row_count, 1)
        stats.is_low_cardinality = (
            stats.distinct_count <= self.CARDINALITY_THRESHOLD
            or cardinality_ratio < self.CARDINALITY_RATIO_THRESHOLD
        )

        if stats.is_low_cardinality:
            self._sample_categorical(fqn, col_quoted, stats)
        else:
            self._sample_numeric(fqn, col_quoted, basic_result, stats)

        self._cache[cache_key] = stats
        return stats

    def _sample_categorical(
        self,
        fqn: str,
        col_quoted: str,
        stats: ColumnStatistics,
    ) -> None:
        """Sample top-k values for categorical columns."""
        top_k_sql = f"""
        SELECT {col_quoted} AS value, COUNT(*) AS cnt
        FROM {fqn}
        WHERE {col_quoted} IS NOT NULL
        GROUP BY {col_quoted}
        ORDER BY cnt DESC
        LIMIT {self.TOP_K_LIMIT}
        """

        result = self.session.sql(top_k_sql).collect()

        values = [row["VALUE"] for row in result]
        counts = [int(row["CNT"]) for row in result]
        total = sum(counts)

        stats.categorical_stats = CategoricalStats(
            values=values,
            counts=counts,
            total_count=total,
        )

    def _sample_numeric(
        self,
        fqn: str,
        col_quoted: str,
        basic_result: Any,
        stats: ColumnStatistics,
    ) -> None:
        """Sample percentiles for numeric columns."""
        data_type_upper = stats.data_type.upper()
        is_numeric = any(
            t in data_type_upper
            for t in ["NUMBER", "FLOAT", "INT", "DECIMAL", "DOUBLE", "REAL"]
        )
        is_temporal = any(t in data_type_upper for t in ["DATE", "TIME", "TIMESTAMP"])

        if not is_numeric and not is_temporal:
            return

        percentile_cols = ", ".join(
            f"APPROX_PERCENTILE({col_quoted}, {p}) AS p{int(p*100)}"
            for p in self.PERCENTILE_POINTS
        )

        percentile_sql = f"""
        SELECT {percentile_cols}
        FROM {fqn}
        WHERE {col_quoted} IS NOT NULL
        """

        try:
            perc_result = self.session.sql(percentile_sql).collect()[0]

            percentiles = {}
            for p in self.PERCENTILE_POINTS:
                key = f"P{int(p * 100)}"
                if perc_result[key] is not None:
                    try:
                        percentiles[p] = float(perc_result[key])
                    except (TypeError, ValueError):
                        pass

            min_val = basic_result["MIN_VALUE"]
            max_val = basic_result["MAX_VALUE"]
            mean_val = basic_result["MEAN_VALUE"]
            stddev_val = basic_result["STDDEV_VALUE"]

            try:
                min_float = float(min_val) if min_val is not None else 0.0
                max_float = float(max_val) if max_val is not None else 100.0
            except (TypeError, ValueError):
                min_float = 0.0
                max_float = 100.0

            stats.numeric_stats = NumericStats(
                min_value=min_float,
                max_value=max_float,
                mean=float(mean_val) if mean_val is not None else None,
                stddev=float(stddev_val) if stddev_val is not None else None,
                percentiles=percentiles,
            )
        except Exception:
            pass

    def materialize_stats_table(
        self,
        stats: ColumnStatistics,
        database: str,
        schema: str,
    ) -> str:
        """Materialize stats to a transient table for SQL-based sampling.

        Args:
            stats: Column statistics to materialize.
            database: Target database.
            schema: Target schema.

        Returns:
            Fully qualified name of the transient stats table.
        """
        table_id = uuid.uuid4().hex[:8]
        table_name = f"SF_SYNTH_STATS_{stats.column_name}_{table_id}"
        fqn = f'"{database}"."{schema}"."{table_name}"'

        if stats.categorical_stats:
            values_data = []
            for val, count in zip(
                stats.categorical_stats.values,
                stats.categorical_stats.counts,
            ):
                values_data.append((val, count))

            create_sql = f"""
            CREATE OR REPLACE TRANSIENT TABLE {fqn} (
                VALUE VARIANT,
                COUNT NUMBER,
                CUMULATIVE_WEIGHT FLOAT
            )
            """
            self.session.sql(create_sql).collect()

            total = stats.categorical_stats.total_count
            cumulative = 0.0

            for val, count in values_data:
                weight = count / total
                cumulative += weight
                val_str = f"'{val}'" if isinstance(val, str) else str(val)
                insert_sql = f"""
                INSERT INTO {fqn} (VALUE, COUNT, CUMULATIVE_WEIGHT)
                SELECT PARSE_JSON('{val_str}'), {count}, {cumulative}
                """
                self.session.sql(insert_sql).collect()

        elif stats.numeric_stats:
            create_sql = f"""
            CREATE OR REPLACE TRANSIENT TABLE {fqn} (
                PERCENTILE FLOAT,
                VALUE FLOAT
            )
            """
            self.session.sql(create_sql).collect()

            points = [(0.0, stats.numeric_stats.min_value)]
            points.extend(
                (p, v) for p, v in stats.numeric_stats.percentiles.items()
            )
            points.append((1.0, stats.numeric_stats.max_value))
            points.sort(key=lambda x: x[0])

            for p, v in points:
                insert_sql = f"""
                INSERT INTO {fqn} (PERCENTILE, VALUE) VALUES ({p}, {v})
                """
                self.session.sql(insert_sql).collect()

        self._temp_tables.append(fqn)
        return fqn

    def cleanup(self) -> None:
        """Drop all transient stats tables."""
        for fqn in self._temp_tables:
            try:
                self.session.sql(f"DROP TABLE IF EXISTS {fqn}").collect()
            except Exception:
                pass
        self._temp_tables.clear()

    def batch_sample(
        self,
        columns: list[tuple[str, str, str, str]],
    ) -> dict[str, ColumnStatistics]:
        """Batch sample statistics for multiple columns.

        Args:
            columns: List of (database, schema, table, column) tuples.

        Returns:
            Dictionary mapping column keys to statistics.
        """
        results = {}
        for database, schema, table, column in columns:
            key = f"{database}.{schema}.{table}.{column}"
            try:
                results[key] = self.sample_column(database, schema, table, column)
            except Exception:
                pass
        return results


def generate_sampling_sql(stats: ColumnStatistics, alias: str) -> str:
    """Generate SQL expression to sample from statistics.

    Args:
        stats: Column statistics to sample from.
        alias: Column alias for the result.

    Returns:
        SQL expression string.
    """
    if stats.categorical_stats:
        cases = []
        cumulative = 0.0
        for val, count in zip(
            stats.categorical_stats.values,
            stats.categorical_stats.counts,
        ):
            weight = count / stats.categorical_stats.total_count
            cumulative += weight
            val_str = f"'{val}'" if isinstance(val, str) else str(val)
            cases.append(f"WHEN RANDOM() < {cumulative} THEN {val_str}")

        default_val = stats.categorical_stats.values[-1] if stats.categorical_stats.values else "NULL"
        default_str = f"'{default_val}'" if isinstance(default_val, str) else str(default_val)

        sql = f"CASE {' '.join(cases)} ELSE {default_str} END AS {alias}"
        return sql

    elif stats.numeric_stats:
        min_v = stats.numeric_stats.min_value
        max_v = stats.numeric_stats.max_value
        sql = f"UNIFORM({min_v}::FLOAT, {max_v}::FLOAT, RANDOM()) AS {alias}"
        return sql

    return f"NULL AS {alias}"
