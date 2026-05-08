"""Referential Integrity Manager.

Handles parent-key materialization and FK value sampling with
support for uniform and skewed (Zipf) distributions.
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sf_synth.errors import ReferentialIntegrityError

if TYPE_CHECKING:
    from snowflake.snowpark import Column, DataFrame, Session

    from sf_synth.dag import ForeignKeyEdge


@dataclass
class ParentKeyCache:
    """Cache of parent table primary key values."""

    parent_table_fqn: str
    key_columns: list[str]
    key_table_fqn: str
    key_count: int
    is_materialized: bool = False


@dataclass
class SelfRefUpdate:
    """Pending self-referential FK update."""

    table_fqn: str
    fk_column: str
    pk_column: str
    null_ratio: float = 0.0
    skew: str = "uniform"
    skew_param: float = 1.5


class RIManager:
    """Manages referential integrity during data generation."""

    def __init__(
        self,
        session: Session,
        target_database: str,
        target_schema: str,
    ) -> None:
        self.session = session
        self.target_database = target_database
        self.target_schema = target_schema
        self._key_caches: dict[str, ParentKeyCache] = {}
        self._pending_self_refs: list[SelfRefUpdate] = []
        self._temp_tables: list[str] = []

    def materialize_parent_keys(
        self,
        parent_table_fqn: str,
        key_columns: list[str],
    ) -> ParentKeyCache:
        """Materialize parent table keys to a transient table.

        Args:
            parent_table_fqn: Fully qualified parent table name.
            key_columns: Primary key column names.

        Returns:
            ParentKeyCache with materialized keys info.
        """
        if parent_table_fqn in self._key_caches:
            return self._key_caches[parent_table_fqn]

        table_id = uuid.uuid4().hex[:8]
        key_table_name = f"SF_SYNTH_KEYS_{table_id}"
        key_table_fqn = f'"{self.target_database}"."{self.target_schema}"."{key_table_name}"'

        col_list = ", ".join(f'"{c}"' for c in key_columns)
        create_sql = f"""
        CREATE OR REPLACE TRANSIENT TABLE {key_table_fqn} AS
        SELECT DISTINCT {col_list}, ROW_NUMBER() OVER (ORDER BY {col_list}) AS _key_idx
        FROM {parent_table_fqn}
        """

        self.session.sql(create_sql).collect()
        self._temp_tables.append(key_table_fqn)

        count_result = self.session.sql(f"SELECT COUNT(*) AS CNT FROM {key_table_fqn}").collect()
        key_count = int(count_result[0]["CNT"])

        cache = ParentKeyCache(
            parent_table_fqn=parent_table_fqn,
            key_columns=key_columns,
            key_table_fqn=key_table_fqn,
            key_count=key_count,
            is_materialized=True,
        )

        self._key_caches[parent_table_fqn] = cache
        return cache

    def generate_fk_column(
        self,
        edge: ForeignKeyEdge,
        row_count: int,
        seed: int | None = None,
    ) -> Column:
        """Generate FK column expression sampling from parent keys.

        Args:
            edge: Foreign key edge definition.
            row_count: Number of rows being generated.
            seed: Random seed for reproducibility.

        Returns:
            Snowpark Column expression for the FK values.
        """
        from snowflake.snowpark.functions import col, iff, lit, random

        if edge.parent_table not in self._key_caches:
            raise ReferentialIntegrityError(
                f"Parent table {edge.parent_table} not yet materialized. "
                f"Ensure tables are generated in topological order."
            )

        cache = self._key_caches[edge.parent_table]

        if cache.key_count == 0:
            raise ReferentialIntegrityError(
                f"Parent table {edge.parent_table} has no rows. "
                f"Cannot generate FK values."
            )

        seed_val = seed if seed is not None else 0

        if edge.skew == "zipf":
            index_expr = self._zipf_sample_expr(cache.key_count, edge.skew_param, seed_val)
        else:
            index_expr = self._uniform_sample_expr(cache.key_count, seed_val)

        if len(edge.child_columns) == 1 and len(edge.parent_columns) == 1:
            parent_col = edge.parent_columns[0]

            fk_expr = self.session.sql(f"""
            SELECT "{parent_col}"
            FROM {cache.key_table_fqn}
            WHERE _key_idx = {{index}}
            """.replace("{index}", f"({index_expr})"))

            fk_column = col(parent_col)
        else:
            fk_column = col(edge.parent_columns[0])

        if edge.null_ratio > 0:
            fk_column = iff(random(seed_val) < edge.null_ratio, lit(None), fk_column)

        return fk_column

    def generate_fk_join_sql(
        self,
        edge: ForeignKeyEdge,
        base_alias: str = "base",
        seed: int | None = None,
    ) -> str:
        """Generate SQL for joining with parent keys.

        Args:
            edge: Foreign key edge definition.
            base_alias: Alias for the base (child) table being generated.
            seed: Random seed.

        Returns:
            SQL fragment for the FK join.
        """
        if edge.parent_table not in self._key_caches:
            raise ReferentialIntegrityError(
                f"Parent table {edge.parent_table} not yet materialized."
            )

        cache = self._key_caches[edge.parent_table]
        seed_val = seed if seed is not None else 0

        if edge.skew == "zipf":
            index_expr = self._zipf_sample_sql(cache.key_count, edge.skew_param, seed_val)
        else:
            index_expr = self._uniform_sample_sql(cache.key_count, seed_val)

        parent_alias = f"pk_{uuid.uuid4().hex[:4]}"

        col_mappings = []
        for child_col, parent_col in zip(edge.child_columns, edge.parent_columns):
            if edge.null_ratio > 0:
                col_mappings.append(
                    f'IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {edge.null_ratio}, '
                    f'NULL, {parent_alias}."{parent_col}") AS "{child_col}"'
                )
            else:
                col_mappings.append(f'{parent_alias}."{parent_col}" AS "{child_col}"')

        join_sql = f"""
        INNER JOIN (
            SELECT *, ({index_expr}) AS _sample_idx
            FROM {cache.key_table_fqn}
        ) {parent_alias}
        ON {base_alias}._rownum % {cache.key_count} + 1 = {parent_alias}._key_idx
        """

        return join_sql, col_mappings

    def _uniform_sample_expr(self, key_count: int, seed: int) -> str:
        """Generate uniform sampling expression."""
        return f"UNIFORM(1, {key_count}, RANDOM({seed}))"

    def _uniform_sample_sql(self, key_count: int, seed: int) -> str:
        """Generate uniform sampling SQL."""
        return f"UNIFORM(1, {key_count}, RANDOM({seed}))"

    def _zipf_sample_expr(self, key_count: int, s: float, seed: int) -> str:
        """Generate Zipf-weighted sampling expression.

        Uses inverse transform sampling for Zipf distribution.
        """
        return f"""
        LEAST(
            {key_count},
            GREATEST(
                1,
                FLOOR(
                    POW(
                        UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed})),
                        -1.0 / {s}
                    )
                )::INTEGER
            )
        )
        """

    def _zipf_sample_sql(self, key_count: int, s: float, seed: int) -> str:
        """Generate Zipf-weighted sampling SQL."""
        return self._zipf_sample_expr(key_count, s, seed)

    def queue_self_ref_update(
        self,
        table_fqn: str,
        fk_column: str,
        pk_column: str,
        null_ratio: float = 0.0,
        skew: str = "uniform",
        skew_param: float = 1.5,
    ) -> None:
        """Queue a self-referential FK update for later execution.

        Args:
            table_fqn: Table with self-referential FK.
            fk_column: Foreign key column name.
            pk_column: Primary key column name.
            null_ratio: Ratio of null FK values.
            skew: Distribution type.
            skew_param: Skew parameter.
        """
        self._pending_self_refs.append(
            SelfRefUpdate(
                table_fqn=table_fqn,
                fk_column=fk_column,
                pk_column=pk_column,
                null_ratio=null_ratio,
                skew=skew,
                skew_param=skew_param,
            )
        )

    def execute_self_ref_updates(self, seed: int | None = None) -> None:
        """Execute all queued self-referential FK updates.

        Args:
            seed: Random seed for reproducibility.
        """
        seed_val = seed if seed is not None else 0

        for update in self._pending_self_refs:
            count_sql = f'SELECT COUNT(*) AS CNT FROM {update.table_fqn}'
            count_result = self.session.sql(count_sql).collect()
            row_count = int(count_result[0]["CNT"])

            if row_count <= 1:
                continue

            if update.skew == "zipf":
                sample_expr = self._zipf_sample_sql(row_count, update.skew_param, seed_val)
            else:
                sample_expr = self._uniform_sample_sql(row_count, seed_val)

            if update.null_ratio > 0:
                value_expr = f"""
                IFF(
                    UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {update.null_ratio},
                    NULL,
                    (
                        SELECT "{update.pk_column}"
                        FROM (
                            SELECT "{update.pk_column}", ROW_NUMBER() OVER (ORDER BY "{update.pk_column}") AS _rn
                            FROM {update.table_fqn}
                        )
                        WHERE _rn = {sample_expr}
                        AND "{update.pk_column}" != t."{update.pk_column}"
                        LIMIT 1
                    )
                )
                """
            else:
                value_expr = f"""
                (
                    SELECT "{update.pk_column}"
                    FROM (
                        SELECT "{update.pk_column}", ROW_NUMBER() OVER (ORDER BY "{update.pk_column}") AS _rn
                        FROM {update.table_fqn}
                    )
                    WHERE _rn = {sample_expr}
                    AND "{update.pk_column}" != t."{update.pk_column}"
                    LIMIT 1
                )
                """

            update_sql = f"""
            UPDATE {update.table_fqn} t
            SET "{update.fk_column}" = {value_expr}
            """

            try:
                self.session.sql(update_sql).collect()
            except Exception as e:
                alt_update_sql = f"""
                MERGE INTO {update.table_fqn} t
                USING (
                    SELECT
                        a."{update.pk_column}" AS target_pk,
                        b."{update.pk_column}" AS parent_pk
                    FROM (
                        SELECT "{update.pk_column}", ROW_NUMBER() OVER (ORDER BY RANDOM({seed_val})) AS _rn
                        FROM {update.table_fqn}
                    ) a
                    JOIN (
                        SELECT "{update.pk_column}", ROW_NUMBER() OVER (ORDER BY "{update.pk_column}") AS _parent_rn
                        FROM {update.table_fqn}
                    ) b
                    ON a._rn % ({row_count} - 1) + 1 = b._parent_rn
                    AND a."{update.pk_column}" != b."{update.pk_column}"
                ) s
                ON t."{update.pk_column}" = s.target_pk
                WHEN MATCHED THEN UPDATE SET "{update.fk_column}" = s.parent_pk
                """
                self.session.sql(alt_update_sql).collect()

        self._pending_self_refs.clear()

    def get_parent_key_count(self, parent_table_fqn: str) -> int:
        """Get the count of keys for a parent table.

        Args:
            parent_table_fqn: Parent table FQN.

        Returns:
            Number of unique keys, or 0 if not materialized.
        """
        cache = self._key_caches.get(parent_table_fqn)
        return cache.key_count if cache else 0

    def cleanup(self) -> None:
        """Drop all transient key cache tables."""
        for fqn in self._temp_tables:
            try:
                self.session.sql(f"DROP TABLE IF EXISTS {fqn}").collect()
            except Exception:
                pass
        self._temp_tables.clear()
        self._key_caches.clear()
        self._pending_self_refs.clear()
