"""Snowpark Engine for synthetic data generation.

Orchestrates the generation of synthetic data across tables
following the dependency DAG order.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from sf_synth.config import ColumnConfig, GeneratorType, SynthConfig, TableConfig
from sf_synth.dag import GenerationPlan, build_dag_from_config
from sf_synth.discovery import SchemaModel, TableInfo
from sf_synth.errors import GeneratorError, SynthError, UnsupportedTypeError
from sf_synth.generators.base import ColumnGenerator
from sf_synth.generators.distribution import DistributionGenerator
from sf_synth.generators.faker_udf import FakerUDFGenerator, FakerUDFManager
from sf_synth.generators.sql import (
    ChoiceGenerator,
    RangeGenerator,
    RegexGenerator,
    SeqGenerator,
    UniformGenerator,
)
from sf_synth.ri import RIManager
from sf_synth.semantic import suggest_generator_for_column
from sf_synth.stats import StatsSampler

if TYPE_CHECKING:
    from snowflake.snowpark import DataFrame, Session


@dataclass
class GenerationResult:
    """Result of generating a single table."""

    table_fqn: str
    rows_generated: int
    elapsed_seconds: float
    success: bool
    error: str | None = None


@dataclass
class SynthesisResult:
    """Result of a complete synthesis run."""

    tables: list[GenerationResult]
    total_rows: int
    total_elapsed_seconds: float
    success: bool
    errors: list[str] = field(default_factory=list)


class SynthEngine:
    """Main engine for synthetic data generation."""

    UNIQUE_OVERSAMPLE_FACTOR = 1.3

    def __init__(
        self,
        session: Session,
        config: SynthConfig,
        schema_model: SchemaModel | None = None,
    ) -> None:
        """Initialize the synthesis engine.

        Args:
            session: Active Snowpark session.
            config: Synthesis configuration.
            schema_model: Optional discovered schema model.
        """
        self.session = session
        self.config = config
        self.schema_model = schema_model

        database = config.defaults.database or session.get_current_database() or "DATABASE"
        schema = config.defaults.schema_name or session.get_current_schema() or "PUBLIC"

        self.target_database = database.strip('"')
        self.target_schema = schema.strip('"')

        self._ri_manager = RIManager(session, self.target_database, self.target_schema)
        self._stats_sampler = StatsSampler(session)
        self._faker_manager = FakerUDFManager(session)
        self._plan: GenerationPlan | None = None
        self._progress_callback: Callable[[str, int, int], None] | None = None

    def set_progress_callback(
        self, callback: Callable[[str, int, int], None]
    ) -> None:
        """Set a callback for progress updates.

        Args:
            callback: Function(table_name, current, total) called during generation.
        """
        self._progress_callback = callback

    def plan(self) -> GenerationPlan:
        """Build the generation plan.

        Returns:
            GenerationPlan with ordered tables and dependencies.
        """
        self._plan = build_dag_from_config(self.config)
        return self._plan

    def generate(
        self,
        dry_run: bool = False,
        truncate: bool = True,
    ) -> SynthesisResult:
        """Execute the synthesis.

        Args:
            dry_run: If True, validate but don't write data.
            truncate: If True, truncate target tables before writing.

        Returns:
            SynthesisResult with generation details.
        """
        import time

        if self._plan is None:
            self._plan = self.plan()

        results: list[GenerationResult] = []
        total_start = time.time()
        errors: list[str] = []

        total_tables = len(self._plan.generation_order)

        for idx, table_fqn in enumerate(self._plan.generation_order):
            if self._progress_callback:
                self._progress_callback(table_fqn, idx + 1, total_tables)

            table_config = self._get_table_config(table_fqn)
            if table_config is None:
                continue

            table_start = time.time()

            try:
                if not dry_run:
                    rows = self._generate_table(table_fqn, table_config, truncate)
                else:
                    rows = table_config.rows

                elapsed = time.time() - table_start
                results.append(
                    GenerationResult(
                        table_fqn=table_fqn,
                        rows_generated=rows,
                        elapsed_seconds=elapsed,
                        success=True,
                    )
                )

            except Exception as e:
                elapsed = time.time() - table_start
                error_msg = str(e)
                errors.append(f"{table_fqn}: {error_msg}")
                results.append(
                    GenerationResult(
                        table_fqn=table_fqn,
                        rows_generated=0,
                        elapsed_seconds=elapsed,
                        success=False,
                        error=error_msg,
                    )
                )

        if not dry_run and self._plan.self_referential_tables:
            try:
                self._ri_manager.execute_self_ref_updates(self.config.defaults.seed)
            except Exception as e:
                errors.append(f"Self-ref updates: {e}")

        total_elapsed = time.time() - total_start

        return SynthesisResult(
            tables=results,
            total_rows=sum(r.rows_generated for r in results),
            total_elapsed_seconds=total_elapsed,
            success=len(errors) == 0,
            errors=errors,
        )

    def _get_table_config(self, table_fqn: str) -> TableConfig | None:
        """Get table config by FQN."""
        for table in self.config.tables:
            config_fqn = table.get_fqn(self.target_database, self.target_schema)
            if config_fqn == table_fqn:
                return table
        return None

    def _generate_table(
        self,
        table_fqn: str,
        table_config: TableConfig,
        truncate: bool,
    ) -> int:
        """Generate data for a single table.

        Args:
            table_fqn: Fully qualified table name.
            table_config: Table configuration.
            truncate: Whether to truncate before writing.

        Returns:
            Number of rows generated.
        """
        row_count = table_config.rows
        seed = self.config.defaults.seed

        if truncate and table_config.truncate_before:
            self.session.sql(f"TRUNCATE TABLE IF EXISTS {table_fqn}").collect()

        table_info = self.schema_model.get_table(table_fqn) if self.schema_model else None

        base_sql = f"""
        SELECT
            SEQ8() AS _rownum,
            RANDOM({seed or 0}) AS _rand
        FROM TABLE(GENERATOR(ROWCOUNT => {row_count}))
        """

        df = self.session.sql(base_sql)

        columns_to_generate = self._get_columns_to_generate(table_config, table_info)
        fk_columns = {rel.column for rel in table_config.relationships}

        column_exprs: list[str] = []

        for col_name, col_config, col_info in columns_to_generate:
            if col_name in fk_columns:
                continue

            try:
                expr = self._generate_column_expr(
                    col_name,
                    col_config,
                    col_info,
                    row_count,
                    seed,
                )
                column_exprs.append(f"{expr} AS \"{col_name}\"")
            except UnsupportedTypeError:
                column_exprs.append(f"NULL AS \"{col_name}\"")

        for rel in table_config.relationships:
            is_self_ref = self._is_self_referential(table_fqn, rel.references)

            if is_self_ref:
                column_exprs.append(f"NULL AS \"{rel.column}\"")
                ref_parts = rel.references.split(".")
                pk_col = ref_parts[-1]
                self._ri_manager.queue_self_ref_update(
                    table_fqn=table_fqn,
                    fk_column=rel.column,
                    pk_column=pk_col,
                    null_ratio=rel.null_ratio,
                    skew=rel.skew.value,
                    skew_param=rel.skew_param,
                )
            else:
                fk_expr = self._generate_fk_expr(rel, row_count, seed)
                column_exprs.append(f"{fk_expr} AS \"{rel.column}\"")

        if column_exprs:
            select_sql = f"""
            SELECT
                {', '.join(column_exprs)}
            FROM ({base_sql})
            """
            df = self.session.sql(select_sql)

        df.write.mode("append").save_as_table(table_fqn)

        deps = self._plan.get_dependents_for(table_fqn) if self._plan else []
        if deps:
            pk_columns = self._get_pk_columns(table_fqn, table_info)
            if pk_columns:
                self._ri_manager.materialize_parent_keys(table_fqn, pk_columns)

        return row_count

    def _get_columns_to_generate(
        self,
        table_config: TableConfig,
        table_info: TableInfo | None,
    ) -> list[tuple[str, ColumnConfig | None, Any]]:
        """Get list of columns to generate with their configs."""
        columns = []

        explicit_cols = set(table_config.columns.keys())
        fk_cols = {rel.column for rel in table_config.relationships}

        for col_name, col_config in table_config.columns.items():
            col_info = table_info.columns.get(col_name) if table_info else None
            columns.append((col_name, col_config, col_info))

        if table_info:
            for col_name, col_info in table_info.columns.items():
                if col_name not in explicit_cols and col_name not in fk_cols:
                    columns.append((col_name, None, col_info))

        return columns

    def _generate_column_expr(
        self,
        col_name: str,
        col_config: ColumnConfig | None,
        col_info: Any,
        row_count: int,
        seed: int | None,
    ) -> str:
        """Generate SQL expression for a column."""
        if col_config:
            return self._config_to_sql_expr(col_name, col_config, row_count, seed)

        if col_info:
            if not col_info.is_supported:
                raise UnsupportedTypeError(col_name, col_info.data_type)

            suggested = suggest_generator_for_column(
                col_name,
                col_info.data_type,
                col_info.is_nullable,
                False,
                col_name in (col_info.name for col_info in [col_info]),
            )

            gen_type = suggested.get("generator", "uniform")
            return self._generator_to_sql_expr(col_name, gen_type, suggested, seed)

        return "NULL"

    def _config_to_sql_expr(
        self,
        col_name: str,
        config: ColumnConfig,
        row_count: int,
        seed: int | None,
    ) -> str:
        """Convert column config to SQL expression."""
        seed_val = seed if seed is not None else 0

        g = config.generator
        if g == GeneratorType.UNIFORM:
            min_v = config.min_value if config.min_value is not None else 0
            max_v = config.max_value if config.max_value is not None else 100
            expr = f"UNIFORM({min_v}::FLOAT, {max_v}::FLOAT, RANDOM({seed_val}))"

        elif g == GeneratorType.SEQ:
            expr = f"(SEQ8() * {config.step} + {config.start})"

        elif g == GeneratorType.CHOICE:
            if not config.values:
                return "NULL"
            weights = config.weights or [1.0 / len(config.values)] * len(config.values)
            total = sum(weights)
            weights = [w / total for w in weights]

            cases = []
            cumulative = 0.0
            for val, weight in zip(config.values, weights):
                cumulative += weight
                val_str = f"'{val}'" if isinstance(val, str) else str(val)
                cases.append(f"WHEN RANDOM({seed_val}) < {cumulative} THEN {val_str}")

            default_val = config.values[-1]
            default_str = f"'{default_val}'" if isinstance(default_val, str) else str(default_val)
            expr = f"CASE {' '.join(cases)} ELSE {default_str} END"

        elif g == GeneratorType.RANGE:
            min_v = config.min_value
            max_v = config.max_value
            expr = f"UNIFORM({min_v}::FLOAT, {max_v}::FLOAT, RANDOM({seed_val}))"

        elif g == GeneratorType.DISTRIBUTION:
            if config.source:
                parts = config.source.split(".")
                if len(parts) == 4:
                    stats = self._stats_sampler.sample_column(
                        parts[0], parts[1], parts[2], parts[3]
                    )
                    from sf_synth.stats import generate_sampling_sql
                    return generate_sampling_sql(stats, col_name).replace(f" AS {col_name}", "")
            expr = "NULL"

        elif g == GeneratorType.FAKER:
            provider = config.provider or "name"
            locale = config.locale

            try:
                udf_name = self._faker_manager.get_or_register_udf(
                    provider, locale, seed
                )
                expr = f"{udf_name}()"
            except Exception:
                expr = f"'fake_{provider}_' || SEQ8()::VARCHAR"

        elif g == GeneratorType.REGEX:
            expr = f"'pattern_' || SEQ8()::VARCHAR"

        else:
            expr = "NULL"

        if config.null_ratio > 0:
            expr = f"IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {config.null_ratio}, NULL, {expr})"

        return expr

    def _generator_to_sql_expr(
        self,
        col_name: str,
        gen_type: str,
        params: dict[str, Any],
        seed: int | None,
    ) -> str:
        """Convert generator params to SQL expression."""
        seed_val = seed if seed is not None else 0

        if gen_type == "uniform":
            min_v = params.get("min_value", 0)
            max_v = params.get("max_value", 100)
            return f"UNIFORM({min_v}::FLOAT, {max_v}::FLOAT, RANDOM({seed_val}))"

        elif gen_type == "seq":
            start = params.get("start", 1)
            step = params.get("step", 1)
            return f"(SEQ8() * {step} + {start})"

        elif gen_type == "choice":
            values = params.get("values", [])
            if not values:
                return "NULL"
            weights = params.get("weights", [1.0 / len(values)] * len(values))

            cases = []
            cumulative = 0.0
            for val, weight in zip(values, weights):
                cumulative += weight
                val_str = f"'{val}'" if isinstance(val, str) else str(val)
                cases.append(f"WHEN RANDOM({seed_val}) < {cumulative} THEN {val_str}")

            default_val = values[-1]
            default_str = f"'{default_val}'" if isinstance(default_val, str) else str(default_val)
            return f"CASE {' '.join(cases)} ELSE {default_str} END"

        elif gen_type == "faker":
            provider = params.get("provider", "name")
            locale = params.get("locale", "en_US")
            try:
                udf_name = self._faker_manager.get_or_register_udf(provider, locale, seed)
                return f"{udf_name}()"
            except Exception:
                return f"'fake_{provider}_' || SEQ8()::VARCHAR"

        else:
            return "NULL"

    def _generate_fk_expr(
        self,
        rel: Any,
        row_count: int,
        seed: int | None,
    ) -> str:
        """Generate FK expression by sampling from parent keys."""
        seed_val = seed if seed is not None else 0

        ref_parts = rel.references.split(".")
        if len(ref_parts) == 4:
            parent_fqn = ".".join(ref_parts[:3])
            parent_col = ref_parts[3]
        elif len(ref_parts) == 2:
            parent_fqn = f"{self.target_database}.{self.target_schema}.{ref_parts[0]}"
            parent_col = ref_parts[1]
        else:
            return "NULL"

        key_count = self._ri_manager.get_parent_key_count(parent_fqn)
        if key_count == 0:
            cache = self._ri_manager.materialize_parent_keys(parent_fqn, [parent_col])
            key_count = cache.key_count

        if key_count == 0:
            return "NULL"

        if rel.skew.value == "zipf":
            idx_expr = f"""
            LEAST(
                {key_count},
                GREATEST(
                    1,
                    FLOOR(
                        POW(
                            UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})),
                            -1.0 / {rel.skew_param}
                        )
                    )::INTEGER
                )
            )
            """
        else:
            idx_expr = f"UNIFORM(1, {key_count}, RANDOM({seed_val}))"

        cache = self._ri_manager._key_caches.get(parent_fqn)
        if cache:
            expr = f"""
            (SELECT "{parent_col}" FROM {cache.key_table_fqn} WHERE _key_idx = {idx_expr})
            """
        else:
            expr = "NULL"

        if rel.null_ratio > 0:
            expr = f"IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {rel.null_ratio}, NULL, {expr})"

        return expr

    def _is_self_referential(self, table_fqn: str, ref: str) -> bool:
        """Check if a reference is self-referential."""
        ref_parts = ref.split(".")
        if len(ref_parts) == 4:
            ref_table_fqn = ".".join(ref_parts[:3])
        elif len(ref_parts) == 2:
            ref_table_fqn = f"{self.target_database}.{self.target_schema}.{ref_parts[0]}"
        else:
            return False

        return ref_table_fqn == table_fqn

    def _get_pk_columns(
        self,
        table_fqn: str,
        table_info: TableInfo | None,
    ) -> list[str]:
        """Get primary key columns for a table."""
        if table_info and table_info.primary_key:
            return table_info.primary_key.columns

        table_config = self._get_table_config(table_fqn)
        if table_config:
            for col_name, col_config in table_config.columns.items():
                if col_config.generator == GeneratorType.SEQ:
                    return [col_name]

            if table_info:
                for col_name, col_info in table_info.columns.items():
                    if col_info.is_identity:
                        return [col_name]

        return ["ID"] if table_info and "ID" in table_info.columns else []

    def cleanup(self) -> None:
        """Clean up temporary tables and resources."""
        self._ri_manager.cleanup()
        self._stats_sampler.cleanup()
        self._faker_manager.cleanup()

    def estimate_size(self) -> dict[str, Any]:
        """Estimate the size of generated data.

        Returns:
            Dictionary with size estimates.
        """
        if self._plan is None:
            self._plan = self.plan()

        estimates = {
            "tables": {},
            "total_rows": 0,
            "estimated_bytes": 0,
        }

        for table_fqn in self._plan.generation_order:
            table_config = self._get_table_config(table_fqn)
            if table_config:
                row_count = table_config.rows
                avg_row_bytes = 200

                table_info = self.schema_model.get_table(table_fqn) if self.schema_model else None
                if table_info:
                    avg_row_bytes = self._estimate_row_size(table_info)

                table_bytes = row_count * avg_row_bytes

                estimates["tables"][table_fqn] = {
                    "rows": row_count,
                    "estimated_bytes": table_bytes,
                }
                estimates["total_rows"] += row_count
                estimates["estimated_bytes"] += table_bytes

        return estimates

    def _estimate_row_size(self, table_info: TableInfo) -> int:
        """Estimate average row size in bytes."""
        total = 0
        for col in table_info.columns.values():
            dtype = col.data_type.upper()
            if "INT" in dtype or "NUMBER" in dtype:
                total += 8
            elif "FLOAT" in dtype or "DOUBLE" in dtype:
                total += 8
            elif "BOOLEAN" in dtype:
                total += 1
            elif "DATE" in dtype:
                total += 4
            elif "TIME" in dtype or "TIMESTAMP" in dtype:
                total += 8
            elif "VARCHAR" in dtype or "TEXT" in dtype or "STRING" in dtype:
                max_len = col.character_maximum_length or 100
                total += min(max_len, 100)
            else:
                total += 50
        return max(total, 50)
