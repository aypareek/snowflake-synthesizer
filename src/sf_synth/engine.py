"""Snowpark Engine for synthetic data generation.

Orchestrates the generation of synthetic data across tables
following the dependency DAG order.
"""

from __future__ import annotations

import re as _re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from sf_synth.config import (
    ColumnConfig,
    GeneratorType,
    SynthConfig,
    TableConfig,
    WriteMode,
)
from sf_synth.dag import GenerationPlan, build_dag_from_config
from sf_synth.discovery import SchemaModel, TableInfo
from sf_synth.errors import UnsupportedTypeError
from sf_synth.generators.faker_udf import FakerUDFManager
from sf_synth.ri import RIManager
from sf_synth.semantic import suggest_generator_for_column
from sf_synth.stats import StatsSampler

if TYPE_CHECKING:
    from snowflake.snowpark import DataFrame, Session


@dataclass
class ColumnSample:
    """A small sample of a generated column."""

    name: str
    values: list[Any]


@dataclass
class GenerationResult:
    """Result of generating a single table."""

    table_fqn: str
    rows_generated: int
    elapsed_seconds: float
    success: bool
    error: str | None = None
    error_column: str | None = None
    sample_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SynthesisResult:
    """Result of a complete synthesis run."""

    tables: list[GenerationResult]
    total_rows: int
    total_elapsed_seconds: float
    success: bool
    errors: list[str] = field(default_factory=list)


_NUMERIC_BASE_TYPES = frozenset(
    {
        "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER",
        "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
        "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL",
    }
)
_TEMPORAL_BASE_TYPES = frozenset(
    {"DATE", "TIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
)


class _ColumnFailure(Exception):
    """Internal: surface which column caused a generation failure."""

    def __init__(self, column: str, original: Exception) -> None:
        super().__init__(f"column '{column}': {original}")
        self.column = column
        self.original = original


class SynthEngine:
    """Main engine for synthetic data generation."""

    UNIQUE_OVERSAMPLE_FACTOR = 1.3

    def __init__(
        self,
        session: Session,
        config: SynthConfig,
        schema_model: SchemaModel | None = None,
        max_parallel_tables: int = 1,
    ) -> None:
        """Initialize the synthesis engine."""
        self.session = session
        self.config = config
        self.schema_model = schema_model
        self.max_parallel_tables = max(1, max_parallel_tables)

        database = config.defaults.database or session.get_current_database() or "DATABASE"
        schema = config.defaults.schema_name or session.get_current_schema() or "PUBLIC"

        self.target_database = database.strip('"')
        self.target_schema = schema.strip('"')

        self._ri_manager = RIManager(session, self.target_database, self.target_schema)
        self._stats_sampler = StatsSampler(session)
        self._faker_manager = FakerUDFManager(session)
        self._plan: GenerationPlan | None = None
        self._progress_callback: Callable[[str, int, int], None] | None = None
        self._sample_buffer: dict[str, list[dict[str, Any]]] = {}

    def set_progress_callback(self, callback: Callable[[str, int, int], None]) -> None:
        """Set a callback for progress updates."""
        self._progress_callback = callback

    def plan(self) -> GenerationPlan:
        """Build the generation plan."""
        self._plan = build_dag_from_config(self.config)
        return self._plan

    def generate(
        self,
        dry_run: bool = False,
        truncate: bool | None = None,
        capture_samples: int = 0,
    ) -> SynthesisResult:
        """Execute the synthesis."""
        if self._plan is None:
            self._plan = self.plan()

        try:
            self.session.use_database(self.target_database)
            self.session.use_schema(self.target_schema)
        except Exception:
            pass

        self._sample_buffer = {} if capture_samples > 0 else self._sample_buffer

        results: list[GenerationResult] = []
        total_start = time.time()
        errors: list[str] = []

        if self.max_parallel_tables == 1:
            results, errors = self._run_sequential(dry_run, truncate, capture_samples)
        else:
            results, errors = self._run_parallel(dry_run, truncate, capture_samples)

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

    def preview(self, rows: int = 10) -> dict[str, list[dict[str, Any]]]:
        """Generate a small preview without writing anything to Snowflake.

        Returns a mapping of table_fqn -> list of row dicts.
        """
        if self._plan is None:
            self._plan = self.plan()

        try:
            self.session.use_database(self.target_database)
            self.session.use_schema(self.target_schema)
        except Exception:
            pass

        previews: dict[str, list[dict[str, Any]]] = {}
        for table_fqn in self._plan.generation_order:
            table_config = self._get_table_config(table_fqn)
            if table_config is None:
                continue
            try:
                rows_data = self._generate_preview_rows(table_fqn, table_config, rows)
                previews[table_fqn] = rows_data

                deps = self._plan.get_dependents_for(table_fqn) if self._plan else []
                if deps and rows_data:
                    pk_columns = self._get_pk_columns(table_fqn, None)
                    if pk_columns and self._table_exists(table_fqn):
                        try:
                            self._ri_manager.materialize_parent_keys(table_fqn, pk_columns)
                        except Exception:
                            pass
            except Exception as e:
                previews[table_fqn] = [{"_error": str(e)}]

        return previews

    def _run_sequential(
        self, dry_run: bool, truncate: bool | None, capture_samples: int
    ) -> tuple[list[GenerationResult], list[str]]:
        results: list[GenerationResult] = []
        errors: list[str] = []
        assert self._plan is not None
        total_tables = len(self._plan.generation_order)

        for idx, table_fqn in enumerate(self._plan.generation_order):
            if self._progress_callback:
                self._progress_callback(table_fqn, idx + 1, total_tables)

            res = self._generate_one_table(table_fqn, dry_run, truncate, capture_samples)
            if res is None:
                continue
            results.append(res)
            if not res.success and res.error:
                errors.append(f"{table_fqn}: {res.error}")
        return results, errors

    def _run_parallel(
        self, dry_run: bool, truncate: bool | None, capture_samples: int
    ) -> tuple[list[GenerationResult], list[str]]:
        """Run independent tables in parallel by DAG depth level."""
        assert self._plan is not None
        results: list[GenerationResult] = []
        errors: list[str] = []

        depth_map = self._compute_depth_levels()
        levels: dict[int, list[str]] = {}
        for fqn, depth in depth_map.items():
            levels.setdefault(depth, []).append(fqn)

        ordered_levels = sorted(levels.keys())
        completed = 0
        total_tables = len(self._plan.generation_order)

        for lvl in ordered_levels:
            tables_at_level = levels[lvl]
            with ThreadPoolExecutor(max_workers=self.max_parallel_tables) as ex:
                futures = {
                    ex.submit(
                        self._generate_one_table,
                        fqn,
                        dry_run,
                        truncate,
                        capture_samples,
                    ): fqn
                    for fqn in tables_at_level
                }
                for fut in as_completed(futures):
                    fqn = futures[fut]
                    completed += 1
                    if self._progress_callback:
                        self._progress_callback(fqn, completed, total_tables)
                    try:
                        res = fut.result()
                    except Exception as e:
                        errors.append(f"{fqn}: {e}")
                        continue
                    if res is None:
                        continue
                    results.append(res)
                    if not res.success and res.error:
                        errors.append(f"{fqn}: {res.error}")

        return results, errors

    def _compute_depth_levels(self) -> dict[str, int]:
        """Compute DAG depth (longest path from any source) for each table."""
        assert self._plan is not None
        deps: dict[str, list[str]] = {n.fqn: list(n.dependencies) for n in self._plan.tables}
        depth: dict[str, int] = {}

        def _depth(fqn: str) -> int:
            if fqn in depth:
                return depth[fqn]
            ds = deps.get(fqn, [])
            if not ds:
                depth[fqn] = 0
                return 0
            depth[fqn] = 1 + max(_depth(d) for d in ds)
            return depth[fqn]

        for fqn in self._plan.generation_order:
            _depth(fqn)
        return depth

    def _generate_one_table(
        self,
        table_fqn: str,
        dry_run: bool,
        truncate: bool | None,
        capture_samples: int,
    ) -> GenerationResult | None:
        table_config = self._get_table_config(table_fqn)
        if table_config is None:
            return None

        start = time.time()
        try:
            if not dry_run:
                rows = self._generate_table(table_fqn, table_config, truncate)
            else:
                rows = table_config.rows

            sample = self._sample_buffer.pop(table_fqn, []) if capture_samples > 0 else []
            return GenerationResult(
                table_fqn=table_fqn,
                rows_generated=rows,
                elapsed_seconds=time.time() - start,
                success=True,
                sample_rows=sample,
            )
        except _ColumnFailure as cf:
            err = str(cf.original)
            return GenerationResult(
                table_fqn=table_fqn,
                rows_generated=0,
                elapsed_seconds=time.time() - start,
                success=False,
                error=err,
                error_column=cf.column,
            )
        except Exception as e:
            err = str(e)
            return GenerationResult(
                table_fqn=table_fqn,
                rows_generated=0,
                elapsed_seconds=time.time() - start,
                success=False,
                error=err,
                error_column=self._extract_offending_column(err),
            )

    @staticmethod
    def _extract_offending_column(error_msg: str) -> str | None:
        """Heuristic extraction of the column name from a Snowflake error."""
        m = _re.search(r'column[s]?\s+["\']?([A-Z_][A-Z0-9_]*)', error_msg, _re.I)
        if m:
            return m.group(1)
        return None

    def _get_table_config(self, table_fqn: str) -> TableConfig | None:
        for table in self.config.tables:
            if table.get_fqn(self.target_database, self.target_schema) == table_fqn:
                return table
        return None

    def _table_exists(self, table_fqn: str) -> bool:
        try:
            self.session.sql(f"DESCRIBE TABLE {table_fqn}").collect()
            return True
        except Exception:
            return False

    def _describe_table(
        self, table_fqn: str
    ) -> tuple[list[str], dict[str, int], dict[str, tuple[float, float]], dict[str, str]]:
        """Return (col_order, varchar_lengths, numeric_bounds, type_map)."""
        col_order: list[str] = []
        varchar_lens: dict[str, int] = {}
        numeric_bounds: dict[str, tuple[float, float]] = {}
        type_map: dict[str, str] = {}
        try:
            desc_rows = self.session.sql(f"DESCRIBE TABLE {table_fqn}").collect()
            col_order = [r["name"] for r in desc_rows]
            for r in desc_rows:
                col_type = r.get("type", "")
                type_map[r["name"]] = col_type
                m = _re.search(r"VARCHAR\((\d+)\)", col_type, _re.I)
                if m:
                    varchar_lens[r["name"]] = int(m.group(1))
                m = _re.search(r"NUMBER\((\d+),(\d+)\)", col_type, _re.I)
                if m:
                    p, s = int(m.group(1)), int(m.group(2))
                    max_abs = 10 ** (p - s) - 10 ** (-s)
                    numeric_bounds[r["name"]] = (-max_abs, max_abs)
        except Exception:
            pass
        return col_order, varchar_lens, numeric_bounds, type_map

    def _build_select_sql(
        self,
        table_fqn: str,
        table_config: TableConfig,
        row_count: int,
        seed: int | None,
        col_order: list[str],
        varchar_lens: dict[str, int],
        numeric_bounds: dict[str, tuple[float, float]],
        type_map: dict[str, str],
    ) -> str:
        """Construct the full SELECT SQL string used for synthesis."""
        table_info = self.schema_model.get_table(table_fqn) if self.schema_model else None
        columns_to_generate = self._get_columns_to_generate(table_config, table_info)
        fk_columns = {rel.column for rel in table_config.relationships}

        seed_val = seed if seed is not None else 0

        base_sql = (
            f"SELECT SEQ8() AS _rownum FROM TABLE(GENERATOR(ROWCOUNT => {row_count}))"
        )

        correlation_groups = self._gather_correlation_groups(table_config, columns_to_generate)
        correlation_udfs: dict[str, str] = {}
        for grp_id, members in correlation_groups.items():
            providers = {col: cfg.provider or "name" for col, cfg in members.items()}
            locale = next(iter(members.values())).locale
            try:
                udf_name = self._faker_manager.get_or_register_correlated_udf(
                    f"{table_fqn}_{grp_id}", providers, locale, seed
                )
                correlation_udfs[grp_id] = udf_name
            except Exception:
                continue

        column_exprs: list[str] = []
        generated_col_names: list[str] = []
        column_meta: dict[str, ColumnConfig | None] = {}
        temporal_relations: list[tuple[str, ColumnConfig]] = []

        for col_name, col_cfg, col_info in columns_to_generate:
            if col_name in fk_columns:
                continue
            column_meta[col_name] = col_cfg

            if col_cfg and col_cfg.after:
                temporal_relations.append((col_name, col_cfg))
                column_exprs.append(f"NULL AS \"{col_name}\"")
                generated_col_names.append(col_name)
                continue

            if col_cfg and col_cfg.correlation_group and col_cfg.correlation_group in correlation_udfs:
                udf_name = correlation_udfs[col_cfg.correlation_group]
                base_expr = (
                    f"{udf_name}(_rownum):\"{col_name}\"::STRING"
                )
                base_expr = self._wrap_post(base_expr, col_cfg, col_name, varchar_lens, numeric_bounds)
                column_exprs.append(f"{base_expr} AS \"{col_name}\"")
                generated_col_names.append(col_name)
                continue

            try:
                expr = self._generate_column_expr(col_name, col_cfg, col_info, row_count, seed)
            except UnsupportedTypeError:
                column_exprs.append(f"NULL AS \"{col_name}\"")
                generated_col_names.append(col_name)
                continue
            except Exception as e:
                raise _ColumnFailure(col_name, e) from e

            expr = self._apply_truncation_and_clamp(
                expr, col_name, varchar_lens, numeric_bounds
            )
            if col_cfg and col_cfg.condition:
                expr = self._wrap_condition(expr, col_cfg)
            column_exprs.append(f"{expr} AS \"{col_name}\"")
            generated_col_names.append(col_name)

        fk_join_info: list[tuple[str, str, str, str, Any]] = []
        fk_seed_offset = 0
        for rel in table_config.relationships:
            is_self_ref = self._is_self_referential(table_fqn, rel.references)

            if is_self_ref:
                column_exprs.append(f"NULL AS \"{rel.column}\"")
                generated_col_names.append(rel.column)
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
                fk_info = self._prepare_fk_join(rel, seed, fk_seed_offset)
                if fk_info:
                    alias, idx_col_expr, parent_col, key_table_fqn, _ = fk_info
                    column_exprs.append(f"{idx_col_expr} AS _fk_idx_{alias}")
                    fk_join_info.append((alias, rel.column, parent_col, key_table_fqn, rel))
                else:
                    column_exprs.append(f"NULL AS \"{rel.column}\"")
                    generated_col_names.append(rel.column)
                fk_seed_offset += 1

        if not column_exprs:
            return ""

        column_exprs.append("_rownum")
        inner_sql = f"SELECT {', '.join(column_exprs)} FROM ({base_sql})"

        outer_col_names: list[str] = list(generated_col_names)

        if fk_join_info:
            select_cols: list[str] = []
            for col_name in generated_col_names:
                select_cols.append(f'_base."{col_name}"')
            select_cols.append('_base._rownum')

            join_clauses: list[str] = []
            for alias, fk_col, parent_col, key_table_fqn, rel_obj in fk_join_info:
                join_clauses.append(
                    f'INNER JOIN {key_table_fqn} {alias} ON {alias}._key_idx = _base._fk_idx_{alias}'
                )
                fk_expr = f'{alias}."{parent_col}"'
                if rel_obj.null_ratio > 0:
                    fk_expr = (
                        f"IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {rel_obj.null_ratio}, "
                        f"NULL, {fk_expr})"
                    )
                select_cols.append(f'{fk_expr} AS "{fk_col}"')
                outer_col_names.append(fk_col)

            sql = (
                f"SELECT {', '.join(select_cols)} FROM ({inner_sql}) _base "
                f"{' '.join(join_clauses)}"
            )
        else:
            sql = inner_sql

        if temporal_relations:
            sql = self._apply_temporal_offsets(
                sql, temporal_relations, outer_col_names, seed_val, type_map
            )

        return sql

    def _apply_temporal_offsets(
        self,
        inner_sql: str,
        temporal: list[tuple[str, ColumnConfig]],
        all_cols: list[str],
        seed_val: int,
        type_map: dict[str, str],
    ) -> str:
        """Wrap an inner SELECT to compute `after` columns from base columns."""
        select_parts = []
        for col_name in all_cols:
            if any(c == col_name for c, _ in temporal):
                cfg = next(c for n, c in temporal if n == col_name)
                base_col = cfg.after
                offset = (
                    f"UNIFORM({cfg.after_offset_min}, {cfg.after_offset_max}, "
                    f"RANDOM({seed_val + abs(hash(col_name)) % 100000}))"
                )
                expr = (
                    f"DATEADD('{cfg.after_offset_unit}', {offset}, _t.\"{base_col}\")"
                )
                select_parts.append(f'{expr} AS "{col_name}"')
            else:
                select_parts.append(f'_t."{col_name}"')
        return f"SELECT {', '.join(select_parts)} FROM ({inner_sql}) _t"

    def _gather_correlation_groups(
        self,
        table_config: TableConfig,
        columns_to_generate: list[tuple[str, ColumnConfig | None, Any]],
    ) -> dict[str, dict[str, ColumnConfig]]:
        """Group faker columns by their correlation_group ID."""
        groups: dict[str, dict[str, ColumnConfig]] = {}
        for col_name, col_cfg, _ in columns_to_generate:
            if not col_cfg:
                continue
            if col_cfg.generator != GeneratorType.FAKER:
                continue
            if not col_cfg.correlation_group:
                continue
            groups.setdefault(col_cfg.correlation_group, {})[col_name] = col_cfg

        return {gid: members for gid, members in groups.items() if len(members) >= 2}

    def _wrap_condition(self, expr: str, cfg: ColumnConfig) -> str:
        """Wrap an expression with an IFF condition."""
        else_val = self._sql_literal(cfg.else_value) if cfg.else_value is not None else "NULL"
        return f"IFF({cfg.condition}, {expr}, {else_val})"

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        s = str(value).replace("'", "''")
        return f"'{s}'"

    def _apply_truncation_and_clamp(
        self,
        expr: str,
        col_name: str,
        varchar_lens: dict[str, int],
        numeric_bounds: dict[str, tuple[float, float]],
    ) -> str:
        max_len = varchar_lens.get(col_name)
        if max_len and max_len < 16777216:
            expr = f"LEFT({expr}, {max_len})"
        bounds = numeric_bounds.get(col_name)
        if bounds:
            lo, hi = bounds
            expr = f"LEAST({hi}, GREATEST({lo}, {expr}))"
        return expr

    def _wrap_post(
        self,
        expr: str,
        col_cfg: ColumnConfig,
        col_name: str,
        varchar_lens: dict[str, int],
        numeric_bounds: dict[str, tuple[float, float]],
    ) -> str:
        seed_val = (self.config.defaults.seed or 0)
        if col_cfg.null_ratio > 0:
            expr = (
                f"IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {col_cfg.null_ratio}, "
                f"NULL, {expr})"
            )
        return self._apply_truncation_and_clamp(expr, col_name, varchar_lens, numeric_bounds)

    def _generate_table(
        self,
        table_fqn: str,
        table_config: TableConfig,
        truncate_override: bool | None,
    ) -> int:
        """Generate data for a single table."""
        write_mode = table_config.write_mode
        seed = self.config.defaults.seed

        col_order, varchar_lens, numeric_bounds, type_map = self._describe_table(table_fqn)

        existing_count = self._get_existing_row_count(table_fqn) if self._table_exists(table_fqn) else 0
        if write_mode == WriteMode.FILL_TO:
            remaining = max(0, table_config.rows - existing_count)
            if remaining == 0:
                return existing_count
            row_count = remaining
        else:
            row_count = table_config.rows

        do_truncate = (
            truncate_override
            if truncate_override is not None
            else (write_mode == WriteMode.REPLACE and table_config.truncate_before)
        )
        if do_truncate and self._table_exists(table_fqn):
            self.session.sql(f"TRUNCATE TABLE IF EXISTS {table_fqn}").collect()

        sql = self._build_select_sql(
            table_fqn, table_config, row_count, seed, col_order, varchar_lens,
            numeric_bounds, type_map,
        )
        if not sql:
            return 0

        df = self.session.sql(sql)

        if col_order:
            try:
                df_cols = {c.upper() for c in df.columns}
                ordered = [f'"{c}"' for c in col_order if c.upper() in df_cols]
                if ordered:
                    df = df.select(ordered)
            except Exception:
                pass

        if write_mode == WriteMode.UPSERT and table_config.upsert_keys and self._table_exists(table_fqn):
            self._merge_into_table(df, table_fqn, table_config.upsert_keys)
        else:
            df.write.mode("append").save_as_table(table_fqn)

        deps = self._plan.get_dependents_for(table_fqn) if self._plan else []
        if deps:
            pk_columns = self._get_pk_columns(
                table_fqn, self.schema_model.get_table(table_fqn) if self.schema_model else None
            )
            if pk_columns:
                self._ri_manager.materialize_parent_keys(table_fqn, pk_columns)

        return row_count if write_mode != WriteMode.FILL_TO else row_count

    def _generate_preview_rows(
        self,
        table_fqn: str,
        table_config: TableConfig,
        rows: int,
    ) -> list[dict[str, Any]]:
        """Generate small preview without writing."""
        seed = self.config.defaults.seed
        col_order: list[str] = []
        varchar_lens: dict[str, int] = {}
        numeric_bounds: dict[str, tuple[float, float]] = {}
        type_map: dict[str, str] = {}
        if self._table_exists(table_fqn):
            col_order, varchar_lens, numeric_bounds, type_map = self._describe_table(table_fqn)

        sql = self._build_select_sql(
            table_fqn, table_config, rows, seed, col_order, varchar_lens,
            numeric_bounds, type_map,
        )
        if not sql:
            return []
        try:
            res = self.session.sql(sql).limit(rows).collect()
        except Exception:
            return []
        return [dict(r.as_dict()) for r in res]

    def _get_existing_row_count(self, table_fqn: str) -> int:
        try:
            r = self.session.sql(f"SELECT COUNT(*) AS C FROM {table_fqn}").collect()
            return int(r[0]["C"])
        except Exception:
            return 0

    def _merge_into_table(
        self,
        df: DataFrame,
        table_fqn: str,
        keys: list[str],
    ) -> None:
        """Run a MERGE to upsert df into the target table."""
        staging = f"SF_SYNTH_STAGING_{abs(hash(table_fqn)) % 1_000_000}"
        df.write.mode("overwrite").save_as_table(staging)
        try:
            on_clause = " AND ".join(f't."{k}" = s."{k}"' for k in keys)
            cols = df.columns
            insert_cols = ", ".join(f'"{c}"' for c in cols)
            insert_vals = ", ".join(f's."{c}"' for c in cols)
            update_set = ", ".join(
                f't."{c}" = s."{c}"' for c in cols if c not in keys
            )
            update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_set else ""
            sql = f"""
            MERGE INTO {table_fqn} t
            USING {staging} s
            ON {on_clause}
            {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            self.session.sql(sql).collect()
        finally:
            try:
                self.session.sql(f"DROP TABLE IF EXISTS {staging}").collect()
            except Exception:
                pass

    def _get_columns_to_generate(
        self,
        table_config: TableConfig,
        table_info: TableInfo | None,
    ) -> list[tuple[str, ColumnConfig | None, Any]]:
        columns: list[tuple[str, ColumnConfig | None, Any]] = []
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
                False,
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
        seed_val = seed if seed is not None else 0

        g = config.generator
        if g == GeneratorType.UNIFORM:
            min_v = config.min_value if config.min_value is not None else 0
            max_v = config.max_value if config.max_value is not None else 100
            expr = f"UNIFORM({min_v}::FLOAT, {max_v}::FLOAT, RANDOM({seed_val}))"

        elif g == GeneratorType.SEQ:
            expr = f"(SEQ8() * {config.step} + {config.start})"

        elif g == GeneratorType.CHOICE:
            expr = self._build_choice_expr(config.values or [], config.weights, seed_val)

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
                udf_name = self._faker_manager.get_or_register_udf(provider, locale, seed)
                expr = f"{udf_name}(SEQ8())"
            except Exception:
                expr = f"'fake_{provider}_' || SEQ8()::VARCHAR"

        elif g == GeneratorType.REGEX:
            try:
                udf_name = self._faker_manager.get_or_register_regex_udf(seed)
                pattern = (config.pattern or "").replace("'", "''")
                expr = f"{udf_name}('{pattern}', SEQ8())"
            except Exception:
                expr = "'pattern_' || SEQ8()::VARCHAR"

        elif g == GeneratorType.EXPRESSION:
            expr = config.sql or "NULL"

        elif g == GeneratorType.JSON_TEMPLATE:
            expr = self._render_json_template(config.template or "{}", seed_val)

        elif g == GeneratorType.ARRAY:
            expr = self._build_array_expr(config, seed_val, seed)

        elif g == GeneratorType.OBJECT:
            expr = self._build_object_expr(col_name, config, seed_val, seed, row_count)

        else:
            expr = "NULL"

        if config.null_ratio > 0:
            expr = (
                f"IFF(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})) < {config.null_ratio}, "
                f"NULL, {expr})"
            )

        return expr

    def _build_choice_expr(
        self,
        values: list[Any],
        weights: list[float] | None,
        seed_val: int,
    ) -> str:
        if not values:
            return "NULL"
        ws = weights or [1.0 / len(values)] * len(values)
        total = sum(ws)
        ws = [w / total for w in ws]

        cases: list[str] = []
        cumulative = 0.0
        for val, weight in zip(values, ws):
            cumulative += weight
            cases.append(f"WHEN RANDOM({seed_val}) < {cumulative} THEN {self._sql_literal(val)}")

        return f"CASE {' '.join(cases)} ELSE {self._sql_literal(values[-1])} END"

    def _render_json_template(self, template: str, seed_val: int) -> str:
        """Render a {{...}} JSON template into a Snowflake PARSE_JSON expression.

        Supports placeholders:
          {{faker.<provider>}}     -> string from faker
          {{uniform(<a>,<b>)}}     -> UNIFORM(a, b, RANDOM(seed))
          {{choice('a','b','c')}}  -> random choice
          {{seq}}                  -> SEQ8()
        Strings outside placeholders are kept as JSON literals.
        """
        token_re = _re.compile(r"\{\{\s*(.*?)\s*\}\}")
        parts: list[str] = []
        last = 0
        for m in token_re.finditer(template):
            literal = template[last:m.start()]
            if literal:
                escaped = literal.replace("'", "''")
                parts.append(f"'{escaped}'")
            tok = m.group(1).strip()
            parts.append(self._json_token_to_sql(tok, seed_val))
            last = m.end()
        tail = template[last:]
        if tail:
            escaped = tail.replace("'", "''")
            parts.append(f"'{escaped}'")
        if not parts:
            return "PARSE_JSON('null')"
        concat_sql = " || ".join(parts)
        return f"TRY_PARSE_JSON({concat_sql})"

    def _json_token_to_sql(self, tok: str, seed_val: int) -> str:
        """Convert a `{{...}}` token inside a json_template into SQL.

        The token's value is concatenated with surrounding template literals,
        so we emit the raw text. The user is responsible for placing quotes
        around string placeholders inside the template (just like any other
        string-template DSL). Embedded single-quotes are escaped.
        """
        if tok.startswith("faker."):
            provider = tok[len("faker."):].strip()
            try:
                udf_name = self._faker_manager.get_or_register_udf(
                    provider, "en_US", self.config.defaults.seed
                )
                return f"REPLACE({udf_name}(SEQ8()), '\"', '\\\\\"')"
            except Exception:
                return f"'fake_{provider}'"
        m = _re.match(r"^uniform\(\s*([\d\.\-]+)\s*,\s*([\d\.\-]+)\s*\)$", tok)
        if m:
            lo, hi = m.group(1), m.group(2)
            return f"TO_VARCHAR(UNIFORM({lo}::FLOAT, {hi}::FLOAT, RANDOM({seed_val})))"
        if tok == "seq":
            return "SEQ8()::VARCHAR"
        m = _re.match(r"^choice\(\s*(.*)\s*\)$", tok)
        if m:
            inside = m.group(1)
            vals = [v.strip().strip("'\"") for v in inside.split(",")]
            return self._build_choice_expr(vals, None, seed_val)
        return "'null'"

    def _build_array_expr(
        self, cfg: ColumnConfig, seed_val: int, seed: int | None
    ) -> str:
        """Build an ARRAY using ARRAY_CONSTRUCT with N element expressions."""
        if isinstance(cfg.length, list):
            n = cfg.length[1]
        else:
            n = int(cfg.length)
        n = max(1, min(50, n))

        elem_cfg = ColumnConfig(
            generator=cfg.element_generator or GeneratorType.UNIFORM,
            provider=cfg.element_provider,
            values=cfg.element_values,
            min_value=cfg.element_min,
            max_value=cfg.element_max,
            locale=cfg.locale,
        )
        elements: list[str] = []
        for i in range(n):
            elements.append(self._config_to_sql_expr(f"_arr_{i}", elem_cfg, n, (seed or 0) + i))
        if isinstance(cfg.length, list):
            actual_n_expr = f"UNIFORM({cfg.length[0]}, {cfg.length[1]}, RANDOM({seed_val}))"
            constructed = f"ARRAY_CONSTRUCT({', '.join(elements)})"
            return f"ARRAY_SLICE({constructed}, 0, {actual_n_expr})"
        return f"ARRAY_CONSTRUCT({', '.join(elements)})"

    def _build_object_expr(
        self,
        col_name: str,
        cfg: ColumnConfig,
        seed_val: int,
        seed: int | None,
        row_count: int,
    ) -> str:
        """Build an OBJECT using OBJECT_CONSTRUCT(key, value, ...)."""
        if not cfg.fields:
            return "OBJECT_CONSTRUCT()"
        kv_pairs: list[str] = []
        for fname, fcfg in cfg.fields.items():
            v_expr = self._config_to_sql_expr(f"{col_name}_{fname}", fcfg, row_count, seed)
            kv_pairs.append(f"'{fname}', {v_expr}")
        return f"OBJECT_CONSTRUCT({', '.join(kv_pairs)})"

    def _generator_to_sql_expr(
        self,
        col_name: str,
        gen_type: str,
        params: dict[str, Any],
        seed: int | None,
    ) -> str:
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
            return self._build_choice_expr(params.get("values", []), params.get("weights"), seed_val)

        elif gen_type == "faker":
            provider = params.get("provider", "name")
            locale = params.get("locale", "en_US")
            try:
                udf_name = self._faker_manager.get_or_register_udf(provider, locale, seed)
                return f"{udf_name}(SEQ8())"
            except Exception:
                return f"'fake_{provider}_' || SEQ8()::VARCHAR"

        else:
            return "NULL"

    def _prepare_fk_join(
        self,
        rel: Any,
        seed: int | None,
        seed_offset: int,
    ) -> tuple[str, str, str, str, int] | None:
        """Prepare FK column for JOIN-based sampling.

        Uses HASH(_rownum, seed_offset) for deterministic but well-distributed
        index selection — guarantees reproducibility across runs at the same seed.
        """
        seed_val = (seed if seed is not None else 0) + 10000 + seed_offset

        ref_parts = rel.references.split(".")
        if len(ref_parts) == 4:
            parent_fqn = ".".join(ref_parts[:3])
            parent_col = ref_parts[3]
        elif len(ref_parts) == 2:
            parent_fqn = f"{self.target_database}.{self.target_schema}.{ref_parts[0]}"
            parent_col = ref_parts[1]
        else:
            return None

        key_count = self._ri_manager.get_parent_key_count(parent_fqn)
        if key_count == 0:
            cache = self._ri_manager.materialize_parent_keys(parent_fqn, [parent_col])
            key_count = cache.key_count

        if key_count == 0:
            return None

        cache = self._ri_manager._key_caches.get(parent_fqn)
        if not cache:
            return None

        alias = f"_fk{seed_offset}"

        if rel.skew.value == "zipf":
            idx_expr = (
                f"LEAST({key_count}, GREATEST(1, "
                f"FLOOR(POW(UNIFORM(0::FLOAT, 1::FLOAT, RANDOM({seed_val})), "
                f"-1.0 / {rel.skew_param}))::INTEGER))"
            )
        else:
            idx_expr = (
                f"((ABS(HASH(_rownum, {seed_val})) % {key_count}) + 1)"
            )

        return (alias, idx_expr, parent_col, cache.key_table_fqn, key_count)

    def _is_self_referential(self, table_fqn: str, ref: str) -> bool:
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
        self._ri_manager.cleanup()
        self._stats_sampler.cleanup()
        self._faker_manager.cleanup()

    def estimate_size(self) -> dict[str, Any]:
        if self._plan is None:
            self._plan = self.plan()

        estimates: dict[str, Any] = {
            "tables": {},
            "total_rows": 0,
            "estimated_bytes": 0,
        }

        for table_fqn in self._plan.generation_order:
            table_config = self._get_table_config(table_fqn)
            if table_config:
                row_count = table_config.rows
                avg_row_bytes = 200

                table_info = (
                    self.schema_model.get_table(table_fqn) if self.schema_model else None
                )
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
