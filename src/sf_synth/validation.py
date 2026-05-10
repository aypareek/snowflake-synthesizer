"""Config validation against live Snowflake DDL.

Used by `sf-synth validate` to surface problems before a generation run:
  * Missing columns referenced in the config but absent from the table.
  * Type mismatches (faker provider on a NUMBER column, etc).
  * Range overflows (uniform max exceeds NUMBER(p,s) representation).
  * Invalid foreign-key references (parent table/column doesn't exist).
"""

from __future__ import annotations

import re as _re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Iterable

from sf_synth.config import ColumnConfig, GeneratorType, SynthConfig, TableConfig

if TYPE_CHECKING:
    from snowflake.snowpark import Session


@dataclass
class ValidationIssue:
    """Single validation finding."""

    table: str
    severity: str
    message: str
    column: str | None = None


@dataclass
class ValidationReport:
    """Aggregated validation result."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == "error" for i in self.issues)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]


_NUMERIC_BASE = frozenset(
    {
        "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER",
        "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
        "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL",
    }
)
_DATE_BASE = frozenset(
    {"DATE", "TIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
)
_BOOL_BASE = frozenset({"BOOLEAN"})
_TEXT_FAKER_PROVIDERS = frozenset(
    {
        "name", "first_name", "last_name", "email", "address", "city", "state",
        "country", "phone_number", "company", "job", "text", "sentence",
        "paragraph", "url", "user_name", "ssn", "country_code", "state_abbr",
        "zipcode", "postcode", "color_name", "currency_code", "domain_name",
        "credit_card_number", "uuid4", "month", "day_of_week", "word",
        "catch_phrase", "bs", "ipv4", "ipv6", "mac_address",
    }
)
_NUMERIC_FAKER_PROVIDERS = frozenset(
    {"random_int", "pyint", "pyfloat", "random_number"}
)
_DATE_FAKER_PROVIDERS = frozenset(
    {
        "date", "date_of_birth", "date_time", "date_this_year", "date_this_month",
        "time",
    }
)


def _base_type(snowflake_type: str) -> str:
    return snowflake_type.split("(")[0].upper().strip()


def _describe_table(
    session: Session, table_fqn: str
) -> dict[str, dict[str, object]] | None:
    try:
        rows = session.sql(f"DESCRIBE TABLE {table_fqn}").collect()
    except Exception:
        return None
    out: dict[str, dict[str, object]] = {}
    for r in rows:
        col_type = r.get("type", "")
        m_v = _re.search(r"VARCHAR\((\d+)\)", col_type, _re.I)
        m_n = _re.search(r"NUMBER\((\d+),(\d+)\)", col_type, _re.I)
        out[r["name"]] = {
            "type": col_type,
            "base_type": _base_type(col_type),
            "varchar_length": int(m_v.group(1)) if m_v else None,
            "number_precision": int(m_n.group(1)) if m_n else None,
            "number_scale": int(m_n.group(2)) if m_n else None,
            "nullable": r.get("null?", "Y") == "Y",
        }
    return out


def _check_provider_compat(
    base_type: str, provider: str
) -> str | None:
    """Return None if compatible, else a message."""
    if base_type in _NUMERIC_BASE and provider in _TEXT_FAKER_PROVIDERS:
        return f"Faker '{provider}' produces text but column is {base_type}."
    if base_type in _DATE_BASE and provider in _TEXT_FAKER_PROVIDERS:
        return f"Faker '{provider}' produces text but column is {base_type}."
    if base_type in _BOOL_BASE:
        return f"Faker '{provider}' produces text but column is BOOLEAN."
    return None


def _check_column(
    table_fqn: str,
    col_name: str,
    cfg: ColumnConfig,
    ddl: dict[str, dict[str, object]],
) -> Iterable[ValidationIssue]:
    if col_name not in ddl:
        yield ValidationIssue(
            table=table_fqn,
            column=col_name,
            severity="error",
            message=f"Column '{col_name}' is not in the table DDL.",
        )
        return

    info = ddl[col_name]
    base_type = info["base_type"]

    if cfg.generator == GeneratorType.FAKER and cfg.provider:
        msg = _check_provider_compat(base_type, cfg.provider)
        if msg:
            yield ValidationIssue(
                table=table_fqn, column=col_name, severity="error", message=msg
            )

    if cfg.generator in (GeneratorType.UNIFORM, GeneratorType.RANGE):
        precision = info.get("number_precision")
        scale = info.get("number_scale")
        if precision is not None and scale is not None and base_type in _NUMERIC_BASE:
            max_abs = 10 ** (int(precision) - int(scale)) - 10 ** (-int(scale))
            try:
                if cfg.max_value is not None and float(cfg.max_value) > max_abs:
                    yield ValidationIssue(
                        table=table_fqn,
                        column=col_name,
                        severity="error",
                        message=(
                            f"max_value={cfg.max_value} exceeds column capacity "
                            f"NUMBER({precision},{scale}) (max={max_abs})."
                        ),
                    )
                if cfg.min_value is not None and float(cfg.min_value) < -max_abs:
                    yield ValidationIssue(
                        table=table_fqn,
                        column=col_name,
                        severity="error",
                        message=(
                            f"min_value={cfg.min_value} below column capacity "
                            f"NUMBER({precision},{scale}) (min={-max_abs})."
                        ),
                    )
            except (TypeError, ValueError):
                pass

    if cfg.generator == GeneratorType.CHOICE and cfg.values:
        varchar_len = info.get("varchar_length")
        if base_type == "VARCHAR" and varchar_len:
            for v in cfg.values:
                if isinstance(v, str) and len(v) > int(varchar_len):
                    yield ValidationIssue(
                        table=table_fqn,
                        column=col_name,
                        severity="warning",
                        message=(
                            f"choice value '{v[:30]}...' exceeds VARCHAR({varchar_len}) and will be truncated."
                        ),
                    )


def _resolve_fk_target(
    references: str,
    default_db: str | None,
    default_schema: str | None,
) -> tuple[str, str] | None:
    parts = references.split(".")
    if len(parts) == 4:
        return ".".join(parts[:3]), parts[3]
    if len(parts) == 3 and default_db:
        return f"{default_db}.{parts[0]}.{parts[1]}", parts[2]
    if len(parts) == 2:
        if default_db and default_schema:
            return f"{default_db}.{default_schema}.{parts[0]}", parts[1]
    return None


def validate_config_against_ddl(
    session: Session, config: SynthConfig
) -> ValidationReport:
    """Run all validation checks against live Snowflake DDL."""
    report = ValidationReport()
    default_db = config.defaults.database
    default_schema = config.defaults.schema_name

    table_ddls: dict[str, dict[str, dict[str, object]]] = {}
    for tbl in config.tables:
        fqn = tbl.get_fqn(default_db, default_schema)
        ddl = _describe_table(session, fqn)
        if ddl is None:
            report.issues.append(
                ValidationIssue(
                    table=fqn,
                    severity="error",
                    message=f"Table {fqn} does not exist or is not accessible.",
                )
            )
            continue
        table_ddls[fqn] = ddl

    for tbl in config.tables:
        fqn = tbl.get_fqn(default_db, default_schema)
        if fqn not in table_ddls:
            continue
        ddl = table_ddls[fqn]

        for col_name, col_cfg in tbl.columns.items():
            for issue in _check_column(fqn, col_name, col_cfg, ddl):
                report.issues.append(issue)

        explicit_cols = set(tbl.columns.keys())
        fk_cols = {rel.column for rel in tbl.relationships}
        non_nullable_missing = [
            c for c, info in ddl.items()
            if not info["nullable"]
            and c not in explicit_cols
            and c not in fk_cols
            and not _is_identity(info)
        ]
        if non_nullable_missing:
            report.issues.append(
                ValidationIssue(
                    table=fqn,
                    severity="warning",
                    message=(
                        f"Non-nullable columns not configured (will rely on inference): "
                        f"{', '.join(non_nullable_missing)}"
                    ),
                )
            )

        for rel in tbl.relationships:
            target = _resolve_fk_target(rel.references, default_db, default_schema)
            if target is None:
                report.issues.append(
                    ValidationIssue(
                        table=fqn,
                        column=rel.column,
                        severity="error",
                        message=f"Invalid FK reference format: {rel.references}",
                    )
                )
                continue
            ref_fqn, ref_col = target
            ref_ddl = table_ddls.get(ref_fqn)
            if ref_ddl is None:
                ref_ddl = _describe_table(session, ref_fqn)
            if ref_ddl is None:
                report.issues.append(
                    ValidationIssue(
                        table=fqn,
                        column=rel.column,
                        severity="error",
                        message=f"FK references missing table {ref_fqn}.",
                    )
                )
                continue
            if ref_col not in ref_ddl:
                report.issues.append(
                    ValidationIssue(
                        table=fqn,
                        column=rel.column,
                        severity="error",
                        message=f"FK references missing column {ref_fqn}.{ref_col}.",
                    )
                )

    return report


def _is_identity(info: dict[str, object]) -> bool:
    return False
