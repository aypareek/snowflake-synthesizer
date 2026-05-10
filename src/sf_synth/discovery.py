"""Schema discovery from Snowflake INFORMATION_SCHEMA."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sf_synth.errors import DiscoveryError

if TYPE_CHECKING:
    from snowflake.snowpark import Session


UNSUPPORTED_TYPES = frozenset(
    {"VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY", "VECTOR"}
)


@dataclass
class ColumnInfo:
    """Information about a single column."""

    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None
    column_default: str | None = None
    is_identity: bool = False
    identity_start: int | None = None
    identity_increment: int | None = None

    @property
    def is_supported(self) -> bool:
        """Check if this column type is supported."""
        base_type = self.data_type.split("(")[0].upper()
        return base_type not in UNSUPPORTED_TYPES


@dataclass
class PrimaryKeyInfo:
    """Information about a primary key constraint."""

    constraint_name: str
    columns: list[str] = field(default_factory=list)


@dataclass
class ForeignKeyInfo:
    """Information about a foreign key constraint."""

    constraint_name: str
    columns: list[str] = field(default_factory=list)
    referenced_database: str = ""
    referenced_schema: str = ""
    referenced_table: str = ""
    referenced_columns: list[str] = field(default_factory=list)

    @property
    def referenced_fqn(self) -> str:
        """Get fully qualified name of referenced table."""
        return f"{self.referenced_database}.{self.referenced_schema}.{self.referenced_table}"


@dataclass
class UniqueConstraintInfo:
    """Information about a unique constraint."""

    constraint_name: str
    columns: list[str] = field(default_factory=list)


@dataclass
class TableInfo:
    """Information about a single table."""

    database: str
    schema: str
    name: str
    columns: dict[str, ColumnInfo] = field(default_factory=dict)
    primary_key: PrimaryKeyInfo | None = None
    foreign_keys: list[ForeignKeyInfo] = field(default_factory=list)
    unique_constraints: list[UniqueConstraintInfo] = field(default_factory=list)
    row_count: int | None = None

    @property
    def fqn(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.name}"

    @property
    def pk_columns(self) -> list[str]:
        """Get primary key column names."""
        return self.primary_key.columns if self.primary_key else []

    def get_fk_for_column(self, column_name: str) -> ForeignKeyInfo | None:
        """Get FK info for a column if it's a foreign key."""
        for fk in self.foreign_keys:
            if column_name in fk.columns:
                return fk
        return None

    def is_column_unique(self, column_name: str) -> bool:
        """Check if a column has a unique constraint."""
        if column_name in self.pk_columns:
            return True
        for uc in self.unique_constraints:
            if column_name in uc.columns and len(uc.columns) == 1:
                return True
        return False


@dataclass
class SchemaModel:
    """Complete schema model for a database."""

    database: str
    tables: dict[str, TableInfo] = field(default_factory=dict)

    def get_table(self, fqn: str) -> TableInfo | None:
        """Get table by fully qualified name."""
        return self.tables.get(fqn)

    def get_table_by_name(self, name: str) -> TableInfo | None:
        """Get table by name (searches all schemas)."""
        for fqn, table in self.tables.items():
            if table.name == name:
                return table
        return None

    def list_tables(self) -> list[str]:
        """List all table FQNs."""
        return list(self.tables.keys())


def discover_schema(
    session: Session,
    database: str,
    schemas: list[str] | None = None,
    tables: list[str] | None = None,
    include_row_counts: bool = False,
) -> SchemaModel:
    """Discover schema from Snowflake INFORMATION_SCHEMA.

    Args:
        session: Active Snowpark or connector session.
        database: Database name to discover.
        schemas: Optional list of schemas to include. If None, all schemas.
        tables: Optional list of table names to include. If None, all tables.
        include_row_counts: Whether to query row counts (slower).

    Returns:
        SchemaModel with discovered tables and constraints.

    Raises:
        DiscoveryError: If column discovery fails (fatal).
            Constraint discovery failures (e.g. shared/read-only databases
            that don't expose KEY_COLUMN_USAGE) are non-fatal and result in
            a schema with no PK/FK information.
    """
    import warnings

    model = SchemaModel(database=database)

    try:
        _discover_columns(session, database, schemas, tables, model)
    except Exception as e:
        raise DiscoveryError(f"Schema discovery failed: {e}") from e

    if model.tables:
        try:
            _discover_constraints(session, database, schemas, tables, model)
        except Exception as e:
            warnings.warn(
                f"Could not discover PK/FK constraints for '{database}': {e}. "
                "This can happen when the active role lacks REFERENCES privilege "
                "on the tables, or when the database is a shared/read-only database. "
                "Continuing without PK/FK information — you can add relationships "
                "manually in the generated YAML.",
                stacklevel=2,
            )

    if include_row_counts:
        _discover_row_counts(session, model)

    return model


def _discover_columns(
    session: Session,
    database: str,
    schemas: list[str] | None,
    tables: list[str] | None,
    model: SchemaModel,
) -> None:
    """Discover columns from INFORMATION_SCHEMA.COLUMNS."""
    schema_filter = ""
    if schemas:
        schema_list = ", ".join(f"'{s}'" for s in schemas)
        schema_filter = f"AND TABLE_SCHEMA IN ({schema_list})"

    table_filter = ""
    if tables:
        table_list = ", ".join(f"'{t}'" for t in tables)
        table_filter = f"AND TABLE_NAME IN ({table_list})"

    columns_sql = f"""
    SELECT
        TABLE_CATALOG,
        TABLE_SCHEMA,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        ORDINAL_POSITION,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        DATETIME_PRECISION,
        COLUMN_DEFAULT,
        IS_IDENTITY,
        IDENTITY_START,
        IDENTITY_INCREMENT
    FROM "{database}".INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
    AND TABLE_NAME NOT LIKE 'SF\\_SYNTH\\_%'
    {schema_filter}
    {table_filter}
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """

    result = session.sql(columns_sql).collect()

    for row in result:
        db = row["TABLE_CATALOG"]
        schema = row["TABLE_SCHEMA"]
        table_name = row["TABLE_NAME"]
        fqn = f"{db}.{schema}.{table_name}"

        if fqn not in model.tables:
            model.tables[fqn] = TableInfo(
                database=db,
                schema=schema,
                name=table_name,
            )

        col_info = ColumnInfo(
            name=row["COLUMN_NAME"],
            data_type=row["DATA_TYPE"],
            is_nullable=row["IS_NULLABLE"] == "YES",
            ordinal_position=int(row["ORDINAL_POSITION"]),
            character_maximum_length=(
                int(row["CHARACTER_MAXIMUM_LENGTH"])
                if row["CHARACTER_MAXIMUM_LENGTH"]
                else None
            ),
            numeric_precision=(
                int(row["NUMERIC_PRECISION"]) if row["NUMERIC_PRECISION"] else None
            ),
            numeric_scale=(
                int(row["NUMERIC_SCALE"]) if row["NUMERIC_SCALE"] else None
            ),
            datetime_precision=(
                int(row["DATETIME_PRECISION"]) if row["DATETIME_PRECISION"] else None
            ),
            column_default=row["COLUMN_DEFAULT"],
            is_identity=row["IS_IDENTITY"] == "YES",
            identity_start=(
                int(row["IDENTITY_START"]) if row["IDENTITY_START"] else None
            ),
            identity_increment=(
                int(row["IDENTITY_INCREMENT"]) if row["IDENTITY_INCREMENT"] else None
            ),
        )

        model.tables[fqn].columns[col_info.name] = col_info


def _discover_constraints(
    session: Session,
    database: str,
    schemas: list[str] | None,
    tables: list[str] | None,
    model: SchemaModel,
) -> None:
    """Discover PK, FK, and UNIQUE constraints using SHOW commands.

    Uses SHOW PRIMARY KEYS / SHOW IMPORTED KEYS / SHOW UNIQUE KEYS rather than
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE, which is unavailable in some Snowflake
    editions and shared/imported databases.
    """
    schema_set = {s.upper() for s in schemas} if schemas else None
    table_set = {t.upper() for t in tables} if tables else None

    def _in_scope(row_db: str, row_schema: str, row_table: str) -> bool:
        if row_db.upper() != database.upper():
            return False
        if schema_set and row_schema.upper() not in schema_set:
            return False
        if table_set and row_table.upper() not in table_set:
            return False
        return True

    # ── Primary keys ────────────────────────────────────────────────────────
    # Columns: created_on, database_name, schema_name, table_name,
    #          column_name, key_sequence, constraint_name, rely, comment
    pk_rows = session.sql(f"SHOW PRIMARY KEYS IN DATABASE \"{database}\"").collect()

    pk_map: dict[str, dict[str, list[str]]] = {}  # fqn -> constraint_name -> [cols]
    for row in pk_rows:
        db, schema, tbl = row["database_name"], row["schema_name"], row["table_name"]
        if not _in_scope(db, schema, tbl):
            continue
        fqn = f"{db}.{schema}.{tbl}"
        cname = row["constraint_name"]
        pk_map.setdefault(fqn, {}).setdefault(cname, [])
        pk_map[fqn][cname].append(row["column_name"])

    for fqn, constraints in pk_map.items():
        if fqn not in model.tables:
            continue
        for cname, cols in constraints.items():
            model.tables[fqn].primary_key = PrimaryKeyInfo(
                constraint_name=cname, columns=cols
            )

    # ── Unique keys ──────────────────────────────────────────────────────────
    # Same column layout as SHOW PRIMARY KEYS
    try:
        uk_rows = session.sql(f"SHOW UNIQUE KEYS IN DATABASE \"{database}\"").collect()
        uk_map: dict[str, dict[str, list[str]]] = {}
        for row in uk_rows:
            db, schema, tbl = row["database_name"], row["schema_name"], row["table_name"]
            if not _in_scope(db, schema, tbl):
                continue
            fqn = f"{db}.{schema}.{tbl}"
            cname = row["constraint_name"]
            uk_map.setdefault(fqn, {}).setdefault(cname, [])
            uk_map[fqn][cname].append(row["column_name"])

        for fqn, constraints in uk_map.items():
            if fqn not in model.tables:
                continue
            for cname, cols in constraints.items():
                model.tables[fqn].unique_constraints.append(
                    UniqueConstraintInfo(constraint_name=cname, columns=cols)
                )
    except Exception:
        pass  # UNIQUE KEY support is optional

    # ── Foreign keys (imported keys) ─────────────────────────────────────────
    # Columns: created_on, pk_database_name, pk_schema_name, pk_table_name,
    #          pk_column_name, fk_database_name, fk_schema_name, fk_table_name,
    #          fk_column_name, key_sequence, update_rule, delete_rule,
    #          fk_name, pk_name, deferrability, initially, rely, comment
    fk_rows = session.sql(f"SHOW IMPORTED KEYS IN DATABASE \"{database}\"").collect()

    fk_map: dict[str, dict[str, dict]] = {}  # fqn -> fk_name -> {cols, ref info}
    for row in fk_rows:
        fk_db = row["fk_database_name"]
        fk_schema = row["fk_schema_name"]
        fk_tbl = row["fk_table_name"]
        if not _in_scope(fk_db, fk_schema, fk_tbl):
            continue
        fqn = f"{fk_db}.{fk_schema}.{fk_tbl}"
        fk_name = row["fk_name"]

        if fqn not in fk_map:
            fk_map[fqn] = {}
        if fk_name not in fk_map[fqn]:
            fk_map[fqn][fk_name] = {
                "fk_cols": [],
                "ref_db": row["pk_database_name"],
                "ref_schema": row["pk_schema_name"],
                "ref_table": row["pk_table_name"],
                "ref_cols": [],
            }
        fk_map[fqn][fk_name]["fk_cols"].append(row["fk_column_name"])
        fk_map[fqn][fk_name]["ref_cols"].append(row["pk_column_name"])

    for fqn, fks in fk_map.items():
        if fqn not in model.tables:
            continue
        for fk_name, info in fks.items():
            model.tables[fqn].foreign_keys.append(
                ForeignKeyInfo(
                    constraint_name=fk_name,
                    columns=info["fk_cols"],
                    referenced_database=info["ref_db"],
                    referenced_schema=info["ref_schema"],
                    referenced_table=info["ref_table"],
                    referenced_columns=info["ref_cols"],
                )
            )


def _discover_row_counts(session: Session, model: SchemaModel) -> None:
    """Discover approximate row counts for tables."""
    for fqn, table_info in model.tables.items():
        try:
            count_sql = f'SELECT COUNT(*) AS CNT FROM "{table_info.database}"."{table_info.schema}"."{table_info.name}"'
            result = session.sql(count_sql).collect()
            if result:
                table_info.row_count = int(result[0]["CNT"])
        except Exception:
            pass


def schema_to_yaml(model: SchemaModel, include_columns: bool = True) -> dict:
    """Convert SchemaModel to YAML-friendly dict for config generation.

    Writes discovered columns into the ``columns:`` section with inferred
    generator configs so the output is immediately usable by ``generate``
    without any manual editing.

    Args:
        model: The schema model to convert.
        include_columns: Whether to include column details.

    Returns:
        Dictionary suitable for YAML serialization.
    """
    from sf_synth.semantic import suggest_generator_for_column

    tables = []

    for fqn, table_info in model.tables.items():
        table_dict: dict = {
            "name": fqn,
            "rows": table_info.row_count or 1000,
        }

        # FK columns are handled via relationships — exclude them from columns
        fk_cols = {col for fk in table_info.foreign_keys for col in fk.columns}
        pk_cols = set(table_info.pk_columns)

        if include_columns:
            columns: dict = {}
            for col_name, col_info in table_info.columns.items():
                if not col_info.is_supported:
                    continue
                if col_name in fk_cols:
                    continue  # written under relationships below

                is_pk = col_name in pk_cols
                is_unique = table_info.is_column_unique(col_name)

                gen_config = suggest_generator_for_column(
                    col_name,
                    col_info.data_type,
                    is_nullable=col_info.is_nullable,
                    is_unique=is_unique,
                    is_primary_key=is_pk,
                )

                # Guard: if semantic inference suggested a text generator
                # (faker) but the column is numeric/boolean/date, fall back
                # to the data-type default to avoid type-mismatch errors.
                base_type = col_info.data_type.split("(")[0].upper()
                _NUMERIC_TYPES = {
                    "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER",
                    "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
                    "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL",
                }
                gen_type = gen_config.get("generator", "")
                if gen_type == "faker" and base_type in _NUMERIC_TYPES:
                    from sf_synth.semantic import DATA_TYPE_DEFAULTS
                    fallback = DATA_TYPE_DEFAULTS.get(base_type)
                    if fallback:
                        gen_config = {"generator": fallback[0], **fallback[1]}
                        if is_unique:
                            gen_config["unique"] = True

                # Clamp uniform/range min/max to fit NUMBER(p,s) precision
                if (
                    gen_config.get("generator") in ("uniform", "range")
                    and col_info.numeric_precision is not None
                    and col_info.numeric_scale is not None
                ):
                    p, s = col_info.numeric_precision, col_info.numeric_scale
                    max_abs = 10 ** (p - s) - 10 ** (-s)
                    if gen_config.get("max_value") is not None:
                        gen_config["max_value"] = min(gen_config["max_value"], max_abs)
                    else:
                        gen_config["max_value"] = max_abs
                    if gen_config.get("min_value") is not None:
                        gen_config["min_value"] = max(gen_config["min_value"], -max_abs)
                    else:
                        gen_config["min_value"] = 0

                # null_ratio from column nullability
                if col_info.is_nullable and gen_config.get("generator") != "seq":
                    gen_config.setdefault("null_ratio", 0.05)

                columns[col_name] = gen_config

            if columns:
                table_dict["columns"] = columns

        relationships = []
        for fk in table_info.foreign_keys:
            for i, col in enumerate(fk.columns):
                ref_col = fk.referenced_columns[i] if i < len(fk.referenced_columns) else "id"
                relationships.append(
                    {
                        "column": col,
                        "references": f"{fk.referenced_fqn}.{ref_col}",
                    }
                )
        if relationships:
            table_dict["relationships"] = relationships

        tables.append(table_dict)

    return {
        "defaults": {
            "database": model.database,
            "seed": 42,
        },
        "tables": tables,
    }
