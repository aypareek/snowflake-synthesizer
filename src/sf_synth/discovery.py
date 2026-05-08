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
        DiscoveryError: If discovery fails.
    """
    model = SchemaModel(database=database)

    try:
        _discover_columns(session, database, schemas, tables, model)
        _discover_constraints(session, database, schemas, tables, model)

        if include_row_counts:
            _discover_row_counts(session, model)

    except Exception as e:
        raise DiscoveryError(f"Schema discovery failed: {e}") from e

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
    """Discover PK, FK, and UNIQUE constraints."""
    schema_filter = ""
    if schemas:
        schema_list = ", ".join(f"'{s}'" for s in schemas)
        schema_filter = f"AND tc.TABLE_SCHEMA IN ({schema_list})"

    table_filter = ""
    if tables:
        table_list = ", ".join(f"'{t}'" for t in tables)
        table_filter = f"AND tc.TABLE_NAME IN ({table_list})"

    constraints_sql = f"""
    SELECT
        tc.CONSTRAINT_NAME,
        tc.CONSTRAINT_TYPE,
        tc.TABLE_CATALOG,
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        kcu.COLUMN_NAME,
        kcu.ORDINAL_POSITION,
        rc.UNIQUE_CONSTRAINT_CATALOG,
        rc.UNIQUE_CONSTRAINT_SCHEMA,
        rc.UNIQUE_CONSTRAINT_NAME
    FROM "{database}".INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    LEFT JOIN "{database}".INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
    LEFT JOIN "{database}".INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
    WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
    {schema_filter}
    {table_filter}
    ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    """

    result = session.sql(constraints_sql).collect()

    constraints: dict[str, dict[str, list]] = {}

    for row in result:
        db = row["TABLE_CATALOG"]
        schema = row["TABLE_SCHEMA"]
        table_name = row["TABLE_NAME"]
        fqn = f"{db}.{schema}.{table_name}"

        if fqn not in constraints:
            constraints[fqn] = {}

        constraint_name = row["CONSTRAINT_NAME"]
        constraint_type = row["CONSTRAINT_TYPE"]

        key = f"{constraint_type}:{constraint_name}"
        if key not in constraints[fqn]:
            constraints[fqn][key] = []

        constraints[fqn][key].append(row)

    for fqn, table_constraints in constraints.items():
        if fqn not in model.tables:
            continue

        table_info = model.tables[fqn]

        for key, rows in table_constraints.items():
            constraint_type, constraint_name = key.split(":", 1)
            columns = [r["COLUMN_NAME"] for r in rows if r["COLUMN_NAME"]]

            if constraint_type == "PRIMARY KEY":
                table_info.primary_key = PrimaryKeyInfo(
                    constraint_name=constraint_name,
                    columns=columns,
                )
            elif constraint_type == "UNIQUE":
                table_info.unique_constraints.append(
                    UniqueConstraintInfo(
                        constraint_name=constraint_name,
                        columns=columns,
                    )
                )
            elif constraint_type == "FOREIGN KEY":
                first_row = rows[0]
                ref_constraint = first_row["UNIQUE_CONSTRAINT_NAME"]
                if ref_constraint:
                    ref_info = _get_referenced_table(
                        session,
                        database,
                        first_row["UNIQUE_CONSTRAINT_SCHEMA"] or table_info.schema,
                        ref_constraint,
                    )
                    if ref_info:
                        table_info.foreign_keys.append(
                            ForeignKeyInfo(
                                constraint_name=constraint_name,
                                columns=columns,
                                referenced_database=ref_info["database"],
                                referenced_schema=ref_info["schema"],
                                referenced_table=ref_info["table"],
                                referenced_columns=ref_info["columns"],
                            )
                        )


def _get_referenced_table(
    session: Session,
    database: str,
    schema: str,
    constraint_name: str,
) -> dict | None:
    """Get referenced table info for a unique constraint."""
    ref_sql = f"""
    SELECT
        tc.TABLE_CATALOG,
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        kcu.COLUMN_NAME
    FROM "{database}".INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN "{database}".INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_NAME = '{constraint_name}'
    AND tc.TABLE_SCHEMA = '{schema}'
    ORDER BY kcu.ORDINAL_POSITION
    """

    try:
        result = session.sql(ref_sql).collect()
        if result:
            return {
                "database": result[0]["TABLE_CATALOG"],
                "schema": result[0]["TABLE_SCHEMA"],
                "table": result[0]["TABLE_NAME"],
                "columns": [r["COLUMN_NAME"] for r in result],
            }
    except Exception:
        pass

    return None


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

    Args:
        model: The schema model to convert.
        include_columns: Whether to include column details.

    Returns:
        Dictionary suitable for YAML serialization.
    """
    tables = []

    for fqn, table_info in model.tables.items():
        table_dict: dict = {
            "name": fqn,
            "rows": table_info.row_count or 1000,
        }

        if include_columns:
            columns = {}
            for col_name, col_info in table_info.columns.items():
                if col_info.is_supported:
                    columns[col_name] = {
                        "type": col_info.data_type,
                        "nullable": col_info.is_nullable,
                    }
            if columns:
                table_dict["_discovered_columns"] = columns

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
