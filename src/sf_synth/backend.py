"""Backend abstraction for data generation.

Provides an interface for different execution backends (Snowpark, local, etc.)
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Session

    from sf_synth.config import SynthConfig
    from sf_synth.dag import GenerationPlan
    from sf_synth.discovery import SchemaModel
    from sf_synth.engine import SynthesisResult


class Backend(ABC):
    """Abstract base class for synthesis backends."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the backend."""

    @abstractmethod
    def disconnect(self) -> None:
        """Close the backend connection."""

    @abstractmethod
    def discover_schema(
        self,
        database: str,
        schemas: list[str] | None = None,
        tables: list[str] | None = None,
    ) -> SchemaModel:
        """Discover schema from the backend.

        Args:
            database: Database name.
            schemas: Optional list of schemas to include.
            tables: Optional list of tables to include.

        Returns:
            Discovered schema model.
        """

    @abstractmethod
    def generate(
        self,
        config: SynthConfig,
        schema_model: SchemaModel | None = None,
        dry_run: bool = False,
    ) -> SynthesisResult:
        """Execute data generation.

        Args:
            config: Synthesis configuration.
            schema_model: Optional discovered schema.
            dry_run: If True, validate but don't write.

        Returns:
            Synthesis result.
        """

    @abstractmethod
    def clean(self, config: SynthConfig) -> None:
        """Clean up generated data and temp tables.

        Args:
            config: Configuration specifying tables to clean.
        """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the backend is connected."""


class SnowparkBackend(Backend):
    """Snowpark-based backend for generation within Snowflake."""

    def __init__(
        self,
        connection_name: str | None = None,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
    ) -> None:
        """Initialize Snowpark backend.

        Connection can be established via:
        1. connection_name: Use named connection from ~/.snowflake/connections.toml
        2. Individual parameters (account, user, password, etc.)
        3. Environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.)

        Args:
            connection_name: Named connection from config file.
            account: Snowflake account identifier.
            user: Username.
            password: Password.
            database: Default database.
            schema: Default schema.
            warehouse: Warehouse to use.
            role: Role to use.
        """
        self.connection_name = connection_name
        self._connection_params = {
            "account": account or os.environ.get("SNOWFLAKE_ACCOUNT"),
            "user": user or os.environ.get("SNOWFLAKE_USER"),
            "password": password or os.environ.get("SNOWFLAKE_PASSWORD"),
            "database": database or os.environ.get("SNOWFLAKE_DATABASE"),
            "schema": schema or os.environ.get("SNOWFLAKE_SCHEMA"),
            "warehouse": warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "role": role or os.environ.get("SNOWFLAKE_ROLE"),
        }
        self._connection_params = {
            k: v for k, v in self._connection_params.items() if v is not None
        }

        self._session: Session | None = None
        self._engine: Any = None

    def connect(self) -> None:
        """Establish Snowpark session."""
        from snowflake.snowpark import Session

        if self._session is not None:
            return

        if self.connection_name:
            self._session = Session.builder.config(
                "connection_name", self.connection_name
            ).create()
        elif self._connection_params:
            self._session = Session.builder.configs(self._connection_params).create()
        else:
            self._session = Session.builder.create()

    def disconnect(self) -> None:
        """Close Snowpark session."""
        if self._session is not None:
            if self._engine:
                self._engine.cleanup()
                self._engine = None
            self._session.close()
            self._session = None

    @property
    def is_connected(self) -> bool:
        """Check if session is active."""
        return self._session is not None

    @property
    def session(self) -> Session:
        """Get the active session."""
        if self._session is None:
            raise RuntimeError("Backend not connected. Call connect() first.")
        return self._session

    def discover_schema(
        self,
        database: str,
        schemas: list[str] | None = None,
        tables: list[str] | None = None,
    ) -> SchemaModel:
        """Discover schema from Snowflake."""
        from sf_synth.discovery import discover_schema

        return discover_schema(
            self.session,
            database,
            schemas=schemas,
            tables=tables,
            include_row_counts=True,
        )

    def generate(
        self,
        config: SynthConfig,
        schema_model: SchemaModel | None = None,
        dry_run: bool = False,
    ) -> SynthesisResult:
        """Execute synthesis via Snowpark."""
        from sf_synth.engine import SynthEngine

        self._engine = SynthEngine(
            self.session,
            config,
            schema_model=schema_model,
        )

        return self._engine.generate(dry_run=dry_run)

    def clean(self, config: SynthConfig) -> None:
        """Clean up generated tables."""
        if self._engine:
            self._engine.cleanup()

        database = config.defaults.database or self.session.get_current_database()
        schema = config.defaults.schema_name or self.session.get_current_schema()

        cleanup_sql = f"""
        SELECT table_name
        FROM "{database}".INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
        AND table_name LIKE 'SF_SYNTH_%'
        AND table_type = 'TRANSIENT TABLE'
        """

        try:
            result = self.session.sql(cleanup_sql).collect()
            for row in result:
                table_name = row["TABLE_NAME"]
                drop_sql = f'DROP TABLE IF EXISTS "{database}"."{schema}"."{table_name}"'
                self.session.sql(drop_sql).collect()
        except Exception:
            pass

    def get_plan_summary(self, config: SynthConfig) -> dict[str, Any]:
        """Get a summary of the generation plan without executing.

        Args:
            config: Synthesis configuration.

        Returns:
            Dictionary with plan details.
        """
        from sf_synth.dag import build_dag_from_config
        from sf_synth.engine import SynthEngine

        plan = build_dag_from_config(config)

        engine = SynthEngine(self.session, config)
        estimates = engine.estimate_size()

        return {
            "generation_order": plan.generation_order,
            "total_tables": len(plan.tables),
            "total_rows": plan.estimated_total_rows,
            "self_referential_tables": plan.self_referential_tables,
            "dependencies": [
                {
                    "child": e.child_table,
                    "parent": e.parent_table,
                    "columns": e.child_columns,
                }
                for e in plan.edges
            ],
            "size_estimates": estimates,
            "dag_visualization": plan.visualize() if hasattr(plan, "visualize") else None,
        }


def create_backend(
    backend_type: str = "snowpark",
    **kwargs: Any,
) -> Backend:
    """Factory function to create a backend.

    Args:
        backend_type: Type of backend ('snowpark' for now).
        **kwargs: Backend-specific configuration.

    Returns:
        Configured backend instance.

    Raises:
        ValueError: If backend type is not supported.
    """
    if backend_type == "snowpark":
        return SnowparkBackend(**kwargs)
    else:
        raise ValueError(
            f"Unsupported backend type: {backend_type}. "
            f"Supported types: snowpark"
        )
