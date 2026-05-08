"""Pydantic configuration models for sf-synth."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class GeneratorType(str, Enum):
    """Supported generator types."""

    FAKER = "faker"
    CHOICE = "choice"
    DISTRIBUTION = "distribution"
    RANGE = "range"
    REGEX = "regex"
    UNIFORM = "uniform"
    SEQ = "seq"


class SkewType(str, Enum):
    """FK distribution skew types."""

    UNIFORM = "uniform"
    ZIPF = "zipf"


class ColumnConfig(BaseModel):
    """Configuration for a single column generator."""

    model_config = ConfigDict(extra="forbid")

    generator: GeneratorType
    provider: str | None = None
    locale: str = "en_US"
    values: list[Any] | None = None
    weights: list[float] | None = None
    source: str | None = None
    pattern: str | None = None
    min_value: float | int | str | None = None
    max_value: float | int | str | None = None
    start: int = 0
    step: int = 1
    unique: bool = False
    null_ratio: float = 0.0

    @field_validator("null_ratio")
    @classmethod
    def validate_null_ratio(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("null_ratio must be between 0.0 and 1.0")
        return v

    @field_validator("weights")
    @classmethod
    def validate_weights(cls, v: list[float] | None) -> list[float] | None:
        if v is not None and any(w < 0 for w in v):
            raise ValueError("weights must be non-negative")
        return v

    @model_validator(mode="after")
    def validate_generator_params(self) -> "ColumnConfig":
        """Validate that required params are provided for each generator type."""
        g = self.generator
        if g == GeneratorType.FAKER:
            if not self.provider:
                raise ValueError("faker generator requires 'provider' parameter")
        elif g == GeneratorType.CHOICE:
            if not self.values:
                raise ValueError("choice generator requires 'values' parameter")
            if self.weights and len(self.weights) != len(self.values):
                raise ValueError("weights length must match values length")
        elif g == GeneratorType.DISTRIBUTION:
            if not self.source:
                raise ValueError("distribution generator requires 'source' parameter")
        elif g == GeneratorType.REGEX:
            if not self.pattern:
                raise ValueError("regex generator requires 'pattern' parameter")
        elif g == GeneratorType.RANGE:
            if self.min_value is None or self.max_value is None:
                raise ValueError("range generator requires 'min_value' and 'max_value'")
        return self


class RelationshipConfig(BaseModel):
    """Configuration for a foreign key relationship."""

    model_config = ConfigDict(extra="forbid")

    column: str
    references: str
    null_ratio: float = 0.0
    skew: SkewType = SkewType.UNIFORM
    skew_param: float = 1.5

    @field_validator("null_ratio")
    @classmethod
    def validate_null_ratio(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("null_ratio must be between 0.0 and 1.0")
        return v

    @field_validator("references")
    @classmethod
    def validate_references(cls, v: str) -> str:
        parts = v.split(".")
        if len(parts) < 2:
            raise ValueError(
                "references must be at least TABLE.COLUMN format, "
                "preferably DB.SCHEMA.TABLE.COLUMN"
            )
        return v


class TableConfig(BaseModel):
    """Configuration for a single table."""

    model_config = ConfigDict(extra="forbid")

    name: str
    rows: Annotated[int, Field(gt=0)]
    columns: dict[str, ColumnConfig] = Field(default_factory=dict)
    relationships: list[RelationshipConfig] = Field(default_factory=list)
    truncate_before: bool = True
    target_schema: str | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v:
            raise ValueError("table name cannot be empty")
        return v

    def get_fqn(self, default_database: str | None = None, default_schema: str | None = None) -> str:
        """Get fully qualified table name."""
        parts = self.name.split(".")
        if len(parts) == 3:
            return self.name
        elif len(parts) == 2:
            db = default_database or "DATABASE"
            return f"{db}.{self.name}"
        elif len(parts) == 1:
            db = default_database or "DATABASE"
            schema = self.target_schema or default_schema or "PUBLIC"
            return f"{db}.{schema}.{self.name}"
        return self.name


class DefaultsConfig(BaseModel):
    """Default settings for the synthesis run."""

    model_config = ConfigDict(extra="forbid")

    seed: int | None = None
    locale: str = "en_US"
    database: str | None = None
    schema_name: str | None = Field(None, alias="schema")
    warehouse: str | None = None
    role: str | None = None
    null_ratio: float = 0.0
    batch_size: int = 100000


class SynthConfig(BaseModel):
    """Root configuration for sf-synth."""

    model_config = ConfigDict(extra="forbid")

    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    tables: list[TableConfig] = Field(default_factory=list)

    @field_validator("tables")
    @classmethod
    def validate_tables_unique(cls, v: list[TableConfig]) -> list[TableConfig]:
        names = [t.name for t in v]
        duplicates = [n for n in names if names.count(n) > 1]
        if duplicates:
            raise ValueError(f"Duplicate table names: {set(duplicates)}")
        return v

    def get_table(self, name: str) -> TableConfig | None:
        """Get table config by name."""
        for table in self.tables:
            if table.name == name or table.name.endswith(f".{name}"):
                return table
        return None

    def get_total_rows(self) -> int:
        """Get total row count across all tables."""
        return sum(t.rows for t in self.tables)


def load_config(path: str | Path) -> SynthConfig:
    """Load and validate configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated SynthConfig object.

    Raises:
        ConfigError: If the file cannot be read or parsed.
        ValidationError: If the configuration is invalid.
    """
    from sf_synth.errors import ConfigError

    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML: {e}") from e

    if data is None:
        data = {}

    return SynthConfig.model_validate(data)


def save_config(config: SynthConfig, path: str | Path) -> None:
    """Save configuration to a YAML file.

    Args:
        config: Configuration to save.
        path: Path to the output YAML file.
    """
    path = Path(path)
    data = config.model_dump(exclude_none=True, exclude_defaults=True, by_alias=True)

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def generate_config_template(
    tables: list[str] | None = None,
    rows_per_table: int = 1000,
) -> SynthConfig:
    """Generate a template configuration.

    Args:
        tables: List of table names to include.
        rows_per_table: Default row count per table.

    Returns:
        A template SynthConfig object.
    """
    table_configs = []
    if tables:
        for table_name in tables:
            table_configs.append(
                TableConfig(
                    name=table_name,
                    rows=rows_per_table,
                )
            )

    return SynthConfig(
        defaults=DefaultsConfig(seed=42),
        tables=table_configs,
    )
