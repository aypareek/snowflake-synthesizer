# sf-synth

High-fidelity synthetic data generation for Snowflake.

A Snowpark-first Python library and CLI that generates realistic synthetic data inside Snowflake using auto-discovered schema, distribution statistics, Faker-based rules, and a DAG-driven referential-integrity engine. All generation runs server-side, so PII never leaves the account.

## Features

- **Snowpark-first execution**: Data is generated entirely within Snowflake using Snowpark. No data egress required.
- **Auto-discovery**: Automatically detects tables, columns, types, constraints (PK, FK, UNIQUE, NOT NULL) from `INFORMATION_SCHEMA`.
- **Referential integrity**: DAG-based generation ensures parent tables are populated before children. FK values are sampled from actual parent keys.
- **Self-referential tables**: Handles self-referential FKs (e.g., `employees.manager_id → employees.id`) via two-pass generation.
- **Multi-schema support**: Reference tables across different schemas within the same database (e.g., `SALES.CUSTOMERS` → `CORE.COUNTRIES`).
- **Distribution-preserving**: Sample from real column statistics (`APPROX_TOP_K`, `APPROX_PERCENTILE`, `HLL`) to preserve data distributions without exposing PII.
- **Skewed FK distributions**: Support for Zipf-weighted FK sampling (e.g., 80% of orders belong to 20% of customers).
- **Semantic inference**: Automatically infers generators based on column names (e.g., `email`, `phone`, `created_at`).
- **Deterministic output**: Seed-based generation for reproducible results.
- **YAML configuration**: Simple, validated config with Pydantic.

## Installation

```bash
pip install sf-synth
```

Or install from source:

```bash
git clone https://github.com/apareek/snowflake-synthesizer.git
cd snowflake-synthesizer
pip install -e ".[dev]"
```

## Quick Start

### 1. Discover your schema

Generate a starter config by discovering your existing Snowflake schema:

```bash
sf-synth discover MY_DATABASE --output config.yaml
```

### 2. Edit the config

Customize row counts, add generators, and define relationships:

```yaml
defaults:
  seed: 42
  database: MY_DATABASE
  schema: PUBLIC

tables:
  - name: CUSTOMERS
    rows: 10000
    columns:
      EMAIL:
        generator: faker
        provider: email
        unique: true
      MEMBERSHIP:
        generator: choice
        values: [Gold, Silver, Bronze]
        weights: [0.1, 0.3, 0.6]

  - name: ORDERS
    rows: 50000
    relationships:
      - column: CUSTOMER_ID
        references: CUSTOMERS.ID
        skew: zipf
```

### 3. Preview the plan

See the generation order and dependencies without executing:

```bash
sf-synth plan config.yaml
```

### 4. Generate data

```bash
sf-synth generate config.yaml
```

### 5. Clean up

Remove temporary tables created during generation:

```bash
sf-synth clean config.yaml
```

## Configuration Reference

### Defaults

```yaml
defaults:
  seed: 42                    # Random seed for reproducibility
  locale: en_US               # Faker locale
  database: MY_DB             # Default database
  schema: PUBLIC              # Default schema
  null_ratio: 0.0             # Default null ratio for all columns
```

### Generator Types

| Generator | Description | Required Parameters |
|-----------|-------------|---------------------|
| `seq` | Sequential integers | `start`, `step` |
| `uniform` | Uniform random numbers | `min_value`, `max_value` |
| `choice` | Random selection from list | `values`, `weights` (optional) |
| `range` | Values in numeric/date range | `min_value`, `max_value` |
| `faker` | Faker provider | `provider`, `locale` (optional) |
| `distribution` | Sample from source column stats | `source` (FQN: DB.SCHEMA.TABLE.COL) |
| `regex` | Pattern-based strings | `pattern` |

### Faker Providers

Common providers: `email`, `name`, `first_name`, `last_name`, `phone_number`, `address`, `city`, `state`, `zipcode`, `country`, `company`, `job`, `date`, `date_time`, `uuid4`, `url`, `ipv4`, `ssn`, `credit_card_number`.

### Relationships

```yaml
relationships:
  - column: CUSTOMER_ID           # FK column in this table
    references: CUSTOMERS.ID       # Parent table.column
    null_ratio: 0.05              # 5% null FKs
    skew: zipf                    # Distribution: uniform or zipf
    skew_param: 1.5               # Zipf exponent (higher = more skewed)
```

## Python API

```python
from sf_synth import SynthConfig, SynthEngine, discover_schema
from sf_synth.backend import SnowparkBackend

# Connect to Snowflake
backend = SnowparkBackend(connection_name="my_connection")
backend.connect()

# Discover schema
schema = backend.discover_schema("MY_DATABASE")

# Load config
from sf_synth.config import load_config
config = load_config("config.yaml")

# Generate
engine = SynthEngine(backend.session, config, schema_model=schema)
result = engine.generate()

print(f"Generated {result.total_rows} rows in {result.total_elapsed_seconds:.2f}s")

# Cleanup
engine.cleanup()
backend.disconnect()
```

## Examples

The `examples/` directory contains ready-to-use configurations:

| Example | Description |
|---------|-------------|
| [`ecommerce.yaml`](examples/ecommerce.yaml) | E-commerce schema with customers, products, orders, and reviews. Demonstrates FK relationships, Zipf-skewed distributions, and various generators. |
| [`selfref_employees.yaml`](examples/selfref_employees.yaml) | HR schema with self-referential `manager_id` FK. Shows how sf-synth handles circular references via two-pass generation. |
| [`multi_schema.yaml`](examples/multi_schema.yaml) | Enterprise schema spanning CORE, HR, SALES, and FINANCE schemas. Demonstrates cross-schema FK relationships within a single database. |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   CLI       │────▶│   Config     │────▶│  Discovery  │
│   (Typer)   │     │  (Pydantic)  │     │  (INFO_SCH) │
└─────────────┘     └──────────────┘     └─────────────┘
                           │                    │
                           ▼                    ▼
                    ┌─────────────┐     ┌─────────────┐
                    │  DAG Builder│────▶│   Schema    │
                    │  (networkx) │     │   Model     │
                    └─────────────┘     └─────────────┘
                           │
                           ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Stats     │────▶│   Engine    │────▶│ RI Manager  │
│   Sampler   │     │  (Snowpark) │     │ (Parent Keys│
│ (APPROX_*)  │     └─────────────┘     └─────────────┘
└─────────────┘            │
                           ▼
                    ┌─────────────┐
                    │  Snowflake  │
                    │   Tables    │
                    └─────────────┘
```

## Connection Configuration

sf-synth uses standard Snowflake connection methods:

1. **Named connection** (recommended): `~/.snowflake/connections.toml`
2. **Environment variables**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.
3. **CLI parameters**: `--connection`, `--account`, etc.

Example `~/.snowflake/connections.toml`:

```toml
[my_connection]
account = "myaccount"
user = "myuser"
authenticator = "externalbrowser"
database = "MY_DB"
schema = "PUBLIC"
warehouse = "COMPUTE_WH"
```

## Performance Notes

- **SQL-first generators** (seq, uniform, choice, range) are fast and scale to billions of rows.
- **Faker UDFs** are slower due to Python UDF overhead. Use them only when SQL alternatives don't exist.
- **Distribution sampling** requires one-time stats queries per column but generates data efficiently.
- For very large tables (>100M rows), consider chunked generation or Snowflake-native `GENERATOR()` patterns.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/unit/

# Run integration tests (requires Snowflake credentials)
SF_SYNTH_INTEGRATION_TESTS=1 pytest tests/integration/

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## License

MIT License. See [LICENSE](LICENSE) for details.
