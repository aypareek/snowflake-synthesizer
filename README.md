# sf-synth

High-fidelity synthetic data generation for Snowflake.

A Snowpark-first Python library and CLI that generates realistic synthetic data inside Snowflake using auto-discovered schema, distribution statistics, Faker-based rules, and a DAG-driven referential-integrity engine. All generation runs server-side, so PII never leaves the account.

## Features

- **Snowpark-first execution**: Data is generated entirely within Snowflake using Snowpark. No data egress required.
- **Auto-discovery**: Automatically detects tables, columns, types, constraints (PK, FK, UNIQUE, NOT NULL) from `INFORMATION_SCHEMA`.
- **Referential integrity**: DAG-based generation ensures parent tables are populated before children. FK values are sampled from actual parent keys.
- **Self-referential tables**: Handles self-referential FKs (e.g., `employees.manager_id → employees.id`) via two-pass generation.
- **Multi-schema support**: Reference tables across different schemas within the same database.
- **Distribution-preserving**: Sample from real column statistics (`APPROX_TOP_K`, `APPROX_PERCENTILE`, `HLL`) to preserve data distributions without exposing PII.
- **Skewed FK distributions**: Zipf-weighted FK sampling for realistic skew (e.g., 80% of orders belong to 20% of customers).
- **Correlated columns**: Group `faker` columns so `city`/`state`/`country` are drawn from the same Faker profile and stay semantically consistent within a row.
- **Temporal ordering**: Column-level `after` constraints to enforce realistic timelines (`updated_at` always after `created_at`).
- **Semi-structured data**: First-class generators for VARIANT/ARRAY/OBJECT, plus a `json_template` mini-DSL for nested payloads.
- **Computed columns**: Use a raw SQL `expression` generator for derived fields like `FULL_NAME = FIRST_NAME || ' ' || LAST_NAME`.
- **Conditional generation**: `condition` + `else_value` to drive a column's value from another column.
- **Write modes**: `replace` / `append` / `upsert` / `fill_to` per table or via `--mode`.
- **Parallel generation**: Independent tables in the DAG run concurrently with `--parallel N`.
- **Validate & preview**: `sf-synth validate` checks the config against live DDL; `sf-synth preview` shows sample rows before any writes.
- **Run reports**: `--report` and `--profile` produce a markdown summary with row counts, sample rows, and per-column distinct/null/min/max.
- **Semantic inference**: Auto-infers generators from column names (e.g., `email`, `phone`, `created_at`).
- **Deterministic output**: Seed-based, hash-driven FK sampling produces fully reproducible runs.
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

### 4. Validate the config

Catch type mismatches and FK problems before any data is written:

```bash
sf-synth validate config.yaml --connection my_conn
```

### 5. Preview a few rows

Generate a tiny sample (default 10 rows per table) without writing anything:

```bash
sf-synth preview config.yaml --rows 5
```

### 6. Generate data

```bash
sf-synth generate config.yaml \
  --mode replace \
  --parallel 4 \
  --report run_report.md \
  --profile
```

Useful flags:
- `--mode replace|append|upsert|fill_to` — override per-table write mode.
- `--truncate` / `--no-truncate` — force truncate-before-insert behavior.
- `--parallel N` — generate independent tables in parallel.
- `--report PATH` — write a markdown summary of the run.
- `--profile` — include per-column distinct/null/min/max in the report.
- `--seed N` — override the seed for reproducibility.
- `--verbose` / `--quiet` — control log noise.

### 7. Clean up

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
| `regex` | Pattern-based strings via `exrex` UDF | `pattern` |
| `expression` | Raw SQL expression for computed columns | `sql` |
| `json_template` | `{{...}}` template compiled to `TRY_PARSE_JSON` | `template` |
| `array` | `ARRAY_CONSTRUCT(...)` of element-generator outputs | `element_generator`, `length` |
| `object` | `OBJECT_CONSTRUCT(...)` from a `fields` mapping | `fields` |

### Cross-column features

| Field | Description |
|-------|-------------|
| `correlation_group` | Multiple `faker` columns in the same group share a single Faker profile per row (consistent city/state/country). |
| `after` | This date/timestamp column is generated as `DATEADD(unit, offset, <other_col>)`. Tunable via `after_offset_unit`/`after_offset_min`/`after_offset_max`. |
| `condition` / `else_value` | Wrap the generator in `IFF(<condition>, <generated>, <else_value>)`. |

### Write modes

Per-table (`write_mode:`) or globally via `--mode`:

| Mode | Behavior |
|------|----------|
| `replace` | Truncate target then insert (default). |
| `append` | Insert without truncating. |
| `upsert` | MERGE on `upsert_keys` — update existing rows and insert new ones. |
| `fill_to` | Generate only enough rows to reach `rows:` total (no-op if already there). |

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
