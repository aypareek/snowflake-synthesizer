# Changelog

All notable changes to sf-synth are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2026-05-10

### Added
- New CLI command `sf-synth preview` — generates a small sample (default 10 rows
  per table) and prints it as a Rich table without writing anything to Snowflake.
  Catches type mismatches and truncation issues before a long generation run.
- New CLI command `sf-synth validate` — validates a config.yaml against the live
  Snowflake DDL. Reports missing columns, type mismatches (e.g. faker on a NUMBER
  column), range overflows for `NUMBER(p,s)`, and invalid FK references.
- `--mode` flag on `generate` (`replace` / `append` / `upsert` / `fill_to`) —
  controls how synthesized rows interact with existing data.
  - `replace`: truncate and insert (default).
  - `append`: insert without truncating.
  - `upsert`: MERGE on `upsert_keys` so re-running updates instead of duplicating.
  - `fill_to`: only generate enough rows to reach the configured target count.
- `--truncate` / `--no-truncate` flag on `generate` for explicit override.
- `--parallel N` flag on `generate` — generates independent tables (same DAG depth
  level) concurrently using a thread pool.
- `--report PATH` and `--profile` flags on `generate` — writes a markdown report
  with row counts, per-column samples, and optional distinct/null/min/max stats.
- `--verbose` / `--quiet` flags on every command for log-level control.
- New generator: `expression` — raw SQL expression for computed/derived columns
  (e.g. `FULL_NAME = FIRST_NAME || ' ' || LAST_NAME`).
- New generator: `json_template` — `{{faker.x}}`, `{{uniform(a,b)}}`, `{{seq}}`,
  `{{choice(...)}}` placeholders compiled to a Snowflake `TRY_PARSE_JSON(...)`.
- New generator: `array` — `ARRAY_CONSTRUCT(...)` with optional random length
  range (`length: [1, 5]`).
- New generator: `object` — nested OBJECT_CONSTRUCT with sub-column configs.
- New column field `correlation_group` — multiple `faker` columns in the same
  group are drawn from a single Faker instance, so `city`/`state`/`country`
  remain semantically consistent within a row (single VARIANT-returning UDF).
- New column fields `after`, `after_offset_unit`, `after_offset_min`,
  `after_offset_max` — temporal ordering. Date/timestamp columns can be
  generated as a `DATEADD` from another column.
- New column fields `condition` + `else_value` — conditional generation
  (e.g. `SUSPENDED_AT` only filled when `STATUS = 'suspended'`).
- Proper regex generator backed by `exrex` — registered as Snowpark UDF
  `sf_synth_regex_generate(pattern, row_id)`. Falls back to a character-class
  generator if `exrex` isn't available.

### Changed
- FK index sampling now uses `HASH(_rownum, seed)` instead of `RANDOM(seed)` —
  gives fully deterministic FK assignment that's reproducible across runs.
- Faker-version-mismatch warnings from Snowpark are now suppressed by default
  (visible with `--verbose`).
- Failure rows in the results table now show the offending column when one can
  be inferred from the Snowflake error message.

### Internal
- Engine refactored to a single `_build_select_sql` path used by both `generate`
  and `preview`.
- New module `sf_synth.validation` (`validate_config_against_ddl`).
- New module `sf_synth.report` (`build_markdown_report`).
- 41 new unit tests covering new generator types, write modes, validation,
  and engine SQL helpers.

---

## [0.3.0] - 2026-05-10

### Fixed
- Replaced `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` / `REFERENTIAL_CONSTRAINTS` queries
  with `SHOW PRIMARY KEYS`, `SHOW IMPORTED KEYS`, and `SHOW UNIQUE KEYS` — these work
  on all Snowflake editions and user-created databases where `KEY_COLUMN_USAGE` is absent.
- Constraint discovery is now non-fatal: if `SHOW` commands fail (e.g. shared/read-only
  databases), column discovery still succeeds and the CLI prints a clear warning.
- CLI `discover` now exits cleanly with an actionable message when 0 tables are found,
  listing likely causes (empty database, missing USAGE grant, wrong role).
- Constraint discovery is skipped when no tables were found (avoids redundant queries).

### Changed
- Named connection (`--connection`) now correctly passes through to all CLI commands.

---

## [0.2.0] - 2026-05-08

### Fixed
- Replaced `match`/`case` statements with `if/elif` chains for Python 3.10 compatibility.
- Bumped minimum Python version to 3.10.
- Fixed CI workflow to install lightweight test deps via `PYTHONPATH=src` instead of a full editable install.

### Added
- Multi-schema example (`examples/multi_schema.yaml`).
- GitHub Actions workflows for CI and automated PyPI publishing.
- CHANGELOG.md and improved `pyproject.toml` metadata.

---

## [0.1.0] - 2026-05-08

### Added
- Snowpark-first execution engine — data is generated entirely inside Snowflake.
- Auto-discovery of tables, columns, types, PK/FK/UNIQUE/NOT NULL from `INFORMATION_SCHEMA`.
- DAG-based generation order using `networkx` topological sort.
- Self-referential FK support via two-pass generation (insert NULL → UPDATE).
- Distribution-preserving generators using `APPROX_TOP_K`, `APPROX_PERCENTILE`, and `HLL`.
- SQL-native generators: `seq`, `uniform`, `choice`, `range`, `regex`.
- Faker UDF generators for rich fake data (email, name, address, phone, etc.).
- Zipf-weighted FK sampling for realistic skewed distributions.
- Pydantic v2 configuration with YAML loader and strict validation.
- Column-name semantic type inference (80+ patterns).
- Typer CLI with `discover`, `plan`, `generate`, and `clean` commands.
- `plan` command shows generation order, row counts, and byte estimates without writing.
- `--seed` flag for fully deterministic output.
- Multi-schema support: FK references across schemas within the same database.
- Backend abstraction (`Backend` ABC + `SnowparkBackend` implementation).
- Unit tests and opt-in Snowflake integration tests.
- Example configs: e-commerce, self-referential employees, multi-schema enterprise.
