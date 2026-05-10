# Changelog

All notable changes to sf-synth are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
