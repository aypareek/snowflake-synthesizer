#!/usr/bin/env python
"""Offline SQL inspection tool for sf-synth v0.4.0 testing.

Builds the full SELECT statements that would be sent to Snowflake for each
table in a config, *without* connecting. Useful to validate that all the new
features (correlation_group, after, expression, json_template, array, object,
regex, conditional, write modes) emit the expected SQL.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sf_synth.config import load_config  # noqa: E402
from sf_synth.engine import SynthEngine  # noqa: E402


def make_session(register_keys: dict[str, int] | None = None) -> MagicMock:
    session = MagicMock(name="MockSession")
    session.get_current_database.return_value = "ABC"
    session.get_current_schema.return_value = "AYUSH"
    register_keys = register_keys or {}

    def add_packages(*a, **k):
        return None

    session.add_packages.side_effect = add_packages

    def sql_call(query: str):
        result = MagicMock()
        if "DESCRIBE TABLE" in query.upper():
            tbl = query.split()[-1]
            base = tbl.split(".")[-1]
            cols = []
            if "USERS" in base:
                cols = [
                    ("USER_ID", "NUMBER(10,0)"),
                    ("FIRST_NAME", "VARCHAR(50)"),
                    ("LAST_NAME", "VARCHAR(50)"),
                    ("EMAIL", "VARCHAR(255)"),
                    ("FULL_NAME", "VARCHAR(120)"),
                    ("CITY", "VARCHAR(100)"),
                    ("STATE", "VARCHAR(100)"),
                    ("COUNTRY", "VARCHAR(100)"),
                    ("STATUS", "VARCHAR(20)"),
                    ("CREATED_AT", "TIMESTAMP_NTZ(9)"),
                    ("UPDATED_AT", "TIMESTAMP_NTZ(9)"),
                    ("SUSPENDED_AT", "TIMESTAMP_NTZ(9)"),
                    ("ORDER_PATTERN", "VARCHAR(20)"),
                ]
            elif "EVENTS" in base:
                cols = [
                    ("EVENT_ID", "NUMBER(10,0)"),
                    ("METADATA", "OBJECT"),
                    ("TAGS", "ARRAY"),
                    ("PAYLOAD", "VARIANT"),
                ]
            elif "ORDERS" in base:
                cols = [
                    ("ORDER_ID", "NUMBER(10,0)"),
                    ("USER_ID", "NUMBER(10,0)"),
                    ("AMOUNT", "NUMBER(10,2)"),
                ]
            rows = []
            for n, t in cols:
                r = MagicMock()
                r.__getitem__ = lambda self, k, n=n, t=t: {"name": n, "type": t}[k]
                r.get = lambda k, default=None, n=n, t=t: {"name": n, "type": t, "null?": "Y"}.get(k, default)
                rows.append(r)
            result.collect.return_value = rows
        elif "SELECT COUNT(*)" in query.upper():
            row = MagicMock()
            row.__getitem__ = lambda self, k: 0
            row.as_dict = lambda: {"C": 0}
            result.collect.return_value = [row]
        else:
            result.collect.return_value = []
        return result

    session.sql.side_effect = sql_call
    session.use_database.return_value = None
    session.use_schema.return_value = None
    return session


def main() -> int:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "test_features.yaml"
    cfg = load_config(config_path)
    print(f"Loaded config: {config_path}")
    print(f"Default DB: {cfg.defaults.database}, schema: {cfg.defaults.schema_name}")
    print(f"Tables: {len(cfg.tables)}\n")

    session = make_session()
    engine = SynthEngine(session, cfg)
    plan = engine.plan()

    print("=" * 80)
    print(f"GENERATION ORDER: {plan.generation_order}")
    print("=" * 80)
    print()

    engine._faker_manager.check_availability = lambda: True  # type: ignore[assignment]
    engine._faker_manager.get_or_register_udf = lambda p, l, s: f"sf_synth_faker_{p}"  # type: ignore[assignment]
    engine._faker_manager.get_or_register_correlated_udf = (
        lambda gid, providers, locale, seed: f"sf_synth_corr_{gid.split('_')[-1]}"
    )
    engine._faker_manager.get_or_register_regex_udf = lambda s: "sf_synth_regex_generate"

    for tbl in cfg.tables:
        fqn = tbl.get_fqn(cfg.defaults.database, cfg.defaults.schema_name)
        col_order, varchar_lens, num_bounds, type_map = engine._describe_table(fqn)
        print(f"--- Table: {fqn} ({tbl.write_mode.value}) ---")
        print(f"  DDL columns: {col_order}")
        print(f"  Varchar lens: {varchar_lens}")
        print(f"  Numeric bounds: {num_bounds}")
        try:
            sql = engine._build_select_sql(
                fqn, tbl, tbl.rows, cfg.defaults.seed,
                col_order, varchar_lens, num_bounds, type_map,
            )
            if sql:
                print("\n  Generated SQL:")
                pretty = sql.replace(", ", ",\n    ").replace("FROM ", "\n  FROM ")
                print("    " + pretty[:6000])
            else:
                print("  (no SQL generated)")
        except Exception as e:
            print(f"  ERROR: {e}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
