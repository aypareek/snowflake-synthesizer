#!/usr/bin/env python
"""End-to-end offline test of sf-synth v0.4.0 features.

Mocks the Snowpark Session so we can validate the full generation pipeline
(SQL build + FK joins + temporal + correlation + write modes) without an
actual Snowflake connection.

Run from the repo root:
    PYTHONPATH=src python scripts/offline_e2e_test.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sf_synth.config import (  # noqa: E402
    ColumnConfig,
    DefaultsConfig,
    GeneratorType,
    SkewType,
    SynthConfig,
    TableConfig,
    WriteMode,
)
from sf_synth.engine import SynthEngine  # noqa: E402


PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


def expect(label: str, condition: bool, detail: str = "") -> bool:
    icon = PASS if condition else FAIL
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{icon}] {label}{suffix}")
    return condition


def make_session() -> MagicMock:
    """Create a mock Snowpark session that records all SQL queries."""
    session = MagicMock(name="MockSession")
    session.get_current_database.return_value = "ABC"
    session.get_current_schema.return_value = "AYUSH"

    DDL_REGISTRY: dict[str, list[tuple[str, str]]] = {}
    KEY_COUNT_REGISTRY: dict[str, int] = {}

    def sql_call(query: str):
        result = MagicMock()
        upper = query.upper().strip()

        if upper.startswith("DESCRIBE TABLE"):
            tbl_name = query.split()[-1]
            cols = DDL_REGISTRY.get(tbl_name, [])
            if not cols:
                short = tbl_name.split(".")[-1].upper()
                if "USERS" in short:
                    cols = [
                        ("USER_ID", "NUMBER(10,0)"),
                        ("FIRST_NAME", "VARCHAR(50)"),
                        ("LAST_NAME", "VARCHAR(50)"),
                        ("CITY", "VARCHAR(100)"),
                        ("STATE", "VARCHAR(100)"),
                        ("CREATED_AT", "TIMESTAMP_NTZ(9)"),
                        ("UPDATED_AT", "TIMESTAMP_NTZ(9)"),
                    ]
                elif "ORDERS" in short:
                    cols = [
                        ("ORDER_ID", "NUMBER(10,0)"),
                        ("USER_ID", "NUMBER(10,0)"),
                        ("AMOUNT", "NUMBER(10,2)"),
                        ("ORDER_TS", "TIMESTAMP_NTZ(9)"),
                        ("SHIPPED_TS", "TIMESTAMP_NTZ(9)"),
                    ]
                elif "EVENTS" in short:
                    cols = [
                        ("EVENT_ID", "NUMBER(10,0)"),
                        ("METADATA", "OBJECT"),
                        ("TAGS", "ARRAY"),
                    ]
                else:
                    cols = []

            rows = []
            for n, t in cols:
                r = MagicMock()
                r.__getitem__ = lambda self, k, n=n, t=t: {"name": n, "type": t}[k]
                r.get = lambda k, default=None, n=n, t=t: {
                    "name": n, "type": t, "null?": "Y"
                }.get(k, default)
                rows.append(r)
            result.collect.return_value = rows
            return result

        if upper.startswith("SELECT COUNT(*)"):
            row = MagicMock()
            row.__getitem__ = lambda self, k: 0
            row.as_dict = lambda: {"C": 0}
            result.collect.return_value = [row]
            return result

        if "CREATE OR REPLACE TRANSIENT TABLE" in upper:
            m = re.search(r"FROM\s+([\w\.]+)", query, re.I)
            parent = m.group(1) if m else "?"
            KEY_COUNT_REGISTRY[parent] = 100
            result.collect.return_value = []
            return result

        result.collect.return_value = []
        return result

    session.sql.side_effect = sql_call
    session.use_database.return_value = None
    session.use_schema.return_value = None
    session.add_packages.return_value = None
    return session


def patch_faker(engine: SynthEngine) -> None:
    """Replace Faker UDF registration with deterministic stubs."""
    engine._faker_manager.check_availability = lambda: True  # type: ignore[assignment]
    engine._faker_manager.get_or_register_udf = (  # type: ignore[assignment]
        lambda p, l, s: f"sf_synth_faker_{p}"
    )
    engine._faker_manager.get_or_register_correlated_udf = (  # type: ignore[assignment]
        lambda gid, providers, locale, seed: f"sf_synth_corr_{gid.split('_')[-1]}"
    )
    engine._faker_manager.get_or_register_regex_udf = (  # type: ignore[assignment]
        lambda s: "sf_synth_regex_generate"
    )


def patch_ri(engine: SynthEngine) -> None:
    """Stub RI manager so FK key counts are non-zero in offline tests."""
    cache = MagicMock()
    cache.key_table_fqn = "TEMP_KEYS"
    cache.key_count = 100
    engine._ri_manager.get_parent_key_count = lambda fqn: 100  # type: ignore[assignment]
    engine._ri_manager.materialize_parent_keys = lambda fqn, cols: cache  # type: ignore[assignment]
    engine._ri_manager._key_caches = {}

    class FakeCacheDict(dict):
        def get(self, key, default=None):
            return cache

    engine._ri_manager._key_caches = FakeCacheDict()


def section(name: str) -> None:
    print(f"\n=== {name} ===")


def test_correlation_group() -> bool:
    section("Correlation group")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "CITY": ColumnConfig(
                        generator=GeneratorType.FAKER,
                        provider="city",
                        correlation_group="addr",
                    ),
                    "STATE": ColumnConfig(
                        generator=GeneratorType.FAKER,
                        provider="state",
                        correlation_group="addr",
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.USERS")
    sql = e._build_select_sql("ABC.AYUSH.USERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    ok = True
    ok &= expect("UDF reused for both correlated columns", sql.count("sf_synth_corr_addr") == 2)
    ok &= expect('CITY uses :"CITY" extraction', ':"CITY"::STRING' in sql)
    ok &= expect('STATE uses :"STATE" extraction', ':"STATE"::STRING' in sql)
    return ok


def test_temporal_after() -> bool:
    section("Temporal ordering (after)")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "CREATED_AT": ColumnConfig(
                        generator=GeneratorType.FAKER, provider="date_time"
                    ),
                    "UPDATED_AT": ColumnConfig(
                        generator=GeneratorType.FAKER,
                        provider="date_time",
                        after="CREATED_AT",
                        after_offset_unit="day",
                        after_offset_min=1,
                        after_offset_max=30,
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.USERS")
    sql = e._build_select_sql("ABC.AYUSH.USERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    ok = True
    ok &= expect("DATEADD wraps UPDATED_AT", "DATEADD('day'" in sql)
    ok &= expect("UPDATED_AT references CREATED_AT", '"CREATED_AT")' in sql or '_t."CREATED_AT"' in sql)
    ok &= expect("inner SELECT marks UPDATED_AT as NULL", 'NULL AS "UPDATED_AT"' in sql)
    return ok


def test_expression_generator() -> bool:
    section("Expression generator")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "FULL_NAME": ColumnConfig(
                        generator=GeneratorType.EXPRESSION,
                        sql="FIRST_NAME || ' ' || LAST_NAME",
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.USERS")
    sql = e._build_select_sql("ABC.AYUSH.USERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    return expect("Raw SQL injected", "FIRST_NAME || ' ' || LAST_NAME" in sql)


def test_array_object_json() -> bool:
    section("Array / Object / JSON template")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="EVENTS",
                rows=10,
                columns={
                    "EVENT_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "TAGS": ColumnConfig(
                        generator=GeneratorType.ARRAY,
                        element_generator=GeneratorType.FAKER,
                        element_provider="word",
                        length=[1, 5],
                    ),
                    "META": ColumnConfig(
                        generator=GeneratorType.OBJECT,
                        fields={
                            "v": ColumnConfig(
                                generator=GeneratorType.UNIFORM,
                                min_value=1,
                                max_value=10,
                            )
                        },
                    ),
                    "PAYLOAD": ColumnConfig(
                        generator=GeneratorType.JSON_TEMPLATE,
                        template='{"id":{{seq}},"v":{{uniform(1,10)}}}',
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.EVENTS")
    sql = e._build_select_sql("ABC.AYUSH.EVENTS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    ok = True
    ok &= expect("ARRAY_CONSTRUCT present", "ARRAY_CONSTRUCT(" in sql)
    ok &= expect("ARRAY_SLICE for variable length", "ARRAY_SLICE" in sql)
    ok &= expect("OBJECT_CONSTRUCT present", "OBJECT_CONSTRUCT(" in sql)
    ok &= expect("TRY_PARSE_JSON wraps template", "TRY_PARSE_JSON(" in sql)
    return ok


def test_regex_generator() -> bool:
    section("Regex generator (exrex UDF)")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "ORDER_NO": ColumnConfig(
                        generator=GeneratorType.REGEX,
                        pattern="ORD-[0-9]{6}",
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.USERS")
    sql = e._build_select_sql("ABC.AYUSH.USERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    return expect(
        "regex UDF with pattern",
        "sf_synth_regex_generate('ORD-[0-9]{6}'" in sql,
    )


def test_conditional() -> bool:
    section("Conditional (condition + else_value)")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "STATUS": ColumnConfig(
                        generator=GeneratorType.CHOICE,
                        values=["active", "suspended"],
                    ),
                    "SUSPENDED_AT": ColumnConfig(
                        generator=GeneratorType.FAKER,
                        provider="date_time",
                        condition="STATUS = 'suspended'",
                        else_value=None,
                    ),
                },
            )
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.USERS")
    sql = e._build_select_sql("ABC.AYUSH.USERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    return expect("IFF wraps with STATUS check", "IFF(STATUS = 'suspended'" in sql)


def test_fk_with_temporal() -> bool:
    section("FK + temporal (regression: FK column survives temporal wrap)")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "USER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                },
            ),
            TableConfig(
                name="ORDERS",
                rows=10,
                columns={
                    "ORDER_ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "ORDER_TS": ColumnConfig(
                        generator=GeneratorType.FAKER, provider="date_time"
                    ),
                    "SHIPPED_TS": ColumnConfig(
                        generator=GeneratorType.FAKER,
                        provider="date_time",
                        after="ORDER_TS",
                    ),
                },
                relationships=[
                    {"column": "USER_ID", "references": "USERS.USER_ID"}
                ],
            ),
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.ORDERS")
    sql = e._build_select_sql("ABC.AYUSH.ORDERS", cfg.tables[1], 10, 42, co, vl, nb, tm)

    ok = True
    ok &= expect("INNER JOIN to parent keys", "INNER JOIN" in sql)
    ok &= expect("DATEADD for SHIPPED_TS", "DATEADD" in sql)
    ok &= expect("USER_ID survives temporal wrap", '"USER_ID"' in sql)
    ok &= expect("HASH-based deterministic FK index", "HASH(_rownum" in sql)
    return ok


def test_write_modes() -> bool:
    section("Write modes (replace/append/upsert/fill_to)")
    ok = True
    for mode in [WriteMode.REPLACE, WriteMode.APPEND, WriteMode.UPSERT, WriteMode.FILL_TO]:
        cfg = SynthConfig(
            defaults=DefaultsConfig(database="ABC", schema="AYUSH"),
            tables=[
                TableConfig(
                    name="X",
                    rows=10,
                    write_mode=mode,
                    upsert_keys=["ID"] if mode == WriteMode.UPSERT else [],
                    columns={
                        "ID": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    },
                ),
            ],
        )
        ok &= expect(f"{mode.value} mode accepted", cfg.tables[0].write_mode == mode)
    return ok


def test_numeric_clamp_and_truncate() -> bool:
    section("Numeric clamp + VARCHAR truncate")
    cfg = SynthConfig(
        defaults=DefaultsConfig(seed=42, database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="ORDERS",
                rows=10,
                columns={
                    "AMOUNT": ColumnConfig(
                        generator=GeneratorType.UNIFORM,
                        min_value=0,
                        max_value=1_000_000_000,  # exceeds NUMBER(10,2)
                    ),
                },
            ),
        ],
    )
    s = make_session()
    e = SynthEngine(s, cfg)
    patch_faker(e)
    patch_ri(e)
    e.plan()
    co, vl, nb, tm = e._describe_table("ABC.AYUSH.ORDERS")
    sql = e._build_select_sql("ABC.AYUSH.ORDERS", cfg.tables[0], 10, 42, co, vl, nb, tm)

    return expect(
        "LEAST/GREATEST wraps numeric column",
        "LEAST(99999999.99," in sql and "GREATEST(-99999999.99," in sql,
    )


def test_validation_module() -> bool:
    section("Validation module")
    from sf_synth.validation import validate_config_against_ddl

    cfg = SynthConfig(
        defaults=DefaultsConfig(database="ABC", schema="AYUSH"),
        tables=[
            TableConfig(
                name="USERS",
                rows=10,
                columns={
                    "FIRST_NAME": ColumnConfig(generator=GeneratorType.SEQ, start=1),
                    "BAD_COL": ColumnConfig(
                        generator=GeneratorType.FAKER, provider="email"
                    ),
                    "AMOUNT": ColumnConfig(
                        generator=GeneratorType.UNIFORM,
                        min_value=0,
                        max_value=10**12,
                    ),
                },
            ),
        ],
    )
    s = make_session()
    report = validate_config_against_ddl(s, cfg)
    ok = True
    ok &= expect("Missing column flagged", any("BAD_COL" in i.message for i in report.errors))
    return ok


def main() -> int:
    print("\n+++ sf-synth v0.4.0 OFFLINE END-TO-END TEST +++\n")
    results = [
        test_correlation_group(),
        test_temporal_after(),
        test_expression_generator(),
        test_array_object_json(),
        test_regex_generator(),
        test_conditional(),
        test_fk_with_temporal(),
        test_write_modes(),
        test_numeric_clamp_and_truncate(),
        test_validation_module(),
    ]
    print()
    failed = sum(1 for r in results if not r)
    if failed:
        print(f"\n  RESULT: \033[31m{failed} of {len(results)} suites failed\033[0m\n")
        return 1
    print(f"\n  RESULT: \033[32mALL {len(results)} SUITES PASSED\033[0m\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
