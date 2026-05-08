"""Unit tests for DAG builder."""

from __future__ import annotations

import pytest

from sf_synth.config import (
    DefaultsConfig,
    RelationshipConfig,
    SkewType,
    SynthConfig,
    TableConfig,
)
from sf_synth.dag import (
    DAGBuilder,
    ForeignKeyEdge,
    GenerationPlan,
    TableNode,
    build_dag_from_config,
)
from sf_synth.errors import CycleError


class TestDAGBuilder:
    """Tests for DAGBuilder."""

    def test_add_single_table(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 1000)

        plan = builder.build_plan()
        assert len(plan.tables) == 1
        assert plan.tables[0].fqn == "DB.SCHEMA.USERS"
        assert plan.tables[0].row_count == 1000

    def test_add_multiple_tables(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 1000)
        builder.add_table("DB.SCHEMA.ORDERS", 5000)

        plan = builder.build_plan()
        assert len(plan.tables) == 2

    def test_add_foreign_key(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 1000)
        builder.add_table("DB.SCHEMA.ORDERS", 5000)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()
        assert len(plan.edges) == 1
        assert plan.edges[0].child_table == "DB.SCHEMA.ORDERS"
        assert plan.edges[0].parent_table == "DB.SCHEMA.USERS"

    def test_topological_order(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.ORDERS", 5000)
        builder.add_table("DB.SCHEMA.USERS", 1000)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()
        users_idx = plan.generation_order.index("DB.SCHEMA.USERS")
        orders_idx = plan.generation_order.index("DB.SCHEMA.ORDERS")
        assert users_idx < orders_idx

    def test_self_referential_table(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.EMPLOYEES", 100)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.EMPLOYEES",
            child_columns=["MANAGER_ID"],
            parent_table="DB.SCHEMA.EMPLOYEES",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()
        assert "DB.SCHEMA.EMPLOYEES" in plan.self_referential_tables
        assert plan.tables[0].is_self_referential is True

    def test_multi_table_cycle_raises_error(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.A", 100)
        builder.add_table("DB.SCHEMA.B", 100)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.A",
            child_columns=["B_ID"],
            parent_table="DB.SCHEMA.B",
            parent_columns=["ID"],
        )
        builder.add_foreign_key(
            child_table="DB.SCHEMA.B",
            child_columns=["A_ID"],
            parent_table="DB.SCHEMA.A",
            parent_columns=["ID"],
        )

        with pytest.raises(CycleError):
            builder.build_plan()

    def test_complex_dependency_chain(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 100)
        builder.add_table("DB.SCHEMA.ACCOUNTS", 50)
        builder.add_table("DB.SCHEMA.ORDERS", 1000)
        builder.add_table("DB.SCHEMA.ORDER_ITEMS", 5000)

        builder.add_foreign_key(
            child_table="DB.SCHEMA.ACCOUNTS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["ACCOUNT_ID"],
            parent_table="DB.SCHEMA.ACCOUNTS",
            parent_columns=["ID"],
        )
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDER_ITEMS",
            child_columns=["ORDER_ID"],
            parent_table="DB.SCHEMA.ORDERS",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()

        order_map = {t: i for i, t in enumerate(plan.generation_order)}
        assert order_map["DB.SCHEMA.USERS"] < order_map["DB.SCHEMA.ACCOUNTS"]
        assert order_map["DB.SCHEMA.ACCOUNTS"] < order_map["DB.SCHEMA.ORDERS"]
        assert order_map["DB.SCHEMA.ORDERS"] < order_map["DB.SCHEMA.ORDER_ITEMS"]


class TestGenerationPlan:
    """Tests for GenerationPlan."""

    def test_get_dependencies_for(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 100)
        builder.add_table("DB.SCHEMA.ORDERS", 1000)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()
        deps = plan.get_dependencies_for("DB.SCHEMA.ORDERS")
        assert len(deps) == 1
        assert deps[0].parent_table == "DB.SCHEMA.USERS"

    def test_get_dependents_for(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 100)
        builder.add_table("DB.SCHEMA.ORDERS", 1000)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )

        plan = builder.build_plan()
        dependents = plan.get_dependents_for("DB.SCHEMA.USERS")
        assert len(dependents) == 1
        assert dependents[0].child_table == "DB.SCHEMA.ORDERS"


class TestBuildDAGFromConfig:
    """Tests for building DAG from config."""

    def test_simple_config(self) -> None:
        config = SynthConfig(
            defaults=DefaultsConfig(database="DB", schema="SCHEMA"),
            tables=[
                TableConfig(name="USERS", rows=100),
                TableConfig(name="ORDERS", rows=500),
            ],
        )

        plan = build_dag_from_config(config)
        assert len(plan.tables) == 2

    def test_config_with_relationships(self) -> None:
        config = SynthConfig(
            defaults=DefaultsConfig(database="DB", schema="SCHEMA"),
            tables=[
                TableConfig(name="USERS", rows=100),
                TableConfig(
                    name="ORDERS",
                    rows=500,
                    relationships=[
                        RelationshipConfig(
                            column="USER_ID",
                            references="USERS.ID",
                        ),
                    ],
                ),
            ],
        )

        plan = build_dag_from_config(config)
        assert len(plan.edges) == 1

        users_idx = plan.generation_order.index("DB.SCHEMA.USERS")
        orders_idx = plan.generation_order.index("DB.SCHEMA.ORDERS")
        assert users_idx < orders_idx

    def test_config_with_fqn_reference(self) -> None:
        config = SynthConfig(
            defaults=DefaultsConfig(database="DB", schema="SCHEMA"),
            tables=[
                TableConfig(name="DB.SCHEMA.USERS", rows=100),
                TableConfig(
                    name="DB.SCHEMA.ORDERS",
                    rows=500,
                    relationships=[
                        RelationshipConfig(
                            column="USER_ID",
                            references="DB.SCHEMA.USERS.ID",
                        ),
                    ],
                ),
            ],
        )

        plan = build_dag_from_config(config)
        assert len(plan.edges) == 1

    def test_config_with_skew(self) -> None:
        config = SynthConfig(
            defaults=DefaultsConfig(database="DB", schema="SCHEMA"),
            tables=[
                TableConfig(name="USERS", rows=100),
                TableConfig(
                    name="ORDERS",
                    rows=500,
                    relationships=[
                        RelationshipConfig(
                            column="USER_ID",
                            references="USERS.ID",
                            skew=SkewType.ZIPF,
                            skew_param=1.5,
                        ),
                    ],
                ),
            ],
        )

        plan = build_dag_from_config(config)
        assert plan.edges[0].skew == "zipf"
        assert plan.edges[0].skew_param == 1.5


class TestVisualize:
    """Tests for DAG visualization."""

    def test_mermaid_output(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.USERS", 100)
        builder.add_table("DB.SCHEMA.ORDERS", 1000)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.ORDERS",
            child_columns=["USER_ID"],
            parent_table="DB.SCHEMA.USERS",
            parent_columns=["ID"],
        )

        builder.build_plan()
        mermaid = builder.visualize()

        assert "graph TD" in mermaid
        assert "USERS" in mermaid
        assert "ORDERS" in mermaid
        assert "-->" in mermaid

    def test_self_ref_visualization(self) -> None:
        builder = DAGBuilder()
        builder.add_table("DB.SCHEMA.EMPLOYEES", 100)
        builder.add_foreign_key(
            child_table="DB.SCHEMA.EMPLOYEES",
            child_columns=["MANAGER_ID"],
            parent_table="DB.SCHEMA.EMPLOYEES",
            parent_columns=["ID"],
        )

        builder.build_plan()
        mermaid = builder.visualize()

        assert "self-ref" in mermaid
        assert "-.->|" in mermaid
