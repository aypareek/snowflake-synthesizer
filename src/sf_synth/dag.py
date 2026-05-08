"""DAG builder for table dependencies and topological sorting.

Handles table generation order based on foreign key relationships,
including detection and handling of self-referential tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import networkx as nx

from sf_synth.errors import CycleError, DAGError

if TYPE_CHECKING:
    from sf_synth.config import SynthConfig
    from sf_synth.discovery import SchemaModel


@dataclass
class TableNode:
    """Node in the dependency DAG representing a table."""

    fqn: str
    row_count: int
    is_self_referential: bool = False
    self_ref_columns: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    dependents: list[str] = field(default_factory=list)
    generation_order: int = -1


@dataclass
class ForeignKeyEdge:
    """Edge in the dependency DAG representing a foreign key."""

    child_table: str
    child_columns: list[str]
    parent_table: str
    parent_columns: list[str]
    null_ratio: float = 0.0
    skew: str = "uniform"
    skew_param: float = 1.5


@dataclass
class GenerationPlan:
    """Plan for generating tables in dependency order."""

    tables: list[TableNode]
    edges: list[ForeignKeyEdge]
    generation_order: list[str]
    self_referential_tables: list[str]
    estimated_total_rows: int

    def get_dependencies_for(self, table_fqn: str) -> list[ForeignKeyEdge]:
        """Get all FK edges where this table is the child."""
        return [e for e in self.edges if e.child_table == table_fqn]

    def get_dependents_for(self, table_fqn: str) -> list[ForeignKeyEdge]:
        """Get all FK edges where this table is the parent."""
        return [e for e in self.edges if e.parent_table == table_fqn]


class DAGBuilder:
    """Builds and manages the table dependency DAG."""

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()
        self._nodes: dict[str, TableNode] = {}
        self._edges: list[ForeignKeyEdge] = []
        self._self_refs: dict[str, list[str]] = {}

    def add_table(self, fqn: str, row_count: int) -> None:
        """Add a table node to the DAG.

        Args:
            fqn: Fully qualified table name.
            row_count: Number of rows to generate.
        """
        if fqn not in self._nodes:
            self._nodes[fqn] = TableNode(fqn=fqn, row_count=row_count)
            self._graph.add_node(fqn)

    def add_foreign_key(
        self,
        child_table: str,
        child_columns: list[str],
        parent_table: str,
        parent_columns: list[str],
        null_ratio: float = 0.0,
        skew: str = "uniform",
        skew_param: float = 1.5,
    ) -> None:
        """Add a foreign key relationship.

        Args:
            child_table: Child table FQN.
            child_columns: Columns in child table.
            parent_table: Parent table FQN.
            parent_columns: Columns in parent table.
            null_ratio: Ratio of null FK values.
            skew: Distribution type ('uniform' or 'zipf').
            skew_param: Parameter for skewed distribution.
        """
        if child_table == parent_table:
            if child_table not in self._self_refs:
                self._self_refs[child_table] = []
            self._self_refs[child_table].extend(child_columns)
            if child_table in self._nodes:
                self._nodes[child_table].is_self_referential = True
                self._nodes[child_table].self_ref_columns = self._self_refs[child_table]
        else:
            self._graph.add_edge(child_table, parent_table)

        edge = ForeignKeyEdge(
            child_table=child_table,
            child_columns=child_columns,
            parent_table=parent_table,
            parent_columns=parent_columns,
            null_ratio=null_ratio,
            skew=skew,
            skew_param=skew_param,
        )
        self._edges.append(edge)

        if child_table in self._nodes:
            self._nodes[child_table].dependencies.append(parent_table)
        if parent_table in self._nodes:
            self._nodes[parent_table].dependents.append(child_table)

    def build_plan(self) -> GenerationPlan:
        """Build the generation plan with topologically sorted tables.

        Returns:
            GenerationPlan with ordered tables and relationships.

        Raises:
            CycleError: If non-self-referential cycles are detected.
        """
        self._check_cycles()

        try:
            generation_order = list(nx.topological_sort(self._graph))
            generation_order.reverse()
        except nx.NetworkXUnfeasible as e:
            raise CycleError(
                "Cannot determine generation order due to cyclic dependencies. "
                "Only self-referential tables are supported for cycles."
            ) from e

        for i, fqn in enumerate(generation_order):
            if fqn in self._nodes:
                self._nodes[fqn].generation_order = i

        isolated_tables = [
            fqn for fqn in self._nodes if fqn not in generation_order
        ]
        generation_order = isolated_tables + generation_order

        for i, fqn in enumerate(generation_order):
            if fqn in self._nodes:
                self._nodes[fqn].generation_order = i

        return GenerationPlan(
            tables=list(self._nodes.values()),
            edges=self._edges,
            generation_order=generation_order,
            self_referential_tables=list(self._self_refs.keys()),
            estimated_total_rows=sum(n.row_count for n in self._nodes.values()),
        )

    def _check_cycles(self) -> None:
        """Check for non-self-referential cycles.

        Raises:
            CycleError: If problematic cycles are detected.
        """
        try:
            cycles = list(nx.simple_cycles(self._graph))
        except Exception:
            return

        non_self_cycles = [c for c in cycles if len(c) > 1]

        if non_self_cycles:
            cycle_str = " -> ".join(non_self_cycles[0] + [non_self_cycles[0][0]])
            raise CycleError(
                f"Detected non-self-referential cycle: {cycle_str}. "
                "Multi-table cycles are not supported. Consider removing one FK."
            )

    def visualize(self) -> str:
        """Generate a Mermaid diagram of the DAG.

        Returns:
            Mermaid diagram string.
        """
        lines = ["graph TD"]

        for fqn, node in self._nodes.items():
            safe_name = fqn.replace(".", "_").replace('"', "")
            label = f"{node.fqn}\\n({node.row_count:,} rows)"
            if node.is_self_referential:
                label += "\\n[self-ref]"
            lines.append(f"    {safe_name}[{label}]")

        for edge in self._edges:
            if edge.child_table != edge.parent_table:
                child_safe = edge.child_table.replace(".", "_").replace('"', "")
                parent_safe = edge.parent_table.replace(".", "_").replace('"', "")
                cols = ",".join(edge.child_columns)
                lines.append(f"    {child_safe} -->|{cols}| {parent_safe}")

        for fqn, cols in self._self_refs.items():
            safe_name = fqn.replace(".", "_").replace('"', "")
            cols_str = ",".join(cols)
            lines.append(f"    {safe_name} -.->|{cols_str}| {safe_name}")

        return "\n".join(lines)


def build_dag_from_schema(
    schema_model: SchemaModel,
    config: SynthConfig,
) -> GenerationPlan:
    """Build DAG from discovered schema and user config.

    Args:
        schema_model: Discovered schema model.
        config: User configuration.

    Returns:
        GenerationPlan for table generation.
    """
    builder = DAGBuilder()

    for table_config in config.tables:
        fqn = table_config.get_fqn(
            default_database=config.defaults.database,
            default_schema=config.defaults.schema_name,
        )
        builder.add_table(fqn, table_config.rows)

    for table_config in config.tables:
        table_fqn = table_config.get_fqn(
            default_database=config.defaults.database,
            default_schema=config.defaults.schema_name,
        )

        for rel in table_config.relationships:
            ref_parts = rel.references.split(".")
            if len(ref_parts) == 2:
                parent_table = ref_parts[0]
                parent_col = ref_parts[1]
                parent_fqn = f"{config.defaults.database}.{config.defaults.schema_name}.{parent_table}"
            elif len(ref_parts) == 4:
                parent_fqn = ".".join(ref_parts[:3])
                parent_col = ref_parts[3]
            else:
                raise DAGError(f"Invalid reference format: {rel.references}")

            builder.add_foreign_key(
                child_table=table_fqn,
                child_columns=[rel.column],
                parent_table=parent_fqn,
                parent_columns=[parent_col],
                null_ratio=rel.null_ratio,
                skew=rel.skew.value,
                skew_param=rel.skew_param,
            )

    for fqn in builder._nodes:
        table_info = schema_model.get_table(fqn)
        if table_info:
            for fk in table_info.foreign_keys:
                builder.add_foreign_key(
                    child_table=fqn,
                    child_columns=fk.columns,
                    parent_table=fk.referenced_fqn,
                    parent_columns=fk.referenced_columns,
                )

    return builder.build_plan()


def build_dag_from_config(config: SynthConfig) -> GenerationPlan:
    """Build DAG from config only (no schema discovery).

    Args:
        config: User configuration.

    Returns:
        GenerationPlan for table generation.
    """
    builder = DAGBuilder()

    for table_config in config.tables:
        fqn = table_config.get_fqn(
            default_database=config.defaults.database,
            default_schema=config.defaults.schema_name,
        )
        builder.add_table(fqn, table_config.rows)

    for table_config in config.tables:
        table_fqn = table_config.get_fqn(
            default_database=config.defaults.database,
            default_schema=config.defaults.schema_name,
        )

        for rel in table_config.relationships:
            ref_parts = rel.references.split(".")
            if len(ref_parts) == 2:
                parent_table = ref_parts[0]
                parent_col = ref_parts[1]
                parent_fqn = f"{config.defaults.database}.{config.defaults.schema_name}.{parent_table}"
            elif len(ref_parts) == 4:
                parent_fqn = ".".join(ref_parts[:3])
                parent_col = ref_parts[3]
            elif len(ref_parts) == 3:
                parent_fqn = ".".join(ref_parts[:2])
                parent_col = ref_parts[2]
                if config.defaults.database:
                    parent_fqn = f"{config.defaults.database}.{parent_fqn}"
            else:
                raise DAGError(f"Invalid reference format: {rel.references}")

            builder.add_foreign_key(
                child_table=table_fqn,
                child_columns=[rel.column],
                parent_table=parent_fqn,
                parent_columns=[parent_col],
                null_ratio=rel.null_ratio,
                skew=rel.skew.value,
                skew_param=rel.skew_param,
            )

    return builder.build_plan()
