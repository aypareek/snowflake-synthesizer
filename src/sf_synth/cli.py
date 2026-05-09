"""Command-line interface for sf-synth."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.tree import Tree

app = typer.Typer(
    name="sf-synth",
    help="Generate high-fidelity synthetic data for Snowflake.",
    add_completion=False,
)

console = Console()


def _get_backend(
    connection: str | None = None,
    account: str | None = None,
    user: str | None = None,
    database: str | None = None,
    schema: str | None = None,
    warehouse: str | None = None,
    role: str | None = None,
) -> "SnowparkBackend":
    """Create and connect a Snowpark backend."""
    from sf_synth.backend import SnowparkBackend

    backend = SnowparkBackend(
        connection_name=connection,
        account=account,
        user=user,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )
    backend.connect()
    return backend


@app.command()
def discover(
    database: str = typer.Argument(..., help="Database to discover"),
    output: Path = typer.Option(
        Path("schema.yaml"),
        "--output",
        "-o",
        help="Output YAML file path",
    ),
    schemas: Optional[str] = typer.Option(
        None,
        "--schemas",
        "-s",
        help="Comma-separated list of schemas to include",
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help="Comma-separated list of tables to include",
    ),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Named connection from ~/.snowflake/connections.toml",
    ),
    include_row_counts: bool = typer.Option(
        False,
        "--row-counts",
        help="Include actual row counts (slower)",
    ),
) -> None:
    """Discover schema from Snowflake and generate a starter config."""
    import yaml

    from sf_synth.discovery import schema_to_yaml

    schema_list = schemas.split(",") if schemas else None
    table_list = tables.split(",") if tables else None

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task("Connecting to Snowflake...", total=None)
        backend = _get_backend(connection=connection)

        progress.add_task(f"Discovering schema in {database}...", total=None)
        schema_model = backend.discover_schema(
            database,
            schemas=schema_list,
            tables=table_list,
        )

    yaml_data = schema_to_yaml(schema_model)

    with open(output, "w") as f:
        yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[green]Discovered {len(schema_model.tables)} tables[/green]")

    if not schema_model.tables:
        console.print(
            f"\n[yellow]No tables found in '{database}'.[/yellow] Possible reasons:\n"
            "  1. The database is empty — create some tables first\n"
            "  2. The role in your connection lacks USAGE privilege\n"
            f"     Run in Snowflake: [bold]GRANT USAGE ON DATABASE {database} TO ROLE <your_role>;[/bold]\n"
            "  3. Use [bold]--schemas MY_SCHEMA[/bold] to target a specific schema"
        )
        backend.disconnect()
        return

    has_constraints = any(
        tbl.primary_key or tbl.foreign_keys
        for tbl in schema_model.tables.values()
    )
    if not has_constraints:
        console.print(
            "[yellow]Note:[/yellow] No PK/FK constraints found. "
            "Shared or read-only databases (e.g. SNOWFLAKE_SAMPLE_DATA) "
            "don't expose KEY_COLUMN_USAGE — you can define relationships "
            "manually in the generated YAML."
        )

    console.print(f"[blue]Config written to: {output}[/blue]")

    table = Table(title="Discovered Tables")
    table.add_column("Table", style="cyan")
    table.add_column("Columns", justify="right")
    table.add_column("PKs", justify="right")
    table.add_column("FKs", justify="right")

    for fqn, tbl in schema_model.tables.items():
        pk_count = len(tbl.pk_columns)
        fk_count = len(tbl.foreign_keys)
        table.add_row(fqn, str(len(tbl.columns)), str(pk_count), str(fk_count))

    console.print(table)
    backend.disconnect()


@app.command()
def plan(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Named connection from ~/.snowflake/connections.toml",
    ),
    show_dag: bool = typer.Option(
        True,
        "--dag/--no-dag",
        help="Show dependency DAG visualization",
    ),
) -> None:
    """Show generation plan without executing."""
    from sf_synth.config import load_config
    from sf_synth.dag import build_dag_from_config

    cfg = load_config(config)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task("Connecting to Snowflake...", total=None)
        backend = _get_backend(connection=connection)
        progress.add_task("Building generation plan...", total=None)
        plan_summary = backend.get_plan_summary(cfg)

    console.print("\n")
    console.print(Panel("[bold]Generation Plan[/bold]", expand=False))

    summary_table = Table(show_header=False, box=None)
    summary_table.add_column("Key", style="cyan")
    summary_table.add_column("Value")
    summary_table.add_row("Total Tables", str(plan_summary["total_tables"]))
    summary_table.add_row("Total Rows", f"{plan_summary['total_rows']:,}")

    estimates = plan_summary.get("size_estimates", {})
    if estimates.get("estimated_bytes"):
        size_mb = estimates["estimated_bytes"] / (1024 * 1024)
        summary_table.add_row("Estimated Size", f"{size_mb:.1f} MB")

    if plan_summary["self_referential_tables"]:
        summary_table.add_row(
            "Self-Referential",
            ", ".join(plan_summary["self_referential_tables"]),
        )

    console.print(summary_table)

    console.print("\n[bold]Generation Order:[/bold]")
    order_table = Table()
    order_table.add_column("#", justify="right", style="dim")
    order_table.add_column("Table", style="cyan")
    order_table.add_column("Rows", justify="right")
    order_table.add_column("Dependencies")

    table_estimates = estimates.get("tables", {})
    deps_map = {}
    for dep in plan_summary.get("dependencies", []):
        child = dep["child"]
        if child not in deps_map:
            deps_map[child] = []
        deps_map[child].append(dep["parent"].split(".")[-1])

    for idx, table_fqn in enumerate(plan_summary["generation_order"], 1):
        table_name = table_fqn.split(".")[-1]
        est = table_estimates.get(table_fqn, {})
        rows = est.get("rows", "?")
        deps = ", ".join(deps_map.get(table_fqn, ["-"]))
        order_table.add_row(str(idx), table_name, f"{rows:,}" if isinstance(rows, int) else rows, deps)

    console.print(order_table)

    if show_dag and plan_summary.get("dag_visualization"):
        console.print("\n[bold]Dependency DAG (Mermaid):[/bold]")
        console.print(Panel(plan_summary["dag_visualization"], title="DAG", expand=False))

    backend.disconnect()


@app.command()
def generate(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Named connection from ~/.snowflake/connections.toml",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate without writing data",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation for large runs",
    ),
    seed: Optional[int] = typer.Option(
        None,
        "--seed",
        help="Override random seed for reproducibility",
    ),
) -> None:
    """Generate synthetic data."""
    from sf_synth.config import load_config

    cfg = load_config(config)

    if seed is not None:
        cfg.defaults.seed = seed

    total_rows = cfg.get_total_rows()

    if total_rows > 100_000_000 and not yes and not dry_run:
        console.print(
            f"[yellow]Warning: Generating {total_rows:,} rows may consume significant credits.[/yellow]"
        )
        confirm = typer.confirm("Continue?")
        if not confirm:
            raise typer.Abort()

    backend = _get_backend(connection=connection)

    def progress_callback(table: str, current: int, total: int) -> None:
        table_name = table.split(".")[-1]
        console.print(f"  [{current}/{total}] Generating [cyan]{table_name}[/cyan]...")

    try:
        from sf_synth.engine import SynthEngine

        engine = SynthEngine(backend.session, cfg)
        engine.set_progress_callback(progress_callback)

        console.print("\n[bold]Starting synthesis...[/bold]\n")

        if dry_run:
            console.print("[yellow]DRY RUN - no data will be written[/yellow]\n")

        result = engine.generate(dry_run=dry_run)

        console.print("\n")

        if result.success:
            console.print(Panel("[bold green]Synthesis Complete[/bold green]", expand=False))
        else:
            console.print(Panel("[bold red]Synthesis Completed with Errors[/bold red]", expand=False))

        results_table = Table()
        results_table.add_column("Table", style="cyan")
        results_table.add_column("Rows", justify="right")
        results_table.add_column("Time (s)", justify="right")
        results_table.add_column("Status")

        for tr in result.tables:
            table_name = tr.table_fqn.split(".")[-1]
            status = "[green]OK[/green]" if tr.success else f"[red]FAIL: {tr.error}[/red]"
            results_table.add_row(
                table_name,
                f"{tr.rows_generated:,}",
                f"{tr.elapsed_seconds:.2f}",
                status,
            )

        console.print(results_table)

        console.print(f"\n[bold]Total:[/bold] {result.total_rows:,} rows in {result.total_elapsed_seconds:.2f}s")

        if result.errors:
            console.print("\n[red]Errors:[/red]")
            for err in result.errors:
                console.print(f"  - {err}")

    finally:
        backend.disconnect()


@app.command()
def clean(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Named connection from ~/.snowflake/connections.toml",
    ),
    drop_tables: bool = typer.Option(
        False,
        "--drop-tables",
        help="Drop generated target tables (not just temp tables)",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation",
    ),
) -> None:
    """Clean up temporary tables and optionally generated data."""
    from sf_synth.config import load_config

    cfg = load_config(config)

    if drop_tables and not yes:
        confirm = typer.confirm(
            "This will DROP the target tables. Are you sure?",
            default=False,
        )
        if not confirm:
            raise typer.Abort()

    backend = _get_backend(connection=connection)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Cleaning up temporary tables...", total=None)
            backend.clean(cfg)

            if drop_tables:
                progress.add_task("Dropping target tables...", total=None)
                for table_cfg in cfg.tables:
                    fqn = table_cfg.get_fqn(
                        cfg.defaults.database,
                        cfg.defaults.schema_name,
                    )
                    try:
                        backend.session.sql(f"DROP TABLE IF EXISTS {fqn}").collect()
                        console.print(f"  Dropped [cyan]{fqn}[/cyan]")
                    except Exception as e:
                        console.print(f"  [red]Failed to drop {fqn}: {e}[/red]")

        console.print("\n[green]Cleanup complete[/green]")

    finally:
        backend.disconnect()


@app.command()
def version() -> None:
    """Show version information."""
    from sf_synth import __version__

    console.print(f"sf-synth version [bold]{__version__}[/bold]")


@app.callback()
def main() -> None:
    """
    sf-synth: High-fidelity synthetic data generation for Snowflake.

    Generate realistic synthetic data that preserves referential integrity,
    statistical distributions, and semantic patterns.
    """


if __name__ == "__main__":
    app()
