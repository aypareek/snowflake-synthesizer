"""Command-line interface for sf-synth."""

from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(
    name="sf-synth",
    help="Generate high-fidelity synthetic data for Snowflake.",
    add_completion=False,
)

console = Console()


def _configure_verbosity(verbose: bool, quiet: bool) -> None:
    """Configure log levels and warning filters globally for the CLI."""
    if quiet:
        warnings.filterwarnings("ignore")
        logging.getLogger().setLevel(logging.ERROR)
        logging.getLogger("snowflake").setLevel(logging.ERROR)
    elif verbose:
        warnings.simplefilter("default")
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        warnings.filterwarnings(
            "ignore",
            message=r".*The version of package 'faker' in the local environment.*",
        )
        warnings.filterwarnings(
            "ignore",
            message=r".*Bad owner or permissions on .*connections\.toml.*",
        )
        logging.getLogger("snowflake").setLevel(logging.WARNING)


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
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Discover schema from Snowflake and generate a starter config."""
    _configure_verbosity(verbose, quiet)
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
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Show generation plan without executing."""
    _configure_verbosity(verbose, quiet)
    from sf_synth.config import load_config

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
    deps_map: dict[str, list[str]] = {}
    for dep in plan_summary.get("dependencies", []):
        child = dep["child"]
        deps_map.setdefault(child, []).append(dep["parent"].split(".")[-1])

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
    mode: Optional[str] = typer.Option(
        None,
        "--mode",
        help="Write mode: replace | append | upsert | fill_to (overrides per-table config)",
    ),
    truncate: Optional[bool] = typer.Option(
        None,
        "--truncate/--no-truncate",
        help="Force or disable truncate-before-write",
    ),
    parallel: int = typer.Option(
        1,
        "--parallel",
        "-p",
        help="Number of independent tables to generate in parallel (>=1)",
    ),
    report: Optional[Path] = typer.Option(
        None,
        "--report",
        help="Write a markdown report of the run to this file",
    ),
    profile: bool = typer.Option(
        False,
        "--profile",
        help="Include per-column distinct/null/min/max stats in the report",
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help="Comma-separated list of table names (or suffixes) to generate. "
        "All other tables in the config will be skipped.",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Generate synthetic data."""
    _configure_verbosity(verbose, quiet)

    from sf_synth.config import WriteMode, load_config

    cfg = load_config(config)

    if seed is not None:
        cfg.defaults.seed = seed

    if mode is not None:
        try:
            mode_enum = WriteMode(mode)
        except ValueError:
            valid = ", ".join(m.value for m in WriteMode)
            console.print(f"[red]Invalid --mode '{mode}'.[/red] Use one of: {valid}")
            raise typer.Exit(2)
        for tbl in cfg.tables:
            tbl.write_mode = mode_enum

    if tables:
        wanted = [t.strip().upper() for t in tables.split(",") if t.strip()]
        kept: list = []
        for tbl in cfg.tables:
            tbl_name = tbl.name.upper()
            tbl_short = tbl_name.split(".")[-1]
            if any(w == tbl_name or w == tbl_short or tbl_short.endswith(w) for w in wanted):
                kept.append(tbl)
        if not kept:
            console.print(
                f"[red]No tables in the config match --tables filter '{tables}'.[/red]"
            )
            raise typer.Exit(2)
        cfg.tables = kept
        console.print(
            f"[blue]Filtered to {len(kept)} table(s):[/blue] "
            + ", ".join(t.name for t in kept)
        )

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

        engine = SynthEngine(backend.session, cfg, max_parallel_tables=max(1, parallel))
        engine.set_progress_callback(progress_callback)

        console.print("\n[bold]Starting synthesis...[/bold]\n")

        if dry_run:
            console.print("[yellow]DRY RUN - no data will be written[/yellow]\n")

        result = engine.generate(
            dry_run=dry_run,
            truncate=truncate,
            capture_samples=5 if report else 0,
        )

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
            if tr.success:
                status = "[green]OK[/green]"
            else:
                col_hint = f" (column: {tr.error_column})" if tr.error_column else ""
                status = f"[red]FAIL{col_hint}: {tr.error}[/red]"
            results_table.add_row(
                table_name,
                f"{tr.rows_generated:,}",
                f"{tr.elapsed_seconds:.2f}",
                status,
            )

        console.print(results_table)

        console.print(
            f"\n[bold]Total:[/bold] {result.total_rows:,} rows in "
            f"{result.total_elapsed_seconds:.2f}s"
        )

        if result.errors:
            console.print("\n[red]Errors:[/red]")
            for err in result.errors:
                console.print(f"  - {err}")

        if report:
            from sf_synth.report import build_markdown_report

            md = build_markdown_report(result, backend.session, profile=profile)
            report.write_text(md)
            console.print(f"\n[blue]Report written to: {report}[/blue]")

    finally:
        backend.disconnect()


@app.command()
def preview(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    rows: int = typer.Option(10, "--rows", "-n", help="Rows per table to preview"),
    connection: Optional[str] = typer.Option(None, "--connection", "-c"),
    table: Optional[str] = typer.Option(
        None, "--table", "-t", help="Preview only this table (matches by name suffix)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Preview a small sample of generated rows without writing to Snowflake."""
    _configure_verbosity(verbose, quiet)
    from sf_synth.config import load_config

    cfg = load_config(config)
    backend = _get_backend(connection=connection)

    try:
        from sf_synth.engine import SynthEngine

        engine = SynthEngine(backend.session, cfg)
        console.print(f"\n[bold]Previewing {rows} rows per table...[/bold]\n")
        previews = engine.preview(rows=rows)

        for table_fqn, sample in previews.items():
            short = table_fqn.split(".")[-1]
            if table and not short.endswith(table) and not table_fqn.endswith(table):
                continue
            console.print(Panel(f"[bold cyan]{table_fqn}[/bold cyan]", expand=False))
            if not sample:
                console.print("  [dim](no rows generated)[/dim]\n")
                continue
            if "_error" in sample[0]:
                console.print(f"  [red]Error: {sample[0]['_error']}[/red]\n")
                continue
            cols = list(sample[0].keys())
            tbl = Table(show_lines=False)
            for c in cols:
                tbl.add_column(c, style="white")
            for row_data in sample:
                tbl.add_row(*[_short(row_data.get(c)) for c in cols])
            console.print(tbl)
            console.print()
    finally:
        backend.disconnect()


@app.command()
def validate(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    connection: Optional[str] = typer.Option(None, "--connection", "-c"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Validate config against the live Snowflake DDL."""
    _configure_verbosity(verbose, quiet)
    from sf_synth.config import load_config
    from sf_synth.validation import validate_config_against_ddl

    cfg = load_config(config)
    backend = _get_backend(connection=connection)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Validating against Snowflake...", total=None)
            report = validate_config_against_ddl(backend.session, cfg)
    finally:
        backend.disconnect()

    if not report.issues:
        console.print(Panel("[bold green]Config is valid[/bold green]", expand=False))
        return

    if report.errors:
        console.print(Panel("[bold red]Validation Errors[/bold red]", expand=False))
        for issue in report.errors:
            col = f" / {issue.column}" if issue.column else ""
            console.print(f"  [red]ERROR[/red] {issue.table}{col}: {issue.message}")

    if report.warnings:
        console.print(Panel("[bold yellow]Validation Warnings[/bold yellow]", expand=False))
        for issue in report.warnings:
            col = f" / {issue.column}" if issue.column else ""
            console.print(f"  [yellow]WARN[/yellow] {issue.table}{col}: {issue.message}")

    if report.has_errors:
        raise typer.Exit(1)


@app.command()
def count(
    config: Path = typer.Argument(..., help="Path to config YAML file"),
    connection: Optional[str] = typer.Option(
        None,
        "--connection",
        "-c",
        help="Named connection from ~/.snowflake/connections.toml",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Print live row counts for every table in the config.

    Useful for verifying append/fill_to/upsert behavior between runs.
    """
    _configure_verbosity(verbose, quiet)
    from sf_synth.config import load_config

    cfg = load_config(config)
    backend = _get_backend(connection=connection)

    try:
        results_table = Table(title="Live row counts")
        results_table.add_column("Table", style="cyan")
        results_table.add_column("Rows", justify="right")
        results_table.add_column("Note")

        for tbl in cfg.tables:
            fqn = tbl.get_fqn(cfg.defaults.database, cfg.defaults.schema_name)
            try:
                row = backend.session.sql(f"SELECT COUNT(*) AS C FROM {fqn}").collect()
                cnt = row[0][0] if row else 0
                results_table.add_row(fqn, f"{cnt:,}", "")
            except Exception as e:
                results_table.add_row(fqn, "-", f"[red]{type(e).__name__}: {e}[/red]")

        console.print(results_table)
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
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    quiet: bool = typer.Option(False, "--quiet", "-q"),
) -> None:
    """Clean up temporary tables and optionally generated data."""
    _configure_verbosity(verbose, quiet)
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


def _short(value) -> str:  # noqa: ANN001
    if value is None:
        return ""
    s = str(value)
    return s if len(s) <= 50 else s[:47] + "..."


@app.callback()
def main() -> None:
    """
    sf-synth: High-fidelity synthetic data generation for Snowflake.

    Generate realistic synthetic data that preserves referential integrity,
    statistical distributions, and semantic patterns.
    """


if __name__ == "__main__":
    app()
