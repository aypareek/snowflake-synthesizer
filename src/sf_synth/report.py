"""Post-synthesis report generation.

Builds a markdown / HTML summary of a generation run including row counts,
column samples, and basic distribution stats.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Session

    from sf_synth.engine import SynthesisResult


@dataclass
class TableProfile:
    """Profile of a generated table — used inside the post-synthesis report."""

    table_fqn: str
    requested_rows: int
    actual_rows: int
    column_stats: list[dict[str, Any]]


def build_markdown_report(
    result: SynthesisResult,
    session: Session | None = None,
    profile: bool = False,
) -> str:
    """Render a markdown report for a finished synthesis run."""
    lines: list[str] = ["# sf-synth Generation Report", ""]
    lines.append(f"- Total tables: **{len(result.tables)}**")
    lines.append(f"- Total rows: **{result.total_rows:,}**")
    lines.append(f"- Total time: **{result.total_elapsed_seconds:.2f}s**")
    status = "Success" if result.success else "Completed with errors"
    lines.append(f"- Status: **{status}**")
    lines.append("")

    lines.append("## Tables")
    lines.append("")
    lines.append("| Table | Rows | Time (s) | Status |")
    lines.append("|---|---:|---:|---|")
    for tr in result.tables:
        st = "OK" if tr.success else f"FAIL ({tr.error})"
        st = st.replace("|", "\\|")
        lines.append(
            f"| {tr.table_fqn} | {tr.rows_generated:,} | {tr.elapsed_seconds:.2f} | {st} |"
        )
    lines.append("")

    if result.errors:
        lines.append("## Errors")
        lines.append("")
        for err in result.errors:
            lines.append(f"- {err}")
        lines.append("")

    samples_present = any(tr.sample_rows for tr in result.tables)
    if samples_present:
        lines.append("## Sample Rows")
        lines.append("")
        for tr in result.tables:
            if not tr.sample_rows:
                continue
            lines.append(f"### {tr.table_fqn}")
            lines.append("")
            cols = list(tr.sample_rows[0].keys())
            lines.append("| " + " | ".join(cols) + " |")
            lines.append("|" + "|".join(["---"] * len(cols)) + "|")
            for row in tr.sample_rows:
                vals = [_fmt(row.get(c)) for c in cols]
                lines.append("| " + " | ".join(vals) + " |")
            lines.append("")

    if profile and session is not None:
        lines.append("## Column Profiles")
        lines.append("")
        for tr in result.tables:
            if not tr.success or tr.rows_generated == 0:
                continue
            prof = _profile_table(session, tr.table_fqn)
            if not prof:
                continue
            lines.append(f"### {tr.table_fqn}")
            lines.append("")
            lines.append("| Column | Distinct | Nulls | Min | Max |")
            lines.append("|---|---:|---:|---|---|")
            for stat in prof.column_stats:
                lines.append(
                    f"| {stat['name']} | {stat.get('distinct', '?'):,} | "
                    f"{stat.get('nulls', '?'):,} | {stat.get('min', '')} | "
                    f"{stat.get('max', '')} |"
                )
            lines.append("")

    return "\n".join(lines)


def _fmt(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    if len(s) > 60:
        s = s[:57] + "..."
    return s.replace("|", "\\|").replace("\n", " ")


def _profile_table(session: Session, table_fqn: str) -> TableProfile | None:
    try:
        desc = session.sql(f"DESCRIBE TABLE {table_fqn}").collect()
    except Exception:
        return None
    cols = [(r["name"], r.get("type", "")) for r in desc]
    if not cols:
        return None

    select_parts: list[str] = ["COUNT(*) AS _total"]
    for cname, ctype in cols:
        select_parts.append(
            f'COUNT(DISTINCT "{cname}") AS "_d_{cname}"'
        )
        select_parts.append(
            f'SUM(IFF("{cname}" IS NULL, 1, 0)) AS "_n_{cname}"'
        )
        if any(t in ctype.upper() for t in ("NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL")):
            select_parts.append(f'MIN("{cname}") AS "_min_{cname}"')
            select_parts.append(f'MAX("{cname}") AS "_max_{cname}"')

    sql = f"SELECT {', '.join(select_parts)} FROM {table_fqn}"
    try:
        row = session.sql(sql).collect()[0].as_dict()
    except Exception:
        return None
    total = int(row.get("_TOTAL", 0))
    stats: list[dict[str, Any]] = []
    for cname, _ in cols:
        stat: dict[str, Any] = {
            "name": cname,
            "distinct": row.get(f"_D_{cname}".upper()),
            "nulls": row.get(f"_N_{cname}".upper()),
        }
        if f"_MIN_{cname}".upper() in row:
            stat["min"] = row[f"_MIN_{cname}".upper()]
            stat["max"] = row[f"_MAX_{cname}".upper()]
        stats.append(stat)
    return TableProfile(
        table_fqn=table_fqn,
        requested_rows=total,
        actual_rows=total,
        column_stats=stats,
    )
