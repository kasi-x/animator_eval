"""Method Notes auto-generation from meta_lineage (Phase 3 gate).

Provides utilities to render method notes from meta_lineage metadata.
Integrated into BaseReportGenerator.write_report() for v2 compliance.
"""

import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class MethodMetadata:
    """Method metadata extracted from meta_lineage."""
    table_name: str
    audience: str
    source_silver_tables: list[str]
    formula_version: str
    ci_method: Optional[str]
    null_model: Optional[str]
    holdout_method: Optional[str]
    row_count: Optional[int]
    notes: Optional[str]
    inputs_hash: Optional[str]


def load_method_metadata(conn: sqlite3.Connection, table_name: str) -> Optional[MethodMetadata]:
    """Load method metadata for a report from meta_lineage.
    
    Args:
        conn: Database connection
        table_name: Name of the meta_* table (e.g., 'meta_policy_attrition')
    
    Returns:
        MethodMetadata instance or None if not found
    """
    cursor = conn.execute(
        """
        SELECT table_name, audience, source_silver_tables, formula_version,
               ci_method, null_model, holdout_method, row_count, notes, inputs_hash
        FROM meta_lineage
        WHERE table_name = ?
        """,
        (table_name,)
    )
    row = cursor.fetchone()
    if not row:
        return None

    return MethodMetadata(
        table_name=row[0],
        audience=row[1],
        source_silver_tables=[t.strip() for t in row[2].split(",")],
        formula_version=row[3],
        ci_method=row[4],
        null_model=row[5],
        holdout_method=row[6],
        row_count=row[7],
        notes=row[8],
        inputs_hash=row[9],
    )


def render_method_notes(metadata: MethodMetadata) -> str:
    """Render method notes HTML from metadata.
    
    Args:
        metadata: MethodMetadata instance
    
    Returns:
        HTML string suitable for inclusion in report
    """
    html_parts = [
        '<section id="method-notes">',
        '<h2>Method Notes</h2>',
        f'<p><strong>Formula version:</strong> {metadata.formula_version}</p>',
    ]

    # Silver tables used
    if metadata.source_silver_tables:
        tables_str = ", ".join([f"<code>{t}</code>" for t in metadata.source_silver_tables])
        html_parts.append(f'<p><strong>Source tables:</strong> {tables_str}</p>')

    # CI method
    if metadata.ci_method:
        html_parts.append(f'<p><strong>Confidence interval method:</strong> {metadata.ci_method}</p>')

    # Null model
    if metadata.null_model:
        html_parts.append(f'<p><strong>Null model:</strong> {metadata.null_model}</p>')

    # Holdout validation
    if metadata.holdout_method:
        html_parts.append(f'<p><strong>Holdout validation:</strong> {metadata.holdout_method}</p>')

    # Sample size
    if metadata.row_count:
        html_parts.append(f'<p><strong>Sample size:</strong> {metadata.row_count:,} observations</p>')

    # Additional notes
    if metadata.notes:
        html_parts.append(f'<p><strong>Notes:</strong> {metadata.notes}</p>')

    html_parts.append('</section>')
    return "\n".join(html_parts)


def audit_method_completeness(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], list[str]]:
    """Audit method metadata completeness.
    
    Args:
        conn: Database connection
        table_name: Name of the meta_* table
    
    Returns:
        (errors, warnings) tuple - errors are mandatory gaps, warnings are nice-to-haves
    """
    metadata = load_method_metadata(conn, table_name)
    if not metadata:
        return ([f"{table_name} not found in meta_lineage"], [])

    errors = []
    warnings = []

    # Mandatory fields
    if not metadata.formula_version:
        errors.append(f"{table_name}: formula_version is missing")
    if not metadata.source_silver_tables or not metadata.source_silver_tables[0]:
        errors.append(f"{table_name}: source_silver_tables is missing")

    # Nice-to-have fields (warnings if missing)
    if not metadata.ci_method and metadata.audience != "technical_appendix":
        warnings.append(f"{table_name}: ci_method is not specified (needed for public reports)")
    if not metadata.null_model and metadata.audience in ("policy", "hr"):
        warnings.append(f"{table_name}: null_model is not specified (recommended for causal claims)")
    if not metadata.holdout_method and "predictive" in (metadata.notes or "").lower():
        warnings.append(f"{table_name}: holdout_method is not specified (required for predictive claims)")

    return errors, warnings
