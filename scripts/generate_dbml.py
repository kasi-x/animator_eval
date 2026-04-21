#!/usr/bin/env python3
"""Generate DBML documentation from SQLModel schema.

Usage:
    python scripts/generate_dbml.py > docs/schema.dbml
    atlas schema inspect sqlite://./data/animetor.db --format dbml > docs/schema.dbml

This script produces a DBDiagram.io-compatible DBML file from the SQLModel
definitions in src/models_v2.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import inspect
from src.database_v2 import create_sqlalchemy_engine
from src.models_v2 import SQLModel


def generate_dbml(db_path: Path | None = None) -> str:
    """Generate DBML from SQLModel schema.

    Args:
        db_path: Path to database. If None, uses in-memory to inspect schema.

    Returns:
        DBML string.
    """
    # Create engine and inspect schema
    engine = create_sqlalchemy_engine(db_path)
    inspector = inspect(engine)

    dbml = []
    dbml.append("// Generated DBML Schema")
    dbml.append("// Source: src/models_v2.py (SQLModel)")
    dbml.append("// Time: see generated timestamp")
    dbml.append("")
    dbml.append("// ============================================================================")
    dbml.append("// SILVER LAYER: Canonical, Score-Free Data (12 tables)")
    dbml.append("// ============================================================================")
    dbml.append("")

    silver_tables = [
        "anime",
        "anime_external_ids",
        "anime_genres",
        "anime_tags",
        "persons",
        "person_external_ids",
        "person_aliases",
        "roles",
        "sources",
        "credits",
        "ext_ids",
        "analysis",
    ]

    gold_tables = [
        "person_scores",
        "meta_lineage",
        "schema_meta",
    ]

    # Generate table definitions
    all_tables = inspector.get_table_names()

    for table_name in sorted(all_tables):
        if table_name.startswith("_archive"):
            continue

        columns = inspector.get_columns(table_name)
        pk = inspector.get_pk_constraint(table_name)
        fks = inspector.get_foreign_keys(table_name)

        # Table header
        if table_name in silver_tables:
            dbml.append(f"Table {table_name} {{")
        elif table_name in gold_tables:
            dbml.append(f"Table {table_name} {{")
        else:
            dbml.append(f"Table {table_name} {{")

        # Columns
        for col in columns:
            col_name = col["name"]
            col_type = str(col["type"])

            # Simplify type names for DBML
            if "VARCHAR" in col_type:
                col_type = "string"
            elif "TEXT" in col_type:
                col_type = "string"
            elif "INTEGER" in col_type:
                col_type = "integer"
            elif "REAL" in col_type:
                col_type = "float"
            elif "BOOLEAN" in col_type:
                col_type = "boolean"
            elif "TIMESTAMP" in col_type:
                col_type = "timestamp"
            elif "DATETIME" in col_type:
                col_type = "timestamp"

            # Constraints
            constraints = []
            # Check if column is in primary key
            pk_cols = pk.get("constrained_columns", []) if pk else []
            if col_name in pk_cols:
                constraints.append("pk")
            if not col["nullable"]:
                constraints.append("not null")

            constraint_str = ""
            if constraints:
                constraint_str = f" [{', '.join(constraints)}]"

            dbml.append(f"  {col_name} {col_type}{constraint_str}")

        # Foreign keys (in comment form for DBML)
        for fk in fks:
            local_col = ", ".join(fk.get("constrained_columns", []))
            remote_table = fk.get("referred_table", "?")
            remote_col = ", ".join(fk.get("referred_columns", []))
            dbml.append(f"  // FK: {local_col} -> {remote_table}.{remote_col}")

        dbml.append("}")
        dbml.append("")

    return "\n".join(dbml)


if __name__ == "__main__":
    dbml = generate_dbml()
    print(dbml)
