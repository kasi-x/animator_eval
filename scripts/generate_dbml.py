#!/usr/bin/env python3
"""Generate DBML documentation from schema SQL.

DEPRECATED: Use Atlas instead:
    atlas schema inspect sqlite://./data/animetor.db --format dbml

This script is kept for documentation generation in CI. It extracts CREATE TABLE
statements from src/db/schema.py and converts them to DBDiagram.io DBML format.
No longer depends on SQLAlchemy or models_v2.py.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def extract_sql_ddl() -> str:
    """Extract CREATE TABLE SQL from src/db/schema.py."""
    schema_file = Path(__file__).parent.parent / "src" / "db" / "schema.py"
    with open(schema_file) as f:
        content = f.read()

    # Extract the SQL from the executescript call
    match = re.search(r'conn\.executescript\("""(.*?)"""\)', content, re.DOTALL)
    if not match:
        raise ValueError("Could not find SQL in schema.py")
    return match.group(1)


def generate_dbml(db_path: Path | None = None) -> str:
    """Generate DBML from schema SQL.

    Args:
        db_path: Path to database. Currently unused (for API compat).

    Returns:
        DBML string.
    """
    sql = extract_sql_ddl()

    dbml = []
    dbml.append("// Generated DBML Schema from src/db/schema.py")
    dbml.append("// This is the consolidated BRONZE/SILVER/GOLD layer schema")
    dbml.append("")
    dbml.append("// ============================================================================")
    dbml.append("// SILVER LAYER: Canonical, Score-Free Data (12 tables)")
    dbml.append("// ============================================================================")
    dbml.append("")

    # Simple table parsing: find CREATE TABLE statements
    table_pattern = r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\);"
    tables = re.finditer(table_pattern, sql, re.DOTALL)

    for match in tables:
        table_name = match.group(1)
        table_def = match.group(2)

        dbml.append(f"Table {table_name} {{")

        # Parse columns
        lines = table_def.split("\n")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("--") or line.startswith("UNIQUE") or line.startswith("PRIMARY KEY") or line.startswith("FOREIGN KEY"):
                continue
            if line.startswith("CONSTRAINT") or line.startswith("CHECK"):
                continue

            # Parse column definition
            parts = line.split()
            if len(parts) < 2:
                continue

            col_name = parts[0]
            col_type = parts[1]

            # Simplify SQLite types to DBML types
            if "TEXT" in col_type or "CHAR" in col_type:
                dbml_type = "string"
            elif "INT" in col_type:
                dbml_type = "integer"
            elif "REAL" in col_type or "FLOAT" in col_type:
                dbml_type = "float"
            elif "BOOL" in col_type:
                dbml_type = "boolean"
            elif "DATE" in col_type or "TIMESTAMP" in col_type:
                dbml_type = "timestamp"
            else:
                dbml_type = "string"

            # Check for constraints
            constraints = []
            if "PRIMARY KEY" in line:
                constraints.append("pk")
            if "NOT NULL" in line:
                constraints.append("not null")
            if "UNIQUE" in line:
                constraints.append("unique")

            constraint_str = ""
            if constraints:
                constraint_str = f" [{', '.join(constraints)}]"

            dbml.append(f"  {col_name} {dbml_type}{constraint_str}")

        dbml.append("}")
        dbml.append("")

    return "\n".join(dbml)


if __name__ == "__main__":
    try:
        dbml = generate_dbml()
        print(dbml)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
