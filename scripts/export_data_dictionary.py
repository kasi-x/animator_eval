#!/usr/bin/env python3
"""Regenerate ``docs/DATA_DICTIONARY.md`` from the live SQLite schema.

Sources of truth (in order of precedence):

1. ``schema/**/*.yaml`` (Task 1-12 / E-1) if present — holds semantic
   descriptions and curated column metadata.
2. ``meta_lineage`` SQLite table if populated — provides provenance,
   CI methodology, audience class.
3. ``sqlite_master`` + ``PRAGMA table_info`` — always available;
   supplies raw column names, types, nullability, CHECK constraints
   (parsed from the CREATE TABLE SQL), PK, FK and indexes.

Usage::

    pixi run python scripts/export_data_dictionary.py
    pixi run python scripts/export_data_dictionary.py --output docs/DATA_DICTIONARY.md
    pixi run python scripts/export_data_dictionary.py --check   # CI: fails if diff

The ``--check`` flag re-renders to a temp buffer and compares to the
on-disk file; exits 1 on mismatch. Pre-commit invokes this flag so
schema changes and doc commits stay atomic.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Ensure repo root is importable for ``src.*`` modules.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import structlog  # noqa: E402

try:
    import yaml  # type: ignore[import-untyped]  # noqa: E402
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

log = structlog.get_logger()

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "DATA_DICTIONARY.md"
DEFAULT_SCHEMA_DIR = REPO_ROOT / "schema"

_SILVER_PREFIXES = ("anime", "persons", "credits", "studios", "characters")
_FEAT_PREFIX = "feat_"
_META_PREFIX = "meta_"
_AGG_PREFIX = "agg_"


# ----------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------


@dataclass
class ColumnInfo:
    name: str
    type_: str
    notnull: bool
    default: str | None
    pk: bool
    check_constraint: str | None
    semantic: str


@dataclass
class IndexInfo:
    name: str
    sql: str


@dataclass
class FKInfo:
    column: str
    ref_table: str
    ref_column: str


@dataclass
class TableDoc:
    name: str
    layer: str  # 'silver' / 'feat' / 'meta' / 'agg' / 'other'
    description: str
    columns: list[ColumnInfo]
    indexes: list[IndexInfo]
    fks: list[FKInfo]
    lineage: dict[str, str] | None
    raw_sql: str


# ----------------------------------------------------------------------
# Schema extraction
# ----------------------------------------------------------------------


def _classify_layer(name: str) -> str:
    if name.startswith(_META_PREFIX):
        return "meta"
    if name.startswith(_FEAT_PREFIX):
        return "feat"
    if name.startswith(_AGG_PREFIX):
        return "agg"
    if name in {
        "anime",
        "persons",
        "credits",
        "studios",
        "anime_studios",
        "anime_relations",
        "characters",
        "character_voice_actors",
        "person_affiliations",
        "scores",
        "score_history",
        "data_sources",
        "schema_meta",
    } or name.startswith(_SILVER_PREFIXES):
        return "silver"
    return "other"


def _parse_checks(create_sql: str, column: str) -> str | None:
    """Best-effort parse of CHECK constraints tied to a column."""
    if not create_sql:
        return None
    # Inline `col TYPE ... CHECK(expr)`
    pattern = re.compile(
        rf"(?im)^\s*[\"`]?{re.escape(column)}[\"`]?\s+[^,\n]*?CHECK\s*\(([^)]*)\)",
    )
    m = pattern.search(create_sql)
    if m:
        return m.group(1).strip()
    # Table-level CHECK referring to the column.
    tlvl = re.findall(r"CHECK\s*\(([^)]*)\)", create_sql, flags=re.IGNORECASE)
    for expr in tlvl:
        if re.search(rf"\b{re.escape(column)}\b", expr):
            return expr.strip()
    return None


def _load_yaml_schemas(schema_dir: Path) -> dict[str, dict]:
    """Load ``schema/**/*.yaml`` if present. Returns {table_name: payload}."""
    if not schema_dir.exists() or yaml is None:
        return {}
    out: dict[str, dict] = {}
    for yml in sorted(schema_dir.rglob("*.yaml")):
        try:
            payload = yaml.safe_load(yml.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover
            log.warning("yaml_parse_failed", path=str(yml), error=str(exc))
            continue
        if isinstance(payload, dict) and "table" in payload:
            out[payload["table"]] = payload
        elif isinstance(payload, dict):
            for name, body in payload.items():
                if isinstance(body, dict):
                    out[name] = body
    return out


def _load_lineage(conn: sqlite3.Connection) -> dict[str, dict]:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta_lineage'"
    )
    if cur.fetchone() is None:
        return {}
    cur = conn.execute("PRAGMA table_info(meta_lineage)")
    cols = [row[1] for row in cur.fetchall()]
    rows = conn.execute(
        f"SELECT {', '.join(cols)} FROM meta_lineage"
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        rec = dict(zip(cols, row, strict=False))
        name = rec.get("table_name")
        if not name:
            continue
        out[str(name)] = {k: rec.get(k) for k in cols}
    return out


def _collect_tables(
    conn: sqlite3.Connection,
    yaml_schemas: dict[str, dict],
    lineage: dict[str, dict],
) -> list[TableDoc]:
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()

    docs: list[TableDoc] = []
    for name, create_sql in rows:
        layer = _classify_layer(name)
        yaml_payload = yaml_schemas.get(name, {})
        semantic = yaml_payload.get("description", "")

        # Columns via PRAGMA.
        col_rows = conn.execute(f"PRAGMA table_info({name})").fetchall()
        yaml_cols = {c.get("name"): c for c in yaml_payload.get("columns", []) or []}

        columns: list[ColumnInfo] = []
        for (_, col_name, col_type, notnull, default, pk) in col_rows:
            check = _parse_checks(create_sql or "", col_name)
            cdoc = yaml_cols.get(col_name, {})
            columns.append(
                ColumnInfo(
                    name=col_name,
                    type_=col_type or "",
                    notnull=bool(notnull),
                    default=default,
                    pk=bool(pk),
                    check_constraint=check,
                    semantic=cdoc.get("description", ""),
                )
            )

        # Indexes.
        idx_rows = conn.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type='index' AND tbl_name=? AND sql IS NOT NULL "
            "ORDER BY name",
            (name,),
        ).fetchall()
        indexes = [IndexInfo(name=n, sql=(s or "").strip()) for n, s in idx_rows]

        # FKs.
        fk_rows = conn.execute(f"PRAGMA foreign_key_list({name})").fetchall()
        fks = [
            FKInfo(column=r[3], ref_table=r[2], ref_column=r[4])
            for r in fk_rows
        ]

        docs.append(
            TableDoc(
                name=name,
                layer=layer,
                description=semantic,
                columns=columns,
                indexes=indexes,
                fks=fks,
                lineage=lineage.get(name),
                raw_sql=(create_sql or "").strip(),
            )
        )
    return docs


# ----------------------------------------------------------------------
# Markdown rendering
# ----------------------------------------------------------------------


def _render_column_row(c: ColumnInfo) -> str:
    null = "NOT NULL" if c.notnull else "NULL"
    pk = "PK" if c.pk else ""
    check = c.check_constraint or ""
    default = c.default if c.default is not None else ""
    extras = " / ".join(x for x in (pk, check, default) if x)
    semantic = c.semantic or ""
    return f"| `{c.name}` | `{c.type_ or '—'}` | {null} | {extras or '—'} | {semantic} |"


def _render_table(doc: TableDoc) -> str:
    parts: list[str] = [f"### `{doc.name}`  (layer: {doc.layer})"]
    if doc.description:
        parts.append("")
        parts.append(f"> {doc.description}")
    parts.append("")
    parts.append("| Column | Type | Null | PK / CHECK / Default | Description |")
    parts.append("|--------|------|------|----------------------|-------------|")
    for c in doc.columns:
        parts.append(_render_column_row(c))

    if doc.fks:
        parts.append("")
        parts.append("**Foreign keys:**")
        for fk in doc.fks:
            parts.append(
                f"- `{fk.column}` → `{fk.ref_table}.{fk.ref_column}`"
            )
    if doc.indexes:
        parts.append("")
        parts.append("**Indexes:**")
        for idx in doc.indexes:
            parts.append(f"- `{idx.name}` — `{idx.sql}`")

    if doc.lineage:
        parts.append("")
        parts.append("**Lineage (meta_lineage):**")
        for k in sorted(doc.lineage):
            v = doc.lineage[k]
            if v is None or v == "":
                continue
            parts.append(f"- `{k}`: {v}")
    parts.append("")
    return "\n".join(parts)


def render_markdown(docs: list[TableDoc], schema_version: int | None) -> str:
    # Use date-only timestamp for stability between regenerations.
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    by_layer: dict[str, list[TableDoc]] = {}
    for d in docs:
        by_layer.setdefault(d.layer, []).append(d)

    parts: list[str] = []
    parts.append("# Animetor Eval — Data Dictionary")
    parts.append("")
    parts.append(
        "> Auto-generated by `scripts/export_data_dictionary.py`. "
        "Do not edit by hand — commit changes by regenerating."
    )
    parts.append("")
    parts.append(f"- Generated (UTC date): {now}")
    if schema_version is not None:
        parts.append(f"- Schema version (SCHEMA_VERSION): {schema_version}")
    parts.append(f"- Tables documented: {len(docs)}")
    parts.append("")

    layer_titles = {
        "silver": "Silver (normalized, analysis-ready)",
        "feat": "Feat (feature tables derived from silver)",
        "meta": "Meta (lineage, quality, audience tagging)",
        "agg": "Agg (pre-aggregated report inputs)",
        "other": "Other",
    }
    parts.append("## Layer index")
    parts.append("")
    for layer in ("silver", "feat", "meta", "agg", "other"):
        bucket = by_layer.get(layer) or []
        if not bucket:
            continue
        parts.append(f"### {layer_titles[layer]}")
        for d in bucket:
            parts.append(f"- [`{d.name}`](#{d.name.replace('_', '-')})")
        parts.append("")

    parts.append("---")
    parts.append("")
    parts.append("## Table reference")
    parts.append("")

    for layer in ("silver", "feat", "meta", "agg", "other"):
        bucket = by_layer.get(layer) or []
        if not bucket:
            continue
        parts.append(f"## {layer_titles[layer]}")
        parts.append("")
        for d in bucket:
            parts.append(_render_table(d))
    parts.append("")
    return "\n".join(parts).rstrip() + "\n"


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------


def _schema_version(conn: sqlite3.Connection) -> int | None:
    try:
        cur = conn.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])
    except sqlite3.Error:
        return None


def generate(
    *,
    db_path: Path,
    schema_dir: Path,
    output: Path,
    check_only: bool,
) -> int:
    if not db_path.exists():
        # Build an ephemeral schema-only DB by importing src.db.init.
        log.info("bootstrapping_temp_db", reason="db_missing", db_path=str(db_path))
        import tempfile

        from src.db import init_db

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with sqlite3.connect(str(tmp_path)) as seed:
                init_db(seed)
            return _run(tmp_path, schema_dir, output, check_only)
        finally:
            try:
                tmp_path.unlink()
            except OSError:
                pass
    return _run(db_path, schema_dir, output, check_only)


def _run(
    db_path: Path,
    schema_dir: Path,
    output: Path,
    check_only: bool,
) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        yaml_schemas = _load_yaml_schemas(schema_dir)
        lineage = _load_lineage(conn)
        docs = _collect_tables(conn, yaml_schemas, lineage)
        version = _schema_version(conn)
    rendered = render_markdown(docs, version)

    if check_only:
        existing = output.read_text(encoding="utf-8") if output.exists() else ""

        def _strip_volatile(text: str) -> str:
            # Exclude the "Generated" header line so day-of-run
            # differences do not flap the CI gate.
            return "\n".join(
                line for line in text.splitlines()
                if not line.startswith("- Generated")
            )

        if _strip_volatile(existing) == _strip_volatile(rendered):
            print(f"export_data_dictionary: {output} is up to date.")
            return 0
        print(
            f"export_data_dictionary: FAIL — {output} is stale. "
            "Re-run `pixi run python scripts/export_data_dictionary.py`."
        )
        # Also emit a brief diff hint (first 20 mismatch lines).
        old_lines = existing.splitlines()
        new_lines = rendered.splitlines()
        import difflib

        diff = list(
            difflib.unified_diff(
                old_lines, new_lines, fromfile=str(output), tofile="<expected>",
                n=2, lineterm="",
            )
        )
        for line in diff[:30]:
            print(line)
        return 1

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(
        f"export_data_dictionary: wrote {output} "
        f"({len(docs)} tables, schema_version={version})."
    )
    return 0


def _default_db_path() -> Path:
    try:
        from src.utils.config import DB_PATH  # type: ignore[import-not-found]
        return Path(DB_PATH)
    except Exception:
        return REPO_ROOT / "data" / "animetor.db"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--schema-dir", type=Path, default=DEFAULT_SCHEMA_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true",
                        help="Fail if output is stale (CI mode).")
    args = parser.parse_args(argv)
    db_path = args.db or _default_db_path()
    return generate(
        db_path=db_path,
        schema_dir=args.schema_dir,
        output=args.output,
        check_only=args.check,
    )


if __name__ == "__main__":
    sys.exit(main())
