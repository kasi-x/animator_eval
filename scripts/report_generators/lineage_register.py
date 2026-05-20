"""Auto-register report SPEC → mart.meta_lineage rows.

各 v2 report の SPEC.data_lineage / method_gate / meta_table を集約し、
`meta_lineage` テーブルへ upsert。data lineage を 1 箇所で見渡せる。

Usage:
    pixi run python scripts/report_generators/lineage_register.py
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import structlog

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

log = structlog.get_logger(__name__)


def _collect_lineage_rows() -> list[dict]:
    """V2_REPORT_CLASSES から meta_lineage 行を構築。"""
    from scripts.report_generators.reports import V2_REPORT_CLASSES

    rows: list[dict] = []
    seen_meta_tables: set[str] = set()

    for cls in V2_REPORT_CLASSES:
        mod = inspect.getmodule(cls)
        if mod is None:
            continue
        spec = getattr(mod, "SPEC", None)
        if spec is None:
            continue
        meta_table = getattr(spec, "data_lineage", None)
        if meta_table is None:
            continue
        mt_name = getattr(meta_table, "meta_table", None) or getattr(spec, "name", "")
        if not mt_name or mt_name in seen_meta_tables:
            continue
        seen_meta_tables.add(mt_name)

        audience = getattr(spec, "audience", "common")
        sources = list(getattr(meta_table, "sources", []) or [])
        snapshot = getattr(meta_table, "snapshot_date", "") or ""
        version = getattr(meta_table, "pipeline_version", "") or "dev"

        mg = getattr(spec, "method_gate", None)
        ci_obj = getattr(mg, "ci", None) if mg else None
        ci_method = getattr(ci_obj, "estimator", "") if ci_obj else ""
        null_model = ",".join(getattr(spec, "null_model", []) or [])
        holdout_obj = getattr(mg, "holdout", None) if mg else None
        holdout_method = (
            getattr(holdout_obj, "split_strategy", "")
            if holdout_obj is not None else ""
        )

        rows.append({
            "table_name": mt_name,
            "audience": audience,
            "source_silver_tables": json.dumps(sources, ensure_ascii=False),
            "source_bronze_forbidden": 1,
            "source_display_allowed": 0,
            "description": (getattr(spec, "claim", "") or "")[:500],
            "formula_version": version,
            "computed_at": snapshot or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "ci_method": ci_method or None,
            "null_model": null_model or None,
            "holdout_method": holdout_method or None,
            "row_count": None,
            "notes": f"auto-registered from {mod.__name__}",
            "rng_seed": getattr(mg, "rng_seed", None) if mg else None,
        })

    return rows


def upsert_meta_lineage(rows: list[dict]) -> int:
    """meta_lineage に upsert。失敗 graceful。"""
    if not rows:
        return 0
    try:
        from src.analysis.io.mart_writer import gold_connect_write
    except ImportError as exc:
        log.warning("lineage_upsert_unavailable", error=str(exc))
        return 0

    inserted = 0
    try:
        with gold_connect_write() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
            conn.execute("SET schema='mart'")
            # ensure DDL applied (idempotent)
            from src.analysis.io.mart_writer import _DDL
            for stmt in _DDL.split(";"):
                s = stmt.strip()
                if s:
                    try:
                        conn.execute(s)
                    except Exception:
                        pass
            cols = list(rows[0].keys())
            placeholders = ",".join("?" * len(cols))
            update_set = ",".join(f"{c}=excluded.{c}" for c in cols if c != "table_name")
            sql = (
                f"INSERT INTO meta_lineage ({','.join(cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT(table_name) DO UPDATE SET {update_set}"
            )
            for row in rows:
                try:
                    conn.execute(sql, list(row.values()))
                    inserted += 1
                except Exception as exc:
                    log.debug("lineage_row_skip", table=row.get("table_name"), error=str(exc))
    except Exception as exc:
        log.warning("lineage_upsert_failed", error=str(exc))
        return 0
    log.info("lineage_upserted", n=inserted)
    return inserted


def write_lineage_json(rows: list[dict], path: Path | str) -> Path:
    """meta_lineage rows を JSON 出力 (mart 不在環境用)。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="result/json/meta_lineage.json")
    parser.add_argument(
        "--no-db", action="store_true",
        help="skip DB upsert, JSON only",
    )
    args = parser.parse_args()

    rows = _collect_lineage_rows()
    write_lineage_json(rows, args.json)
    print(f"Collected {len(rows)} lineage rows, written to {args.json}")

    if not args.no_db:
        n = upsert_meta_lineage(rows)
        print(f"Upserted {n} rows into mart.meta_lineage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
