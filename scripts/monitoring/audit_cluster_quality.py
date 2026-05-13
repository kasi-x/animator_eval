#!/usr/bin/env python3
"""Audit cluster quality in resolved.duckdb (Phase 2b post-fix verification).

Performs:
1. Cluster size distribution (anime / persons / studios)
2. Reason distribution (merge strategy breakdown)
3. Over-merge detection: same-source large clusters (homonym guard gaps)
4. Known over-merge regression tests (Jonas / David / TMDb homonyms)
5. Credits row count preservation (information leak detection)

Usage:
    pixi run audit  # or python -m scripts.monitoring.audit_cluster_quality
"""

import json
import sqlite3
from pathlib import Path

import duckdb
import structlog

logger = structlog.get_logger()


def connect_resolved() -> duckdb.DuckDBPyConnection:
    """Connect to resolved.duckdb (read-only)."""
    db_path = Path("result/resolved.duckdb")
    if not db_path.exists():
        raise FileNotFoundError(f"Resolved database not found: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def audit_cluster_size_distribution(con: duckdb.DuckDBPyConnection) -> dict:
    """Audit cluster size distribution by entity type.

    Returns:
        {entity_type: {size: count, max_size: N, min_size: 1, mean_size: float}}
    """
    results = {}

    for entity_type in ["anime", "persons", "studios"]:
        # Get cluster size distribution
        query = f"""
            SELECT
                json_array_length(source_ids_json) AS cluster_size,
                COUNT(*) AS freq
            FROM {entity_type}
            GROUP BY cluster_size
            ORDER BY cluster_size DESC
        """
        df = con.execute(query).fetch_df()

        size_dist = dict(zip(df["cluster_size"], df["freq"]))
        max_size = df["cluster_size"].max() if len(df) > 0 else 0
        min_size = df["cluster_size"].min() if len(df) > 0 else 0
        mean_size = (
            (df["cluster_size"] * df["freq"]).sum() / df["freq"].sum()
            if len(df) > 0
            else 0
        )

        results[entity_type] = {
            "size_distribution": size_dist,
            "max_size": int(max_size),
            "min_size": int(min_size),
            "mean_size": float(mean_size),
            "total_clusters": int(df["freq"].sum()),
        }

        logger.info(
            "cluster_size_distribution",
            entity=entity_type,
            max_size=max_size,
            mean_size=round(mean_size, 2),
            total_clusters=int(df["freq"].sum()),
        )

    return results


def audit_reason_distribution(con: duckdb.DuckDBPyConnection) -> dict:
    """Audit merge reason distribution from resolution audit table.

    Returns:
        {reason: count, ...} (strategy breakdown)
    """
    # Check if audit table exists
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    table_names = [t[0] for t in tables]

    if "ops_entity_resolution_audit" not in table_names:
        logger.warning("ops_entity_resolution_audit table not found; skipping reason distribution")
        return {"status": "not_available"}

    query = """
        SELECT
            strategy AS reason,
            COUNT(*) AS freq
        FROM ops_entity_resolution_audit
        GROUP BY strategy
        ORDER BY freq DESC
    """
    try:
        df = con.execute(query).fetch_df()
        reason_dist = dict(zip(df["reason"], df["freq"]))

        total_merges = df["freq"].sum()
        logger.info(
            "reason_distribution",
            total_merges=int(total_merges),
            strategies=len(reason_dist),
        )

        return {
            "reason_distribution": reason_dist,
            "total_merges": int(total_merges),
        }
    except Exception as e:
        logger.warning("reason_distribution_query_failed", error=str(e))
        return {"status": "query_failed"}


def audit_over_merge_candidates(con: duckdb.DuckDBPyConnection) -> dict:
    """Audit potential over-merges: large clusters with single source types.

    Note: Simplified query; full source distribution analysis would require
    parsing source_ids_json in application code.

    Returns:
        {entity_type: {candidates: [cluster_info, ...], count: N}}
    """
    results = {}

    for entity_type in ["anime", "persons", "studios"]:
        # Simple heuristic: find clusters larger than 10
        query = f"""
            SELECT
                canonical_id,
                source_ids_json,
                json_array_length(source_ids_json) AS size
            FROM {entity_type}
            WHERE json_array_length(source_ids_json) > 10
            ORDER BY size DESC
            LIMIT 20
        """
        try:
            df = con.execute(query).fetch_df()
            candidates = [
                {
                    "canonical_id": row["canonical_id"],
                    "size": row["size"],
                }
                for _, row in df.iterrows()
            ]
            results[entity_type] = {
                "candidates": candidates,
                "count": len(candidates),
            }
            if len(candidates) > 0:
                logger.info(
                    "over_merge_candidate_found",
                    entity=entity_type,
                    count=len(candidates),
                    max_size=candidates[0]["size"],
                )
        except Exception as e:
            logger.warning(
                "over_merge_query_failed",
                entity=entity_type,
                error=str(e),
            )
            results[entity_type] = {"status": "query_failed"}

    return results


def audit_known_over_merge_regression(con: duckdb.DuckDBPyConnection) -> dict:
    """Audit known over-merge cases (regression detection).

    Tests:
    - Jonas: Should be multiple clusters (one per tmdb_id), not 1
    - David: Should be multiple clusters, not 1
    - Sazae-san: anime should remain single cluster (not split)
    """
    results = {}

    # Check Jonas persons (TMDb homonym case)
    jonas_query = """
        SELECT
            canonical_id,
            name_en,
            json_array_length(source_ids_json) AS cluster_size,
            source_ids_json
        FROM persons
        WHERE name_en = 'Jonas'
            AND source_ids_json LIKE '%tmdb%'
        ORDER BY cluster_size DESC
    """
    try:
        df = con.execute(jonas_query).fetch_df()
        jonas_clusters = len(df)
        jonas_max_size = df["cluster_size"].max() if len(df) > 0 else 0

        results["jonas"] = {
            "clusters": jonas_clusters,
            "max_cluster_size": int(jonas_max_size),
            "status": "✅ PASS" if jonas_clusters >= 5 else "⚠️  WARN",
        }
        logger.info(
            "known_regression_check",
            entity="Jonas",
            clusters=jonas_clusters,
            max_size=jonas_max_size,
        )
    except Exception as e:
        logger.warning("jonas_query_failed", error=str(e))
        results["jonas"] = {"status": "query_failed"}

    # Check David persons
    david_query = """
        SELECT
            canonical_id,
            name_en,
            json_array_length(source_ids_json) AS cluster_size
        FROM persons
        WHERE name_en = 'David'
            AND source_ids_json LIKE '%tmdb%'
        ORDER BY cluster_size DESC
    """
    try:
        df = con.execute(david_query).fetch_df()
        david_clusters = len(df)
        david_max_size = df["cluster_size"].max() if len(df) > 0 else 0

        results["david"] = {
            "clusters": david_clusters,
            "max_cluster_size": int(david_max_size),
            "status": "✅ PASS" if david_clusters >= 3 else "⚠️  WARN",
        }
        logger.info(
            "known_regression_check",
            entity="David",
            clusters=david_clusters,
            max_size=david_max_size,
        )
    except Exception as e:
        logger.warning("david_query_failed", error=str(e))
        results["david"] = {"status": "query_failed"}

    # Check Sazae-san (should be single anime cluster)
    sazae_query = """
        SELECT
            canonical_id,
            title_en,
            json_array_length(source_ids_json) AS cluster_size
        FROM anime
        WHERE title_en LIKE '%Sazae%'
    """
    try:
        df = con.execute(sazae_query).fetch_df()
        sazae_rows = len(df)
        if sazae_rows > 0:
            sazae_size = df.iloc[0]["cluster_size"]
            results["sazae_san"] = {
                "clusters": sazae_rows,
                "cluster_size": int(sazae_size),
                "status": "✅ PASS" if sazae_rows == 1 else "⚠️  WARN",
            }
        else:
            results["sazae_san"] = {"status": "not_found"}
        logger.info(
            "known_regression_check",
            entity="Sazae-san",
            clusters=sazae_rows,
        )
    except Exception as e:
        logger.warning("sazae_san_query_failed", error=str(e))
        results["sazae_san"] = {"status": "query_failed"}

    return results


def audit_credits_preservation(con: duckdb.DuckDBPyConnection) -> dict:
    """Verify credits row count is preserved (information leak detection).

    Returns:
        {credits_count: N, persons_count: N, anime_count: N, studios_count: N}
    """
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    table_names = [t[0] for t in tables]

    results = {}

    for table in ["credits", "persons", "anime", "studios"]:
        if table in table_names:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            results[table] = count
            logger.info("entity_count", entity=table, count=count)
        else:
            logger.warning("table_not_found", table=table)
            results[table] = None

    return results


def generate_audit_report(
    size_dist: dict,
    reason_dist: dict,
    over_merges: dict,
    known_regressions: dict,
    credits: dict,
) -> str:
    """Generate markdown audit report."""
    report_lines = [
        "# Cluster Quality Audit v1.3",
        "",
        "## Summary",
        "",
        f"- **Audit date**: {__import__('datetime').datetime.now().isoformat()}",
        f"- **Persons clusters**: {size_dist.get('persons', {}).get('total_clusters', 'N/A')}",
        f"- **Anime clusters**: {size_dist.get('anime', {}).get('total_clusters', 'N/A')}",
        f"- **Studios clusters**: {size_dist.get('studios', {}).get('total_clusters', 'N/A')}",
        "",
        "## Cluster Size Distribution",
        "",
    ]

    for entity_type, data in size_dist.items():
        report_lines.extend([
            f"### {entity_type.capitalize()}",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Max size | {data.get('max_size', 'N/A')} |",
            f"| Min size | {data.get('min_size', 'N/A')} |",
            f"| Mean size | {data.get('mean_size', 'N/A'):.2f} |",
            f"| Total clusters | {data.get('total_clusters', 'N/A')} |",
            "",
        ])

    report_lines.extend([
        "## Merge Strategy Breakdown",
        "",
    ])

    if "reason_distribution" in reason_dist:
        report_lines.append("| Strategy | Count |")
        report_lines.append("|----------|-------|")
        for reason, count in reason_dist["reason_distribution"].items():
            report_lines.append(f"| {reason} | {count} |")
    else:
        report_lines.append("_No audit table available_")

    report_lines.extend([
        "",
        "## Known Regression Tests",
        "",
    ])

    regression_status = []
    for name, data in known_regressions.items():
        status = data.get("status", "unknown")
        regression_status.append(f"- {name}: {status}")
        if "clusters" in data:
            regression_status.append(f"  - Clusters: {data['clusters']}")
        if "max_cluster_size" in data:
            regression_status.append(f"  - Max size: {data['max_cluster_size']}")
    report_lines.extend(regression_status)

    report_lines.extend([
        "",
        "## Over-Merge Candidates (same-source large clusters)",
        "",
    ])

    for entity_type, data in over_merges.items():
        if data.get("count", 0) > 0:
            report_lines.append(f"### {entity_type.capitalize()} ({data['count']} candidates)")
            report_lines.append("")
            for cand in data.get("candidates", [])[:5]:
                report_lines.append(f"- {cand['canonical_id']}: size={cand['size']}")
            if data.get("count", 0) > 5:
                report_lines.append(f"- ... and {data['count'] - 5} more")
            report_lines.append("")

    report_lines.extend([
        "## Entity Counts",
        "",
        "| Entity | Count |",
        "|--------|-------|",
    ])

    for entity, count in credits.items():
        report_lines.append(f"| {entity} | {count} |")

    report_lines.extend([
        "",
        "## Recommendations",
        "",
        "- If Jonas has >1 cluster: TMDb homonym guard working ✅",
        "- If David has >1 cluster: ANN/MAL homonym guard working ✅",
        "- If Sazae-san is single cluster: anime merging working ✅",
        "- If over-merge candidates > 5: potential issue in entity resolution",
        "",
    ])

    return "\n".join(report_lines)


def main():
    """Run full audit."""
    logger.info("audit_start")

    try:
        con = connect_resolved()
        logger.info("connected_to_resolved_db")

        # Run all audits
        size_dist = audit_cluster_size_distribution(con)
        reason_dist = audit_reason_distribution(con)
        over_merges = audit_over_merge_candidates(con)
        known_regressions = audit_known_over_merge_regression(con)
        credits = audit_credits_preservation(con)

        # Generate report
        report = generate_audit_report(
            size_dist,
            reason_dist,
            over_merges,
            known_regressions,
            credits,
        )

        # Write report
        output_dir = Path("result/audit")
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "cluster_audit_v1.3.md"

        with open(report_path, "w") as f:
            f.write(report)

        logger.info("audit_complete", report_path=str(report_path))
        print(f"\n✅ Audit complete. Report written to: {report_path}\n")
        print(report)

        con.close()

    except Exception as e:
        logger.error("audit_failed", error=str(e), exc_info=True)
        raise


if __name__ == "__main__":
    main()
