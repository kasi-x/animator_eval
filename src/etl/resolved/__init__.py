"""Resolved layer ETL package (Phase 2a).

Provides full-rebuild builders for result/resolved.duckdb:

    from src.etl.resolved.resolve_anime import build_resolved_anime
    from src.etl.resolved.resolve_persons import build_resolved_persons
    from src.etl.resolved.resolve_studios import build_resolved_studios

Source priority rankings are declared in:
    src.etl.resolved.source_ranking

Representative value selection algorithm:
    src.etl.resolved._select
"""
