"""Credit restoration ETL: multi-source fuzzy match for pre-1990 anime credits.

Pipeline:
1. ``multi_source_match.find_restoration_candidates()`` — cross-source fuzzy match
2. ``insert_restored.insert_restored_credits()``        — INSERT RESTORED-tier rows

All inserted rows carry ``evidence_source = 'restoration_estimated'`` and
``confidence_tier = 'RESTORED'``.  Existing SILVER credits are never mutated.
"""
