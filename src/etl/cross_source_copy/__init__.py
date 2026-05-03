"""Cross-source copy utilities for SILVER layer enrichment.

This package provides COALESCE-copy functions that propagate extra columns
(country_of_origin, synonyms, description, external_links_json, etc.) from
source-authoritative rows to rows of other sources that represent the same
real-world entity.

Current scope:
    anime_extras: copy from anilist rows → non-anilist rows via anilist_id_int mapping

IMPORTANT: This is a forward-port of Resolved-layer functionality.
Phase 2 (5-tier architecture) will introduce a proper Resolved layer that
makes this cross-source copy logic redundant.  When the Resolved layer is
complete, this package should be removed and callers updated to read from
the Resolved layer instead.

See: docs/ARCHITECTURE_5_TIER_PROPOSAL.md
"""
