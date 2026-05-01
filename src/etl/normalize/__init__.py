"""ETL normalization utilities.

Provides text normalization helpers for SILVER enrichment.
The canonical_name module implements NFKC + 旧字体→新字体 name normalization
for the persons.canonical_name_ja column (Card 21/03).
"""
from src.etl.normalize.canonical_name import canonical_name_ja

__all__ = ["canonical_name_ja"]
