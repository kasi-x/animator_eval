"""ETL layer: bronze Parquet → silver DuckDB integration.

Entry point: ``src.etl.integrate_duckdb``
"""
from src.etl.integrate_duckdb import integrate

__all__ = ["integrate"]
