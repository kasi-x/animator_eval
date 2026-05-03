"""SILVER loader modules — one per source.

Each loader exposes ``integrate(conn, bronze_root)`` that adds rows to
existing SILVER tables. Composed by integrate_duckdb.py at the top level
(integration is out of scope for Card 14).
"""
