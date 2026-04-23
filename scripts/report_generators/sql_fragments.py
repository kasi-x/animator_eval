"""Shared SQL fragment builders for report generators."""


def person_display_name_sql(fallback_col: str, alias: str = "name") -> str:
    """Build SQL fragment for person display name with fallback.

    Selects the first non-empty name in order: ja -> zh -> en -> fallback column.

    Args:
        fallback_col: Column name to use as final fallback (usually person_id).
        alias: SQL alias for the result column.

    Returns:
        SQL fragment like "COALESCE(NULLIF(p.name_ja, ''), ...) AS name".
    """
    return f"COALESCE(NULLIF(p.name_ja, ''), NULLIF(p.name_zh, ''), NULLIF(p.name_en, ''), {fallback_col}) AS {alias}"
