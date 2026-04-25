"""KeyFrame Staff List — API response parsers.

Covers:
  parse_roles_master  — /api/data/roles.php
  parse_person_show   — /api/person/show.php?type=person
  parse_preview       — /api/stafflists/preview.php
"""

from __future__ import annotations

import structlog

log = structlog.get_logger()


def parse_roles_master(data: list[dict]) -> list[dict]:
    """Normalize roles.php response.

    Args:
        data: Raw list from roles.php

    Returns:
        list of dicts with keys: role_id, name_en, name_ja, category,
        episode_category, description
    """
    rows = []
    for r in data:
        try:
            rows.append(
                {
                    "role_id": int(r["id"]),
                    "name_en": r.get("name_en"),
                    "name_ja": r.get("name_ja"),
                    "category": r.get("category"),
                    "episode_category": r.get("episode_category"),
                    "description": r.get("description"),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("keyframe_roles_parse_error", err=str(exc)[:120], row=str(r)[:120])
    return rows


def parse_person_show(data: dict) -> dict:
    """Normalize show.php?type=person response.

    Args:
        data: Raw dict from show.php

    Returns:
        dict with keys:
          profile: {id, is_studio, name_ja, name_en, aliases_json, avatar, bio}
          jobs: list[str]
          studios: list[{studio_name, alt_names}]
          credits: list[flat credit dicts]
    """
    staff = data.get("staff") or {}
    studios_raw = data.get("studios") or {}
    credits_raw = data.get("credits") or []

    profile = _parse_person_profile(staff)
    jobs = _parse_person_jobs(data)
    studios = _parse_person_studios(studios_raw)
    credits = _parse_person_credits(credits_raw)

    return {
        "profile": profile,
        "jobs": jobs,
        "studios": studios,
        "credits": credits,
    }


def _parse_person_profile(staff: dict) -> dict:
    """Extract profile fields from staff dict."""
    try:
        person_id = int(staff["id"])
    except (KeyError, TypeError, ValueError) as exc:
        log.warning("keyframe_person_profile_no_id", err=str(exc)[:80])
        person_id = 0

    return {
        "id": person_id,
        "is_studio": bool(staff.get("isStudio")),
        "name_ja": staff.get("ja"),
        "name_en": staff.get("en"),
        "aliases_json": staff.get("aliases") or [],
        "avatar": staff.get("avatar"),
        "bio": staff.get("bio"),
    }


def _parse_person_jobs(data: dict) -> list[str]:
    """Extract jobs list from show.php data."""
    raw = data.get("jobs")
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(j) for j in raw]
    return []


def _parse_person_studios(studios_raw: dict) -> list[dict]:
    """Parse studios dict {name: [alt_names]} from show.php data.

    Returns list[{studio_name, alt_names}].
    """
    out = []
    for name, alts in studios_raw.items():
        alt_names = alts if isinstance(alts, list) else []
        out.append({"studio_name": str(name), "alt_names": alt_names})
    return out


def _parse_person_credits(credits_raw: list[dict]) -> list[dict]:
    """Flatten the nested credits tree into a list of flat credit dicts.

    Structure: credits[].names[].categories[].roles[].credits[]

    Each output row carries anime context (uuid, slug, season, etc.)
    plus the per-episode credit fields (episode, studio, is_nc, comment,
    is_primary_alias).
    """
    rows = []
    for credit_entry in credits_raw:
        anime_ctx = _extract_anime_context(credit_entry)
        for name_obj in credit_entry.get("names") or []:
            name_used_ja = name_obj.get("ja")
            name_used_en = name_obj.get("en")
            for cat in name_obj.get("categories") or []:
                category = cat.get("category")
                for role in cat.get("roles") or []:
                    role_ja = role.get("role_ja")
                    role_en = role.get("role_en")
                    for ep_credit in role.get("credits") or []:
                        rows.append(
                            {
                                **anime_ctx,
                                "name_used_ja": name_used_ja,
                                "name_used_en": name_used_en,
                                "category": category,
                                "role_ja": role_ja,
                                "role_en": role_en,
                                "episode": ep_credit.get("episode"),
                                "studio_at_credit": ep_credit.get("studio"),
                                "is_nc": bool(ep_credit.get("is_nc")),
                                "comment": ep_credit.get("comment"),
                                "is_primary_alias": bool(ep_credit.get("is_primary_alias")),
                            }
                        )
    return rows


def _extract_anime_context(credit_entry: dict) -> dict:
    """Extract anime-level fields from a credit entry."""
    return {
        "anime_uuid": credit_entry.get("uuid"),
        "anime_slug": credit_entry.get("slug"),
        "anime_episodes": credit_entry.get("episodes"),
        "anime_status": credit_entry.get("status"),
        "anime_name_en": credit_entry.get("stafflist_name"),
        "anime_name_ja": credit_entry.get("stafflist_name_ja"),
        "anime_studios_str": credit_entry.get("stafflist_studios"),
        "anime_kv": credit_entry.get("stafflist_kv"),
        "anime_is_adult": bool(credit_entry.get("stafflist_is_adult"))
        if credit_entry.get("stafflist_is_adult") is not None
        else None,
        "anime_season_year": credit_entry.get("seasonYear"),
    }


def parse_preview(data: dict) -> dict:
    """Normalize preview.php response.

    Args:
        data: Raw dict from preview.php

    Returns:
        dict with keys:
          total, total_contributors, total_updated,
          recent, airing, data  (each: list[normalized entry dicts])
    """
    return {
        "total": int(data.get("total") or 0),
        "total_contributors": int(data.get("totalContributors") or 0),
        "total_updated": int(data.get("totalUpdated") or 0),
        "recent": _normalize_preview_entries(data.get("recent") or []),
        "airing": _normalize_preview_entries(data.get("airing") or []),
        "data": _normalize_preview_entries(data.get("data") or []),
    }


def _normalize_preview_entry(entry: dict) -> dict:
    """Normalize a single preview list entry (recent/airing/data)."""
    anilist_id_raw = entry.get("anilistId")
    try:
        anilist_id: int | None = int(anilist_id_raw) if anilist_id_raw is not None else None
    except (TypeError, ValueError):
        anilist_id = None

    last_modified_raw = entry.get("lastModified")
    try:
        last_modified: int | None = int(last_modified_raw) if last_modified_raw is not None else None
    except (TypeError, ValueError):
        last_modified = None

    season_year_raw = entry.get("seasonYear")
    try:
        season_year: int | None = int(season_year_raw) if season_year_raw is not None else None
    except (TypeError, ValueError):
        season_year = None

    studios_raw = entry.get("studios")
    if isinstance(studios_raw, list):
        studios_str: list[str] = [str(s) for s in studios_raw]
    else:
        studios_str = []

    contributors_raw = entry.get("contributors")
    if isinstance(contributors_raw, list):
        contributors: list[dict] = contributors_raw
    else:
        contributors = []

    return {
        "uuid": entry.get("uuid") or "",
        "slug": entry.get("slug"),
        "title": entry.get("title"),
        "title_native": entry.get("native"),
        "status": entry.get("status"),
        "last_modified": last_modified,
        "anilist_id": anilist_id,
        "season": entry.get("season"),
        "season_year": season_year,
        "studios_str": studios_str,
        "contributors_json": contributors,
    }


def _normalize_preview_entries(entries: list[dict]) -> list[dict]:
    """Normalize a list of preview entries."""
    return [_normalize_preview_entry(e) for e in entries]
