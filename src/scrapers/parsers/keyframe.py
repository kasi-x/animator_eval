"""KeyFrame Staff List — HTML preloadData parsers."""

from __future__ import annotations

import json
import re

import structlog

log = structlog.get_logger()


def extract_preload_data(html: str) -> dict | None:
    """Extract preloadData JSON object from page HTML.

    The site embeds data as: `preloadData = {...};` in a <script> tag.
    """
    match = re.search(
        r"preloadData\s*=\s*(\{.*?\})\s*;?\s*(?:</script>|$)", html, re.DOTALL
    )
    if not match:
        return None

    json_str = match.group(1)
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        cleaned = re.sub(r",\s*([}\]])", r"\1", json_str)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            log.warning("keyframe_json_parse_error", error=str(e)[:200])
            return None


def _extract_episode_num(menu_name: str) -> int:
    """Extract episode number from menu name like '#01', '#1234', 'Episode 5'.

    Returns -1 for non-episode menus (Overview, OP, ED, etc.).
    """
    match = re.match(r"#(\d+)", menu_name)
    if match:
        return int(match.group(1))
    match = re.search(r"(?:Episode|Ep\.?)\s*(\d+)", menu_name, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return -1


def _extract_episode_title(menu_name: str) -> str | None:
    """Extract episode title from menu name like '#01「夜明けの冒険！」'.

    Returns the text inside 「」or 『』brackets, or None if absent.
    """
    m = re.search(r"[「『](.+?)[」』]", menu_name)
    return m.group(1) if m else None


def _parse_int(v: object) -> int | None:
    """Safely parse an int from an arbitrary value."""
    if v is None:
        return None
    try:
        return int(v)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def parse_anime_meta(data: dict, slug: str) -> dict:
    """Extract anime-level metadata from preloadData top-level + anilist nest.

    Args:
        data: preloadData dict parsed from page HTML.
        slug: URL slug used as the canonical keyframe anime identifier.

    Returns:
        Flat dict with all anime-level fields (A-group + delimiter settings).
    """
    a = data.get("anilist") or {}
    title = a.get("title") or {}
    settings = data.get("settings") or {}
    return {
        "kf_uuid": data.get("uuid"),
        "kf_saving_id": data.get("savingId"),
        "kf_author": data.get("author"),
        "kf_status": data.get("status"),
        "kf_comment": data.get("comment"),
        "title_ja": title.get("native"),
        "title_en": title.get("english") or data.get("title"),
        "title_romaji": title.get("romaji"),
        "synonyms": a.get("synonyms") or [],
        "format": a.get("format"),
        "episodes": a.get("episodes"),
        "season": a.get("season"),
        "season_year": a.get("seasonYear"),
        "start_date": a.get("startDate") or {},
        "end_date": a.get("endDate") or {},
        "cover_image_url": (a.get("coverImage") or {}).get("extraLarge"),
        "is_adult": a.get("isAdult"),
        "anilist_status": a.get("status"),
        "anilist_id": a.get("id") or _parse_int(data.get("anilistId")),
        "slug": slug,
        "delimiters": settings.get("delimiters"),
        "episode_delimiters": settings.get("episodeDelimiters"),
        "role_delimiters": settings.get("roleDelimiters"),
        "staff_delimiters": settings.get("staffDelimiters"),
    }


def parse_anime_studios(data: dict) -> list[dict]:
    """Extract studio list from anilist.studios.edges[].

    Args:
        data: preloadData dict.

    Returns:
        List of dicts with studio_name and is_main flag.
    """
    edges = ((data.get("anilist") or {}).get("studios") or {}).get("edges") or []
    return [
        {"studio_name": e["node"]["name"], "is_main": bool(e.get("isMain"))}
        for e in edges
        if (e.get("node") or {}).get("name")
    ]


def parse_settings_categories(data: dict) -> list[dict]:
    """Extract settings.categories[] role classification list.

    Args:
        data: preloadData dict.

    Returns:
        List of dicts preserving insertion order (category_order = list index).
    """
    cats = (data.get("settings") or {}).get("categories") or []
    return [
        {"category_name": c.get("name"), "category_order": i}
        for i, c in enumerate(cats)
        if c.get("name")
    ]


def parse_credits_from_data(data: dict, slug: str) -> list[dict]:
    """Parse preloadData into a list of credit dicts.

    Extended fields vs. original:
    - section_name, episode_title, menu_note from menu/section context
    - studio_ja, studio_en, studio_id, studio_is_studio from staff.studio
    - is_studio_role preserved (was skipped before; now retained)

    isStudio=true entries are kept (changed from skip to keep) so that
    studio roles appear in the credits table and the studio_master can be
    built from the same pass.

    Args:
        data: preloadData dict.
        slug: URL slug (unused here, kept for API symmetry).

    Returns:
        List of credit dicts. One dict per (menu × section × role × staff).
    """
    credits: list[dict] = []

    for menu in data.get("menus", []):
        menu_name = menu.get("name", "")
        episode_num = _extract_episode_num(menu_name)
        episode_title = _extract_episode_title(menu_name)
        menu_note = menu.get("note")

        for section in menu.get("credits", []):
            section_name = section.get("name")

            for role_entry in section.get("roles", []):
                role_ja = role_entry.get("original", "")
                role_en = role_entry.get("name", "")

                for staff in role_entry.get("staff", []):
                    is_studio_role = bool(staff.get("isStudio"))
                    studio_obj = staff.get("studio") or {}
                    person_id = staff.get("id")
                    name_ja = staff.get("ja", "")
                    name_en = staff.get("en", "")

                    if person_id is None and not (name_ja or name_en):
                        continue

                    credits.append(
                        {
                            "episode": episode_num,
                            "episode_title": episode_title,
                            "menu_note": menu_note,
                            "section_name": section_name,
                            "role_ja": role_ja,
                            "role_en": role_en,
                            "person_id": person_id,
                            "name_ja": name_ja,
                            "name_en": name_en,
                            "is_studio_role": is_studio_role,
                            "studio_ja": studio_obj.get("ja"),
                            "studio_en": studio_obj.get("en"),
                            "studio_id": studio_obj.get("id"),
                            "studio_is_studio": (
                                bool(studio_obj.get("isStudio"))
                                if studio_obj
                                else None
                            ),
                        }
                    )

    return credits


def collect_studio_master(credits: list[dict]) -> list[dict]:
    """Derive studio master records from credit rows where is_studio_role=True.

    Studio master deduplicates by studio_id so that one row per studio is
    produced regardless of how many credit rows reference it.

    Args:
        credits: list as returned by parse_credits_from_data().

    Returns:
        List of dicts with studio_id, name_ja, name_en. Sorted by studio_id.
    """
    seen: dict[int, dict] = {}
    for row in credits:
        if not row.get("is_studio_role"):
            continue
        sid_raw = row.get("person_id")
        if sid_raw is None:
            continue
        try:
            sid = int(sid_raw)
        except (TypeError, ValueError):
            continue
        seen.setdefault(
            sid,
            {
                "studio_id": sid,
                "name_ja": row.get("name_ja") or "",
                "name_en": row.get("name_en") or "",
            },
        )
    return sorted(seen.values(), key=lambda x: x["studio_id"])
