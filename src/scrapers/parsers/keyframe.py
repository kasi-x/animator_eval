"""KeyFrame Staff List response parsers."""

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


def parse_credits_from_data(data: dict, slug: str) -> list[dict]:
    """Parse preloadData into a list of credit dicts.

    Returns: [{
        "episode": int (-1 for series-level),
        "role_ja": str,
        "role_en": str,
        "person_id": int|str,
        "name_ja": str,
        "name_en": str,
        "is_studio": bool,
    }, ...]
    """
    credits: list[dict] = []
    menus = data.get("menus", [])

    for menu in menus:
        menu_name = menu.get("name", "")
        episode_num = _extract_episode_num(menu_name)

        credit_sections = menu.get("credits", [])

        for section in credit_sections:
            roles = section.get("roles", [])

            for role_entry in roles:
                role_ja = role_entry.get("original", "")
                role_en = role_entry.get("name", "")
                staff_list = role_entry.get("staff", [])

                for staff in staff_list:
                    if staff.get("isStudio"):
                        continue

                    person_id = staff.get("id")
                    if person_id is None:
                        continue

                    name_ja = staff.get("ja", "")
                    name_en = staff.get("en", "")

                    if not name_ja and not name_en:
                        continue

                    credits.append(
                        {
                            "episode": episode_num,
                            "role_ja": role_ja,
                            "role_en": role_en,
                            "person_id": person_id,
                            "name_ja": name_ja,
                            "name_en": name_en,
                            "is_studio": False,
                        }
                    )

    return credits
