"""Media Arts Database (MADB) JSON-LD parsers."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path


def parse_contributor_text(text: str) -> list[tuple[str, str]]:
    """Parse MADB contributor text into (role_ja, name_ja) pairs.

    Input:  "[脚本]仲倉重郎 ／ [演出]須永 司 ／ [作画監督]数井浩子"
    Output: [("脚本", "仲倉重郎"), ("演出", "須永 司"), ("作画監督", "数井浩子")]

    Handles variations:
    - Fullwidth brackets: ［脚本］ -> [脚本]
    - No brackets: "仲倉重郎" -> ("other", "仲倉重郎")
    - Multiple roles: "[脚本・演出]名前" -> [("脚本", "名前"), ("演出", "名前")]
    - Fullwidth slash: ／ (JSON-LD dump format)
    """
    if not text or not text.strip():
        return []

    text = unicodedata.normalize("NFKC", text)

    results: list[tuple[str, str]] = []
    segments = re.split(r"\s+[/／]\s+", text)

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        match = re.match(r"\[([^\]]+)\]\s*(.+)", segment)
        if match:
            role_text = match.group(1).strip()
            name = match.group(2).strip()
            if not name:
                continue
            roles = re.split(r"[・/]", role_text)
            for role in roles:
                role = role.strip()
                if role:
                    results.append((role, name))
        else:
            name = segment.strip()
            if name:
                results.append(("other", name))

    return results


def make_madb_person_id(name_ja: str) -> str:
    """Generate a deterministic ID from the SHA256 hash of a normalized name.

    Format: "madb:p_{hash12}"
    Same name -> always same ID (idempotent).
    """
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"madb:p_{hash_hex}"


def normalize_title(title: str) -> str:
    """Normalize a title for comparison."""
    if not title:
        return ""
    title = unicodedata.normalize("NFKC", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _extract_name_from_schema(name_field: str | list | dict) -> str:
    """Extract a title string from the schema:name field.

    JSON-LD may contain:
    - String: "タイトル"
    - List: ["タイトル", {"@value": "カタカナ", "@language": "ja-hrkt"}]
    - Dict: {"@value": "タイトル", "@language": "ja"}
    """
    if isinstance(name_field, str):
        return name_field
    if isinstance(name_field, list):
        for item in name_field:
            if isinstance(item, str):
                return item
            if isinstance(item, dict) and "@value" in item:
                lang = item.get("@language", "")
                if lang not in ("ja-hrkt",):
                    return item["@value"]
        for item in name_field:
            if isinstance(item, str):
                return item
            if isinstance(item, dict) and "@value" in item:
                return item["@value"]
        return ""
    if isinstance(name_field, dict) and "@value" in name_field:
        return name_field["@value"]
    return str(name_field) if name_field else ""


def _extract_year(item: dict) -> int | None:
    """Extract year from datePublished or startDate."""
    for field in ("schema:datePublished", "schema:startDate"):
        val = item.get(field, "")
        if val:
            if isinstance(val, dict):
                val = val.get("@value", "")
            if isinstance(val, str) and len(val) >= 4:
                try:
                    return int(val[:4])
                except ValueError:
                    pass
    return None


def _extract_studios(item: dict) -> list[str]:
    """Extract studio names from schema:productionCompany."""
    raw = item.get("schema:productionCompany", "")
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = raw.get("@value", "")
    if not isinstance(raw, str):
        return []

    studios = []
    text = unicodedata.normalize("NFKC", raw)
    segments = re.split(r"\s+[/／]\s+", text)
    for seg in segments:
        seg = seg.strip()
        match = re.match(r"\[([^\]]*)\]\s*(.+)", seg)
        if match:
            name = match.group(2).strip()
            if name:
                studios.append(name)
        elif seg:
            studios.append(seg)
    return studios


def parse_jsonld_dump(json_path: Path, format_code: str = "") -> list[dict]:
    """Parse a JSON-LD file and extract anime information.

    Returns: [{
        "id": "C10001",
        "title": "ギャラクシー エンジェル",
        "year": 2001,
        "format": "TV",
        "contributors": [(role, name), ...],
        "studios": ["マッドハウス"],
    }]
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    graph = data.get("@graph", [])
    results = []

    for item in graph:
        identifier = item.get("schema:identifier", "")
        if not identifier:
            continue

        name_field = item.get("schema:name", "")
        title = _extract_name_from_schema(name_field)
        if not title:
            continue

        year = _extract_year(item)

        contributors: list[tuple[str, str]] = []
        for field in ("schema:contributor", "schema:creator", "ma:originalWorkCreator"):
            raw = item.get(field, "")
            if not raw:
                continue
            if isinstance(raw, dict):
                raw = raw.get("@value", "")
            if isinstance(raw, str) and raw.strip():
                parsed = parse_contributor_text(raw)
                contributors.extend(parsed)

        studios = _extract_studios(item)

        results.append(
            {
                "id": identifier,
                "title": title,
                "year": year,
                "format": format_code,
                "contributors": contributors,
                "studios": studios,
            }
        )

    return results
