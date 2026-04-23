"""Anime News Network XML/HTML parsers and data classes."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime

import structlog
from bs4 import BeautifulSoup

from src.utils.name_utils import assign_native_name_fields

log = structlog.get_logger()

# Maps ANN type attribute (case-insensitive) → internal format string.
# Keys are pre-normalized to lowercase; callers must call .lower() before lookup.
_ANN_TYPE_MAP: dict[str, str] = {
    "tv": "TV",
    "tv special": "SPECIAL",
    "movie": "MOVIE",
    "ova": "OVA",
    "oav": "OVA",  # ANN uses "OAV" more often than "OVA"
    "ona": "ONA",
    "web": "ONA",
    "special": "SPECIAL",
    "music video": "MUSIC_VIDEO",
}

# Maps <strong> label text (lowercased, colon stripped) → AnnPersonDetail field name
_HTML_LABEL_MAP: dict[str, str] = {
    "birthdate": "date_of_birth",
    "birth date": "date_of_birth",
    "born": "date_of_birth",
    "date of birth": "date_of_birth",
    "hometown": "hometown",
    "blood type": "blood_type",
    "website": "website",
    "home page": "website",
    "biography": "description",
}


@dataclass
class AnnStaffEntry:
    ann_person_id: int
    name_en: str
    task: str  # raw role string as returned by ANN


@dataclass
class AnnAnimeRecord:
    ann_id: int
    title_en: str
    title_ja: str = ""
    year: int | None = None
    episodes: int | None = None
    format: str | None = None
    genres: list[str] = field(default_factory=list)
    start_date: str | None = None
    end_date: str | None = None
    staff: list[AnnStaffEntry] = field(default_factory=list)


@dataclass
class AnnPersonDetail:
    ann_id: int
    name_en: str
    name_ja: str = ""
    name_ko: str = ""
    name_zh: str = ""
    names_alt: str = "{}"
    date_of_birth: str | None = None  # YYYY-MM-DD
    hometown: str | None = None
    blood_type: str | None = None
    website: str | None = None
    description: str | None = None


def _normalize_format(ann_type: str) -> str | None:
    """Normalize ANN type attribute to internal format string (case-insensitive)."""
    return _ANN_TYPE_MAP.get(ann_type.strip().lower())


def _parse_vintage(vintage: str) -> tuple[int | None, str | None, str | None]:
    """Parse a vintage string into (year, start_date, end_date).

    Examples:
      "Apr 3, 1998 to Apr 24, 1999" → (1998, "1998-04-03", "1999-04-24")
      "2001" → (2001, None, None)
    """
    vintage = vintage.strip()
    year: int | None = None
    start_date: str | None = None
    end_date: str | None = None

    date_re = re.compile(
        r"(\w{3})\s+(\d{1,2}),\s+(\d{4})"
        r"(?:\s+to\s+(\w{3})\s+(\d{1,2}),\s+(\d{4}))?"
    )
    m = date_re.search(vintage)
    if m:
        months = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
            "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
            "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        sm, sd, sy = m.group(1), int(m.group(2)), int(m.group(3))
        year = sy
        start_date = f"{sy}-{months.get(sm, 1):02d}-{sd:02d}"
        if m.group(4):
            em, ed, ey = m.group(4), int(m.group(5)), int(m.group(6))
            end_date = f"{ey}-{months.get(em, 1):02d}-{ed:02d}"
        return year, start_date, end_date

    m2 = re.search(r"\b(\d{4})\b", vintage)
    if m2:
        year = int(m2.group(1))

    return year, start_date, end_date


def parse_anime_xml(root: ET.Element) -> list[AnnAnimeRecord]:
    """Parse an <ann> root element and return a list of AnnAnimeRecord."""
    records: list[AnnAnimeRecord] = []

    for anime_el in root.findall("anime"):
        ann_id_str = anime_el.get("id")
        if not ann_id_str:
            continue
        try:
            ann_id = int(ann_id_str)
        except ValueError:
            continue

        title_en = anime_el.get("name", "")
        fmt = _normalize_format(anime_el.get("type", ""))

        rec = AnnAnimeRecord(ann_id=ann_id, title_en=title_en, format=fmt)

        for info in anime_el.findall("info"):
            itype = info.get("type", "")
            text = (info.text or "").strip()

            if itype == "Alternative title" and info.get("lang") in ("JA", "ja"):
                rec.title_ja = text
            elif itype == "Vintage":
                rec.year, rec.start_date, rec.end_date = _parse_vintage(text)
            elif itype == "Number of episodes":
                try:
                    rec.episodes = int(text)
                except ValueError:
                    pass
            elif itype == "Genres":
                rec.genres = [g.strip() for g in text.split(";") if g.strip()]

        for staff_el in anime_el.findall("staff"):
            person_el = staff_el.find("person")
            task_el = staff_el.find("task")
            if person_el is None or task_el is None:
                continue
            pid_str = person_el.get("id")
            if not pid_str:
                continue
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            name = (person_el.text or "").strip()
            task = (task_el.text or "").strip()
            if name and task:
                rec.staff.append(
                    AnnStaffEntry(ann_person_id=pid, name_en=name, task=task)
                )

        records.append(rec)

    return records


def parse_person_xml(root: ET.Element) -> list[AnnPersonDetail]:
    """Parse an ANN XML API response (<ann>) and return a list of AnnPersonDetail."""
    results: list[AnnPersonDetail] = []
    for person_el in root.findall("person"):
        ann_id_str = person_el.get("id")
        if not ann_id_str:
            continue
        try:
            ann_id = int(ann_id_str)
        except ValueError:
            continue

        name_en = person_el.get("name", "")
        if not name_en:
            continue

        name_ja = ""
        date_of_birth: str | None = None
        hometown: str | None = None
        blood_type: str | None = None
        website: str | None = None
        description: str | None = None

        for info in person_el.findall("info"):
            itype = info.get("type", "")
            text = (info.text or "").strip()
            if itype == "Japanese name":
                name_ja = text
            elif itype == "birth date" and text:
                if re.match(r"\d{4}-\d{2}-\d{2}", text):
                    date_of_birth = text
                else:
                    try:
                        dt = datetime.strptime(text, "%b %d, %Y")
                        date_of_birth = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        yr_m = re.search(r"\d{4}", text)
                        if yr_m:
                            date_of_birth = yr_m.group()
            elif itype == "hometown" and text:
                hometown = text
            elif itype == "blood type" and text:
                blood_type = text.upper()
            elif itype == "website" and text:
                website = text
            elif itype == "biography" and text:
                description = text[:2000]

        results.append(
            AnnPersonDetail(
                ann_id=ann_id,
                name_en=name_en,
                name_ja=name_ja,
                date_of_birth=date_of_birth,
                hometown=hometown,
                blood_type=blood_type,
                website=website,
                description=description,
            )
        )
    return results


def _parse_dob_html(text: str) -> str | None:
    """Normalise a date-of-birth string to YYYY-MM-DD or YYYY.

    Supported formats:
      "1941-01-05"       → "1941-01-05"  (ISO, returned as-is)
      "Jan 5, 1941"      → "1941-01-05"  (abbreviated month name)
      "January 5, 1941"  → "1941-01-05"  (full month name)
      "1941"             → "1941"         (year only)
    """
    text = text.strip()
    if re.match(r"\d{4}-\d{2}-\d{2}", text):
        return text
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"\b(\d{4})\b", text)
    return m.group(1) if m else None


def parse_person_html(html: str, ann_id: int) -> AnnPersonDetail | None:
    """Extract AnnPersonDetail from an ANN person page (people.php?id=N).

    Page structure (as of April 2026):
      <div id="page-title">
        <h1 id="page_header">English name</h1>
        Japanese name text node (e.g. "山口 祐司")
      </div>
      <div id="infotype-N" class="encyc-info-type ...">
        <strong>Label:</strong> <span>value</span>
      </div>
    """
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    title_text = title_tag.get_text(" ", strip=True) if title_tag else ""
    if "just a moment" in title_text.lower():
        log.debug("ann_person_html_cf_block", ann_id=ann_id)
        return None

    h1 = soup.find("h1", id="page_header")
    if h1:
        name_en = h1.get_text(" ", strip=True)
    elif " - " in title_text:
        name_en = title_text.split(" - ")[0].strip()
    else:
        name_en = title_text.strip()
    if not name_en:
        return None

    name_ja = ""
    if h1:
        for sib in h1.next_siblings:
            raw = str(sib).strip()
            if raw and re.search(r"[　-鿿＀-￯]", raw):
                name_ja = raw
                break

    if not name_ja:
        family, given = "", ""
        for div in soup.find_all("div", id=re.compile(r"^infotype-(?:12|13)$")):
            strong = div.find("strong")
            span = div.find("span")
            if strong and span:
                label = strong.get_text(strip=True).lower()
                val = span.get_text(strip=True)
                if "family" in label:
                    family = val
                elif "given" in label:
                    given = val
        if family or given:
            name_ja = f"{family} {given}".strip()

    fields: dict[str, str] = {}
    for div in soup.find_all("div", id=re.compile(r"^infotype-")):
        strong = div.find("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True).rstrip(":").lower().strip()
        if label not in _HTML_LABEL_MAP:
            continue
        value_el = div.find("span") or div.find("div", class_="tab")
        if value_el:
            val = value_el.get_text(" ", strip=True)
        else:
            val = div.get_text(" ", strip=True)
            val = val.replace(strong.get_text(strip=True), "").lstrip(":").strip()
        if val and label not in fields:
            fields[label] = val

    date_of_birth: str | None = None
    hometown: str | None = None
    blood_type: str | None = None
    website: str | None = None
    description: str | None = None

    for label, val in fields.items():
        fname = _HTML_LABEL_MAP.get(label)
        if fname == "date_of_birth":
            date_of_birth = _parse_dob_html(val)
        elif fname == "hometown":
            hometown = val
        elif fname == "blood_type":
            blood_type = val.upper()
        elif fname == "website":
            website = val
        elif fname == "description":
            description = val[:2000]

    # Route name_ja via script detection (ANN has no native script info, so fallback to empty nationality)
    name_ja_routed, name_ko_routed, name_zh_routed, names_alt_dict = assign_native_name_fields(name_ja, [])
    names_alt_json = json.dumps(names_alt_dict, ensure_ascii=False) if names_alt_dict else "{}"

    return AnnPersonDetail(
        ann_id=ann_id,
        name_en=name_en,
        name_ja=name_ja_routed,
        name_ko=name_ko_routed,
        name_zh=name_zh_routed,
        names_alt=names_alt_json,
        date_of_birth=date_of_birth,
        hometown=hometown,
        blood_type=blood_type,
        website=website,
        description=description,
    )
