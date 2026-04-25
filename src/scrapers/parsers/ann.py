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
    "gender": "gender",
    "nickname": "nickname",
    "nicknames": "nickname",
    "family name": "family_name_ja",
    "family name (in kanji)": "family_name_ja",
    "given name": "given_name_ja",
    "given name (in kanji)": "given_name_ja",
    "height": "height_raw",
    "picture": "image_url",
}


@dataclass
class AnnStaffEntry:
    ann_person_id: int
    name_en: str
    task: str  # raw role string as returned by ANN
    # task_raw: same value as task; preserved so future parsed `task` can diverge without losing raw
    task_raw: str = ""
    # gid: ANN staff group ID from <staff gid="N">. Encodes internal order; NOT safe for sorting.
    gid: int | None = None


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
    titles_alt: str = "{}"
    staff: list[AnnStaffEntry] = field(default_factory=list)
    themes: list[str] = field(default_factory=list)
    plot_summary: str | None = None
    running_time_raw: str | None = None
    objectionable_content: str | None = None
    # JSON arrays: [{"title": ..., "artist": ...}]
    opening_themes_json: str = "[]"
    ending_themes_json: str = "[]"
    insert_songs_json: str = "[]"
    # JSON array: [{"lang": ..., "url": ...}]
    official_websites_json: str = "[]"
    vintage_raw: str | None = None
    image_url: str | None = None
    # Hard Rule: display-only. MUST NOT flow into scoring / edge weights / optimization targets.
    display_rating_votes: int | None = None
    display_rating_weighted: float | None = None
    display_rating_bayesian: float | None = None


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
    # description: deprecated (2000-char truncated). Use description_raw.
    description: str | None = None
    gender: str | None = None
    nickname: str | None = None
    family_name_ja: str | None = None
    given_name_ja: str | None = None
    height_raw: str | None = None
    image_url: str | None = None
    # description_raw: full text without truncation
    description_raw: str | None = None
    # JSON array: [{"ann_anime_id": ..., "anime_title": ..., "task": ...}]
    credits_json: str = "[]"
    # JSON array: [{"lang": ..., "name": ...}]
    alt_names_json: str = "[]"


@dataclass
class AnnCastEntry:
    """One voice actor × character × anime row.

    Hard Rule: cast role is structural credit fact only. Popularity / rankings MUST NOT
    flow into scoring or edge weights.
    cast_role: raw ANN value ("Main" / "Supporting" / "Japanese" etc.)
    """

    ann_anime_id: int
    ann_person_id: int
    voice_actor_name: str
    cast_role: str
    character_name: str
    character_id: int | None = None


@dataclass
class AnnCompanyEntry:
    """One company × anime relationship row.

    task: raw ANN value ("Animation Production" / "Distributor" / "Licensed by" etc.)
    """

    ann_anime_id: int
    company_name: str
    task: str
    company_id: int | None = None


@dataclass
class AnnEpisodeEntry:
    """One episode × language title row.

    episode_num is str because ANN uses "1" / "1a" / "OP" etc.
    lang: raw ("EN" / "JA" / ...)
    """

    ann_anime_id: int
    episode_num: str
    lang: str
    title: str
    aired_date: str | None = None


@dataclass
class AnnReleaseEntry:
    """One DVD/BD release row. region derived from XML element name."""

    ann_anime_id: int
    product_title: str
    release_date: str | None = None
    href: str | None = None
    region: str | None = None


@dataclass
class AnnNewsEntry:
    """One news item mentioning an anime."""

    ann_anime_id: int
    datetime: str
    title: str
    href: str | None = None


@dataclass
class AnnRelatedEntry:
    """One directed relationship between two anime.

    rel: raw ANN value ("sequel" / "prequel" / "spinoff" / "remake" / "summary" etc.)
    direction: "prev" or "next" derived from XML element name.
    """

    ann_anime_id: int
    target_ann_id: int
    rel: str
    direction: str


@dataclass
class AnimeXmlParseResult:
    """Structured result of parse_anime_xml — all tables extracted from one XML response."""

    anime: list[AnnAnimeRecord] = field(default_factory=list)
    cast: list[AnnCastEntry] = field(default_factory=list)
    company: list[AnnCompanyEntry] = field(default_factory=list)
    episodes: list[AnnEpisodeEntry] = field(default_factory=list)
    releases: list[AnnReleaseEntry] = field(default_factory=list)
    news: list[AnnNewsEntry] = field(default_factory=list)
    related: list[AnnRelatedEntry] = field(default_factory=list)


def _normalize_format(ann_type: str) -> str | None:
    """Normalize ANN type attribute to internal format string (case-insensitive)."""
    return _ANN_TYPE_MAP.get(ann_type.strip().lower())


def _parse_theme(text: str) -> dict[str, str]:
    """Parse a theme credit line into {title, artist}.

    Handles: '"Title" by Artist', '"Title"', 'Title by Artist', 'plain title'
    """
    t = text.strip()
    m = re.match(r'^["“「『](.+?)["”」』]\s+by\s+(.+)$', t, re.IGNORECASE)
    if m:
        return {"title": m.group(1).strip(), "artist": m.group(2).strip()}
    m2 = re.match(r'^["“「『](.+?)["”」』]$', t)
    if m2:
        return {"title": m2.group(1).strip(), "artist": ""}
    parts = t.split(" by ", 1)
    if len(parts) == 2:
        return {"title": parts[0].strip(), "artist": parts[1].strip()}
    return {"title": t, "artist": ""}


def _extract_ratings(
    ratings_el: ET.Element,
) -> tuple[int | None, float | None, float | None]:
    """Extract (nb_votes, weighted_score, bayesian_score) from a <ratings> element.

    Hard Rule: callers MUST store results only in display_rating_* fields.
    MUST NOT flow into scoring, edge weights, or optimization targets.
    """
    def _float(s: str | None) -> float | None:
        try:
            return float(s) if s else None
        except ValueError:
            return None

    def _int(s: str | None) -> int | None:
        try:
            return int(s) if s else None
        except ValueError:
            return None

    return (
        _int(ratings_el.get("nb_votes")),
        _float(ratings_el.get("weighted_score")),
        _float(ratings_el.get("bayesian_score")),
    )


def _extract_link_task(link) -> str:  # type: ignore[no-untyped-def]
    """Get the task/role text immediately following an anime link in person HTML."""
    sib = link.next_sibling
    if sib is None:
        return ""
    raw = str(sib).lstrip(" :").strip()
    if raw:
        return raw
    # Task may be in the next <span> element (e.g., localized title spans)
    span = link.find_next_sibling("span")
    if span:
        return span.get_text(strip=True)
    return ""


def _extract_credit_list(soup) -> list[dict]:  # type: ignore[no-untyped-def]
    """Extract anime credit history from a person HTML page's maincontent section."""
    items = []
    mc = soup.find(id="maincontent")
    if not mc:
        return items
    for link in mc.find_all("a", href=re.compile(r"/encyclopedia/anime\.php\?id=(\d+)")):
        m = re.search(r"id=(\d+)", link["href"])
        if not m:
            continue
        items.append({
            "ann_anime_id": int(m.group(1)),
            "anime_title": link.get_text(strip=True),
            "task": _extract_link_task(link),
        })
    return items


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


def parse_anime_xml(root: ET.Element) -> AnimeXmlParseResult:
    """Parse an <ann> root element into a structured multi-table result.

    Raw preservation: all string fields stored as-is from the XML.
    Rating fields stored in display_rating_* only — MUST NOT enter scoring path.
    """
    result = AnimeXmlParseResult()

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

        # Temporary lists — JSON-serialized into rec fields after the loop
        opening_themes: list[dict] = []
        ending_themes: list[dict] = []
        insert_songs: list[dict] = []
        official_websites: list[dict] = []
        alt_titles_by_lang: dict[str, list[str]] = {}

        for info in anime_el.findall("info"):
            itype = info.get("type", "")
            text = (info.text or "").strip()

            if itype == "Alternative title":
                lang = info.get("lang") or ""
                if lang in ("JA", "ja"):
                    rec.title_ja = text  # last JA title wins (kanji > romaji in XML order)
                alt_titles_by_lang.setdefault(lang, []).append(text)
            elif itype == "Vintage":
                rec.vintage_raw = text
                rec.year, rec.start_date, rec.end_date = _parse_vintage(text)
            elif itype == "Number of episodes":
                try:
                    rec.episodes = int(text)
                except ValueError:
                    pass
            elif itype == "Genres":
                rec.genres.append(text)
            elif itype == "Themes":
                rec.themes.append(text)
            elif itype == "Plot Summary":
                rec.plot_summary = text
            elif itype == "Running time":
                rec.running_time_raw = text
            elif itype == "Objectionable content":
                rec.objectionable_content = text
            elif itype == "Opening Theme":
                opening_themes.append(_parse_theme(text))
            elif itype == "Ending Theme":
                ending_themes.append(_parse_theme(text))
            elif itype == "Insert song":
                insert_songs.append(_parse_theme(text))
            elif itype == "Official website":
                official_websites.append({"lang": info.get("lang") or "", "url": text})
            elif itype == "Picture":
                src = info.get("src")
                if src and rec.image_url is None:
                    rec.image_url = src

        rec.opening_themes_json = json.dumps(opening_themes, ensure_ascii=False)
        rec.ending_themes_json = json.dumps(ending_themes, ensure_ascii=False)
        rec.insert_songs_json = json.dumps(insert_songs, ensure_ascii=False)
        rec.official_websites_json = json.dumps(official_websites, ensure_ascii=False)
        rec.titles_alt = json.dumps(alt_titles_by_lang, ensure_ascii=False)

        ratings_el = anime_el.find("ratings")
        if ratings_el is not None:
            votes, w, b = _extract_ratings(ratings_el)
            rec.display_rating_votes = votes
            rec.display_rating_weighted = w
            rec.display_rating_bayesian = b

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
            gid_str = staff_el.get("gid")
            gid = int(gid_str) if gid_str and gid_str.isdigit() else None
            if name and task:
                rec.staff.append(
                    AnnStaffEntry(
                        ann_person_id=pid, name_en=name, task=task, task_raw=task, gid=gid
                    )
                )

        result.anime.append(rec)

        for cast_el in anime_el.findall("cast"):
            cast_role = cast_el.get("lang") or ""
            person_el = cast_el.find("person")
            role_el = cast_el.find("role")
            if person_el is None or role_el is None:
                continue
            pid_str = person_el.get("id")
            if not pid_str:
                continue
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            va_name = (person_el.text or "").strip()
            char_name = (role_el.text or "").strip()
            if va_name and char_name:
                result.cast.append(
                    AnnCastEntry(
                        ann_anime_id=ann_id,
                        ann_person_id=pid,
                        voice_actor_name=va_name,
                        cast_role=cast_role,
                        character_name=char_name,
                    )
                )

        for credit_el in anime_el.findall("credit"):
            task_el = credit_el.find("task")
            company_el = credit_el.find("company")
            if task_el is None or company_el is None:
                continue
            task = (task_el.text or "").strip()
            company_name = (company_el.text or "").strip()
            company_id_str = company_el.get("id")
            company_id = (
                int(company_id_str)
                if company_id_str and company_id_str.isdigit()
                else None
            )
            if company_name and task:
                result.company.append(
                    AnnCompanyEntry(
                        ann_anime_id=ann_id,
                        company_name=company_name,
                        task=task,
                        company_id=company_id,
                    )
                )

        for ep_el in anime_el.findall("episode"):
            ep_num = ep_el.get("num") or ""
            aired = ep_el.findtext("aired")
            for title_el in ep_el.findall("title"):
                lang = title_el.get("lang") or ""
                title_text = (title_el.text or "").strip()
                if title_text:
                    result.episodes.append(
                        AnnEpisodeEntry(
                            ann_anime_id=ann_id,
                            episode_num=ep_num,
                            lang=lang,
                            title=title_text,
                            aired_date=aired,
                        )
                    )

        for rel_el in anime_el.findall("release"):
            product_title = (rel_el.text or "").strip()
            if product_title:
                result.releases.append(
                    AnnReleaseEntry(
                        ann_anime_id=ann_id,
                        product_title=product_title,
                        release_date=rel_el.get("date"),
                        href=rel_el.get("href"),
                    )
                )

        for news_el in anime_el.findall("news"):
            news_title = (news_el.text or "").strip()
            dt = news_el.get("datetime") or ""
            if news_title and dt:
                result.news.append(
                    AnnNewsEntry(
                        ann_anime_id=ann_id,
                        datetime=dt,
                        title=news_title,
                        href=news_el.get("href"),
                    )
                )

        for direction in ("prev", "next"):
            for rel_el in anime_el.findall(f"related-{direction}"):
                target_id_str = rel_el.get("id")
                rel_type = rel_el.get("rel") or ""
                if target_id_str and rel_type:
                    try:
                        result.related.append(
                            AnnRelatedEntry(
                                ann_anime_id=ann_id,
                                target_ann_id=int(target_id_str),
                                rel=rel_type,
                                direction=direction,
                            )
                        )
                    except ValueError:
                        pass

    return result


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
    description_raw: str | None = None
    gender: str | None = None
    nickname: str | None = None
    family_name_ja: str | None = None
    given_name_ja: str | None = None
    height_raw: str | None = None
    image_url: str | None = None

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
            description_raw = val
            description = val[:2000]
        elif fname == "gender":
            gender = val
        elif fname == "nickname":
            nickname = val
        elif fname == "family_name_ja":
            family_name_ja = val
        elif fname == "given_name_ja":
            given_name_ja = val
        elif fname == "height_raw":
            height_raw = val
        elif fname == "image_url":
            image_url = val

    pic_img = soup.find("img", id="pic")
    if pic_img:
        image_url = pic_img.get("src")

    # Collect "also known as" entries (infotype with negative IDs like infotype--4)
    alt_names_list: list[dict] = []
    for div in soup.find_all("div", id=re.compile(r"^infotype--")):
        strong = div.find("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True).rstrip(":").lower().strip()
        if "also known as" in label or "nickname" in label:
            val_el = div.find("span")
            if val_el:
                val = val_el.get_text(" ", strip=True)
            else:
                val = div.get_text(" ", strip=True)
                val = val.replace(strong.get_text(strip=True), "").lstrip(":").strip()
            if val:
                alt_names_list.append({"lang": "?", "name": val})

    credits_list = _extract_credit_list(soup)

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
        description_raw=description_raw,
        gender=gender,
        nickname=nickname,
        family_name_ja=family_name_ja,
        given_name_ja=given_name_ja,
        height_raw=height_raw,
        image_url=image_url,
        credits_json=json.dumps(credits_list, ensure_ascii=False),
        alt_names_json=json.dumps(alt_names_list, ensure_ascii=False),
    )
