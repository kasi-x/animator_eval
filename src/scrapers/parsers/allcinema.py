"""allcinema.net HTML parsers and data classes."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

import structlog

log = structlog.get_logger()


@dataclass
class AllcinemaCredit:
    allcinema_person_id: int
    name_ja: str
    name_en: str
    job_name: str  # raw job name from source
    job_id: int


@dataclass
class AllcinemaAnimeRecord:
    cinema_id: int
    title_ja: str
    title_en: str = ""
    year: int | None = None
    start_date: str | None = None
    synopsis: str = ""
    staff: list[AllcinemaCredit] = field(default_factory=list)
    cast: list[AllcinemaCredit] = field(default_factory=list)


@dataclass
class AllcinemaPersonRecord:
    allcinema_id: int
    name_ja: str
    yomigana: str = ""  # reading (hiragana)
    name_en: str = ""
    name_ko: str = ""
    name_zh: str = ""
    names_alt: str = "{}"  # JSON dict for non-JA/EN/KO/ZH scripts (not typically populated from allcinema)
    hometown: str = ""


def _parse_cinema_html(html: str, cinema_id: int) -> AllcinemaAnimeRecord | None:
    """Parse a cinema page HTML and return an AllcinemaAnimeRecord, or None for non-anime."""
    m_ps = re.search(r"var PageSetting = function\(\)\{(.*?)\};", html, re.DOTALL)
    if not m_ps:
        return None

    ps_text = m_ps.group(1)
    anime_flag = ""
    m_af = re.search(r'this\.animeFlag\s*=\s*"([^"]*)"', ps_text)
    if m_af:
        anime_flag = m_af.group(1)
    if anime_flag != "アニメ":
        return None

    synopsis = ""
    m_sy = re.search(r'this\.synopsis\s*=\s*"((?:[^"\\]|\\.)*)"', ps_text)
    if m_sy:
        synopsis = m_sy.group(1).replace('\\"', '"').replace("\\n", "\n")

    title_ja = ""
    year: int | None = None
    m_title = re.search(r"<title>(.*?)</title>", html)
    if m_title:
        raw_title = m_title.group(1)
        m_year = re.search(r"\((\d{4})\)", raw_title)
        if m_year:
            year = int(m_year.group(1))
        clean = re.sub(r"\s*\(\d{4}\)\s*-\s*allcinema\s*$", "", raw_title)
        clean = re.sub(r"^(映画アニメ|テレビアニメ|映画)\s*", "", clean).strip()
        title_ja = clean

    m_cj = re.search(
        r"CreditJson = function\(\)\{\s*this\.data = (\{.*?\});\s*\}",
        html,
        re.DOTALL,
    )
    staff_list: list[AllcinemaCredit] = []
    cast_list: list[AllcinemaCredit] = []

    if m_cj:
        try:
            cdata = json.loads(m_cj.group(1))
            for section_key, target_list in [
                ("staff", staff_list),
                ("cast", cast_list),
            ]:
                for job_entry in cdata.get(section_key, {}).get("jobs", []):
                    job = job_entry.get("job", {})
                    job_name = job.get("jobname", "")
                    job_id = job.get("jobid", 0)
                    for p_entry in job_entry.get("persons", []):
                        p = p_entry.get("person", {})
                        pid = p.get("personid")
                        if not pid:
                            continue
                        pname = p.get("personnamemain", {})
                        name_ja = pname.get("personname", "")
                        name_en = pname.get("englishname", "")
                        target_list.append(
                            AllcinemaCredit(
                                allcinema_person_id=pid,
                                name_ja=name_ja,
                                name_en=name_en,
                                job_name=job_name,
                                job_id=job_id,
                            )
                        )
        except (json.JSONDecodeError, KeyError) as exc:
            log.warning(
                "allcinema_credit_parse_error", cinema_id=cinema_id, error=str(exc)
            )

    return AllcinemaAnimeRecord(
        cinema_id=cinema_id,
        title_ja=title_ja,
        year=year,
        synopsis=synopsis,
        staff=staff_list,
        cast=cast_list,
    )


def _parse_person_html(html: str, person_id: int) -> AllcinemaPersonRecord:
    """Parse a person page HTML and return an AllcinemaPersonRecord."""
    name_ja = ""
    m_h1 = re.search(r'<div\s+class\s*=\s*"person-area-name"\s*>(.*?)</div>', html)
    if m_h1:
        name_ja = re.sub(r"<[^>]+>", "", m_h1.group(1)).strip()
    if not name_ja:
        m_title = re.search(r"<title>(.*?)\s*-\s*allcinema</title>", html)
        if m_title:
            name_ja = m_title.group(1).strip()

    yomigana = ""
    m_kw = re.search(r'name="keywords"\s+content="([^"]*)"', html)
    if m_kw:
        parts = [p.strip() for p in m_kw.group(1).split(",")]
        if len(parts) >= 2:
            yomigana = parts[1]

    hometown = ""
    m_profile = re.search(r'<div\s+class\s*=\s*"profile-content"\s*>(.*?)</div>', html, re.DOTALL)
    if m_profile:
        profile_text = m_profile.group(1)
        m_hometown = re.search(r'<th>出身地</th>\s*<td>(.*?)</td>', profile_text)
        if m_hometown:
            hometown = re.sub(r"<[^>]+>", "", m_hometown.group(1)).strip()

    return AllcinemaPersonRecord(
        allcinema_id=person_id,
        name_ja=name_ja,
        yomigana=yomigana,
        name_ko="",
        name_zh="",
        names_alt="{}",
        hometown=hometown,
    )
