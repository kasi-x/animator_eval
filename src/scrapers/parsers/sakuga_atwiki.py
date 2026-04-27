"""作画@wiki page classifier, link extractor, and person parser."""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Literal

import structlog
from bs4 import BeautifulSoup, Tag

from src.runtime.models import ParsedSakugaCredit, ParsedSakugaPerson, ParsedSakugaWork, ParsedSakugaWorkStaff

log = structlog.get_logger()

PageKind = Literal["person", "work", "index", "meta", "unknown"]

# ---------------------------------------------------------------------------
# Page classification
# ---------------------------------------------------------------------------

_PAGE_LINK_RE = re.compile(
    r'href="(?:(?:https:)?//[^/]*\.atwiki\.jp)?/sakuga/pages/(\d+)\.html"'
)
_META_TITLE_KW = re.compile(r"メニュー|サイトマップ")
_INDEX_TITLE_KW = re.compile(r"一覧|索引")
_INDEX_H_KW = re.compile(
    r"[あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわ]行"
)
_PERSON_H_KW = re.compile(r"フィルモグラフィ|参加作品|代表作|出演作")
_WORK_H_KW = re.compile(r"スタッフ|キャスト|話数|制作スタッフ|エピソード")
_BULLET_CREDIT_RE = re.compile(r"■.{1,50}(?:TV|OVA|OAD|劇場|映画|TVSP|配信)")
_BULLET_STAFF_RE = re.compile(r"■スタッフ")
_EP_BLOCK_RE = re.compile(r"(?:^|\n)\s*\d+話")


def classify_page_kind(title: str, html: str) -> PageKind:
    if _META_TITLE_KW.search(title):
        return "meta"

    soup = BeautifulSoup(html, "lxml")
    headings = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2", "h3", "h4"])]

    if _INDEX_TITLE_KW.search(title):
        return "index"
    if any(_INDEX_H_KW.search(h) for h in headings):
        return "index"

    has_person = any(_PERSON_H_KW.search(h) for h in headings)
    has_work = any(_WORK_H_KW.search(h) for h in headings)

    if has_person and not has_work:
        return "person"
    if has_work:
        return "work"

    wikibody = soup.find("div", id="wikibody") or soup.find("body")
    if wikibody:
        body_text = wikibody.get_text(separator="\n")
        body_nfkc = unicodedata.normalize("NFKC", body_text)
        # ■スタッフ or N話 episode blocks → work page
        if _BULLET_STAFF_RE.search(body_text) or _EP_BLOCK_RE.search(body_text):
            return "work"
        # role:name patterns (colon or slash separator) → work page
        if _WORK_COLON_ROLE_RE.search(body_nfkc):
            return "work"
        # ■作品名（TV/...）style credit bullets → person page (check before standalone labels)
        if _BULLET_CREDIT_RE.search(body_text):
            return "person"
        # Standalone / bullet / dot / bracket role formats → work page
        nfkc_lines = [l.strip() for l in body_nfkc.splitlines() if l.strip()]
        if any(_GENGA_LABEL_RE.match(l) for l in nfkc_lines):
            return "work"
        if any(l.startswith("■") and _ROLE_INLINE_RE.search(l[1:].strip()) for l in nfkc_lines):
            return "work"
        if any(_WORK_DOT_ROLE_RE.match(l) for l in nfkc_lines):
            return "work"
        if any(_BRACKET_ROLE_RE.match(l) and _ROLE_INLINE_RE.search(_BRACKET_ROLE_RE.match(l).group(1)) for l in nfkc_lines):
            return "work"

    return "unknown"


def extract_page_ids(html: str) -> list[int]:
    """Return deduplicated page IDs in order of appearance."""
    seen: dict[int, None] = {}
    for m in _PAGE_LINK_RE.finditer(html):
        seen[int(m.group(1))] = None
    return list(seen)


# ---------------------------------------------------------------------------
# Person page parser
# ---------------------------------------------------------------------------

_TITLE_SITE_SUFFIX = re.compile(r"\s*[-–]\s*作画@wiki.*$")
_ALIAS_LABEL_RE = re.compile(r"別名[：:]\s*|旧名[：:]\s*|英字[：:]\s*|読み[：:]\s*|英名[：:]\s*")
_YEAR_RE = re.compile(r"((?:19|20)\d{2})")
_FORMAT_RE = re.compile(r"劇場(?:版|アニメ)?|映画|Movie|OVA|OAD|TVSP|TV特番|TV Special|\bTV\b|テレビ|配信|Web配信|ネット配信", re.IGNORECASE)
_EP_SINGLE_RE = re.compile(r"(?:第\s*)?(\d+)\s*話|#(\d+)|EP\.?\s*(\d+)|第(\d+)回")
_EP_RANGE_RE = re.compile(r"(?:第\s*)?(\d+)\s*[〜~\-ー–]\s*(?:第\s*)?(\d+)\s*話?")
_ROLE_INLINE_RE = re.compile(
    r"(?:原画|第?二?原画|作画監督(?:補佐)?|総作画監督|動画(?:検査|チェック)?|"
    r"絵コンテ|コンテ|演出(?:助手)?|監督|副監督|助監督|"
    r"キャラクターデザイン|キャラデザ|メカデザイン?|"
    r"美術監督?|背景|色彩設計?|撮影監督?|音楽|音響監督?|"
    r"プロデューサー|制作進行?|アニメーション制作|"
    r"レイアウト|仕上げ?|特殊効果|CG(?:ディレクター|監督|I監督)?|"
    r"エフェクト|3DCG|制作)"
)

# Subjective evaluation words to strip (not store)
_SUBJECTIVE_RE = re.compile(r"神作画|作画崩壊|作監暴走|sakuga|[Ss]akuga")

# Inline credit format: 「作品名」(役職) embedded in narrative text
_BRACKET_CREDIT_RE = re.compile(
    r"「([^」]{1,60})」[（(]([^)）]{1,50})[)）]"
)


def parse_person_page(html: str, page_id: int = 0) -> ParsedSakugaPerson:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    raw_title = title_tag.get_text(strip=True) if title_tag else ""
    name = _TITLE_SITE_SUFFIX.sub("", raw_title).strip()

    wikibody: Tag = soup.find("div", id="wikibody") or soup.find("body") or soup  # type: ignore[assignment]

    aliases = _extract_aliases(wikibody)
    credits = _extract_credits(wikibody)

    wikibody_text = wikibody.get_text(separator="\n")
    if not credits and len(wikibody_text) >= 500:
        credits = _llm_fallback(wikibody_text)

    years = [c.work_year for c in credits if c.work_year is not None]
    active_since_year = min(years) if years else None

    return ParsedSakugaPerson(
        page_id=page_id,
        name=name,
        aliases=aliases,
        active_since_year=active_since_year,
        credits=credits,
        source_html_sha256=hashlib.sha256(html.encode()).hexdigest(),
    )


def _extract_aliases(wikibody: Tag) -> list[str]:
    aliases: list[str] = []
    text = wikibody.get_text(separator="\n")
    for line in text.splitlines():
        line = line.strip()
        if _ALIAS_LABEL_RE.match(line):
            alias = _ALIAS_LABEL_RE.sub("", line).strip()
            if alias:
                aliases.append(unicodedata.normalize("NFKC", alias))
    return aliases


def _extract_credits(wikibody: Tag) -> list[ParsedSakugaCredit]:
    # Find filmography section
    filmography_h: Tag | None = None
    for h in wikibody.find_all(["h2", "h3", "h4"]):
        if _PERSON_H_KW.search(h.get_text(strip=True)):
            filmography_h = h
            break

    credits: list[ParsedSakugaCredit] = []

    if filmography_h is not None:
        h_level = int(filmography_h.name[1])
        block_elements: list[Tag] = []
        for sib in filmography_h.find_next_siblings():
            if isinstance(sib, Tag) and sib.name in ("h2", "h3", "h4"):
                if int(sib.name[1]) <= h_level:
                    break
            block_elements.append(sib)  # type: ignore[arg-type]
        credits = _parse_block(block_elements)
    else:
        # No filmography heading — try whole body (div recursion handles nested wrappers)
        credits = _parse_block(list(wikibody.children))

    # Fallback: extract 「作品名」(役職) inline credits from full body text
    if not credits:
        full_text = unicodedata.normalize("NFKC", wikibody.get_text())
        credits = _extract_bracket_credits(full_text)

    return credits


def _extract_bracket_credits(text: str) -> list[ParsedSakugaCredit]:
    """Extract 「作品名」(役職...) patterns from narrative text."""
    credits: list[ParsedSakugaCredit] = []
    for m in _BRACKET_CREDIT_RE.finditer(text):
        title = m.group(1).strip()
        roles_raw = m.group(2).strip()
        if not title or not _ROLE_INLINE_RE.search(roles_raw):
            continue
        year = _extract_year(roles_raw) or _extract_year(text[max(0, m.start()-30):m.start()])
        fmt = _extract_format(roles_raw) or _extract_format(title)
        for role_m in _ROLE_INLINE_RE.finditer(roles_raw):
            credits.append(ParsedSakugaCredit(
                work_title=_clean_title(title),
                work_year=year,
                work_format=fmt,
                role_raw=role_m.group(0),
                episode_raw=None,
                episode_num=None,
            ))
    return credits


def _parse_block(elements: list) -> list[ParsedSakugaCredit]:
    credits: list[ParsedSakugaCredit] = []
    current_work: str | None = None
    current_year: int | None = None
    current_fmt: str | None = None

    for el in elements:
        if not isinstance(el, Tag):
            continue

        if el.name in ("h3", "h4"):
            heading_text = el.get_text(strip=True)
            # Could be work title or role heading
            if _ROLE_INLINE_RE.search(heading_text):
                # Role-first format — skip, handle via list
                pass
            else:
                current_work = _clean_title(heading_text)
                current_year = _extract_year(heading_text)
                current_fmt = _extract_format(heading_text)

        elif el.name in ("ul", "ol"):
            for li in el.find_all("li", recursive=False):
                li_text = unicodedata.normalize("NFKC", li.get_text(separator=" ", strip=True))
                c = _parse_list_item(li_text, current_work, current_year, current_fmt)
                if c is not None:
                    credits.append(c)

        elif el.name == "table":
            credits.extend(_parse_table(el))

        elif el.name in ("p", "font"):
            p_text = unicodedata.normalize("NFKC", el.get_text(strip=True))
            if "■" in p_text:
                for segment in p_text.split("■"):
                    segment = segment.strip()
                    if segment:
                        credits.extend(_parse_bullet_segment(segment))
            else:
                c = _parse_inline_line(p_text)
                if c is not None:
                    credits.append(c)

        elif el.name == "div":
            # Recurse into content wrappers; skip UI/navigation divs
            inner = unicodedata.normalize("NFKC", el.get_text())
            if "■" in inner or _ROLE_INLINE_RE.search(inner):
                credits.extend(_parse_block(list(el.children)))

    return credits


def _parse_list_item(
    text: str,
    work: str | None,
    year: int | None,
    fmt: str | None,
) -> ParsedSakugaCredit | None:
    text = text.strip()
    if not text or _SUBJECTIVE_RE.search(text):
        return None

    # If we have a current work context, items are episode+role lines
    if work:
        role_m = _ROLE_INLINE_RE.search(text)
        if role_m:
            role_raw = role_m.group(0)
            ep_raw, ep_num = _parse_episode(text)
            return ParsedSakugaCredit(
                work_title=work,
                work_year=year,
                work_format=fmt,
                role_raw=role_raw,
                episode_raw=ep_raw,
                episode_num=ep_num,
            )
        # No role marker — could be just an episode annotation, skip
        return None

    # No work context — full inline format: "作品名 (2020) 第3話 原画"
    return _parse_inline_line(text)


def _parse_inline_line(text: str) -> ParsedSakugaCredit | None:
    if not text or _SUBJECTIVE_RE.search(text):
        return None
    role_m = _ROLE_INLINE_RE.search(text)
    if not role_m:
        return None
    role_raw = role_m.group(0)
    year = _extract_year(text)
    fmt = _extract_format(text)
    ep_raw, ep_num = _parse_episode(text)
    # Work title: text before the year/episode/role markers
    work_title = _extract_work_title(text)
    if not work_title:
        return None
    return ParsedSakugaCredit(
        work_title=work_title,
        work_year=year,
        work_format=fmt,
        role_raw=role_raw,
        episode_raw=ep_raw,
        episode_num=ep_num,
    )


def _parse_bullet_segment(segment: str) -> list[ParsedSakugaCredit]:
    """Parse one ■-delimited credit segment.

    Format: 作品名（フォーマット/年〜年）　役職　話数　話数　役職2　話数...
    segment arrives NFKC-normalized (fullwidth parens → halfwidth).
    """
    if not segment or _SUBJECTIVE_RE.search(segment):
        return []

    # Title = everything up to the first paren (both fullwidth and halfwidth survive NFKC edge cases)
    # Greedy [^(（]+ stops at the first opening paren
    paren_m = re.match(r"^([^(（]+)(?:[（(]([^)）]*)[)）])?\s*(.*)", segment, re.DOTALL)
    if not paren_m:
        return []

    raw_title = paren_m.group(1).strip()
    paren_content = paren_m.group(2) or ""
    rest = (paren_m.group(3) or "").strip()

    # Fallback: no paren → split on first role keyword
    if not rest:
        role_m = _ROLE_INLINE_RE.search(segment)
        if not role_m:
            return []
        raw_title = segment[: role_m.start()].strip()
        rest = segment[role_m.start() :]
        paren_content = ""

    work_title = _clean_title(raw_title)
    if not work_title:
        return []

    year = _extract_year(paren_content) if paren_content else _extract_year(raw_title)
    fmt = _extract_format(paren_content) if paren_content else _extract_format(raw_title)

    role_matches = list(_ROLE_INLINE_RE.finditer(rest))
    if not role_matches:
        return []

    credits: list[ParsedSakugaCredit] = []
    for i, rm in enumerate(role_matches):
        role_raw = rm.group(0)
        ep_text_start = rm.end()
        ep_text_end = role_matches[i + 1].start() if i + 1 < len(role_matches) else len(rest)
        ep_text = rest[ep_text_start:ep_text_end]

        ep_nums = [int(m) for m in re.findall(r"(\d+)\s*話", ep_text)]
        if ep_nums:
            ep_raw = " ".join(f"{n}話" for n in ep_nums)
            ep_num = ep_nums[0]
        else:
            ep_raw, ep_num = _parse_episode(ep_text)

        credits.append(ParsedSakugaCredit(
            work_title=work_title,
            work_year=year,
            work_format=fmt,
            role_raw=role_raw,
            episode_raw=ep_raw,
            episode_num=ep_num,
        ))

    return credits


def _parse_table(table: Tag) -> list[ParsedSakugaCredit]:
    credits: list[ParsedSakugaCredit] = []
    rows = table.find_all("tr")
    if not rows:
        return credits

    # Detect column order from header row
    header_cells = rows[0].find_all(["th", "td"])
    headers = [unicodedata.normalize("NFKC", c.get_text(strip=True)) for c in header_cells]

    work_col = _find_col(headers, ["作品", "タイトル", "作品名", "title"])
    role_col = _find_col(headers, ["役職", "クレジット", "担当", "役", "スタッフ"])
    ep_col = _find_col(headers, ["話数", "エピソード", "回", "#", "EP"])

    for row in rows[1:]:
        cells = [unicodedata.normalize("NFKC", c.get_text(strip=True)) for c in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue

        work = cells[work_col] if work_col is not None and work_col < len(cells) else None
        role_raw_cell = cells[role_col] if role_col is not None and role_col < len(cells) else None
        ep_cell = cells[ep_col] if ep_col is not None and ep_col < len(cells) else None

        if not work or not role_raw_cell:
            continue
        if _SUBJECTIVE_RE.search(role_raw_cell):
            continue

        ep_raw, ep_num = _parse_episode(ep_cell or "")
        year = _extract_year(work)
        fmt = _extract_format(work)
        credits.append(ParsedSakugaCredit(
            work_title=_clean_title(work),
            work_year=year,
            work_format=fmt,
            role_raw=role_raw_cell,
            episode_raw=ep_raw or (ep_cell if ep_cell else None),
            episode_num=ep_num,
        ))
    return credits


# ---------------------------------------------------------------------------
# LLM fallback (Ollama/Qwen3) — mirrors seesaawiki pattern
# ---------------------------------------------------------------------------

_LLM_FEW_SHOT = """\
以下は作画@wikiの人物ページ本文です。参加作品・役職・話数の情報をJSON配列で抽出してください。

フォーマット:
[{{"work_title": "作品名", "work_year": 年(数字またはnull), "role_raw": "役職", "episode_raw": "話数文字列またはnull", "episode_num": 話数数字またはnull}}]

例1:
本文: 「ある作品 (2020) 第3話 原画」
出力: [{{"work_title": "ある作品", "work_year": 2020, "role_raw": "原画", "episode_raw": "第3話", "episode_num": 3}}]

例2:
本文: 「別の作品\\n第7話、第9話 作画監督」
出力: [{{"work_title": "別の作品", "work_year": null, "role_raw": "作画監督", "episode_raw": "第7話、第9話", "episode_num": 7}}]

本文:
{body}

JSON配列のみ出力:"""


def _llm_fallback(wikibody_text: str) -> list[ParsedSakugaCredit]:
    from src.utils.config import LLM_BASE_URL, LLM_MODEL_NAME, LLM_TIMEOUT

    import httpx

    prompt = _LLM_FEW_SHOT.format(body=wikibody_text[:4000])
    ollama_base = LLM_BASE_URL.replace("/v1", "")
    try:
        resp = httpx.post(
            f"{ollama_base}/api/generate",
            json={"model": LLM_MODEL_NAME, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0, "num_predict": 2000}},
            timeout=LLM_TIMEOUT * 3,
        )
        resp.raise_for_status()
        answer = resp.json().get("response", "").strip()
        # Strip markdown fences
        fence = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", answer, re.DOTALL)
        if fence:
            answer = fence.group(1)
        bracket = re.search(r"\[.*\]", answer, re.DOTALL)
        if bracket:
            answer = bracket.group(0)
        items = json.loads(answer)
        credits: list[ParsedSakugaCredit] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            work = str(item.get("work_title", "")).strip()
            role = str(item.get("role_raw", "")).strip()
            if not work or not role:
                continue
            if _SUBJECTIVE_RE.search(role):
                continue
            ep_raw = item.get("episode_raw")
            ep_num = item.get("episode_num")
            credits.append(ParsedSakugaCredit(
                work_title=work,
                work_year=item.get("work_year"),
                work_format=None,
                role_raw=role,
                episode_raw=str(ep_raw) if ep_raw is not None else None,
                episode_num=int(ep_num) if ep_num is not None else None,
            ))
        log.info("llm_fallback_ok", credits=len(credits))
        return credits
    except Exception as exc:
        log.warning("llm_fallback_failed", error=str(exc))
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_year(text: str) -> int | None:
    m = _YEAR_RE.search(text)
    return int(m.group(1)) if m else None


def _extract_format(text: str) -> str | None:
    m = _FORMAT_RE.search(text)
    if not m:
        return None
    v = m.group(0)
    if re.search(r"劇場|映画|Movie", v, re.IGNORECASE):
        return "劇場"
    if re.search(r"OVA|OAD", v, re.IGNORECASE):
        return "OVA"
    if re.search(r"TVSP|TV特番|TV Special", v, re.IGNORECASE):
        return "TVSP"
    return "TV"


def _parse_episode(text: str) -> tuple[str | None, int | None]:
    # Range first (returns first number)
    m = _EP_RANGE_RE.search(text)
    if m:
        return m.group(0), int(m.group(1))
    # Single episode
    m = _EP_SINGLE_RE.search(text)
    if m:
        num = int(next(g for g in m.groups() if g is not None))
        return m.group(0), num
    # OP/ED/SP as episode_raw
    sp = re.search(r"\b(OP|ED|SP|OVA|PV|CM)\b", text, re.IGNORECASE)
    if sp:
        return sp.group(0).upper(), None
    return None, None


def _clean_title(text: str) -> str:
    # Remove year/format parentheticals and trailing noise
    text = re.sub(r"\((?:19|20)\d{2}\)", "", text)
    text = re.sub(r"\s*(?:TV|OVA|OAD|劇場版?|映画)\s*$", "", text)
    return text.strip()


def _extract_work_title(text: str) -> str:
    # Take text before first year/episode/role marker
    cutoffs = []
    m = _YEAR_RE.search(text)
    if m:
        cutoffs.append(m.start())
    m = _EP_SINGLE_RE.search(text)
    if m:
        cutoffs.append(m.start())
    m = _ROLE_INLINE_RE.search(text)
    if m:
        cutoffs.append(m.start())
    if cutoffs:
        cut = min(cutoffs)
        return _clean_title(text[:cut])
    return _clean_title(text)


def _find_col(headers: list[str], candidates: list[str]) -> int | None:
    for i, h in enumerate(headers):
        if any(c in h for c in candidates):
            return i
    return None


# ---------------------------------------------------------------------------
# Work page parser
# ---------------------------------------------------------------------------

# Roles that appear in `役職:名前` format on work pages
_WORK_COLON_ROLE_RE = re.compile(
    r"(監督補佐|副監督|助監督|監督|脚本|シリーズ構成|シリーズディレクター|ストーリーエディター|"
    r"キャラクターデザイン|キャラデザ|メカニック?デザイン?|プロダクションデザイン|"
    r"総作画監督|作画監督(?:補佐)?|作監補佐|"
    r"絵コンテ|コンテ|演出(?:助手)?|"
    r"美術監督?|美術デザイン?|背景|色彩設計?|撮影監督?|音楽|音響監督?|"
    r"プロデューサー|制作進行?|アニメーション制作|"
    r"原画|第?二?原画|動画(?:検査|チェック)?|"
    r"CG(?:ディレクター|監督)?|エフェクト)"
    r"\s*[：:/]\s*"
)
_EP_NUM_RE = re.compile(r"^(\d+)話")
_DATE_EP_RE = re.compile(r"^\d{4}[./]\d{1,2}[./]\d{1,2}$")
# Standalone role label that introduces a name list on following lines (expanded for all roles)
_GENGA_LABEL_RE = re.compile(
    r"^(監督補佐|副監督|助監督|監督|脚本|シリーズ構成|シリーズディレクター|ストーリーエディター|"
    r"キャラクターデザイン|キャラデザ|メカニック?デザイン?|プロダクションデザイン|"
    r"総作画監督|作画監督(?:補佐)?|作監補佐|"
    r"絵コンテ|コンテ|演出(?:助手)?|"
    r"美術監督?|美術デザイン?|背景|色彩設計?|撮影監督?|音楽|音響監督?|"
    r"プロデューサー|制作進行?|アニメーション制作|"
    r"原画|第?二?原画|動画(?:検査|チェック)?|"
    r"CG(?:ディレクター|監督)?|エフェクト)$"
)
# Name separator: spaces/　between Japanese names
_NAME_SPLIT_RE = re.compile(r"[\s　]+")

# `役職・名前` dot format: role followed by ・ then a non-role name (e.g. 監督・アミノテツロー)
_WORK_DOT_ROLE_RE = re.compile(
    r"^(監督補佐|副監督|助監督|監督|脚本|シリーズ構成|シリーズディレクター|ストーリーエディター|"
    r"キャラクターデザイン|キャラデザ|メカニック?デザイン?|プロダクションデザイン|"
    r"総作画監督|作画監督(?:補佐)?|作監補佐|"
    r"絵コンテ|コンテ|演出(?:助手)?|"
    r"美術監督?|美術デザイン?|背景|色彩設計?|撮影監督?|音楽|音響監督?|"
    r"プロデューサー|制作進行?|アニメーション制作|"
    r"原画|第?二?原画|動画(?:検査|チェック)?|"
    r"CG(?:ディレクター|監督)?|エフェクト)"
    r"[・&＆]"
)
# `[役職]` bracket format
_BRACKET_ROLE_RE = re.compile(r"^\[([^\]]{1,30})\]$")


def parse_work_page(html: str, page_id: int = 0) -> ParsedSakugaWork:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    raw_title = title_tag.get_text(strip=True) if title_tag else ""
    title = _TITLE_SITE_SUFFIX.sub("", raw_title).strip()
    year = _extract_year(title)
    fmt = _extract_format(title)

    wikibody: Tag = soup.find("div", id="wikibody") or soup.find("body") or soup  # type: ignore[assignment]
    staff = _extract_work_staff(wikibody)

    return ParsedSakugaWork(
        page_id=page_id,
        title=title,
        year=year,
        work_format=fmt,
        staff=staff,
        source_html_sha256=hashlib.sha256(html.encode()).hexdigest(),
    )


def _extract_work_staff(wikibody: Tag) -> list[ParsedSakugaWorkStaff]:
    staff: list[ParsedSakugaWorkStaff] = []
    current_ep: int | None = None
    current_ep_raw: str | None = None
    is_main = True        # True until we hit the first episode block
    genga_role: str | None = None   # set when we see standalone role label or pending colon-role
    seen_main_staff = False  # True once ■スタッフ block is processed

    staff_blocks_seen = 0  # count of blocks with role:name content

    for block in wikibody.find_all(["font", "p", "div", "table"], recursive=False):
        if block.name in ("div", "table"):
            continue
        block_text = unicodedata.normalize("NFKC", block.get_text(separator="\n"))
        lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]
        if not lines:
            continue

        has_role_content = (
            bool(_WORK_COLON_ROLE_RE.search(block_text))
            or any(_BRACKET_ROLE_RE.match(l) for l in [ln.strip() for ln in block_text.splitlines() if ln.strip()])
            or any(_WORK_DOT_ROLE_RE.match(l) for l in [ln.strip() for ln in block_text.splitlines() if ln.strip()])
        )
        if not has_role_content:
            continue  # description / navigation block

        # Detect block type from first line
        first = lines[0]
        block_is_date_ep = _DATE_EP_RE.match(first) is not None
        block_has_staff_marker = "■スタッフ" in block_text

        if block_has_staff_marker:
            seen_main_staff = True

        staff_blocks_seen += 1
        # After the first staff block: subsequent blocks without episode markers
        # are episode-level (e.g. エウレカセブン where each <p> = one episode)
        if staff_blocks_seen > 1 and not block_has_staff_marker and not block_is_date_ep:
            is_main = False
            current_ep = None
            current_ep_raw = None

        # If block starts with a date, it's an episode block (date-indexed series)
        if block_is_date_ep:
            is_main = False
            current_ep = None
            current_ep_raw = first  # e.g. "2007.5.12"
            genga_role = None
            ep_lines = lines[1:]  # skip the date line itself
        else:
            ep_lines = lines

        for line in ep_lines:
            if line.startswith("■"):
                stripped = line[1:].strip()
                if "スタッフ" in stripped:
                    seen_main_staff = True
                    genga_role = None
                    continue
                colon_pos = stripped.find(":")
                if colon_pos != -1:
                    # ■シーン説明:人名
                    names_raw = stripped[colon_pos + 1:].strip()
                    for name in _split_names(names_raw):
                        staff.append(ParsedSakugaWorkStaff(
                            person_name=name,
                            role_raw="原画",
                            episode_num=current_ep,
                            episode_raw=current_ep_raw,
                            is_main_staff=False,
                        ))
                    genga_role = None
                elif _GENGA_LABEL_RE.match(stripped) or _ROLE_INLINE_RE.search(stripped):
                    # ■役職名 → next lines are names
                    genga_role = stripped
                else:
                    genga_role = None
                continue

            # Episode start marker: "N話 ..."
            ep_m = _EP_NUM_RE.match(line)
            if ep_m:
                current_ep = int(ep_m.group(1))
                current_ep_raw = f"{current_ep}話"
                is_main = False
                genga_role = None
                rest = line[ep_m.end():].strip()
                staff.extend(_parse_colon_staff_line(rest, current_ep, current_ep_raw, is_main_staff=False))
                continue

            # `[役職]` bracket format
            bm = _BRACKET_ROLE_RE.match(line)
            if bm:
                genga_role = bm.group(1)
                continue

            # Standalone role label (no colon)
            if _GENGA_LABEL_RE.match(line):
                genga_role = line
                continue

            # Name list after standalone role label OR pending colon-role
            if genga_role is not None:
                # If this line is itself a role marker, reset and fall through
                is_role_line = (
                    _WORK_COLON_ROLE_RE.search(line)
                    or _BRACKET_ROLE_RE.match(line)
                    or _GENGA_LABEL_RE.match(line)
                    or _WORK_DOT_ROLE_RE.match(line)
                )
                if not is_role_line:
                    ep_here = None if (is_main and not block_is_date_ep) else current_ep
                    ep_raw_here = None if (is_main and not block_is_date_ep) else current_ep_raw
                    for name in _split_names(line):
                        staff.append(ParsedSakugaWorkStaff(
                            person_name=name,
                            role_raw=genga_role,
                            episode_num=ep_here,
                            episode_raw=ep_raw_here,
                            is_main_staff=(is_main and not block_is_date_ep),
                        ))
                    continue
                genga_role = None  # reset, fall through to role processing below

            # role:name line
            if _WORK_COLON_ROLE_RE.search(line):
                block_main = is_main and not block_is_date_ep
                new_staff = _parse_colon_staff_line(
                    line,
                    current_ep if not block_main else None,
                    current_ep_raw if not block_main else None,
                    is_main_staff=block_main,
                )
                if new_staff:
                    genga_role = None
                    staff.extend(new_staff)
                else:
                    # Colon-role with no names on same line → names are on the next line
                    m = _WORK_COLON_ROLE_RE.search(line)
                    if m:
                        genga_role = m.group(1)
                continue

            # `役職・名前` dot format (e.g. 監督・アミノテツロー)
            block_main = is_main and not block_is_date_ep
            dot_staff = _parse_dot_staff_line(
                line,
                current_ep if not block_main else None,
                current_ep_raw if not block_main else None,
                is_main_staff=block_main,
            )
            if dot_staff:
                genga_role = None
                staff.extend(dot_staff)
            elif _WORK_DOT_ROLE_RE.match(line):
                # All-role composite label → next line is names
                genga_role = line

    # Fallback: NavigableString content between <br> tags at wikibody level
    # (pages like ラーゼフォン, 住めば都, Halo Legends where content isn't in block elements)
    if not staff:
        staff = _extract_work_staff_ns(wikibody)

    return staff


def _extract_work_staff_ns(wikibody: Tag) -> list[ParsedSakugaWorkStaff]:
    """Extract staff from NavigableString text at wikibody level (fallback for <br>-delimited pages)."""
    from bs4 import NavigableString as _NS

    raw_lines: list[str] = []
    for child in wikibody.children:
        if isinstance(child, _NS):
            t = unicodedata.normalize("NFKC", str(child)).strip()
            if t:
                raw_lines.append(t)
        elif isinstance(child, Tag) and child.name not in ("br", "hr"):
            # Include text from non-br/hr children (e.g. <h3>, <p>, <font>)
            block_text = unicodedata.normalize("NFKC", child.get_text(separator="\n"))
            for ln in block_text.splitlines():
                ln = ln.strip()
                if ln:
                    raw_lines.append(ln)

    staff: list[ParsedSakugaWorkStaff] = []
    genga_role: str | None = None
    current_ep: int | None = None
    current_ep_raw: str | None = None
    is_main = True

    for line in raw_lines:
        if not line or _SUBJECTIVE_RE.search(line):
            continue

        if line.startswith("▼"):
            genga_role = None
            continue

        if line.startswith("■"):
            stripped = line[1:].strip()
            if "スタッフ" in stripped:
                genga_role = None
            elif _GENGA_LABEL_RE.match(stripped) or _ROLE_INLINE_RE.search(stripped):
                genga_role = stripped  # ■役職名 → next lines are names
            else:
                genga_role = None
            continue

        ep_m = _EP_NUM_RE.match(line)
        if ep_m:
            current_ep = int(ep_m.group(1))
            current_ep_raw = f"{current_ep}話"
            is_main = False
            genga_role = None
            rest = line[ep_m.end():].strip()
            if rest:
                staff.extend(_parse_colon_staff_line(rest, current_ep, current_ep_raw, is_main_staff=False))
            continue

        # `[役職]` bracket format
        bm = _BRACKET_ROLE_RE.match(line)
        if bm:
            genga_role = bm.group(1)
            continue

        if _GENGA_LABEL_RE.match(line):
            genga_role = line
            continue

        if genga_role is not None:
            is_role_marker = (
                _WORK_COLON_ROLE_RE.search(line)
                or _BRACKET_ROLE_RE.match(line)
                or _GENGA_LABEL_RE.match(line)
                or _WORK_DOT_ROLE_RE.match(line)
            )
            if not is_role_marker:
                ep_here = current_ep if not is_main else None
                ep_raw_here = current_ep_raw if not is_main else None
                for name in _split_names(line):
                    staff.append(ParsedSakugaWorkStaff(
                        person_name=name,
                        role_raw=genga_role,
                        episode_num=ep_here,
                        episode_raw=ep_raw_here,
                        is_main_staff=is_main,
                    ))
                continue
            genga_role = None  # reset, fall through

        if _WORK_COLON_ROLE_RE.search(line):
            ep_n = current_ep if not is_main else None
            ep_r = current_ep_raw if not is_main else None
            new_staff = _parse_colon_staff_line(line, ep_n, ep_r, is_main_staff=is_main)
            if new_staff:
                genga_role = None
                staff.extend(new_staff)
            else:
                m = _WORK_COLON_ROLE_RE.search(line)
                if m:
                    genga_role = m.group(1)
            continue

        # `役職・名前` dot format (e.g. 監督・アミノテツロー)
        dot_staff = _parse_dot_staff_line(line, current_ep, current_ep_raw, is_main_staff=is_main)
        if dot_staff:
            genga_role = None
            staff.extend(dot_staff)
        elif _WORK_DOT_ROLE_RE.match(line):
            # All-role composite label (e.g. キャラクターデザイン・作画監督) → next line is names
            genga_role = line

    return staff


def _parse_dot_staff_line(
    line: str,
    episode_num: int | None,
    episode_raw: str | None,
    *,
    is_main_staff: bool,
) -> list[ParsedSakugaWorkStaff]:
    """Parse `役職・名前` dot-separated format (e.g. 監督・アミノテツロー, 脚本・伊藤恒久、会川昇).

    Splits on ・, finds the boundary where tokens stop being role names.
    """
    if not _WORK_DOT_ROLE_RE.match(line):
        return []
    # Normalise & and ＆ → split separator for composite roles
    parts = re.split(r"[・]", line)
    role_parts: list[str] = []
    name_raw = ""
    for i, part in enumerate(parts):
        # A part is a role if it matches _ROLE_INLINE_RE exactly (whole part is a role word)
        # or if it's a role combined with & (キャラクターデザイン&作画監督)
        clean = re.sub(r"[&＆]", "・", part).strip()
        # Check each &-separated sub-part
        sub_parts = [s.strip() for s in clean.split("・") if s.strip()]
        all_roles = all(_ROLE_INLINE_RE.fullmatch(s) or _GENGA_LABEL_RE.match(s) for s in sub_parts) if sub_parts else False
        if all_roles:
            role_parts.append(part.strip())
        else:
            # This part contains a name — everything from here on is names
            name_raw = "・".join(parts[i:])
            break
    if not role_parts or not name_raw:
        return []
    role_raw = "・".join(role_parts)
    result = []
    for name in _split_names(name_raw):
        result.append(ParsedSakugaWorkStaff(
            person_name=name,
            role_raw=role_raw,
            episode_num=episode_num,
            episode_raw=episode_raw,
            is_main_staff=is_main_staff,
        ))
    return result


def _parse_colon_staff_line(
    line: str,
    episode_num: int | None,
    episode_raw: str | None,
    *,
    is_main_staff: bool,
) -> list[ParsedSakugaWorkStaff]:
    """Parse 'role:name1 name2 role2:name3' inline format."""
    result: list[ParsedSakugaWorkStaff] = []
    matches = list(_WORK_COLON_ROLE_RE.finditer(line))
    if not matches:
        return result

    for i, m in enumerate(matches):
        role_raw = m.group(1)
        name_start = m.end()
        name_end = matches[i + 1].start() if i + 1 < len(matches) else len(line)
        names_raw = line[name_start:name_end].strip()
        # Multiple names for same role separated by spaces (e.g. "伊藤嘉之 稲留和美")
        for name in _split_names(names_raw):
            result.append(ParsedSakugaWorkStaff(
                person_name=name,
                role_raw=role_raw,
                episode_num=episode_num,
                episode_raw=episode_raw,
                is_main_staff=is_main_staff,
            ))

    return result


def _split_names(raw: str) -> list[str]:
    """Split a name list (space/comma/ideographic-comma separated), filtering noise tokens."""
    # Strip bracketed annotations like [各担当作画パート]
    raw = re.sub(r"[（(【\[（][^)）】\]）]*[)）】\]）]", "", raw)
    # Split on spaces, ideographic spaces, commas, ideographic commas
    parts = re.split(r"[\s　、,，]+", raw.strip())
    result = []
    for p in parts:
        p = p.strip("→")
        # Skip obvious non-names: empty, pure ASCII/number noise, colon-terminated role labels
        if not p or len(p) < 2 or re.match(r"^[A-Za-z0-9\-・]+$", p):
            continue
        if p.endswith(":") or p.endswith("："):
            continue
        result.append(p)
    return result
