"""作画@wiki page classifier, link extractor, and person parser."""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Literal

import structlog
from bs4 import BeautifulSoup, Tag

from src.runtime.models import ParsedSakugaCredit, ParsedSakugaPerson

log = structlog.get_logger()

PageKind = Literal["person", "work", "index", "meta", "unknown"]

# ---------------------------------------------------------------------------
# Page classification
# ---------------------------------------------------------------------------

_PAGE_LINK_RE = re.compile(
    r'href="(?:https://www18\.atwiki\.jp)?/sakuga/pages/(\d+)\.html"'
)
_META_TITLE_KW = re.compile(r"メニュー|サイトマップ")
_INDEX_TITLE_KW = re.compile(r"一覧|索引")
_INDEX_H_KW = re.compile(
    r"[あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわ]行"
)
_PERSON_H_KW = re.compile(r"フィルモグラフィ|参加作品|代表作|出演作")
_WORK_H_KW = re.compile(r"スタッフ|キャスト|話数|制作スタッフ|エピソード")


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
        # No filmography heading — try whole body
        credits = _parse_block(list(wikibody.children))

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

        elif el.name == "p":
            p_text = unicodedata.normalize("NFKC", el.get_text(strip=True))
            c = _parse_inline_line(p_text)
            if c is not None:
                credits.append(c)

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
[{"work_title": "作品名", "work_year": 年(数字またはnull), "role_raw": "役職", "episode_raw": "話数文字列またはnull", "episode_num": 話数数字またはnull}]

例1:
本文: 「ある作品 (2020) 第3話 原画」
出力: [{"work_title": "ある作品", "work_year": 2020, "role_raw": "原画", "episode_raw": "第3話", "episode_num": 3}]

例2:
本文: 「別の作品\\n第7話、第9話 作画監督」
出力: [{"work_title": "別の作品", "work_year": null, "role_raw": "作画監督", "episode_raw": "第7話、第9話", "episode_num": 7}]

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
