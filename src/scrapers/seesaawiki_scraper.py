"""SeesaaWiki anime staff credit scraper.

Scrapes per-episode credit data from seesaawiki.jp/w/radioi_34/,
a community-maintained anime staff database with ~8,694 pages.

Data is more granular than AniList/MAL (episode-level credits:
脚本, 演出, 作画監督, 原画, etc.).

Two-tier parsing: regex (primary, ~80%+) with LLM fallback (Ollama/Qwen3)
for non-standard formats.

This scraper only fetches and stores data. Matching with AniList is handled
separately in the pipeline's Entity Resolution phase.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    DataDirOpt,
    DelayOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    resolve_progress_enabled,
)
from src.scrapers.hash_utils import hash_anime_data
from src.scrapers.progress import scrape_progress

import httpx
import structlog
import typer
from bs4 import BeautifulSoup

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.checkpoint import Checkpoint
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.parsers.seesaawiki import (  # noqa: F401
    KNOWN_ROLES_JA,
    ParsedCredit,
    _clean_name,
    _parse_episode_ranges,
    parse_credit_line,
    parse_episodes,
    parse_series_staff,
    parse_inline_sections,
    collapse_inline_sections,
    _has_inline_sections,
    _RE_EPISODE,
    _is_cast_section_header,
    _is_staff_section_header,
    # §10.1 extended extractors
    parse_episode_titles,
    parse_gross_studios,
    parse_theme_songs,
    parse_production_committee,
    parse_original_work_info,
    parse_credit_listing_positions,
)
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

# =============================================================================
# Constants
# =============================================================================

BASE_URL = "https://seesaawiki.jp/w/radioi_34"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DEFAULT_DELAY = SCRAPE_DELAY_SECONDS  # overridable via ANIMETOR_SCRAPE_DELAY
DEFAULT_DATA_DIR = Path("data/seesaawiki")
PAGE_LIST_CACHE_TTL_HOURS = 24

_DATE_RE = re.compile(r"(\d{4}[/-]\d{2}[/-]\d{2}(?:\s+\d{2}:\d{2})?)")


app = typer.Typer()


# =============================================================================
# URL and ID helpers
# =============================================================================


def decode_euc_jp_url(url: str) -> str:
    """Decode EUC-JP percent-encoded URL components."""
    return unquote(url, encoding="euc-jp", errors="replace")


def make_seesaa_person_id(name_ja: str) -> str:
    """Generate deterministic person ID from normalized name.

    Format: "seesaa:p_{hash12}"
    """
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"seesaa:p_{hash_hex}"


def make_seesaa_anime_id(title: str) -> str:
    """Generate deterministic anime ID from normalized title.

    Format: "seesaa:{hash12}"
    """
    normalized = unicodedata.normalize("NFKC", title)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"seesaa:{hash_hex}"


# =============================================================================
# Page list enumeration (Phase 1)
# =============================================================================


async def fetch_page_list(
    client: RetryingHttpClient,
    page_num: int,
) -> list[dict[str, str]]:
    """Fetch one page of the wiki page index.

    Returns list of {"url": str, "title": str}.
    """
    url = f"{BASE_URL}/l/?p={page_num}&order=lastupdate&on_desc=1"
    resp = await client.get(url, headers=HEADERS)
    html = resp.content.decode("euc-jp", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    pages: list[dict[str, str]] = []
    for a_tag in soup.select("a[href]"):
        href = a_tag.get("href", "")
        if not isinstance(href, str):
            continue
        if "/w/radioi_34/d/" in href:
            title = a_tag.get_text(strip=True)
            if title:
                full_url = (
                    f"https://seesaawiki.jp{href}" if href.startswith("/") else href
                )
                # Search parent (and grandparent for table layouts) for update date
                container = a_tag.parent
                gp = container.parent if container else None
                search_text = (gp or container).get_text(" ", strip=True) if (gp or container) else ""
                date_match = _DATE_RE.search(search_text)
                last_updated = date_match.group(1) if date_match else None
                pages.append({"url": full_url, "title": title, "last_updated": last_updated})

    return pages


async def fetch_all_page_urls(
    client: RetryingHttpClient,
    delay: float = DEFAULT_DELAY,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> list[dict[str, str]]:
    """Enumerate all wiki pages via the page list index.

    Caches result to data_dir/page_urls.json.
    """
    cache_path = data_dir / "page_urls.json"
    if cache_path.exists():
        age_hours = (datetime.now(timezone.utc).timestamp() - cache_path.stat().st_mtime) / 3600
        if age_hours < PAGE_LIST_CACHE_TTL_HOURS:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            log.info("seesaa_page_list_cached", count=len(cached))
            return cached
        log.info("seesaa_page_list_cache_stale", age_hours=f"{age_hours:.1f}h")

    all_pages: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for page_num in range(87):  # ~87 list pages (100 items each)
        try:
            pages = await fetch_page_list(client, page_num)
        except httpx.HTTPStatusError as e:
            log.warning(
                "seesaa_list_page_error", page=page_num, status=e.response.status_code
            )
            break
        except httpx.HTTPError as e:
            log.warning("seesaa_list_page_error", page=page_num, error=str(e))
            break

        if not pages:
            log.info("seesaa_list_page_empty", page=page_num)
            break

        new_count = 0
        for p in pages:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                all_pages.append(p)
                new_count += 1

        log.info("seesaa_list_page", page=page_num, new=new_count, total=len(all_pages))
        await asyncio.sleep(delay)

    data_dir.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(all_pages, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("seesaa_page_list_complete", total=len(all_pages))
    return all_pages


# =============================================================================
# HTML extraction
# =============================================================================


def extract_wiki_body(html: str) -> str:
    """Extract the main wiki content text from a page's HTML.

    Uses <br> → newline replacement instead of get_text(separator="\\n")
    to preserve inline element text (e.g. <span>) on the same line as
    preceding text nodes.
    """
    soup = BeautifulSoup(html, "html.parser")

    body = None
    for selector in ("#page-body", "#page-body-inner", "#content-body", ".wiki-body"):
        body = soup.select_one(selector)
        if body:
            break

    if not body:
        body = soup.find("body")
    if not body:
        return ""

    for tag in body.find_all(
        [
            "script",
            "noscript",
            "style",
            "iframe",
            "object",
            "embed",
            "form",
            "input",
            "select",
            "textarea",
            "nav",
            "footer",
            "header",
        ]
    ):
        tag.decompose()
    for div in body.find_all(
        "div",
        class_=lambda c: (
            c
            and any(
                kw in c
                for kw in (
                    "ad-",
                    "ads-",
                    "ad_",
                    "ads_",
                    "seesaa-ad",
                    "page-info",
                    "page-footer",
                    "side-bar",
                    "ad-label",
                    "page-navi",
                    "comment",
                )
            )
        ),
    ):
        div.decompose()
    for div in body.find_all(
        "div",
        id=lambda i: (
            i and any(kw in i for kw in ("ad", "comment", "navi", "footer", "sidebar"))
        ),
    ):
        div.decompose()

    for br in body.find_all("br"):
        br.replace_with("\n")

    return body.get_text()


# =============================================================================
# BRONZE partition utilities
# =============================================================================


def _clear_bronze_partitions(
    source: str,
    tables: list[str],
    *,
    date: str | None = None,
    root: "Path | None" = None,
) -> int:
    """Delete all parquet files in the given (source, tables, date) partitions.

    Used by reparse_from_raw to make reruns idempotent.
    Returns number of files deleted.
    """
    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT

    bronze_root = Path(root or DEFAULT_BRONZE_ROOT)
    import datetime as _dt

    date_str = date or _dt.date.today().isoformat()
    deleted = 0
    for table in tables:
        partition = bronze_root / f"source={source}" / f"table={table}" / f"date={date_str}"
        if partition.exists():
            for f in partition.glob("*.parquet"):
                f.unlink()
                deleted += 1
    if deleted:
        log.info("bronze_partitions_cleared", source=source, date=date_str, files=deleted)
    return deleted


# =============================================================================
# LLM parser (Tier 2 — fallback)
# =============================================================================


def check_llm_available() -> bool:
    """Check if Ollama endpoint is available."""
    from src.utils.config import LLM_BASE_URL, LLM_TIMEOUT

    try:
        ollama_base = LLM_BASE_URL.replace("/v1", "")
        response = httpx.get(f"{ollama_base}/api/tags", timeout=LLM_TIMEOUT)
        return response.status_code == 200
    except Exception as e:
        log.info("llm_not_available", error=str(e))
        return False


def build_extraction_prompt(body_text: str) -> str:
    """Build the LLM prompt for credit extraction."""
    # Truncate to 4000 chars to fit context
    truncated = body_text[:4000]
    return f"""以下のアニメスタッフクレジットテキストから、スタッフ情報をJSON配列で抽出してください。

各要素のフォーマット:
{{"episode": 話数(数字またはnull), "role": "役職名", "name": "人名"}}

役職は原文のまま（脚本、演出、作画監督、原画、動画、etc.）。
エピソード番号がない場合はnullにしてください。

テキスト:
{truncated}

JSON配列のみを出力してください:"""


def build_inline_extraction_prompt(body_text: str) -> str:
    """Prompt for pages with numbered date-range sections (OP/ED制作 format).

    These pages have numbered sections like:
        1. 1月10日-3月27日 宮崎県版
          演出：森田浩光
          美術：佐藤博
    """
    truncated = body_text[:5000]
    return f"""アニメスタッフ情報のテキストです。番号付き日付セクション（例: "1. 1月10日-3月27日"）で区切られています。

各スタッフ情報をJSON配列で抽出してください。
フォーマット: {{"section": セクション番号(整数), "role": "役職名", "name": "人名"}}

- section: そのスタッフが属する番号付きセクションの番号（1, 2, 3...）
- role: 役職（演出、美術、作画監督、原画、etc.）
- name: 人名のみ（会社名は除く）
- 同じ人が複数セクションに登場する場合、それぞれ別のエントリとして出力
- 人名に含まれる脚注マーカー（*1, ※など）は除去

テキスト:
{truncated}

JSON配列のみを出力してください:"""


def _log_inline_experiment(
    title: str,
    regex_sections: list[dict],
    llm_records: list[dict],
    data_dir: "Path",
) -> None:
    """Save side-by-side comparison of regex vs LLM inline parsing to disk."""
    from collections import Counter

    regex_credits = [c for sec in regex_sections for c in sec["credits"]]
    regex_pairs = Counter((c.name, c.role) for c in regex_credits)
    llm_pairs = Counter((r.get("name", ""), r.get("role", "")) for r in llm_records)

    comparison = {
        "title": title,
        "regex": {
            "sections": len(regex_sections),
            "total_credits": len(regex_credits),
            "unique_name_role": len(regex_pairs),
            "top10": [{"name": n, "role": r, "count": c} for (n, r), c in regex_pairs.most_common(10)],
        },
        "llm": {
            "total_credits": len(llm_records),
            "unique_name_role": len(llm_pairs),
            "top10": [{"name": n, "role": r, "count": c} for (n, r), c in llm_pairs.most_common(10)],
        },
    }

    out_dir = data_dir / "inline_experiment"
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^\w\-]", "_", title)[:60]
    out_path = out_dir / f"{safe}.json"
    out_path.write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(
        "inline_experiment_saved",
        title=title,
        regex_credits=len(regex_credits),
        llm_credits=len(llm_records),
        path=str(out_path),
    )


def parse_with_llm(body_text: str) -> list[dict]:
    """Extract credits using Ollama LLM.

    Returns list of {"episode": int|None, "role": str, "name": str}.
    Gracefully returns empty list if LLM is unavailable.
    """
    from src.utils.config import (
        LLM_BASE_URL,
        LLM_MAX_TOKENS,
        LLM_MODEL_NAME,
        LLM_TEMPERATURE,
        LLM_TIMEOUT,
    )

    prompt = build_extraction_prompt(body_text)
    ollama_base = LLM_BASE_URL.replace("/v1", "")

    try:
        response = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": LLM_MAX_TOKENS,
                },
            },
            timeout=LLM_TIMEOUT * 3,  # LLM extraction needs more time
        )
        response.raise_for_status()
        result = response.json()

        answer = result.get("response", "").strip()
        if not answer:
            answer = result.get("thinking", "").strip()

        # Parse JSON from response (handle markdown code fences)
        json_text = answer
        # Remove ```json ... ``` fences
        fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", json_text, re.DOTALL)
        if fence_match:
            json_text = fence_match.group(1)

        # Try to find JSON array in text
        bracket_match = re.search(r"\[.*\]", json_text, re.DOTALL)
        if bracket_match:
            json_text = bracket_match.group(0)

        records = json.loads(json_text)
        if not isinstance(records, list):
            log.warning("llm_invalid_response", type=type(records).__name__)
            return []

        # Validate and clean records
        valid: list[dict] = []
        for r in records:
            if not isinstance(r, dict):
                continue
            role = r.get("role", "")
            name = r.get("name", "")
            if role and name and len(name) >= 2:
                valid.append(
                    {
                        "episode": r.get("episode"),
                        "role": str(role),
                        "name": _clean_name(str(name))[0],
                    }
                )

        log.info("llm_extraction", raw_count=len(records), valid_count=len(valid))
        return valid

    except (httpx.HTTPError, httpx.TimeoutException) as e:
        log.warning("llm_extraction_error", error=str(e))
        return []
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("llm_json_parse_error", error=str(e))
        return []


def parse_inline_with_llm(body_text: str) -> list[dict]:
    """LLM extraction specialized for inline date-section format.

    Returns list of {"section": int|None, "role": str, "name": str}.
    """
    from src.utils.config import (
        LLM_BASE_URL,
        LLM_MAX_TOKENS,
        LLM_MODEL_NAME,
        LLM_TEMPERATURE,
        LLM_TIMEOUT,
    )

    prompt = build_inline_extraction_prompt(body_text)
    ollama_base = LLM_BASE_URL.replace("/v1", "")

    try:
        response = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": LLM_TEMPERATURE, "num_predict": LLM_MAX_TOKENS},
            },
            timeout=LLM_TIMEOUT * 4,
        )
        response.raise_for_status()
        answer = response.json().get("response", "").strip()

        fence_m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", answer, re.DOTALL)
        if fence_m:
            answer = fence_m.group(1)
        bracket_m = re.search(r"\[.*\]", answer, re.DOTALL)
        if bracket_m:
            answer = bracket_m.group(0)

        records = json.loads(answer)
        if not isinstance(records, list):
            return []

        valid: list[dict] = []
        for r in records:
            if not isinstance(r, dict):
                continue
            role = r.get("role", "")
            name = r.get("name", "")
            if role and name and len(name) >= 2:
                valid.append({
                    "section": r.get("section"),
                    "role": str(role),
                    "name": _clean_name(str(name)),
                })

        log.info("inline_llm_extraction", raw=len(records), valid=len(valid))
        return valid

    except (httpx.HTTPError, httpx.TimeoutException) as e:
        log.warning("inline_llm_error", error=str(e))
        return []
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("inline_llm_json_error", error=str(e))
        return []


def validate_parse_with_llm(
    body_text: str,
    parsed_credits: list[ParsedCredit],
) -> dict:
    """Ask LLM to validate whether regex-parsed credits look correct.

    Called when >50% of parsed credits have unknown roles —
    might indicate the regex is matching non-credit lines.

    Returns:
        {"should_halt": bool, "reason": str}
    """
    from src.utils.config import (
        LLM_BASE_URL,
        LLM_MODEL_NAME,
        LLM_TEMPERATURE,
        LLM_TIMEOUT,
    )

    # Build sample of what regex parsed
    sample_lines: list[str] = []
    for c in parsed_credits[:20]:
        tag = "KNOWN" if c.is_known_role else "UNKNOWN"
        sample_lines.append(f"  [{tag}] {c.role}：{c.name} (pos={c.position})")
    sample = "\n".join(sample_lines)

    # Truncate body for context
    truncated_body = body_text[:2000]

    prompt = f"""/no_think
以下はアニメスタッフクレジットページのテキストと、正規表現パーサーの出力です。

パーサーの出力を検証してください:
- [KNOWN] = 既知の役職名として認識済み
- [UNKNOWN] = 未知の役職名（新しい役職かパースミスか）

パーサー出力:
{sample}

元テキスト（冒頭2000文字）:
{truncated_body}

質問: パーサーの出力は正しいですか？
- [UNKNOWN]の項目は本当にスタッフクレジットの役職名ですか？
- パースミス（非クレジット行を誤ってパースした等）はありますか？
- 人名が役職としてパースされていませんか？

以下のJSON形式のみで回答してください。説明不要:
{{"correct": true/false, "reason": "問題点の簡潔な説明（問題なければ'OK'）"}}"""

    ollama_base = LLM_BASE_URL.replace("/v1", "")
    try:
        response = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": LLM_TEMPERATURE,
                    "num_predict": 2000,
                },
            },
            timeout=LLM_TIMEOUT * 5,
        )
        response.raise_for_status()
        result = response.json()

        answer = result.get("response", "").strip()
        if not answer:
            answer = result.get("thinking", "").strip()

        # Qwen3 thinking mode: JSON may be at the very end after reasoning.
        # Combine both fields and search for the last JSON object.
        full_text = (
            result.get("thinking", "") + "\n" + result.get("response", "")
        ).strip()
        if not answer:
            answer = full_text

        # Try to extract JSON — use findall and take the LAST match (most likely the answer)
        json_matches = re.findall(r"\{[^{}]*\}", full_text)
        json_match = None
        for candidate in reversed(json_matches):
            if "correct" in candidate:
                json_match = candidate
                break
        if not json_match and json_matches:
            json_match = json_matches[-1]
        if json_match:
            validation = json.loads(json_match)
            is_correct = validation.get("correct", True)
            reason = validation.get("reason", "")

            log.info(
                "llm_validation",
                correct=is_correct,
                reason=reason[:200],
            )

            return {
                "should_halt": not is_correct,
                "reason": reason,
                "llm_raw": answer,
            }

        # Can't parse response — don't halt (be conservative)
        log.warning("llm_validation_unparseable", answer=answer[:200])
        return {"should_halt": False, "reason": "LLM response unparseable"}

    except (httpx.HTTPError, httpx.TimeoutException, json.JSONDecodeError) as e:
        log.warning("llm_validation_error", error=str(e))
        return {"should_halt": False, "reason": f"LLM error: {e}"}


# =============================================================================
# Local data saving (raw HTML + parsed intermediate)
# =============================================================================


def _safe_filename(title: str) -> str:
    """Convert a page title to a safe filename (hash-based to avoid encoding issues)."""
    normalized = unicodedata.normalize("NFKC", title)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
    # Keep a truncated title prefix for human readability
    safe = re.sub(r"[^\w\-]", "_", normalized)[:60]
    return f"{safe}_{hash_hex}"


def save_raw_html(data_dir: Path, title: str, html: str) -> Path:
    """Save raw HTML to data_dir/raw/{safe_filename}.html."""
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"{_safe_filename(title)}.html"
    path.write_text(html, encoding="utf-8")
    return path


def save_parsed_intermediate(
    data_dir: Path,
    title: str,
    anime_id: str,
    body_text: str,
    episodes: list[dict],
    series_staff: list[ParsedCredit],
    llm_records: list[dict],
    parser_used: str,
    llm_validation: dict | None = None,
) -> Path:
    """Save parsed intermediate data to data_dir/parsed/{safe_filename}.json.

    Stores body text, regex-parsed episodes, series staff, LLM records,
    and LLM validation results for later verification.
    """
    parsed_dir = data_dir / "parsed"
    parsed_dir.mkdir(parents=True, exist_ok=True)

    intermediate = {
        "title": title,
        "anime_id": anime_id,
        "parser_used": parser_used,  # "regex", "llm", "regex+llm"
        "body_text_length": len(body_text),
        "body_text": body_text,
        "llm_validation": llm_validation,
        "episodes": [
            {
                "episode": ep["episode"],
                "credits": [c.to_dict() for c in ep["credits"]],
            }
            for ep in episodes
        ],
        "series_staff": [c.to_dict() for c in series_staff],
        "llm_records": llm_records,
    }

    path = parsed_dir / f"{_safe_filename(title)}.json"
    path.write_text(
        json.dumps(intermediate, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


# =============================================================================
# Helpers
# =============================================================================


def _is_newer(wiki_date: str, scraped_at: str) -> bool:
    """Return True if wiki_date is strictly newer than scraped_at ISO timestamp."""
    try:
        wiki_dt: datetime | None = None
        for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%Y/%m/%d", "%Y-%m-%d"):
            try:
                wiki_dt = datetime.strptime(wiki_date.strip(), fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        if wiki_dt is None:
            return True  # unparseable → re-scrape (safe)

        scraped_dt = datetime.fromisoformat(scraped_at)
        if scraped_dt.tzinfo is None:
            scraped_dt = scraped_dt.replace(tzinfo=timezone.utc)
        return wiki_dt > scraped_dt
    except Exception:
        return True  # error → re-scrape


# =============================================================================
# Checkpoint
# =============================================================================




# =============================================================================
# Orchestrator
# =============================================================================


async def scrape_seesaawiki(
    data_dir: Path | None = None,
    max_pages: int = 0,
    checkpoint_interval: int = SCRAPE_CHECKPOINT_INTERVAL,
    delay: float = DEFAULT_DELAY,
    use_llm: bool = True,
    fresh: bool = False,
    list_only: bool = False,
    fetch_only: bool = False,
    progress_override: bool | None = None,
) -> dict:
    """Scrape credit data from SeesaaWiki.

    1. Enumerate all wiki pages
    2. Fetch each page and parse credits (regex + optional LLM fallback)
    3. Write to BRONZE parquet

    Args:
        data_dir: Data directory for caches/checkpoints
        max_pages: Maximum pages to process (0 = all)
        checkpoint_interval: How often to save checkpoint (pages)
        delay: Seconds between requests
        use_llm: Whether to use LLM fallback for unparseable pages
        fresh: Ignore existing checkpoint
        list_only: Only enumerate pages, don't scrape

    Returns:
        Statistics dict
    """
    from src.scrapers.bronze_writer import BronzeWriterGroup

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    stats = {
        "pages_processed": 0,
        "pages_skipped": 0,
        "pages_updated": 0,
        "pages_failed": 0,
        "anime_created": 0,
        "credits_created": 0,
        "persons_created": 0,
        "llm_fallbacks": 0,
    }

    with BronzeWriterGroup(
        "seesaawiki",
        tables=["anime", "persons", "credits", "studios", "anime_studios"],
    ) as group:
        group = BronzeWriterGroup(
            "seesaawiki",
            tables=["anime", "persons", "credits", "studios", "anime_studios"],
        )
        anime_bw = group["anime"]
        persons_bw = group["persons"]
        credits_bw = group["credits"]
        studios_bw = group["studios"]
        anime_studios_bw = group["anime_studios"]

        client = RetryingHttpClient(
            source="seesaawiki",
            delay=delay,
            timeout=30.0,
            headers=HEADERS,
            max_attempts=5,
            initial_backoff=2.0,
        )
        try:
            # Phase 1: Enumerate pages
            all_pages = await fetch_all_page_urls(client, delay=delay, data_dir=data_dir)

            if list_only:
                log.info("seesaa_list_only", total_pages=len(all_pages))
                stats["pages_processed"] = len(all_pages)
                return stats

            if max_pages > 0:
                all_pages = all_pages[:max_pages]

            # Load checkpoint
            from src.scrapers.checkpoint import resolve_checkpoint
            cp = resolve_checkpoint(data_dir / "checkpoint.json", force=fresh, resume=not fresh)

            # Handle legacy migration: old "processed_urls" list format
            if "processed_urls" in cp.data and "scraped" not in cp.data:
                now_iso = datetime.now(timezone.utc).isoformat()
                scraped_times = {url: now_iso for url in cp.get("processed_urls", [])}
                log.info("seesaa_checkpoint_migrated", count=len(scraped_times))
            else:
                scraped_times = cp.get("scraped", {})

            saved_stats = cp.get("stats", {})
            if saved_stats:
                stats.update(saved_stats)
            if scraped_times:
                log.info("seesaa_checkpoint_loaded", processed=len(scraped_times))

            # Check LLM availability once
            llm_available = use_llm and check_llm_available()
            if use_llm and not llm_available:
                log.warning("seesaa_llm_unavailable", mode="regex_only")

            person_cache: dict[str, Person] = {}  # name -> Person (dedup)

            # Phase 2 & 3: Fetch, parse, and save
            with scrape_progress(
                total=len(all_pages),
                description="seesaawiki scrape",
                enabled=progress_override,
            ) as p:
                for idx, page_info in enumerate(all_pages):
                    page_url = page_info["url"]
                    page_title = page_info["title"]

                    if page_url in scraped_times:
                        page_last_updated = page_info.get("last_updated")
                        if page_last_updated and _is_newer(page_last_updated, scraped_times[page_url]):
                            stats["pages_updated"] += 1
                            log.debug("seesaa_update_detected", url=page_url, last_updated=page_last_updated)
                        else:
                            stats["pages_skipped"] += 1
                            continue

                    try:
                        resp = await client.get(page_url, headers=HEADERS)
                        html = resp.content.decode("euc-jp", errors="replace")
                    except (httpx.HTTPError, httpx.TimeoutException) as e:
                        log.warning("seesaa_fetch_error", url=page_url, error=str(e))
                        stats["pages_failed"] += 1
                        await asyncio.sleep(delay)
                        continue

                    # Save raw HTML locally (always — for later reparse)
                    save_raw_html(data_dir, page_title, html)

                    if fetch_only:
                        scraped_times[page_url] = datetime.now(timezone.utc).isoformat()
                        stats["pages_processed"] += 1
                        p.advance()
                        if stats["pages_processed"] % checkpoint_interval == 0:
                            cp["scraped"] = scraped_times
                            cp["stats"] = stats
                            cp.save(stamp_time=False)
                            p.log(
                                "seesaa_fetch_checkpoint",
                                progress=f"{idx + 1}/{len(all_pages)}",
                                pages=stats["pages_processed"],
                            )
                        await asyncio.sleep(delay)
                        continue

                    body_text = extract_wiki_body(html)

                    # Parse with regex (Tier 1)
                    episodes = parse_episodes(body_text)
                    series_staff = parse_series_staff(body_text)

                    # Count total regex credits
                    all_credits: list[ParsedCredit] = []
                    for ep in episodes:
                        all_credits.extend(ep["credits"])
                    all_credits.extend(series_staff)
                    regex_credits = len(all_credits)

                    # Count unknown-role credits for LLM validation
                    unknown_credits = [c for c in all_credits if not c.is_known_role]

                    # LLM fallback (Tier 2): if regex yields <3 credits and text is substantial
                    llm_records: list[dict] = []
                    if llm_available and regex_credits < 3 and len(body_text) > 500:
                        llm_records = parse_with_llm(body_text)
                        if llm_records:
                            stats["llm_fallbacks"] += 1

                    # LLM validation (Tier 1.5): validate unknown-role regex results
                    # If many credits have unknown roles, ask LLM to verify the parse
                    llm_validation: dict | None = None
                    if llm_available and unknown_credits and regex_credits >= 3:
                        unknown_ratio = len(unknown_credits) / regex_credits
                        if unknown_ratio > 0.5:
                            llm_validation = validate_parse_with_llm(
                                body_text,
                                all_credits,
                            )
                            if llm_validation and llm_validation.get("should_halt"):
                                # LLM says the parse looks wrong — halt
                                log.error(
                                    "seesaa_parse_validation_failed",
                                    url=page_url,
                                    title=page_title,
                                    unknown_ratio=f"{unknown_ratio:.0%}",
                                    llm_reason=llm_validation.get("reason", ""),
                                    sample_unknowns=[
                                        f"{c.role}:{c.name}" for c in unknown_credits[:5]
                                    ],
                                )
                                # Flush what we have so far
                                group.flush_all()
                                cp["scraped"] = scraped_times
                                cp["stats"] = stats
                                cp.save(stamp_time=False)
                                log.error(
                                    "seesaa_halted",
                                    message=(
                                        "Parse validation failed — regex produced mostly "
                                        "unknown roles and LLM flagged the result as incorrect. "
                                        "Check the page manually and update the parser."
                                    ),
                                    page_url=page_url,
                                )
                                sys.exit(1)

                    # Generate anime ID
                    anime_id = make_seesaa_anime_id(page_title)

                    # Determine which parser was used
                    parser_used = "regex"
                    if llm_records and regex_credits < 3:
                        parser_used = "llm" if regex_credits == 0 else "regex+llm"

                    # Save parsed intermediate data for verification
                    save_parsed_intermediate(
                        data_dir,
                        page_title,
                        anime_id,
                        body_text,
                        episodes,
                        series_staff,
                        llm_records,
                        parser_used,
                        llm_validation=llm_validation,
                    )

                    # Write anime to bronze
                    anime = BronzeAnime(
                        id=anime_id,
                        title_ja=page_title,
                    )
                    anime_dict = anime.model_dump(mode="json")
                    # Add hash tracking for diff detection
                    anime_dict["fetched_at"] = datetime.now(timezone.utc).isoformat()
                    anime_dict["content_hash"] = hash_anime_data(anime_dict)
                    anime_bw.append(anime_dict)
                    stats["anime_created"] += 1

                    # Derive total episode count from parsed data (for open-ended ranges)
                    total_episodes = (
                        max((ep_data["episode"] or 0 for ep_data in episodes), default=0)
                        or None
                    )

                    # Save regex-parsed credits
                    for ep_data in episodes:
                        episode_num = ep_data["episode"]
                        for credit in ep_data["credits"]:
                            _save_credit(
                                persons_bw,
                                credits_bw,
                                studios_bw,
                                anime_studios_bw,
                                person_cache,
                                stats,
                                anime_id,
                                credit,
                                episode=episode_num,
                                total_episodes=total_episodes,
                            )

                    # Save series-level staff
                    for credit in series_staff:
                        _save_credit(
                            persons_bw,
                            credits_bw,
                            studios_bw,
                            anime_studios_bw,
                            person_cache,
                            stats,
                            anime_id,
                            credit,
                            episode=None,
                            total_episodes=total_episodes,
                        )

                    # Save LLM-parsed credits (only if regex didn't find much)
                    if llm_records and regex_credits < 3:
                        for record in llm_records:
                            pc = ParsedCredit(
                                role=record["role"],
                                name=record["name"],
                                position=0,  # LLM doesn't preserve ordering
                                is_known_role=record["role"] in KNOWN_ROLES_JA,
                            )
                            _save_credit(
                                persons_bw,
                                credits_bw,
                                studios_bw,
                                anime_studios_bw,
                                person_cache,
                                stats,
                                anime_id,
                                pc,
                                episode=record.get("episode"),
                                total_episodes=total_episodes,
                            )

                    scraped_times[page_url] = datetime.now(timezone.utc).isoformat()
                    stats["pages_processed"] += 1
                    p.advance()

                    # Checkpoint
                    if stats["pages_processed"] % checkpoint_interval == 0:
                        group.flush_all()
                        cp["scraped"] = scraped_times
                        cp["stats"] = stats
                        cp.save(stamp_time=False)
                        p.log(
                            "seesaa_checkpoint",
                            progress=f"{idx + 1}/{len(all_pages)}",
                            **stats,
                        )

                    await asyncio.sleep(delay)
        finally:
            await client.aclose()

    # Final checkpoint (context manager handles flush + compact)
    cp["scraped"] = scraped_times
    cp["stats"] = stats
    cp.save(stamp_time=False)

    log.info("seesaa_scrape_complete", source="seesaawiki", **stats)
    return stats


def _save_credit(
    persons_bw,
    credits_bw,
    studios_bw,
    anime_studios_bw,
    person_cache: dict[str, Person],
    stats: dict,
    anime_id: str,
    parsed: ParsedCredit,
    episode: int | None,
    total_episodes: int | None = None,
    source_listing_position: int | None = None,
) -> None:
    """Write a single credit record to BRONZE parquet.

    Handles three cases:
    - Person credit → write to persons + credits bronze
    - Company/studio credit (is_company=True) → write to studios/anime_studios bronze
    - Person with affiliation → write credit (affiliation stored in raw_role)

    When parsed.episode_from is set (open-ended range like "4話〜"),
    expands to episode_from..total_episodes using the anime's episode count.

    source_listing_position: 0-based global listing position on the page (ED-order proxy).
    """
    if parsed.is_company:
        # Store as studio involvement, not person credit
        _save_studio_credit(studios_bw, anime_studios_bw, anime_id, parsed.name, parsed.role, stats)
        return

    if parsed.name not in person_cache:
        person_id = make_seesaa_person_id(parsed.name)
        person = Person(
            id=person_id,
            name_ja=parsed.name,
        )
        persons_bw.append(person.model_dump(mode="json"))
        person_cache[parsed.name] = person
        stats["persons_created"] += 1
    else:
        person = person_cache[parsed.name]

    role = parse_role(parsed.role)

    # Resolve episode list: merge explicit episodes + open-ended range
    resolved_episodes: list[int] = list(parsed.episodes) if parsed.episodes else []
    if parsed.episode_from is not None and total_episodes:
        resolved_episodes.extend(range(parsed.episode_from, total_episodes + 1))
    # Deduplicate and sort
    if resolved_episodes:
        resolved_episodes = sorted(set(resolved_episodes))

    # If the credit has per-name episode ranges, expand into one credit per episode
    if resolved_episodes:
        for ep in resolved_episodes:
            credit = Credit(
                person_id=person.id,
                anime_id=anime_id,
                role=role,
                raw_role=parsed.role,
                episode=ep,
                source="seesaawiki",
                affiliation=parsed.affiliation,
                position=parsed.position,
                source_listing_position=source_listing_position,
            )
            credits_bw.append(credit.model_dump(mode="json"))
            stats["credits_created"] += 1
    else:
        credit = Credit(
            person_id=person.id,
            anime_id=anime_id,
            role=role,
            raw_role=parsed.role,
            episode=episode,
            source="seesaawiki",
            affiliation=parsed.affiliation,
            position=parsed.position,
            source_listing_position=source_listing_position,
        )
        credits_bw.append(credit.model_dump(mode="json"))
        stats["credits_created"] += 1


# In-memory caches to avoid redundant DB writes for studios/affiliations
_studio_id_cache: set[str] = set()  # studio IDs already upserted
_anime_studio_cache: set[tuple[str, str]] = (
    set()
)  # (anime_id, studio_id) already inserted
_affiliation_cache: set[tuple[str, str, str]] = (
    set()
)  # (person_id, anime_id, studio_name)


def _reset_save_caches() -> None:
    """Reset in-memory caches (called at start of reparse/scrape)."""
    _studio_id_cache.clear()
    _anime_studio_cache.clear()
    _affiliation_cache.clear()


def _save_studio_credit(
    studios_bw,
    anime_studios_bw,
    anime_id: str,
    studio_name: str,
    role: str,
    stats: dict,
) -> None:
    """Write studio/company credit to BRONZE parquet."""
    from src.runtime.models import AnimeStudio, Studio

    studio_id = f"seesaa:s_{hashlib.sha256(unicodedata.normalize('NFKC', studio_name).encode()).hexdigest()[:12]}"

    if studio_id not in _studio_id_cache:
        studio = Studio(
            id=studio_id,
            name=studio_name,
            is_animation_studio=True,
        )
        studios_bw.append(studio.model_dump(mode="json"))
        _studio_id_cache.add(studio_id)

    pair = (anime_id, studio_id)
    if pair not in _anime_studio_cache:
        is_main = role in ("アニメーション制作", "制作", "アニメーション")
        anime_studio = AnimeStudio(
            anime_id=anime_id,
            studio_id=studio_id,
            is_main=is_main,
        )
        anime_studios_bw.append(anime_studio.model_dump(mode="json"))
        _anime_studio_cache.add(pair)
        stats["studios_recorded"] = stats.get("studios_recorded", 0) + 1


# =============================================================================
# CLI
# =============================================================================


def reparse_from_raw(
    data_dir: Path | None = None,
    use_llm: bool = False,
    checkpoint_interval: int = SCRAPE_CHECKPOINT_INTERVAL,
    progress_override: bool | None = None,
    limit: int = 0,
    clear_first: bool = True,
    experiment_inline: bool = False,
) -> dict:
    """Re-parse all saved raw HTML files and write to BRONZE parquet.

    Args:
        clear_first: Delete today's partition files before writing (makes reparse idempotent).
        experiment_inline: For inline-section pages, run both regex and LLM, save comparison
            to data_dir/inline_experiment/. Requires LLM to be available.
    """
    from src.scrapers.bronze_writer import BronzeWriterGroup

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    raw_dir = data_dir / "raw"
    if not raw_dir.exists():
        log.error("reparse_no_raw_dir", path=str(raw_dir))
        return {}

    # Load page URL list for title mapping
    page_urls_path = data_dir / "page_urls.json"
    title_by_filename: dict[str, str] = {}
    if page_urls_path.exists():
        pages = json.loads(page_urls_path.read_text(encoding="utf-8"))
        for p in pages:
            fname = _safe_filename(p["title"])
            title_by_filename[fname] = p["title"]

    # Clear today's partitions before writing (idempotent reparse)
    _reparse_tables = [
        "anime", "persons", "credits", "studios", "anime_studios",
        # §10.1 extended tables
        "episode_titles", "gross_studios", "theme_songs",
        "production_committee", "original_work_info",
    ]
    if clear_first:
        _clear_bronze_partitions("seesaawiki", _reparse_tables)

    # Reset in-memory caches
    _reset_save_caches()

    stats = {
        "pages_processed": 0,
        "pages_failed": 0,
        "anime_created": 0,
        "credits_created": 0,
        "persons_created": 0,
        "llm_fallbacks": 0,
        "inline_pages": 0,
        "episode_titles_created": 0,
        "gross_studios_created": 0,
        "theme_songs_created": 0,
        "committee_members_created": 0,
        "original_work_created": 0,
    }

    group = BronzeWriterGroup(
        "seesaawiki",
        tables=_reparse_tables,
    )
    anime_bw = group["anime"]
    persons_bw = group["persons"]
    credits_bw = group["credits"]
    studios_bw = group["studios"]
    anime_studios_bw = group["anime_studios"]
    episode_titles_bw = group["episode_titles"]
    gross_studios_bw = group["gross_studios"]
    theme_songs_bw = group["theme_songs"]
    production_committee_bw = group["production_committee"]
    original_work_info_bw = group["original_work_info"]

    llm_available = use_llm and check_llm_available()
    person_cache: dict[str, Person] = {}
    html_files = sorted(raw_dir.glob("*.html"))
    if limit > 0:
        html_files = html_files[:limit]
    log.info("reparse_start", total_files=len(html_files))

    with scrape_progress(
        total=len(html_files),
        description="seesaawiki reparse",
        enabled=progress_override,
    ) as p:
        for idx, html_path in enumerate(html_files):
            stem = html_path.stem
            title = title_by_filename.get(stem, stem)

            try:
                html = html_path.read_text(encoding="utf-8")
            except Exception as e:
                log.warning("reparse_read_error", file=str(html_path), error=str(e))
                stats["pages_failed"] += 1
                continue

            body_text = extract_wiki_body(html)

            # Detect inline-section format (numbered date-range blocks, no 第N話 headers)
            is_inline = _has_inline_sections(body_text)

            # Parse
            episodes = parse_episodes(body_text)
            if is_inline:
                # Inline pages: use collapsed inline sections as series staff
                inline_sections = parse_inline_sections(body_text)
                series_staff = collapse_inline_sections(inline_sections)
                stats["inline_pages"] += 1
            else:
                series_staff = parse_series_staff(body_text)
            regex_credits = sum(len(ep["credits"]) for ep in episodes) + len(series_staff)

            llm_records: list[dict] = []
            if is_inline and experiment_inline and llm_available:
                # Run LLM in parallel for comparison only — don't use for actual write
                inline_llm = parse_inline_with_llm(body_text)
                _log_inline_experiment(title, inline_sections if is_inline else [], inline_llm, data_dir)
            elif llm_available and regex_credits < 3 and len(body_text) > 500:
                llm_records = parse_with_llm(body_text)
                if llm_records:
                    stats["llm_fallbacks"] += 1

            anime_id = make_seesaa_anime_id(title)

            parser_used = "regex"
            if is_inline:
                parser_used = "regex+inline"
            elif llm_records and regex_credits < 3:
                parser_used = "llm" if regex_credits == 0 else "regex+llm"

            # Save parsed intermediate
            save_parsed_intermediate(
                data_dir,
                title,
                anime_id,
                body_text,
                episodes,
                series_staff,
                llm_records,
                parser_used,
            )

            # Write anime to bronze
            anime = BronzeAnime(id=anime_id, title_ja=title)
            anime_dict = anime.model_dump(mode="json")
            # Add hash tracking for diff detection
            anime_dict["fetched_at"] = datetime.now(timezone.utc).isoformat()
            anime_dict["content_hash"] = hash_anime_data(anime_dict)
            anime_bw.append(anime_dict)
            stats["anime_created"] += 1

            total_episodes = (
                max((ep_data["episode"] or 0 for ep_data in episodes), default=0) or None
            )

            # Build source_listing_position index for this page
            listing_positions: dict[tuple[str, str, int | None], int] = {}
            for cwp in parse_credit_listing_positions(body_text):
                key = (cwp.credit.role, cwp.credit.name, cwp.episode)
                if key not in listing_positions:
                    listing_positions[key] = cwp.source_listing_position

            for ep_data in episodes:
                for credit in ep_data["credits"]:
                    lp = listing_positions.get(
                        (credit.role, credit.name, ep_data["episode"])
                    )
                    _save_credit(
                        persons_bw,
                        credits_bw,
                        studios_bw,
                        anime_studios_bw,
                        person_cache,
                        stats,
                        anime_id,
                        credit,
                        episode=ep_data["episode"],
                        total_episodes=total_episodes,
                        source_listing_position=lp,
                    )

            for credit in series_staff:
                lp = listing_positions.get((credit.role, credit.name, None))
                _save_credit(
                    persons_bw,
                    credits_bw,
                    studios_bw,
                    anime_studios_bw,
                    person_cache,
                    stats,
                    anime_id,
                    credit,
                    episode=None,
                    total_episodes=total_episodes,
                    source_listing_position=lp,
                )

            if llm_records and regex_credits < 3:
                for record in llm_records:
                    pc = ParsedCredit(
                        role=record["role"],
                        name=record["name"],
                        position=0,
                        is_known_role=record["role"] in KNOWN_ROLES_JA,
                    )
                    _save_credit(
                        persons_bw,
                        credits_bw,
                        studios_bw,
                        anime_studios_bw,
                        person_cache,
                        stats,
                        anime_id,
                        pc,
                        episode=record.get("episode"),
                        total_episodes=total_episodes,
                        source_listing_position=None,
                    )

            # --- §10.1 extended table writes ---

            # episode_titles
            for et in parse_episode_titles(body_text):
                episode_titles_bw.append({
                    "anime_id": anime_id,
                    "episode": et.episode,
                    "title": et.title,
                })
                stats["episode_titles_created"] += 1

            # gross_studios
            for gs in parse_gross_studios(body_text):
                gross_studios_bw.append({
                    "anime_id": anime_id,
                    "studio_name": gs.studio_name,
                    "episode": gs.episode,
                })
                stats["gross_studios_created"] += 1

            # theme_songs
            for ts in parse_theme_songs(body_text):
                theme_songs_bw.append({
                    "anime_id": anime_id,
                    "song_type": ts.song_type,
                    "song_title": ts.song_title,
                    "role": ts.role,
                    "name": ts.name,
                })
                stats["theme_songs_created"] += 1

            # production_committee
            for cm in parse_production_committee(body_text):
                production_committee_bw.append({
                    "anime_id": anime_id,
                    "member_name": cm.member_name,
                })
                stats["committee_members_created"] += 1

            # original_work_info
            owi = parse_original_work_info(body_text)
            if owi is not None:
                original_work_info_bw.append({
                    "anime_id": anime_id,
                    "author": owi.author,
                    "publisher": owi.publisher,
                    "label": owi.label,
                    "magazine": owi.magazine,
                    "serialization_type": owi.serialization_type,
                })
                stats["original_work_created"] += 1

            stats["pages_processed"] += 1
            p.advance()

            if stats["pages_processed"] % checkpoint_interval == 0:
                group.flush_all()
                p.log(
                    "reparse_checkpoint",
                    progress=f"{idx + 1}/{len(html_files)}",
                    **stats,
                )

    group.flush_all()
    group.compact_all()

    log.info("reparse_complete", **stats)
    return stats


@app.command()
def run(
    limit: LimitOpt = 0,
    checkpoint: CheckpointIntervalOpt = 10,
    delay: DelayOpt = DEFAULT_DELAY,
    use_llm: bool = typer.Option(
        True, "--llm/--no-llm", help="Use LLM fallback for unparseable pages"
    ),
    force: ForceOpt = False,
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    list_only: bool = typer.Option(
        False, "--list-only", help="Only enumerate pages, don't scrape"
    ),
    fetch_only: bool = typer.Option(
        False, "--fetch-only", help="Only fetch raw HTML, don't parse or save to DB"
    ),
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Fetch and parse credit data from SeesaaWiki."""
    from src.infra.logging import setup_logging

    setup_logging()

    stats = asyncio.run(
        scrape_seesaawiki(
            data_dir=data_dir,
            max_pages=limit,
            checkpoint_interval=checkpoint,
            delay=delay,
            use_llm=use_llm,
            fresh=force,
            list_only=list_only,
            fetch_only=fetch_only,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )

    log.info("seesaa_scrape_saved", **stats)


@app.command()
def reparse(
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    limit: int = typer.Option(
        0, "--limit", "-n", help="Max HTML files to reparse (0=all)"
    ),
    use_llm: bool = typer.Option(
        False, "--llm/--no-llm", help="Use LLM fallback for unparseable pages"
    ),
    checkpoint: int = typer.Option(
        50, "--checkpoint", "-c", help="Checkpoint interval"
    ),
    clear_first: bool = typer.Option(
        True, "--clear/--no-clear",
        help="Clear today's partitions before writing (default: on, makes reparse idempotent)"
    ),
    experiment_inline: bool = typer.Option(
        False, "--experiment-inline",
        help="For inline-section pages: run LLM in parallel and save comparison to inline_experiment/"
    ),
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Re-parse saved raw HTML files (no HTTP requests).

    Re-parses all raw/*.html files and writes to BRONZE parquet.
    Use this after updating the parser.
    """
    from src.infra.logging import setup_logging

    setup_logging()

    stats = reparse_from_raw(
        data_dir=data_dir,
        use_llm=use_llm,
        checkpoint_interval=checkpoint,
        progress_override=resolve_progress_enabled(quiet, progress),
        limit=limit,
        clear_first=clear_first,
        experiment_inline=experiment_inline,
    )

    log.info("seesaa_reparse_saved", **stats)


@app.command("validate-samples")
def validate_samples(
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    num_samples: int = typer.Option(
        10, "--num-samples", "-n", help="Number of random pages to validate"
    ),
    seed: int = typer.Option(42, "--seed", help="Random seed for sample selection"),
) -> None:
    """Validate parsed data quality using local LLM (Ollama/Qwen3).

    Picks random raw HTML files, parses them with the regex parser,
    and asks the local LLM to check for systemic issues.
    """
    import random
    from src.infra.logging import setup_logging

    setup_logging()

    if not check_llm_available():
        log.error("llm_not_available", hint="Start Ollama: ollama serve")
        raise typer.Exit(1)

    raw_dir = data_dir / "raw"
    if not raw_dir.exists():
        log.error("no_raw_dir", path=str(raw_dir))
        raise typer.Exit(1)

    html_files = sorted(raw_dir.glob("*.html"))
    if not html_files:
        log.error("no_raw_files")
        raise typer.Exit(1)

    # Load title mapping
    page_urls_path = data_dir / "page_urls.json"
    title_by_filename: dict[str, str] = {}
    if page_urls_path.exists():
        pages = json.loads(page_urls_path.read_text(encoding="utf-8"))
        for p in pages:
            title_by_filename[_safe_filename(p["title"])] = p["title"]

    rng = random.Random(seed)
    samples = rng.sample(html_files, min(num_samples, len(html_files)))

    issues_found = 0
    total_validated = 0

    for html_path in samples:
        stem = html_path.stem
        title = title_by_filename.get(stem, stem)

        html = html_path.read_text(encoding="utf-8", errors="replace")
        body_text = extract_wiki_body(html)
        if len(body_text.strip()) < 100:
            log.info("validate_skip_short", title=title)
            continue

        episodes = parse_episodes(body_text)
        series_staff = parse_series_staff(body_text)
        all_credits = series_staff[:]
        for ep in episodes:
            all_credits.extend(ep["credits"])

        if not all_credits:
            log.info("validate_skip_no_credits", title=title)
            continue

        total_validated += 1
        log.info(
            "validate_sample",
            title=title,
            credits=len(all_credits),
            known=sum(1 for c in all_credits if c.is_known_role),
        )

        result = validate_parse_with_llm(body_text, all_credits)
        if result.get("should_halt"):
            issues_found += 1
            log.warning(
                "validate_issue_found",
                title=title,
                reason=result.get("reason", "")[:300],
            )
        else:
            log.info("validate_ok", title=title, reason=result.get("reason", "")[:100])

    log.info(
        "validate_complete",
        total_validated=total_validated,
        issues_found=issues_found,
        verdict="PASS"
        if issues_found == 0
        else f"ISSUES ({issues_found}/{total_validated})",
    )


if __name__ == "__main__":
    app()
