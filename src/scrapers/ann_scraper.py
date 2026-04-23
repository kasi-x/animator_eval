"""Anime News Network Encyclopedia スクレイパー.

3フェーズ構成:
  Phase 1 (masterlist): CDN から全アニメID一覧を取得
  Phase 2 (anime):      XML API で各アニメのスタッフクレジットを取得
  Phase 3 (persons):    XML API で人物メタデータを補完 (?people=ID)

データソース:
  Masterlist : https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all
  Anime XML  : https://www.animenewsnetwork.com/encyclopedia/api.xml?anime=<id>
  People XML : https://www.animenewsnetwork.com/encyclopedia/api.xml?people=<id>

レート制限: ANN は公式制限未公表。1 req/sec を基準とし、429/503 で指数バックオフ。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx
import structlog
import typer

from src.models import parse_role
from src.scrapers.logging_utils import configure_file_logging
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

# ─── URLs ────────────────────────────────────────────────────────────────────
MASTERLIST_URL = (
    "https://cdn.animenewsnetwork.com/encyclopedia/reports.xml?tag=masterlist&nlist=all"
)
ANIME_API_URL = "https://www.animenewsnetwork.com/encyclopedia/api.xml"
PEOPLE_API_URL = ANIME_API_URL  # 人物も同じ XML エンドポイント: ?people=ID (現在は ignored を返す)
PEOPLE_HTML_BASE = "https://www.animenewsnetwork.com/encyclopedia/people.php"

# XML API では最大50IDをスラッシュ区切りでバッチ取得できる
BATCH_SIZE = 50

DEFAULT_DELAY = max(SCRAPE_DELAY_SECONDS, 1.5)  # ANN は最低1.5秒間隔

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AnimtorEval/1.0; "
        "+https://github.com/kashi-x/animetor_eval)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
}

DEFAULT_DATA_DIR = Path("data/ann")

# ANN の type 属性 (大文字小文字無視) → 内部 format 文字列
# キーは小文字に正規化済み。lookup 側で .lower() してから引く。
_ANN_TYPE_MAP: dict[str, str] = {
    "tv": "TV",
    "tv special": "SPECIAL",
    "movie": "MOVIE",
    "ova": "OVA",
    "oav": "OVA",  # ANN は OAV のほうが多い
    "ona": "ONA",
    "web": "ONA",
    "special": "SPECIAL",
    "music video": "MUSIC_VIDEO",
}


def _normalize_format(ann_type: str) -> str | None:
    """ANN の type 属性を内部 format に正規化 (case-insensitive)."""
    return _ANN_TYPE_MAP.get(ann_type.strip().lower())

app = typer.Typer()


# ─── データクラス ─────────────────────────────────────────────────────────────


@dataclass
class AnnStaffEntry:
    ann_person_id: int
    name_en: str
    task: str  # 生のロール文字列


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
    date_of_birth: str | None = None  # YYYY-MM-DD
    hometown: str | None = None
    blood_type: str | None = None
    website: str | None = None
    description: str | None = None


# ─── HTTP クライアント ────────────────────────────────────────────────────────


class AnnClient:
    """ANN 非同期 HTTP クライアント (指数バックオフ付き)."""

    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        self._delay = delay
        self._last_request = 0.0
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request
        wait = self._delay - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request = time.monotonic()

    # 指数バックオフでリトライする HTTP ステータス
    # 429: rate limit / 500-504: 一時的サーバエラー / 522,524: Cloudflare timeout
    _RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504, 522, 524})

    # ネットワーク層の一時障害として扱う httpx 例外
    _RETRYABLE_EXC = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.RemoteProtocolError,
        httpx.PoolTimeout,
    )

    async def get(
        self,
        url: str,
        params: dict | None = None,
        *,
        max_attempts: int = 8,
    ) -> httpx.Response:
        """GET リクエスト. 一時障害は指数バックオフでリトライ.

        リトライ対象:
          - HTTP 429, 500-504, 522, 524
          - httpx の Timeout/Connect/Read/RemoteProtocol/PoolTimeout 例外
        max_attempts 到達時は最後の例外 (またはレスポンス) を上位に伝播.
        """
        backoff = 4.0
        attempt = 0
        while True:
            attempt += 1
            await self._throttle()
            try:
                resp = await self._client.get(url, params=params)
            except self._RETRYABLE_EXC as exc:
                if attempt >= max_attempts:
                    log.error(
                        "ann_request_giveup",
                        url=url,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        attempts=attempt,
                    )
                    raise
                wait = min(backoff, 120)
                log.warning(
                    "ann_request_error",
                    url=url,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    attempt=attempt,
                    max_attempts=max_attempts,
                    wait_s=wait,
                )
                await asyncio.sleep(wait)
                backoff *= 2
                continue

            if resp.status_code in self._RETRYABLE_STATUS:
                raw_ra = resp.headers.get("Retry-After", "")
                try:
                    retry_after = max(int(raw_ra), 5)
                except (ValueError, TypeError):
                    retry_after = int(min(backoff * 2, 300))
                if attempt >= max_attempts:
                    log.error(
                        "ann_rate_giveup",
                        url=url,
                        status=resp.status_code,
                        attempts=attempt,
                    )
                    resp.raise_for_status()
                    return resp
                log.warning(
                    "ann_rate_limited",
                    url=url,
                    status=resp.status_code,
                    wait_s=retry_after,
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                await asyncio.sleep(retry_after)
                backoff = min(max(backoff * 2, retry_after), 300)
                continue

            resp.raise_for_status()
            return resp


# ─── フェーズ 1: マスターリスト取得 ─────────────────────────────────────────


async def fetch_masterlist(client: AnnClient) -> list[int]:
    """CDN マスターリスト XML から全アニメ ID を取得する.

    CDN エンドポイントが HTML を返す場合（URL 変更 / ブロック）は
    ANN API への probe バッチで最大 ID を推定して連番リストにフォールバックする。
    """
    log.info("ann_masterlist_fetch_start", url=MASTERLIST_URL)
    try:
        resp = await client.get(MASTERLIST_URL)
        text = resp.text.lstrip()
        if not text.startswith("<") or text.lstrip("<").startswith("!DOCTYPE"):
            raise ValueError("HTML response — masterlist endpoint returned non-XML")
        root = ET.fromstring(text)
        ids: list[int] = []
        for item in root.findall(".//item"):
            if item.get("type", "") != "anime":
                continue
            try:
                ids.append(int(item.get("id", "")))
            except (ValueError, TypeError):
                continue
        if ids:
            log.info("ann_masterlist_fetched", total=len(ids))
            return ids
        raise ValueError("masterlist returned 0 anime items")
    except (ET.ParseError, ValueError) as exc:
        log.warning("ann_masterlist_fallback", reason=str(exc))

    return await _probe_max_id(client)


async def _probe_max_id(client: AnnClient) -> list[int]:
    """API に probe バッチを投げて現在の最大 anime ID を推定し、連番リストを返す.

    ANN の anime ID は概ね連番 (欠番あり)。実際の取得時に欠番は無視される。
    2026年時点の上限は ~27000 程度。
    """
    # 既知の高 ID から二分探索で上限を探す
    known_high = 27000
    probe_ids = list(range(known_high - 49, known_high + 1))
    try:
        resp = await client.get(
            f"{ANIME_API_URL}?anime={'/'.join(str(i) for i in probe_ids)}"
        )
        root = ET.fromstring(resp.text)
        found_ids = [int(el.get("id")) for el in root.findall("anime") if el.get("id")]
        if found_ids:
            max_found = max(found_ids)
            # 上限に余裕を持たせる
            max_id = max_found + 500
        else:
            max_id = known_high
    except Exception:
        max_id = known_high

    ids = list(range(1, max_id + 1))
    log.info("ann_masterlist_sequential_fallback", max_id=max_id, total=len(ids))
    return ids


# ─── フェーズ 2: アニメ XML 解析 ────────────────────────────────────────────


def _parse_vintage(vintage: str) -> tuple[int | None, str | None, str | None]:
    """Vintage 文字列から (year, start_date, end_date) を解析する.

    例: "Apr 3, 1998 to Apr 24, 1999" → (1998, "1998-04-03", "1999-04-24")
    例: "2001" → (2001, None, None)
    """
    vintage = vintage.strip()
    year: int | None = None
    start_date: str | None = None
    end_date: str | None = None

    # "MMM D?, YYYY [to MMM D?, YYYY]" パターン
    date_re = re.compile(
        r"(\w{3})\s+(\d{1,2}),\s+(\d{4})"
        r"(?:\s+to\s+(\w{3})\s+(\d{1,2}),\s+(\d{4}))?"
    )
    m = date_re.search(vintage)
    if m:
        months = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }
        sm, sd, sy = m.group(1), int(m.group(2)), int(m.group(3))
        year = sy
        start_date = f"{sy}-{months.get(sm, 1):02d}-{sd:02d}"
        if m.group(4):
            em, ed, ey = m.group(4), int(m.group(5)), int(m.group(6))
            end_date = f"{ey}-{months.get(em, 1):02d}-{ed:02d}"
        return year, start_date, end_date

    # "YYYY" のみ
    m2 = re.search(r"\b(\d{4})\b", vintage)
    if m2:
        year = int(m2.group(1))

    return year, start_date, end_date


def parse_anime_xml(root: ET.Element) -> list[AnnAnimeRecord]:
    """<ann> ルート要素から AnnAnimeRecord のリストを返す."""
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
                    AnnStaffEntry(
                        ann_person_id=pid,
                        name_en=name,
                        task=task,
                    )
                )

        records.append(rec)

    return records


async def fetch_anime_batch(
    client: AnnClient,
    ann_ids: list[int],
) -> list[AnnAnimeRecord]:
    """最大 BATCH_SIZE 個の ANN アニメ ID を XML API から一括取得する."""
    ids_str = "/".join(str(i) for i in ann_ids)
    resp = await client.get(f"{ANIME_API_URL}?anime={ids_str}")
    text = resp.text.lstrip()
    if not text.startswith("<") or text.lstrip("<").startswith("!DOCTYPE"):
        log.warning("ann_anime_html_response", ids_sample=ann_ids[:3])
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        log.error("ann_xml_parse_error", ids=ann_ids[:5], error=str(exc))
        return []
    return parse_anime_xml(root)


# ─── フェーズ 3: 人物 XML 解析 ─────────────────────────────────────────────


def parse_person_xml(root: ET.Element) -> list[AnnPersonDetail]:
    """ANN XML API レスポンス (<ann>) から AnnPersonDetail リストを返す."""
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
                # ISO 形式 or "Jan 5, 1941" 形式
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


async def fetch_person_batch(
    client: AnnClient,
    ann_ids: list[int],
) -> list[AnnPersonDetail]:
    """ANN XML API から最大 BATCH_SIZE 人の詳細を一括取得する.

    注意: 2026-04-23 時点で ?people=ID は <warning>ignored</warning> を返す。
    Phase 3 では fetch_person_html() を使うこと。
    """
    ids_str = "/".join(str(i) for i in ann_ids)
    resp = await client.get(f"{PEOPLE_API_URL}?people={ids_str}")
    text = resp.text.strip()
    if text.lstrip().startswith("<!"):
        log.warning("ann_people_html_response", ids_sample=ann_ids[:3])
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        log.error("ann_people_xml_parse_error", ids=ann_ids[:5], error=str(exc))
        return []
    return parse_person_xml(root)


# ─── フェーズ 3 (HTML): 人物 HTML スクレイピング ───────────────────────────────

# HTML ページの <strong> ラベルテキスト (小文字, コロン除去) → フィールド名
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


def _parse_dob_html(text: str) -> str | None:
    """生年月日テキストを YYYY-MM-DD または YYYY に変換する.

    対応フォーマット:
      "1941-01-05"            → "1941-01-05"  (ISO: そのまま)
      "Jan 5, 1941"           → "1941-01-05"  (略月名)
      "January 5, 1941"       → "1941-01-05"  (完全月名)
      "1941"                  → "1941"         (年のみ)
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
    """ANN 人物ページ HTML (people.php?id=N) から AnnPersonDetail を抽出する.

    ページ構造 (2026年4月時点):
      <div id="page-title">
        <h1 id="page_header">英語名</h1>
        日本語名テキストノード (例: "山口 祐司")
      </div>
      <div id="infotype-N" class="encyc-info-type ...">
        <strong>ラベル:</strong> <span>値</span>
      </div>
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Cloudflare チャレンジ / エラーページ検出
    title_tag = soup.find("title")
    title_text = title_tag.get_text(" ", strip=True) if title_tag else ""
    if "just a moment" in title_text.lower():
        log.debug("ann_person_html_cf_block", ann_id=ann_id)
        return None

    # 英語名: <h1 id="page_header"> が最も信頼性高い
    h1 = soup.find("h1", id="page_header")
    if h1:
        name_en = h1.get_text(" ", strip=True)
    elif " - " in title_text:
        name_en = title_text.split(" - ")[0].strip()
    else:
        name_en = title_text.strip()
    if not name_en:
        return None

    # 日本語名: h1 の直後テキストノード (日本語文字を含む場合)
    name_ja = ""
    if h1:
        for sib in h1.next_siblings:
            raw = str(sib).strip()
            if raw and re.search(r"[　-鿿＀-￯]", raw):
                name_ja = raw
                break

    # 日本語名フォールバック: infotype-13 (姓) + infotype-12 (名) を結合
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

    # その他フィールド: <div id="infotype-N"> から <strong> ラベルで分類
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

    return AnnPersonDetail(
        ann_id=ann_id,
        name_en=name_en,
        name_ja=name_ja,
        date_of_birth=date_of_birth,
        hometown=hometown,
        blood_type=blood_type,
        website=website,
        description=description,
    )


async def fetch_person_html(
    client: AnnClient,
    ann_id: int,
) -> AnnPersonDetail | None:
    """ANN 人物 HTML ページ (people.php?id=NUM) から 1 人分の詳細を取得する.

    XML API (?people=ID) が <warning>ignored</warning> を返す問題の代替実装。
    バッチ取得不可のため 1 リクエスト = 1 人物。
    """
    try:
        resp = await client.get(PEOPLE_HTML_BASE, params={"id": ann_id})
    except Exception as exc:
        log.warning("ann_person_html_fetch_error", ann_id=ann_id, error=str(exc))
        return None
    if resp.status_code in (403, 404):
        log.debug("ann_person_not_found", ann_id=ann_id, status=resp.status_code)
        return None
    result = parse_person_html(resp.text, ann_id)
    if result is None:
        log.debug("ann_person_html_parse_failed", ann_id=ann_id)
    return result


# ─── Bronze 書き込みヘルパー ─────────────────────────────────────────────────

import dataclasses


def save_ann_anime(anime_bw, credits_bw, rec: AnnAnimeRecord) -> int:
    """AnnAnimeRecord を BRONZE parquet に書き出してクレジット数を返す."""
    anime_row = dataclasses.asdict(rec)
    anime_row.pop("staff", None)
    anime_bw.append(anime_row)
    saved = 0
    for entry in rec.staff:
        credit_row = dataclasses.asdict(entry)
        credit_row["ann_anime_id"] = rec.ann_id
        credit_row["role"] = parse_role(entry.task)
        credits_bw.append(credit_row)
        saved += 1
    return saved


def save_person_detail(persons_bw, detail: AnnPersonDetail) -> None:
    """AnnPersonDetail を BRONZE parquet に書き出す."""
    persons_bw.append(dataclasses.asdict(detail))


# ─── チェックポイント ────────────────────────────────────────────────────────


def _load_checkpoint(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ─── typer コマンド ──────────────────────────────────────────────────────────


@app.command("scrape-anime")
def cmd_scrape_anime(
    limit: int = typer.Option(0, help="取得するアニメ数の上限 (0=全件)"),
    batch_size: int = typer.Option(BATCH_SIZE, help="XML API バッチサイズ"),
    delay: float = typer.Option(DEFAULT_DELAY, help="リクエスト間隔(秒)"),
    checkpoint_interval: int = typer.Option(
        SCRAPE_CHECKPOINT_INTERVAL, help="チェックポイント保存間隔"
    ),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="チェックポイント保存先"),
    resume: bool = typer.Option(True, help="チェックポイントから再開"),
) -> None:
    """Phase 1+2: マスターリスト取得 → アニメ XML スクレイピング."""
    log_path = configure_file_logging("ann")
    log.info("ann_scrape_anime_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_anime(
            limit=limit,
            batch_size=batch_size,
            delay=delay,
            checkpoint_interval=checkpoint_interval,
            data_dir=data_dir,
            resume=resume,
        )
    )


async def _run_scrape_anime(
    limit: int,
    batch_size: int,
    delay: float,
    checkpoint_interval: int,
    data_dir: Path,
    resume: bool,
) -> None:
    from src.scrapers.bronze_writer import BronzeWriter

    cp_path = data_dir / "anime_checkpoint.json"
    cp = _load_checkpoint(cp_path) if resume else {}

    anime_bw = BronzeWriter("ann", table="anime")
    credits_bw = BronzeWriter("ann", table="credits")

    client = AnnClient(delay=delay)
    try:
        # Phase 1: マスターリスト
        if "all_ids" in cp:
            all_ids: list[int] = cp["all_ids"]
            log.info("ann_masterlist_from_checkpoint", count=len(all_ids))
        else:
            all_ids = await fetch_masterlist(client)
            cp["all_ids"] = all_ids
            _save_checkpoint(cp_path, cp)

        completed: set[int] = set(cp.get("completed_ids", []))
        pending = [i for i in all_ids if i not in completed]
        if limit:
            pending = pending[:limit]

        log.info(
            "ann_anime_scrape_start",
            total=len(all_ids),
            completed=len(completed),
            pending=len(pending),
        )

        total_anime = 0
        total_credits = 0

        batches = [
            pending[i : i + batch_size] for i in range(0, len(pending), batch_size)
        ]

        done_this_run = 0
        for batch_idx, batch in enumerate(batches):
            records = await fetch_anime_batch(client, batch)

            for rec in records:
                saved = save_ann_anime(anime_bw, credits_bw, rec)
                total_anime += 1
                total_credits += saved
            # 空レスポンス (ID が存在しない等) も含め、バッチ全IDを完了扱いにする
            for ann_id in batch:
                completed.add(ann_id)
            done_this_run += len(batch)

            if (batch_idx + 1) % max(1, checkpoint_interval // batch_size) == 0:
                anime_bw.flush()
                credits_bw.flush()
                cp["completed_ids"] = list(completed)
                _save_checkpoint(cp_path, cp)
                log.info(
                    "ann_anime_progress",
                    done=done_this_run,
                    remaining=len(pending) - done_this_run,
                    total_anime=total_anime,
                    total_credits=total_credits,
                )

        anime_bw.flush()
        credits_bw.flush()
        cp["completed_ids"] = list(completed)
        _save_checkpoint(cp_path, cp)
        log.info(
            "ann_anime_scrape_done",
            total_anime=total_anime,
            total_credits=total_credits,
        )

    finally:
        await client.close()


@app.command("scrape-persons")
def cmd_scrape_persons(
    limit: int = typer.Option(0, help="取得する人物数の上限 (0=全件)"),
    batch_size: int = typer.Option(BATCH_SIZE, help="(未使用; HTML は per-ID)"),
    delay: float = typer.Option(DEFAULT_DELAY, help="リクエスト間隔(秒)"),
    checkpoint_interval: int = typer.Option(
        SCRAPE_CHECKPOINT_INTERVAL, help="チェックポイント保存間隔"
    ),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="チェックポイント保存先"),
    resume: bool = typer.Option(True, help="チェックポイントから再開"),
) -> None:
    """Phase 3: DB 内の ann_id を持つ全人物の HTML ページをスクレイプしてメタデータを補完する.

    XML API (?people=ID) が <warning>ignored</warning> を返す問題のため、
    people.php?id=NUM の HTML ページを 1 件ずつスクレイプする実装に切り替え済み。
    バッチ取得不可のためスループットは約 40 件/分 (1.5s 間隔)。
    """
    log_path = configure_file_logging("ann")
    asyncio.run(
        _run_scrape_persons(
            limit=limit,
            batch_size=batch_size,
            delay=delay,
            checkpoint_interval=checkpoint_interval,
            data_dir=data_dir,
            resume=resume,
        )
    )


async def _run_scrape_persons(
    limit: int,
    batch_size: int,  # noqa: ARG001 — HTML は per-ID のためバッチ不使用
    delay: float,
    checkpoint_interval: int,
    data_dir: Path,
    resume: bool,
) -> None:
    import pyarrow.dataset as ds

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

    cp_path = data_dir / "persons_checkpoint.json"
    cp = _load_checkpoint(cp_path) if resume else {}
    completed: set[int] = set(cp.get("completed_ids", []))

    # Read ann_person_id list from bronze parquet (written by anime phase)
    credits_path = DEFAULT_BRONZE_ROOT / "source=ann" / "table=credits"
    if not credits_path.exists():
        log.warning("ann_persons_no_credits", msg="Run scrape-anime phase first")
        return

    credits_ds = ds.dataset(credits_path, format="parquet")
    tbl = credits_ds.to_table(columns=["ann_person_id"])
    all_ann_ids: list[int] = list(
        dict.fromkeys(pid for pid in tbl.column("ann_person_id").to_pylist() if pid is not None)
    )

    pending = [i for i in all_ann_ids if i not in completed]
    if limit:
        pending = pending[:limit]

    log.info(
        "ann_persons_scrape_start",
        total=len(all_ann_ids),
        completed=len(completed),
        pending=len(pending),
        method="html",
    )

    done_this_run = 0
    collected = 0
    persons_bw = BronzeWriter("ann", table="persons")
    client = AnnClient(delay=delay)
    try:
        for ann_id in pending:
            detail = await fetch_person_html(client, ann_id)
            if detail is not None:
                save_person_detail(persons_bw, detail)
                collected += 1
            completed.add(ann_id)
            done_this_run += 1

            if done_this_run % checkpoint_interval == 0:
                persons_bw.flush()
                cp["completed_ids"] = list(completed)
                _save_checkpoint(cp_path, cp)
                log.info(
                    "ann_persons_progress",
                    done=done_this_run,
                    collected=collected,
                    remaining=len(pending) - done_this_run,
                )

        persons_bw.flush()
        cp["completed_ids"] = list(completed)
        _save_checkpoint(cp_path, cp)
        log.info("ann_persons_scrape_done", total_requested=done_this_run, collected=collected)

    finally:
        await client.close()


@app.command("scrape-all")
def cmd_scrape_all(
    limit: int = typer.Option(0, help="アニメ取得上限 (0=全件)"),
    delay: float = typer.Option(DEFAULT_DELAY, help="リクエスト間隔(秒)"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="チェックポイント保存先"),
) -> None:
    """Phase 1-3 を順番に実行する."""
    log_path = configure_file_logging("ann")
    log.info("ann_scrape_all_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_anime(
            limit=limit,
            batch_size=BATCH_SIZE,
            delay=delay,
            checkpoint_interval=SCRAPE_CHECKPOINT_INTERVAL,
            data_dir=data_dir,
            resume=True,
        )
    )
    asyncio.run(
        _run_scrape_persons(
            limit=0,
            batch_size=BATCH_SIZE,
            delay=delay,
            checkpoint_interval=SCRAPE_CHECKPOINT_INTERVAL,
            data_dir=data_dir,
            resume=True,
        )
    )


if __name__ == "__main__":
    app()
