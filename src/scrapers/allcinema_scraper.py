"""allcinema.net スクレイパー.

3フェーズ構成:
  Phase 1 (sitemap):  サイトマップ XML.gz から全 cinema/person ID を取得
  Phase 2 (cinema):   各 cinema ページの CreditJson からアニメのスタッフ情報を取得
  Phase 3 (persons):  Phase 2 で収集した person ID の名前・よみがな等を取得

データソース:
  Sitemap (cinema) : https://www.allcinema.net/sitemap_c{N}.xml.gz  (3 files)
  Sitemap (person) : https://www.allcinema.net/sitemap_p{N}.xml.gz  (12 files)
  Cinema page      : https://www.allcinema.net/cinema/{id}   (HTML, CreditJson embedded)
  Person page      : https://www.allcinema.net/person/{id}   (HTML, name + yomigana)

レート制限: robots.txt には Disallow なし。2 req/sec 上限で運用。
著作権: 株式会社スティングレイ。クレジットデータは公表済み事実として使用。
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx
import structlog
import typer

from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL, SCRAPE_DELAY_SECONDS

log = structlog.get_logger()

app = typer.Typer()

# ─── URLs ────────────────────────────────────────────────────────────────────

SITE_BASE = "https://www.allcinema.net"
CINEMA_BASE = f"{SITE_BASE}/cinema/"
PERSON_BASE = f"{SITE_BASE}/person/"
AJAX_PERSON = f"{SITE_BASE}/ajax/person"

SITEMAP_CINEMA_PATTERN = f"{SITE_BASE}/sitemap_c{{n}}.xml.gz"
SITEMAP_PERSON_PATTERN = f"{SITE_BASE}/sitemap_p{{n}}.xml.gz"

# allcinema の cinema サイトマップは 3 ファイル、person は 12 ファイル (2026/04 時点)
SITEMAP_CINEMA_COUNT = 3
SITEMAP_PERSON_COUNT = 12

DEFAULT_DELAY = max(SCRAPE_DELAY_SECONDS, 2.0)
DEFAULT_DATA_DIR = Path("data/allcinema")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AnimtorEval/1.0; "
        "+https://github.com/kashi-x/animetor_eval)"
    ),
    "Accept-Language": "ja,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# allcinema jobname → 内部 Role 文字列へのマッピング
# jobid も参考として記載するが、マッピングは jobname ベース
_JOB_ROLE_MAP: dict[str, str] = {
    "監督": "director",
    "総監督": "director",
    "副監督": "episode_director",
    "シリーズディレクター": "director",
    "演出": "episode_director",
    "絵コンテ": "episode_director",
    "シリーズ構成": "screenplay",
    "脚本": "screenplay",
    "キャラクターデザイン": "character_designer",
    "キャラクター原案": "original_creator",
    "総作画監督": "animation_director",
    "作画監督": "animation_director",
    "原画": "key_animator",
    "第二原画": "second_key_animator",
    "動画": "in_between",
    "動画チェック": "finishing",
    "仕上げ": "finishing",
    "色彩設計": "finishing",
    "撮影監督": "photography_director",
    "３ＤＣＧ監督": "cgi_director",
    "CGディレクター": "cgi_director",
    "美術監督": "background_art",
    "美術設定": "background_art",
    "背景": "background_art",
    "音楽": "music",
    "音響監督": "sound_director",
    "音響効果": "sound_director",
    "録音調整": "sound_director",
    "プロデューサー": "producer",
    "エグゼクティブプロデューサー": "producer",
    "制作プロデューサー": "producer",
    "アニメーションプロデューサー": "producer",
    "アニメーション制作": "production_manager",
    "制作": "production_manager",
    "制作進行": "production_manager",
    "製作総指揮": "producer",
    "原作": "original_creator",
    "出演": "voice_actor",
    "声の出演": "voice_actor",
}


# ─── データクラス ─────────────────────────────────────────────────────────────


@dataclass
class AllcinemaCredit:
    allcinema_person_id: int
    name_ja: str
    name_en: str
    job_name: str  # 生のジョブ名
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
    yomigana: str = ""  # よみがな (ひらがな)
    name_en: str = ""


# ─── HTTP クライアント ────────────────────────────────────────────────────────


class AllcinemaClient:
    """allcinema 非同期 HTTP クライアント.

    GET は共通 RetryingHttpClient (http_client.py) に委譲。
    CSRF を伴う POST (/ajax/person) は 419 リトライ等の独自挙動のため自前。
    """

    def __init__(self, delay: float = DEFAULT_DELAY, transport=None) -> None:
        self._csrf_token: str = ""
        self._http = RetryingHttpClient(
            source="allcinema",
            delay=delay,
            timeout=30.0,
            headers=HEADERS,
            transport=transport,
        )
        # post_ajax で同じ httpx Client が必要なので参照を露出
        self._client = self._http._client

    async def close(self) -> None:
        await self._http.aclose()

    async def get(self, url: str) -> httpx.Response:
        """GET リクエスト. 5xx/429 は共通 client が retry。404 は呼び出し側ハンドリング。"""
        resp = await self._http.get(url)
        if resp.status_code == 404:
            return resp
        resp.raise_for_status()
        if resp.status_code == 200:
            m = re.search(r'name="csrf-token"\s+content="([^"]+)"', resp.text)
            if m:
                self._csrf_token = m.group(1)
        return resp

    async def post_ajax(self, ajax_data: str, key: int, page: int = 1) -> dict:
        """CSRF 付き POST で /ajax/person エンドポイントを叩く."""
        if not self._csrf_token:
            # CSRF トークンを取得するためにまず person ページを GET
            await self.get(f"{PERSON_BASE}{key}")

        backoff = 4.0
        attempt = 0
        while True:
            await self._throttle()
            try:
                resp = await self._client.post(
                    AJAX_PERSON,
                    data={"ajax_data": ajax_data, "key": str(key), "page": str(page)},
                    headers={
                        "X-CSRF-TOKEN": self._csrf_token,
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{PERSON_BASE}{key}",
                    },
                )
            except (httpx.TimeoutException, httpx.ConnectError):
                if attempt >= 5:
                    raise
                await asyncio.sleep(min(backoff, 120))
                backoff *= 2
                attempt += 1
                continue

            if resp.status_code in (429, 503):
                wait = min(backoff * 2, 300)
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 300)
                attempt += 1
                continue

            if resp.status_code == 419:  # CSRF mismatch → refresh token
                log.info("allcinema_csrf_refresh", key=key)
                await self.get(f"{PERSON_BASE}{key}")
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.json()


# ─── フェーズ 1: サイトマップ取得 ────────────────────────────────────────────


async def fetch_sitemap_ids(
    client: AllcinemaClient,
    pattern: str,
    count: int,
) -> list[int]:
    """サイトマップ XML.gz から ID のリストを取得する."""
    ids: list[int] = []
    for n in range(1, count + 1):
        url = pattern.format(n=n)
        log.info("allcinema_sitemap_fetch", url=url)
        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                log.info("allcinema_sitemap_end", n=n)
                break
            gz_data = gzip.decompress(resp.content)
            root = ET.fromstring(gz_data)
            ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//s:loc", ns):
                if loc.text:
                    m = re.search(r"/(\d+)$", loc.text.strip())
                    if m:
                        ids.append(int(m.group(1)))
            log.info("allcinema_sitemap_done", n=n, found=len(ids))
        except Exception as exc:
            log.error("allcinema_sitemap_error", n=n, error=str(exc))
    return ids


# ─── フェーズ 2: Cinema ページ解析 ───────────────────────────────────────────


def _parse_cinema_html(html: str, cinema_id: int) -> AllcinemaAnimeRecord | None:
    """Cinema ページの HTML から AllcinemaAnimeRecord を返す。アニメ以外は None。"""
    # PageSetting からメタデータ取得
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

    # タイトルと年
    title_ja = ""
    year: int | None = None
    m_title = re.search(r"<title>(.*?)</title>", html)
    if m_title:
        raw_title = m_title.group(1)
        # "映画アニメ タイトル (YYYY) - allcinema" → "タイトル", YYYY
        m_year = re.search(r"\((\d{4})\)", raw_title)
        if m_year:
            year = int(m_year.group(1))
        # タイトル部分の抽出
        clean = re.sub(r"\s*\(\d{4}\)\s*-\s*allcinema\s*$", "", raw_title)
        clean = re.sub(r"^(映画アニメ|テレビアニメ|映画)\s*", "", clean).strip()
        title_ja = clean

    # CreditJson 取得 (HTML 中に直接埋め込まれている)
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


async def scrape_cinema(
    client: AllcinemaClient,
    cinema_id: int,
) -> AllcinemaAnimeRecord | None:
    """単一 cinema ページをスクレイプして返す。404 または非アニメは None。"""
    url = f"{CINEMA_BASE}{cinema_id}"
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    return _parse_cinema_html(resp.text, cinema_id)


# ─── フェーズ 3: Person ページ解析 ───────────────────────────────────────────


def _parse_person_html(html: str, person_id: int) -> AllcinemaPersonRecord:
    """Person ページの HTML から AllcinemaPersonRecord を返す."""
    name_ja = ""
    m_h1 = re.search(r'<div\s+class\s*=\s*"person-area-name"\s*>(.*?)</div>', html)
    if m_h1:
        name_ja = re.sub(r"<[^>]+>", "", m_h1.group(1)).strip()
    if not name_ja:
        m_title = re.search(r"<title>(.*?)\s*-\s*allcinema</title>", html)
        if m_title:
            name_ja = m_title.group(1).strip()

    # よみがな: keywords meta に "name,よみがな" 形式で記載
    yomigana = ""
    m_kw = re.search(r'name="keywords"\s+content="([^"]*)"', html)
    if m_kw:
        parts = [p.strip() for p in m_kw.group(1).split(",")]
        if len(parts) >= 2:
            yomigana = parts[1]

    return AllcinemaPersonRecord(
        allcinema_id=person_id,
        name_ja=name_ja,
        yomigana=yomigana,
    )


async def scrape_person(
    client: AllcinemaClient,
    person_id: int,
) -> AllcinemaPersonRecord | None:
    """単一 person ページをスクレイプして返す。404 は None。"""
    url = f"{PERSON_BASE}{person_id}"
    resp = await client.get(url)
    if resp.status_code == 404:
        return None
    return _parse_person_html(resp.text, person_id)


# ─── Bronze 書き込み ──────────────────────────────────────────────────────────


def save_anime_record(anime_bw, credits_bw, rec: AllcinemaAnimeRecord) -> int:
    """AllcinemaAnimeRecord を BRONZE parquet に書き出してクレジット数を返す。"""
    anime_row = dataclasses.asdict(rec)
    # credits are in staff/cast lists — write credits separately
    all_credits = rec.staff + rec.cast
    anime_row.pop("staff", None)
    anime_row.pop("cast", None)
    anime_bw.append(anime_row)
    for credit_entry in all_credits:
        credit_row = dataclasses.asdict(credit_entry)
        credit_row["cinema_id"] = rec.cinema_id
        credits_bw.append(credit_row)
    return len(all_credits)


def save_person_record(persons_bw, rec: AllcinemaPersonRecord) -> None:
    """AllcinemaPersonRecord を BRONZE parquet に書き出す."""
    persons_bw.append(dataclasses.asdict(rec))


# ─── チェックポイント ─────────────────────────────────────────────────────────


def _load_checkpoint(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


# ─── Phase 2 実行 ─────────────────────────────────────────────────────────────


async def _run_scrape_cinema(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
) -> None:
    from src.scrapers.bronze_writer import BronzeWriter

    ckpt_path = data_dir / "checkpoint_cinema.json"
    ckpt = _load_checkpoint(ckpt_path)

    completed: set[int] = set(ckpt.get("completed", []))
    cinema_ids: list[int] = ckpt.get("cinema_ids", [])

    anime_bw = BronzeWriter("allcinema", table="anime")
    credits_bw = BronzeWriter("allcinema", table="credits")

    client = AllcinemaClient(delay=delay)
    done_this_run = 0
    anime_count = 0
    credit_count = 0
    try:
        if not cinema_ids:
            log.info("allcinema_cinema_sitemap_start")
            cinema_ids = await fetch_sitemap_ids(
                client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT
            )
            ckpt["cinema_ids"] = cinema_ids
            _save_checkpoint(ckpt_path, ckpt)
            log.info("allcinema_cinema_ids_total", total=len(cinema_ids))

        pending = [cid for cid in cinema_ids if cid not in completed]
        if limit > 0:
            pending = pending[:limit]

        total = len(pending)

        log.info(
            "allcinema_cinema_scrape_start", pending=total, completed=len(completed)
        )

        for cinema_id in pending:
            rec = await scrape_cinema(client, cinema_id)
            completed.add(cinema_id)
            done_this_run += 1

            if rec is not None:
                n_credits = save_anime_record(anime_bw, credits_bw, rec)
                anime_count += 1
                credit_count += n_credits

            if done_this_run % batch_save == 0:
                anime_bw.flush()
                credits_bw.flush()
                ckpt["completed"] = list(completed)
                _save_checkpoint(ckpt_path, ckpt)
                remaining = total - done_this_run
                log.info(
                    "allcinema_cinema_progress",
                    done=done_this_run,
                    remaining=remaining,
                    anime=anime_count,
                    credits=credit_count,
                )

        anime_bw.flush()
        credits_bw.flush()
        ckpt["completed"] = list(completed)
        _save_checkpoint(ckpt_path, ckpt)

    finally:
        await client.close()

    log.info(
        "allcinema_cinema_done",
        total_scraped=done_this_run,
        anime_found=anime_count,
        credits=credit_count,
    )


# ─── Phase 3 実行 ─────────────────────────────────────────────────────────────


async def _run_scrape_persons(
    limit: int,
    batch_save: int,
    delay: float,
    data_dir: Path,
) -> None:
    import pyarrow.dataset as ds

    from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

    ckpt_person_path = data_dir / "checkpoint_persons.json"
    ckpt_person = _load_checkpoint(ckpt_person_path)

    completed_persons: set[int] = set(ckpt_person.get("completed", []))

    # Read person IDs from bronze parquet (written by cinema phase)
    credits_path = DEFAULT_BRONZE_ROOT / "source=allcinema" / "table=credits"
    if not credits_path.exists():
        log.warning(
            "allcinema_persons_no_credits",
            msg="Run cinema phase first to populate credits",
        )
        return

    credits_ds = ds.dataset(credits_path, format="parquet")
    tbl = credits_ds.to_table(columns=["allcinema_person_id"])
    all_person_ids: list[int] = tbl.column("allcinema_person_id").to_pylist()
    all_person_ids = list(dict.fromkeys(pid for pid in all_person_ids if pid is not None))

    if not all_person_ids:
        log.warning(
            "allcinema_persons_no_credits",
            msg="Run cinema phase first to populate credits",
        )
        return

    pending = [pid for pid in all_person_ids if pid not in completed_persons]
    if limit > 0:
        pending = pending[:limit]

    total = len(pending)
    log.info("allcinema_persons_start", pending=total, completed=len(completed_persons))

    persons_bw = BronzeWriter("allcinema", table="persons")
    client = AllcinemaClient(delay=delay)
    done_this_run = 0
    try:
        for allcinema_pid in pending:
            rec = await scrape_person(client, allcinema_pid)
            completed_persons.add(allcinema_pid)
            done_this_run += 1

            if rec is not None:
                save_person_record(persons_bw, rec)

            if done_this_run % batch_save == 0:
                persons_bw.flush()
                ckpt_person["completed"] = list(completed_persons)
                _save_checkpoint(ckpt_person_path, ckpt_person)
                log.info(
                    "allcinema_persons_progress",
                    done=done_this_run,
                    remaining=total - done_this_run,
                )

        persons_bw.flush()
        ckpt_person["completed"] = list(completed_persons)
        _save_checkpoint(ckpt_person_path, ckpt_person)

    finally:
        await client.close()

    log.info("allcinema_persons_done", total_scraped=done_this_run)


# ─── CLI ─────────────────────────────────────────────────────────────────────


@app.command("sitemap")
def cmd_sitemap(
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="チェックポイント保存先"),
) -> None:
    """サイトマップから cinema/person ID 一覧を取得してファイルに保存."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_sitemap_command_start", log_file=str(log_path))

    async def _run() -> None:
        client = AllcinemaClient()
        try:
            cinema_ids = await fetch_sitemap_ids(
                client, SITEMAP_CINEMA_PATTERN, SITEMAP_CINEMA_COUNT
            )
            person_ids = await fetch_sitemap_ids(
                client, SITEMAP_PERSON_PATTERN, SITEMAP_PERSON_COUNT
            )
        finally:
            await client.close()

        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "cinema_ids.json").write_text(json.dumps(cinema_ids))
        (data_dir / "person_ids.json").write_text(json.dumps(person_ids))
        typer.echo(f"cinema: {len(cinema_ids)}, persons: {len(person_ids)}")

    asyncio.run(_run())


@app.command("cinema")
def cmd_cinema(
    limit: int = typer.Option(0, help="0=全件"),
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, help="コミット間隔"),
    delay: float = typer.Option(DEFAULT_DELAY, help="リクエスト間隔(秒)"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    """Phase 2: cinema ページをスクレイプしてアニメとクレジットを BRONZE に保存."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_cinema_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_cinema(
            limit=limit,
            batch_save=batch_save,
            delay=delay,
            data_dir=data_dir,
        )
    )


@app.command("persons")
def cmd_persons(
    limit: int = typer.Option(0, help="0=全件"),
    batch_save: int = typer.Option(SCRAPE_CHECKPOINT_INTERVAL, help="コミット間隔"),
    delay: float = typer.Option(DEFAULT_DELAY, help="リクエスト間隔(秒)"),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    """Phase 3: person ページをスクレイプして名前・よみがなを BRONZE に保存."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_persons_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_persons(
            limit=limit,
            batch_save=batch_save,
            delay=delay,
            data_dir=data_dir,
        )
    )


@app.command("run")
def cmd_run(
    limit: int = typer.Option(0, help="各フェーズの処理件数上限 (0=全件)"),
    delay: float = typer.Option(DEFAULT_DELAY),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
    skip_persons: bool = typer.Option(False, help="Phase 3 をスキップ"),
) -> None:
    """Phase 2 → Phase 3 を続けて実行."""
    log_path = configure_file_logging("allcinema")
    log.info("allcinema_run_command_start", log_file=str(log_path), limit=limit)
    asyncio.run(
        _run_scrape_cinema(
            limit=limit,
            batch_save=SCRAPE_CHECKPOINT_INTERVAL,
            delay=delay,
            data_dir=data_dir,
        )
    )
    if not skip_persons:
        asyncio.run(
            _run_scrape_persons(
                limit=limit,
                batch_save=SCRAPE_CHECKPOINT_INTERVAL,
                delay=delay,
                data_dir=data_dir,
            )
        )


if __name__ == "__main__":
    app()
