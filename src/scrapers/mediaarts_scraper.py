"""メディア芸術データベース (MADB) からのデータ収集.

メディア芸術データベース LOD: https://mediaarts-db.bunka.go.jp/
SPARQL エンドポイント: https://mediaarts-db.bunka.go.jp/sparql

MADBのcontributorデータはプレーンテキスト（例: "[脚本]仲倉重郎 / [演出]須永 司"）
であり、構造化された人物URIがないため、テキストパーサーとdeterministic ID生成を使用する。
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
import unicodedata
import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

SPARQL_ENDPOINT = "https://mediaarts-db.bunka.go.jp/sparql"
REQUEST_INTERVAL = 1.0  # 秒（保守的なレート制限）

# 対象アニメタイプ（正しいオントロジークラス）
ANIME_TYPES = [
    "AnimationTVRegularSeries",
    "AnimationMovie",
    "AnimationMovieSeries",
    "AnimationTVSpecialSeries",
    "AnimationVideoPackageSeries",
]

# アニメ一覧クエリ（軽量 — タイトル・年・外部IDのみ）
ANIME_LIST_QUERY = """
PREFIX schema: <http://schema.org/>
PREFIX madb: <https://mediaarts-db.bunka.go.jp/data/property/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?anime ?title ?year ?identifier
WHERE {{
  ?anime rdf:type <https://mediaarts-db.bunka.go.jp/data/class/{anime_type}> ;
         schema:name ?title .
  OPTIONAL {{ ?anime schema:datePublished ?year . }}
  OPTIONAL {{ ?anime madb:externalIdentifier ?identifier . }}
}}
LIMIT {limit}
OFFSET {offset}
"""

# 個別アニメの contributor 取得クエリ
ANIME_CONTRIBUTOR_QUERY = """
PREFIX schema: <http://schema.org/>
PREFIX madb: <https://mediaarts-db.bunka.go.jp/data/property/>

SELECT ?contributor
WHERE {{
  <{anime_uri}> schema:contributor ?contributor .
}}
"""

app = typer.Typer()


def parse_contributor_text(text: str) -> list[tuple[str, str]]:
    """MADBのcontributorテキストを (role_ja, name_ja) ペアに分解.

    Input:  "[脚本]仲倉重郎 / [演出]須永 司 / [作画監督]数井浩子"
    Output: [("脚本", "仲倉重郎"), ("演出", "須永 司"), ("作画監督", "数井浩子")]

    各種バリエーションに対応:
    - 全角ブラケット: ［脚本］→ [脚本]
    - ブラケットなし: "仲倉重郎" → ("other", "仲倉重郎")
    - 複数ロール: "[脚本・演出]名前" → [("脚本", "名前"), ("演出", "名前")]
    """
    if not text or not text.strip():
        return []

    # NFKC正規化（全角ブラケット → 半角 etc.）
    text = unicodedata.normalize("NFKC", text)

    results: list[tuple[str, str]] = []

    # " / " で分割（前後に空白があるスラッシュのみ）
    # ブラケット内のスラッシュ（[脚本/演出]）は分割しない
    segments = re.split(r"\s+/\s+", text)

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        # [role]name パターン
        match = re.match(r"\[([^\]]+)\]\s*(.+)", segment)
        if match:
            role_text = match.group(1).strip()
            name = match.group(2).strip()
            if not name:
                continue
            # 複数ロール対応: "脚本・演出" → ["脚本", "演出"]
            roles = re.split(r"[・/]", role_text)
            for role in roles:
                role = role.strip()
                if role:
                    results.append((role, name))
        else:
            # ブラケットなし → role="other"
            name = segment.strip()
            if name:
                results.append(("other", name))

    return results


def make_madb_person_id(name_ja: str) -> str:
    """正規化した名前のSHA256ハッシュからdeterministic IDを生成.

    Format: "madb:p_{hash12}"
    同じ名前 → 常に同じID（冪等）
    """
    # NFKC正規化 + 空白統一
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)  # 空白完全除去
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"madb:p_{hash_hex}"


def normalize_title(title: str) -> str:
    """タイトルを正規化して比較可能にする."""
    if not title:
        return ""
    title = unicodedata.normalize("NFKC", title)
    # 空白統一・除去
    title = re.sub(r"\s+", " ", title).strip()
    return title


def match_anime_to_anilist(
    madb_title: str,
    madb_year: int | None,
    madb_identifiers: list[str],
    anilist_anime: list[Anime],
) -> str | None:
    """MADBアニメをAniListアニメにマッチングする.

    3段階マッチング:
    1. 外部ID: madb:externalIdentifier にAniList/MAL URLが含まれていれば直接紐づけ
    2. タイトル+年: 正規化title_ja + 年（±1年許容）で照合。1:1マッチのみ
    3. マッチなし → None

    Returns:
        AniListアニメのid（例: "anilist:12345"）またはNone
    """
    # Step 1: 外部ID照合
    for identifier in madb_identifiers:
        # AniList URL: https://anilist.co/anime/12345
        al_match = re.search(r"anilist\.co/anime/(\d+)", identifier)
        if al_match:
            anilist_id = int(al_match.group(1))
            for a in anilist_anime:
                if a.anilist_id == anilist_id:
                    return a.id

        # MAL URL: https://myanimelist.net/anime/12345
        mal_match = re.search(r"myanimelist\.net/anime/(\d+)", identifier)
        if mal_match:
            mal_id = int(mal_match.group(1))
            for a in anilist_anime:
                if a.mal_id == mal_id:
                    return a.id

    # Step 2: タイトル+年
    if not madb_title:
        return None

    norm_title = normalize_title(madb_title)
    if not norm_title:
        return None

    # AniListのタイトルインデックス（正規化→Anime）
    candidates: list[Anime] = []
    for a in anilist_anime:
        a_norm = normalize_title(a.title_ja)
        if a_norm and a_norm == norm_title:
            # 年チェック（±1年許容 or 年なし）
            if madb_year is None or a.year is None or abs(madb_year - a.year) <= 1:
                candidates.append(a)

    # 1:1マッチのみ採用
    if len(candidates) == 1:
        return candidates[0].id

    return None


class MediaArtsClient:
    """メディア芸術DB SPARQL 非同期クライアント."""

    def __init__(self) -> None:
        self._verify = True
        self._client = httpx.AsyncClient(timeout=60.0, verify=True, follow_redirects=True)
        self._last_request_time = 0.0
        log.info("mediaarts_client_init", source="mediaarts", ssl_verify=True)

    async def _fallback_to_insecure(self) -> None:
        """SSL検証失敗時のフォールバック."""
        if self._verify:
            self._verify = False
            await self._client.aclose()
            self._client = httpx.AsyncClient(timeout=60.0, verify=False, follow_redirects=True)
            log.warning(
                "ssl_verification_failed_falling_back",
                source="mediaarts",
                message="Falling back to verify=False",
            )

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def query(self, sparql: str) -> list[dict]:
        """SPARQL クエリを実行（リトライ + SSL フォールバック）."""
        await self._rate_limit()
        params = {"query": sparql, "format": "json"}
        ssl_fallback_tried = False

        for attempt in range(3):
            try:
                resp = await self._client.get(SPARQL_ENDPOINT, params=params)
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", {}).get("bindings", [])
            except httpx.SSLError as e:
                if not ssl_fallback_tried and self._verify:
                    ssl_fallback_tried = True
                    await self._fallback_to_insecure()
                    log.info("ssl_fallback_retry", source="mediaarts", attempt=attempt + 1)
                    await asyncio.sleep(1)
                    continue
                log.warning("sparql_query_failed", source="mediaarts", error=str(e), attempt=attempt + 1)
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
            except httpx.HTTPError as e:
                log.warning("sparql_query_failed", source="mediaarts", error=str(e), attempt=attempt + 1)
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))

        from src.scrapers.exceptions import EndpointUnreachableError

        log.error("sparql_endpoint_unreachable", source="mediaarts", url=SPARQL_ENDPOINT)
        raise EndpointUnreachableError(
            "MediaArts SPARQL endpoint unreachable after 3 attempts",
            source="mediaarts",
            url=SPARQL_ENDPOINT,
        )

    async def fetch_anime_list(
        self,
        anime_type: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[dict]:
        """指定タイプのアニメ一覧を取得."""
        sparql = ANIME_LIST_QUERY.format(anime_type=anime_type, limit=limit, offset=offset)
        return await self.query(sparql)

    async def fetch_contributor(self, anime_uri: str) -> list[dict]:
        """特定アニメのcontributorを取得."""
        sparql = ANIME_CONTRIBUTOR_QUERY.format(anime_uri=anime_uri)
        return await self.query(sparql)


def parse_anime_list_results(bindings: list[dict]) -> list[dict]:
    """アニメ一覧のSPARQL結果をパースする.

    Returns: [{uri, title, year, identifiers}]
    """
    # 同一URIの結果を統合（identifier が複数行になる場合がある）
    anime_map: dict[str, dict] = {}
    for row in bindings:
        uri = row.get("anime", {}).get("value", "")
        if not uri:
            continue

        if uri not in anime_map:
            title = row.get("title", {}).get("value", "")
            year_str = row.get("year", {}).get("value", "")
            year = None
            if year_str:
                try:
                    year = int(year_str[:4])
                except (ValueError, IndexError):
                    pass
            anime_map[uri] = {
                "uri": uri,
                "title": title,
                "year": year,
                "identifiers": [],
            }

        identifier = row.get("identifier", {}).get("value", "")
        if identifier and identifier not in anime_map[uri]["identifiers"]:
            anime_map[uri]["identifiers"].append(identifier)

    return list(anime_map.values())


async def scrape_madb(
    conn,
    anime_types: list[str] | None = None,
    max_anime: int = 10000,
    checkpoint_interval: int = 50,
    anilist_anime: list[Anime] | None = None,
) -> dict:
    """MADBからクレジットデータを収集する.

    Phase A: アニメ一覧取得（軽量SPARQL）
    Phase B: 各アニメのcontributor取得 → テキストパース → DB保存

    Args:
        conn: SQLite接続
        anime_types: 対象タイプ（デフォルト: 全タイプ）
        max_anime: 最大アニメ数
        checkpoint_interval: DB保存間隔
        anilist_anime: AniListアニメリスト（マッチング用）

    Returns:
        統計情報dict
    """
    from src.database import insert_credit, update_data_source, upsert_anime, upsert_person

    if anime_types is None:
        anime_types = ANIME_TYPES
    if anilist_anime is None:
        anilist_anime = []

    client = MediaArtsClient()
    stats = {
        "anime_fetched": 0,
        "anime_with_contributors": 0,
        "credits_created": 0,
        "persons_created": 0,
        "anime_matched": 0,
        "anime_new": 0,
        "parse_failures": 0,
    }
    person_cache: dict[str, Person] = {}  # name → Person（重複防止）

    try:
        # Phase A: アニメ一覧取得
        all_anime_records: list[dict] = []
        for anime_type in anime_types:
            offset = 0
            page_size = 1000
            while len(all_anime_records) < max_anime:
                log.info(
                    "fetching_madb_anime_list",
                    type=anime_type,
                    offset=offset,
                    total_so_far=len(all_anime_records),
                )
                bindings = await client.fetch_anime_list(anime_type, limit=page_size, offset=offset)
                if not bindings:
                    break

                records = parse_anime_list_results(bindings)
                all_anime_records.extend(records)

                if len(bindings) < page_size:
                    break
                offset += page_size

            if len(all_anime_records) >= max_anime:
                all_anime_records = all_anime_records[:max_anime]
                break

        stats["anime_fetched"] = len(all_anime_records)
        log.info("madb_anime_list_complete", total=len(all_anime_records))

        # Phase B: 各アニメのcontributor取得
        for i, anime_record in enumerate(all_anime_records):
            uri = anime_record["uri"]
            title = anime_record["title"]
            year = anime_record["year"]
            identifiers = anime_record["identifiers"]

            # contributor取得
            contributor_bindings = await client.fetch_contributor(uri)
            if not contributor_bindings:
                continue

            # contributorテキストをパース
            all_credits_for_anime: list[tuple[str, str]] = []
            for binding in contributor_bindings:
                contrib_text = binding.get("contributor", {}).get("value", "")
                if not contrib_text:
                    continue

                parsed = parse_contributor_text(contrib_text)
                if parsed:
                    all_credits_for_anime.extend(parsed)
                elif contrib_text.strip():
                    stats["parse_failures"] += 1
                    log.debug("contributor_parse_empty", anime=title, text=contrib_text[:100])

            if not all_credits_for_anime:
                continue

            stats["anime_with_contributors"] += 1

            # AniListマッチング
            madb_id = uri.split("/")[-1] if "/" in uri else uri
            matched_anilist_id = match_anime_to_anilist(title, year, identifiers, anilist_anime)

            if matched_anilist_id:
                anime_id = matched_anilist_id
                stats["anime_matched"] += 1
                # 既存アニメの madb_id を更新
                conn.execute(
                    "UPDATE anime SET madb_id = ? WHERE id = ? AND madb_id IS NULL",
                    (madb_id, anime_id),
                )
            else:
                anime_id = f"madb:{madb_id}"
                stats["anime_new"] += 1
                # 新規アニメ作成
                anime = Anime(
                    id=anime_id,
                    title_ja=title,
                    year=year,
                    madb_id=madb_id,
                )
                upsert_anime(conn, anime)

            # クレジット登録
            for role_ja, name_ja in all_credits_for_anime:
                # 名前が短すぎる場合はスキップ（法的リスク）
                if len(name_ja) < 2:
                    continue

                # Person取得 or 作成
                if name_ja not in person_cache:
                    person_id = make_madb_person_id(name_ja)
                    person = Person(
                        id=person_id,
                        name_ja=name_ja,
                        madb_id=person_id,
                    )
                    upsert_person(conn, person)
                    person_cache[name_ja] = person
                    stats["persons_created"] += 1
                else:
                    person = person_cache[name_ja]

                role = parse_role(role_ja)
                credit = Credit(
                    person_id=person.id,
                    anime_id=anime_id,
                    role=role,
                    raw_role=role_ja,
                    source="mediaarts",
                )
                insert_credit(conn, credit)
                stats["credits_created"] += 1

            # チェックポイント
            if (i + 1) % checkpoint_interval == 0:
                conn.commit()
                log.info(
                    "madb_checkpoint",
                    progress=f"{i + 1}/{len(all_anime_records)}",
                    credits=stats["credits_created"],
                    persons=stats["persons_created"],
                )

        # 最終コミット
        conn.commit()
        update_data_source(conn, "mediaarts", stats["credits_created"])
        conn.commit()

    finally:
        await client.close()

    log.info(
        "madb_scrape_complete",
        source="mediaarts",
        **stats,
    )
    return stats


@app.command()
def main(
    max_records: int = typer.Option(10000, "--max-records", "-n", help="最大アニメ数"),
    checkpoint: int = typer.Option(50, "--checkpoint", "-c", help="チェックポイント間隔"),
) -> None:
    """メディア芸術DB からクレジットデータを収集する."""
    from src.database import db_connection, init_db, load_all_anime
    from src.log import setup_logging

    setup_logging()

    with db_connection() as conn:
        init_db(conn)
        # AniListアニメをマッチング用にロード
        anilist_anime = load_all_anime(conn)
        stats = asyncio.run(
            scrape_madb(
                conn,
                max_anime=max_records,
                checkpoint_interval=checkpoint,
                anilist_anime=anilist_anime,
            )
        )

    log.info("madb_scrape_saved", **stats)


if __name__ == "__main__":
    app()
