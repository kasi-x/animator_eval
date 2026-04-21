"""Japanese Visual Media Graph (JVMG) データ取得 via Wikidata SPARQL.

Wikidata は日本のアニメ作品とスタッフの情報を RDF で公開している。
SPARQL エンドポイント: https://query.wikidata.org/sparql
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import structlog
import typer

from src.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.cache_store import load_cached_json, save_cached_json

log = structlog.get_logger()

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
REQUEST_INTERVAL = 2.0  # Wikidata は厳しめ

# アニメ作品とスタッフを取得するクエリ
ANIME_STAFF_QUERY = """
SELECT ?anime ?animeLabel ?year ?person ?personLabel ?personLabelJa ?roleLabel
WHERE {{
  ?anime wdt:P31/wdt:P279* wd:Q63952888 .  # アニメシリーズ
  ?anime wdt:P57|wdt:P1040|wdt:P3174|wdt:P58 ?person .
  OPTIONAL {{ ?anime wdt:P577 ?date . BIND(YEAR(?date) AS ?year) }}
  OPTIONAL {{
    ?anime p:P57|p:P1040|p:P3174|p:P58 ?stmt .
    ?stmt ps:P57|ps:P1040|ps:P3174|ps:P58 ?person .
    ?stmt pq:P3831 ?role .
  }}
  OPTIONAL {{ ?person rdfs:label ?personLabelJa . FILTER(LANG(?personLabelJa) = "ja") }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ja" . }}
}}
LIMIT {limit}
OFFSET {offset}
"""

# プロパティマッピング
WIKIDATA_ROLE_MAP = {
    "P57": "director",
    "P1040": "animation director",
    "P3174": "episode_director",
    "P58": "episode director",
}

app = typer.Typer()

CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "jvmg_checkpoint.json"


def _load_checkpoint(path: Path) -> dict | None:
    """チェックポイントファイルを読み込む."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def _save_checkpoint(path: Path, data: dict) -> None:
    """チェックポイントファイルを保存する."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _delete_checkpoint(path: Path) -> None:
    """チェックポイントファイルを削除する."""
    if path.exists():
        path.unlink()


class WikidataClient:
    """Wikidata SPARQL 非同期クライアント."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=120.0,
            headers={
                "User-Agent": "AnimetorEval/0.1 (research project)",
                "Accept": "application/sparql-results+json",
            },
        )
        self._last_request_time = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def query(self, sparql: str) -> list[dict]:
        """SPARQL クエリを実行."""
        cache_key = {"sparql": sparql}
        cached = load_cached_json("wikidata/sparql", cache_key)
        if cached is not None:
            return cached

        await self._rate_limit()
        for attempt in range(3):
            try:
                resp = await self._client.get(
                    WIKIDATA_SPARQL,
                    params={"query": sparql, "format": "json"},
                )
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning(
                        "wikidata_rate_limited",
                        source="wikidata",
                        retry_after_seconds=wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                result = data.get("results", {}).get("bindings", [])
                save_cached_json("wikidata/sparql", cache_key, result)
                return result
            except httpx.HTTPError as e:
                log.warning(
                    "wikidata_query_failed",
                    source="wikidata",
                    attempt=attempt + 1,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
        from src.scrapers.exceptions import EndpointUnreachableError

        log.error("wikidata_sparql_unreachable", source="wikidata", url=WIKIDATA_SPARQL)
        raise EndpointUnreachableError(
            "Wikidata SPARQL endpoint unreachable after 3 attempts",
            source="wikidata",
            url=WIKIDATA_SPARQL,
        )


def parse_wikidata_results(
    bindings: list[dict],
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    """Wikidata SPARQL 結果をパースする."""
    anime_map: dict[str, BronzeAnime] = {}
    person_map: dict[str, Person] = {}
    credits: list[Credit] = []

    for row in bindings:
        anime_uri = row.get("anime", {}).get("value", "")
        anime_label = row.get("animeLabel", {}).get("value", "")
        year_str = row.get("year", {}).get("value", "")
        person_uri = row.get("person", {}).get("value", "")
        person_label = row.get("personLabel", {}).get("value", "")
        person_label_ja = row.get("personLabelJa", {}).get("value", "")
        role_label = row.get("roleLabel", {}).get("value", "")

        if not anime_uri or not person_uri:
            continue

        anime_qid = anime_uri.split("/")[-1]
        person_qid = person_uri.split("/")[-1]
        anime_id = f"wd:{anime_qid}"
        person_id = f"wd:p{person_qid}"

        if anime_id not in anime_map:
            year = None
            if year_str:
                try:
                    year = int(float(year_str))
                except ValueError:
                    pass
            anime_map[anime_id] = BronzeAnime(
                id=anime_id,
                title_en=anime_label,
                year=year,
            )

        if person_id not in person_map:
            person_map[person_id] = Person(
                id=person_id,
                name_ja=person_label_ja,
                name_en=person_label,
            )

        role = parse_role(role_label) if role_label else parse_role("other")
        credits.append(
            Credit(
                person_id=person_id,
                anime_id=anime_id,
                role=role,
                source="wikidata",
            )
        )

    return list(anime_map.values()), list(person_map.values()), credits


async def fetch_anime_staff(
    max_records: int = 5000,
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    """Wikidata からアニメスタッフデータを取得."""
    client = WikidataClient()
    all_anime: list[BronzeAnime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []

    page_size = 500
    offset = 0

    try:
        while offset < max_records:
            log.info("fetching_wikidata", source="wikidata", offset=offset)
            query = ANIME_STAFF_QUERY.format(limit=page_size, offset=offset)
            bindings = await client.query(query)
            if not bindings:
                break

            anime, persons, credits = parse_wikidata_results(bindings)
            all_anime.extend(anime)
            all_persons.extend(persons)
            all_credits.extend(credits)

            if len(bindings) < page_size:
                break
            offset += page_size
    finally:
        await client.close()

    log.info(
        "wikidata_fetch_complete",
        source="wikidata",
        item_count=len(all_credits),
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    max_records: int = typer.Option(5000, "--max-records", "-n", help="最大レコード数"),
    resume: bool = typer.Option(
        True, "--resume/--no-resume", help="チェックポイントから再開する"
    ),
    checkpoint_interval: int = typer.Option(
        2, "--checkpoint-interval", help="チェックポイント保存間隔 (ページ数)"
    ),
) -> None:
    """Wikidata からアニメスタッフデータを収集する."""
    from src.database import (
        db_connection,
        init_db,
        insert_credit,
        update_data_source,
        upsert_anime,
        upsert_person,
    )
    from src.log import setup_logging

    setup_logging()

    # Load checkpoint if resuming
    start_offset = 0
    if resume:
        checkpoint = _load_checkpoint(CHECKPOINT_FILE)
        if checkpoint:
            start_offset = checkpoint.get("last_offset", 0)
            log.info(
                "checkpoint_loaded",
                last_offset=start_offset,
                total_anime=checkpoint.get("total_anime", 0),
                total_persons=checkpoint.get("total_persons", 0),
                total_credits=checkpoint.get("total_credits", 0),
                timestamp=checkpoint.get("timestamp"),
            )

    async def _fetch_and_save() -> None:
        """Fetch Wikidata records incrementally with checkpoint support."""
        client = WikidataClient()
        total_anime = 0
        total_persons = 0
        total_credits = 0

        page_size = 500
        offset = start_offset
        pages_since_checkpoint = 0

        try:
            with db_connection() as conn:
                init_db(conn)

                while offset < max_records:
                    log.info("fetching_wikidata", source="wikidata", offset=offset)
                    query = ANIME_STAFF_QUERY.format(limit=page_size, offset=offset)
                    bindings = await client.query(query)
                    if not bindings:
                        break

                    anime_list, persons, credits = parse_wikidata_results(bindings)
                    for anime in anime_list:
                        upsert_anime(conn, anime)
                        total_anime += 1
                    for person in persons:
                        upsert_person(conn, person)
                        total_persons += 1
                    for credit in credits:
                        insert_credit(conn, credit)
                        total_credits += 1

                    pages_since_checkpoint += 1
                    offset += page_size

                    # Save checkpoint every N pages
                    if pages_since_checkpoint >= checkpoint_interval:
                        conn.commit()
                        _save_checkpoint(
                            CHECKPOINT_FILE,
                            {
                                "last_offset": offset,
                                "total_anime": total_anime,
                                "total_persons": total_persons,
                                "total_credits": total_credits,
                                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                            },
                        )
                        log.info("checkpoint_saved", last_offset=offset)
                        pages_since_checkpoint = 0

                    if len(bindings) < page_size:
                        break

                update_data_source(conn, "jvmg", total_credits)

        finally:
            await client.close()

        # Delete checkpoint on successful completion
        _delete_checkpoint(CHECKPOINT_FILE)
        log.info(
            "saved_to_db",
            source="wikidata",
            anime=total_anime,
            persons=total_persons,
            credits=total_credits,
        )

    asyncio.run(_fetch_and_save())


if __name__ == "__main__":
    app()
