"""Japanese Visual Media Graph (JVMG) データ取得 via Wikidata SPARQL.

Wikidata は日本のアニメ作品とスタッフの情報を RDF で公開している。
SPARQL エンドポイント: https://query.wikidata.org/sparql
"""

import asyncio
import time

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

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
    "P3174": "storyboard",
    "P58": "episode director",
}

app = typer.Typer()


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
        await self._rate_limit()
        for attempt in range(3):
            try:
                resp = await self._client.get(
                    WIKIDATA_SPARQL,
                    params={"query": sparql, "format": "json"},
                )
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 30))
                    log.warning("wikidata_rate_limited", retry_after=wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", {}).get("bindings", [])
            except httpx.HTTPError as e:
                log.warning("wikidata_query_failed", attempt=attempt + 1, error=str(e))
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
        log.error("wikidata_sparql_unreachable")
        return []


def parse_wikidata_results(
    bindings: list[dict],
) -> tuple[list[Anime], list[Person], list[Credit]]:
    """Wikidata SPARQL 結果をパースする."""
    anime_map: dict[str, Anime] = {}
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
            anime_map[anime_id] = Anime(
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
) -> tuple[list[Anime], list[Person], list[Credit]]:
    """Wikidata からアニメスタッフデータを取得."""
    client = WikidataClient()
    all_anime: list[Anime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []

    page_size = 500
    offset = 0

    try:
        while offset < max_records:
            log.info("fetching_wikidata", offset=offset)
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
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    max_records: int = typer.Option(5000, "--max-records", "-n", help="最大レコード数"),
) -> None:
    """Wikidata からアニメスタッフデータを収集する."""
    from src.database import get_connection, init_db, insert_credit, update_data_source, upsert_anime, upsert_person
    from src.log import setup_logging

    setup_logging()

    anime_list, persons, credits = asyncio.run(
        fetch_anime_staff(max_records=max_records)
    )

    conn = get_connection()
    init_db(conn)
    for anime in anime_list:
        upsert_anime(conn, anime)
    for person in persons:
        upsert_person(conn, person)
    for credit in credits:
        insert_credit(conn, credit)
    update_data_source(conn, "jvmg", len(credits))
    conn.commit()
    conn.close()

    log.info("saved_to_db", anime=len(anime_list), persons=len(persons), credits=len(credits))


if __name__ == "__main__":
    app()
