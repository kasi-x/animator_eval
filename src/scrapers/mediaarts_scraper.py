"""メディア芸術データベース (MADB) からのデータ収集.

メディア芸術データベース LOD: https://mediaarts-db.bunka.go.jp/
SPARQL エンドポイント: https://mediaarts-db.bunka.go.jp/sparql

注意: エンドポイントの安定性が不明のため、フォールバック処理を含む。
"""

import asyncio
import time

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

SPARQL_ENDPOINT = "https://mediaarts-db.artmuseums.go.jp/sparql"
REQUEST_INTERVAL = 1.0  # 秒

# アニメ作品とスタッフのクエリ
ANIME_STAFF_QUERY = """
PREFIX schema: <http://schema.org/>
PREFIX ma: <https://mediaarts-db.bunka.go.jp/data/property/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?anime ?title ?year ?person ?personName ?role
WHERE {{
  ?anime a schema:TVSeries ;
         schema:genre "アニメーション" ;
         schema:name ?title .
  OPTIONAL {{ ?anime schema:datePublished ?year . }}
  ?anime schema:contributor ?contribution .
  ?contribution schema:agent ?person ;
                schema:roleName ?role .
  ?person schema:name ?personName .
}}
LIMIT {limit}
OFFSET {offset}
"""

app = typer.Typer()


class MediaArtsClient:
    """メディア芸術DB SPARQL 非同期クライアント."""

    def __init__(self) -> None:
        """Initialize with system CA first, fall back to insecure if needed.

        SSL verification strategy:
        1. Try system CA bundle (verify=True)
        2. If cert validation fails, log warning and fall back to verify=False
        """
        # Start with system CA verification enabled
        self._verify = True
        self._client = httpx.AsyncClient(timeout=60.0, verify=True, follow_redirects=True)
        self._last_request_time = 0.0
        log.info("mediaarts_client_init", ssl_verify=True, follow_redirects=True)

    async def _fallback_to_insecure(self) -> None:
        """Fall back to insecure SSL verification with warning.

        Called when system CA verification fails. Creates new client with verify=False.
        """
        if self._verify:
            self._verify = False
            await self._client.aclose()
            self._client = httpx.AsyncClient(timeout=60.0, verify=False, follow_redirects=True)
            log.warning(
                "ssl_verification_failed_falling_back",
                source="mediaarts",
                message="System SSL verification failed, falling back to insecure (verify=False)",
            )

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def query(self, sparql: str) -> list[dict]:
        """SPARQL クエリを実行.

        SSL verification strategy:
        1. Try with system CA first
        2. On SSL cert error, fall back to insecure and retry
        3. Give up after 3 total attempts
        """
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
                # SSL verification failed - try fallback once
                if not ssl_fallback_tried and self._verify:
                    ssl_fallback_tried = True
                    await self._fallback_to_insecure()
                    log.info("ssl_fallback_retry", attempt=attempt + 1)
                    await asyncio.sleep(1)
                    continue
                log.warning(
                    "sparql_query_failed",
                    source="mediaarts",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    attempt=attempt + 1,
                )
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
            except httpx.HTTPError as e:
                log.warning(
                    "sparql_query_failed",
                    source="mediaarts",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    attempt=attempt + 1,
                )
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))

        from src.scrapers.exceptions import EndpointUnreachableError

        log.error("sparql_endpoint_unreachable", source="mediaarts", url=SPARQL_ENDPOINT)
        raise EndpointUnreachableError(
            "MediaArts SPARQL endpoint unreachable after 3 attempts",
            source="mediaarts",
            url=SPARQL_ENDPOINT,
        )

    async def fetch_anime_staff(
        self, limit: int = 1000, offset: int = 0
    ) -> list[dict]:
        """アニメスタッフデータを取得."""
        query = ANIME_STAFF_QUERY.format(limit=limit, offset=offset)
        return await self.query(query)


def parse_sparql_results(
    bindings: list[dict],
) -> tuple[list[Anime], list[Person], list[Credit]]:
    """SPARQL 結果をパースする."""
    anime_map: dict[str, Anime] = {}
    person_map: dict[str, Person] = {}
    credits: list[Credit] = []

    for row in bindings:
        anime_uri = row.get("anime", {}).get("value", "")
        title = row.get("title", {}).get("value", "")
        year_str = row.get("year", {}).get("value", "")
        person_uri = row.get("person", {}).get("value", "")
        person_name = row.get("personName", {}).get("value", "")
        role_str = row.get("role", {}).get("value", "")

        if not anime_uri or not person_uri:
            continue

        # Anime
        anime_id = f"madb:{anime_uri.split('/')[-1]}"
        if anime_id not in anime_map:
            year = None
            if year_str:
                try:
                    year = int(year_str[:4])
                except ValueError:
                    pass
            anime_map[anime_id] = Anime(
                id=anime_id,
                title_ja=title,
                year=year,
            )

        # Person
        person_id = f"madb:p{person_uri.split('/')[-1]}"
        if person_id not in person_map:
            person_map[person_id] = Person(
                id=person_id,
                name_ja=person_name,
            )

        # Credit
        role = parse_role(role_str)
        credits.append(
            Credit(
                person_id=person_id,
                anime_id=anime_id,
                role=role,
                source="mediaarts",
            )
        )

    return list(anime_map.values()), list(person_map.values()), credits


async def fetch_all_anime_staff(
    max_records: int = 5000,
) -> tuple[list[Anime], list[Person], list[Credit]]:
    """全アニメスタッフデータをページング取得."""
    client = MediaArtsClient()
    all_anime: list[Anime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []

    page_size = 1000
    offset = 0

    try:
        while offset < max_records:
            log.info("fetching_madb", offset=offset)
            bindings = await client.fetch_anime_staff(limit=page_size, offset=offset)
            if not bindings:
                break

            anime, persons, credits = parse_sparql_results(bindings)
            all_anime.extend(anime)
            all_persons.extend(persons)
            all_credits.extend(credits)

            if len(bindings) < page_size:
                break
            offset += page_size
    finally:
        await client.close()

    log.info(
        "madb_fetch_complete",
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    max_records: int = typer.Option(5000, "--max-records", "-n", help="最大レコード数"),
) -> None:
    """メディア芸術DB からクレジットデータを収集する."""
    from src.database import db_connection, init_db, insert_credit, update_data_source, upsert_anime, upsert_person
    from src.log import setup_logging

    setup_logging()

    anime_list, persons, credits = asyncio.run(
        fetch_all_anime_staff(max_records=max_records)
    )

    with db_connection() as conn:
        init_db(conn)
        for anime in anime_list:
            upsert_anime(conn, anime)
        for person in persons:
            upsert_person(conn, person)
        for credit in credits:
            insert_credit(conn, credit)
        update_data_source(conn, "mediaarts", len(credits))

    log.info("saved_to_db", anime=len(anime_list), persons=len(persons), credits=len(credits))


if __name__ == "__main__":
    app()
