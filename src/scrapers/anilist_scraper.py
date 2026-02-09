"""AniList GraphQL API によるスタッフクレジット収集.

httpx (async) + structlog + typer で構成。
レート制限: 90 requests/minute。
"""

import asyncio
import time

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

ANILIST_URL = "https://graphql.anilist.co"
REQUEST_INTERVAL = 0.7

ANIME_STAFF_QUERY = """
query ($id: Int, $page: Int, $perPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    title { romaji english native }
    seasonYear
    season
    episodes
    averageScore
    staff(page: $page, perPage: $perPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native }
        }
      }
    }
  }
}
"""

TOP_ANIME_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    media(type: ANIME, format: TV, sort: POPULARITY_DESC) {
      id
      title { romaji english native }
      seasonYear
      season
      episodes
      averageScore
    }
  }
}
"""

app = typer.Typer()


class AniListClient:
    """AniList GraphQL 非同期クライアント."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        self._client = httpx.AsyncClient(timeout=60.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def query(self, query: str, variables: dict) -> dict:
        await self._rate_limit()
        for attempt in range(5):
            try:
                resp = await self._client.post(
                    ANILIST_URL,
                    json={"query": query, "variables": variables},
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("rate_limited", retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    log.warning("graphql_errors", errors=data["errors"])
                return data.get("data", {})
            except httpx.HTTPError as e:
                log.warning("request_failed", attempt=attempt + 1, error=str(e))
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
        raise RuntimeError("Failed to query AniList after 5 attempts")

    async def get_top_anime(self, page: int = 1, per_page: int = 50) -> dict:
        return await self.query(TOP_ANIME_QUERY, {"page": page, "perPage": per_page})

    async def get_anime_staff(
        self, anilist_id: int, page: int = 1, per_page: int = 25
    ) -> dict:
        return await self.query(
            ANIME_STAFF_QUERY,
            {"id": anilist_id, "page": page, "perPage": per_page},
        )


def parse_anilist_anime(raw: dict) -> Anime:
    anilist_id = raw["id"]
    title = raw.get("title", {})
    season_map = {"WINTER": "winter", "SPRING": "spring", "SUMMER": "summer", "FALL": "fall"}
    avg = raw.get("averageScore")
    return Anime(
        id=f"anilist:{anilist_id}",
        title_ja=title.get("native") or "",
        title_en=title.get("english") or title.get("romaji") or "",
        year=raw.get("seasonYear"),
        season=season_map.get(raw.get("season", ""), None),
        episodes=raw.get("episodes"),
        anilist_id=anilist_id,
        score=avg / 10.0 if avg else None,
    )


def parse_anilist_staff(
    staff_edges: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    persons = []
    credits = []
    for edge in staff_edges:
        node = edge.get("node", {})
        anilist_person_id = node.get("id")
        if not anilist_person_id:
            continue
        person_id = f"anilist:p{anilist_person_id}"
        name = node.get("name", {})
        persons.append(
            Person(
                id=person_id,
                name_ja=name.get("native") or "",
                name_en=name.get("full") or "",
                anilist_id=anilist_person_id,
            )
        )
        role = parse_role(edge.get("role", ""))
        credits.append(
            Credit(person_id=person_id, anime_id=anime_id, role=role, source="anilist")
        )
    return persons, credits


async def fetch_top_anime_credits(
    n_anime: int = 50,
) -> tuple[list[Anime], list[Person], list[Credit]]:
    client = AniListClient()
    all_anime: list[Anime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []
    seen_person_ids: set[str] = set()

    try:
        pages_needed = (n_anime + 49) // 50
        anime_ids: list[tuple[int, str]] = []

        for page in range(1, pages_needed + 1):
            log.info("fetching_top_anime", page=page)
            resp = await client.get_top_anime(page=page, per_page=50)
            page_data = resp.get("Page", {})
            for raw in page_data.get("media", []):
                if len(anime_ids) >= n_anime:
                    break
                anime = parse_anilist_anime(raw)
                all_anime.append(anime)
                anime_ids.append((anime.anilist_id, anime.id))

        for i, (anilist_id, anime_id) in enumerate(anime_ids):
            log.info("fetching_staff", progress=f"{i+1}/{len(anime_ids)}", anime_id=anime_id)
            try:
                staff_page = 1
                while True:
                    resp = await client.get_anime_staff(anilist_id, page=staff_page)
                    media = resp.get("Media", {})
                    staff = media.get("staff", {})
                    edges = staff.get("edges", [])
                    persons, credits = parse_anilist_staff(edges, anime_id)
                    for p in persons:
                        if p.id not in seen_person_ids:
                            all_persons.append(p)
                            seen_person_ids.add(p.id)
                    all_credits.extend(credits)
                    if not staff.get("pageInfo", {}).get("hasNextPage"):
                        break
                    staff_page += 1
            except Exception as e:
                log.error("staff_fetch_failed", anime_id=anime_id, error=str(e))
    finally:
        await client.close()

    log.info(
        "fetch_complete",
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(count: int = typer.Option(50, "--count", "-n", help="取得するアニメ数")) -> None:
    """AniList からクレジットデータを収集する."""
    from src.database import get_connection, init_db, insert_credit, update_data_source, upsert_anime, upsert_person
    from src.log import setup_logging

    setup_logging()

    anime_list, persons, credits = asyncio.run(fetch_top_anime_credits(n_anime=count))

    conn = get_connection()
    init_db(conn)
    for anime in anime_list:
        upsert_anime(conn, anime)
    for person in persons:
        upsert_person(conn, person)
    for credit in credits:
        insert_credit(conn, credit)
    update_data_source(conn, "anilist", len(credits))
    conn.commit()
    conn.close()

    log.info("saved_to_db", anime=len(anime_list), persons=len(persons), credits=len(credits))


if __name__ == "__main__":
    app()
