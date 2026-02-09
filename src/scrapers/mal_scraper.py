"""MyAnimeList クレジットデータ収集 (Jikan API v4 経由).

httpx + structlog + typer で構成。
レート制限: 3 requests/second, 60 requests/minute。
"""

import asyncio
import time

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

BASE_URL = "https://api.jikan.moe/v4"
REQUEST_INTERVAL = 0.4

app = typer.Typer()


class JikanClient:
    """Jikan API 非同期クライアント."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"Accept": "application/json"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        await self._rate_limit()
        url = f"{BASE_URL}{endpoint}"
        for attempt in range(5):
            try:
                resp = await self._client.get(url, params=params)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 3))
                    log.warning("rate_limited", retry_after=retry_after, url=url)
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as e:
                log.warning("request_failed", attempt=attempt + 1, error=str(e))
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
        raise RuntimeError(f"Failed to fetch {url} after 5 attempts")

    async def get_anime_staff(self, mal_id: int) -> list[dict]:
        data = await self.get(f"/anime/{mal_id}/staff")
        return data.get("data", [])

    async def get_top_anime(self, page: int = 1, limit: int = 25, type_filter: str = "tv") -> dict:
        params: dict = {"page": page, "limit": limit}
        if type_filter:
            params["type"] = type_filter
        return await self.get("/top/anime", params=params)


def parse_anime_data(raw: dict) -> Anime:
    mal_id = raw.get("mal_id")
    titles = raw.get("titles", [])
    title_ja, title_en = "", ""
    for t in titles:
        if t.get("type") == "Japanese":
            title_ja = t.get("title", "")
        elif t.get("type") == "Default":
            title_en = t.get("title", "")
        elif t.get("type") == "English" and not title_en:
            title_en = t.get("title", "")
    if not title_en:
        title_en = raw.get("title", "")

    aired = raw.get("aired", {}) or {}
    prop = aired.get("prop", {}) or {}
    from_prop = prop.get("from", {}) or {}
    year = raw.get("year") or from_prop.get("year")

    return Anime(
        id=f"mal:{mal_id}",
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        season=raw.get("season"),
        episodes=raw.get("episodes"),
        mal_id=mal_id,
        score=raw.get("score"),
    )


def parse_staff_data(staff_list: list[dict], anime_id: str) -> tuple[list[Person], list[Credit]]:
    persons, credits = [], []
    for entry in staff_list:
        person_data = entry.get("person", {})
        mal_person_id = person_data.get("mal_id")
        if not mal_person_id:
            continue
        person_id = f"mal:p{mal_person_id}"
        name = person_data.get("name", "")
        parts = name.split(", ", 1)
        name_en = f"{parts[1]} {parts[0]}" if len(parts) == 2 else name
        persons.append(Person(id=person_id, name_en=name_en, mal_id=mal_person_id))
        for pos in entry.get("positions", []):
            credits.append(
                Credit(person_id=person_id, anime_id=anime_id, role=parse_role(pos), source="mal")
            )
    return persons, credits


async def fetch_top_anime_credits(
    n_anime: int = 50, type_filter: str = "tv"
) -> tuple[list[Anime], list[Person], list[Credit]]:
    client = JikanClient()
    all_anime, all_persons, all_credits = [], [], []
    seen: set[str] = set()
    fetched = 0

    try:
        pages_needed = (n_anime + 24) // 25
        for page in range(1, pages_needed + 1):
            if fetched >= n_anime:
                break
            log.info("fetching_top_anime", page=page)
            resp = await client.get_top_anime(page=page, limit=25, type_filter=type_filter)
            for raw_anime in resp.get("data", []):
                if fetched >= n_anime:
                    break
                anime = parse_anime_data(raw_anime)
                all_anime.append(anime)
                fetched += 1
                log.info("fetching_staff", progress=f"{fetched}/{n_anime}", title=anime.display_title)
                try:
                    staff = await client.get_anime_staff(anime.mal_id)
                    persons, credits = parse_staff_data(staff, anime.id)
                    for p in persons:
                        if p.id not in seen:
                            all_persons.append(p)
                            seen.add(p.id)
                    all_credits.extend(credits)
                    log.info("staff_fetched", staff=len(persons), credits=len(credits))
                except Exception as e:
                    log.error("staff_fetch_failed", anime_id=anime.id, error=str(e))
    finally:
        await client.close()

    log.info("fetch_complete", anime=len(all_anime), persons=len(all_persons), credits=len(all_credits))
    return all_anime, all_persons, all_credits


@app.command()
def main(
    count: int = typer.Option(50, "--count", "-n", help="取得するアニメ数"),
    type_filter: str = typer.Option("tv", "--type", help="アニメタイプ"),
) -> None:
    """MAL (Jikan API) からクレジットデータを収集する."""
    from src.database import get_connection, init_db, insert_credit, update_data_source, upsert_anime, upsert_person
    from src.log import setup_logging

    setup_logging()
    anime_list, persons, credits = asyncio.run(
        fetch_top_anime_credits(n_anime=count, type_filter=type_filter)
    )

    conn = get_connection()
    init_db(conn)
    for a in anime_list:
        upsert_anime(conn, a)
    for p in persons:
        upsert_person(conn, p)
    for c in credits:
        insert_credit(conn, c)
    update_data_source(conn, "mal", len(credits))
    conn.commit()
    conn.close()
    log.info("saved_to_db", anime=len(anime_list), persons=len(persons), credits=len(credits))


if __name__ == "__main__":
    app()
