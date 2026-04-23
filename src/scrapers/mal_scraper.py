"""MyAnimeList credit data collection (via Jikan API v4).

Built with httpx + structlog + typer.
Rate limit: 3 requests/second, 60 requests/minute.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import structlog
import typer

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging

log = structlog.get_logger()

BASE_URL = "https://api.jikan.moe/v4"
REQUEST_INTERVAL = 0.4

app = typer.Typer()

CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "mal_checkpoint.json"


def _load_checkpoint(path: Path) -> dict | None:
    """Load a checkpoint file."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def _save_checkpoint(path: Path, data: dict) -> None:
    """Save a checkpoint file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _delete_checkpoint(path: Path) -> None:
    """Delete a checkpoint file."""
    if path.exists():
        path.unlink()


class JikanClient:
    """Async Jikan API client (wraps RetryingHttpClient)."""

    def __init__(self, transport=None) -> None:
        self._http = RetryingHttpClient(
            source="mal",
            base_url=BASE_URL,
            delay=REQUEST_INTERVAL,
            timeout=30.0,
            headers={"Accept": "application/json"},
            transport=transport,
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def get(self, endpoint: str, params: dict | None = None) -> dict:
        cache_key = {"endpoint": endpoint, "params": params or {}}
        cached = load_cached_json("mal/rest", cache_key)
        if cached is not None:
            return cached

        resp = await self._http.get(endpoint, params=params)
        resp.raise_for_status()
        data = resp.json()
        save_cached_json("mal/rest", cache_key, data)
        return data

    async def get_anime_staff(self, mal_id: int) -> list[dict]:
        data = await self.get(f"/anime/{mal_id}/staff")
        return data.get("data", [])

    async def get_top_anime(
        self, page: int = 1, limit: int = 25, type_filter: str = "tv"
    ) -> dict:
        params: dict = {"page": page, "limit": limit}
        if type_filter:
            params["type"] = type_filter
        return await self.get("/top/anime", params=params)

    async def get_all_anime(self, page: int = 1, limit: int = 25) -> dict:
        """Fetch all anime via pagination (stable order by mal_id)."""
        params: dict = {
            "page": page,
            "limit": limit,
            "order_by": "mal_id",
            "sort": "asc",
        }
        return await self.get("/anime", params=params)


def parse_anime_data(raw: dict) -> BronzeAnime:
    mal_id = raw.get("mal_id")
    titles = raw.get("titles", [])
    title_ja, title_en = "", ""
    synonyms: list[str] = []
    for t in titles:
        if t.get("type") == "Japanese":
            title_ja = t.get("title", "")
        elif t.get("type") == "Default":
            title_en = t.get("title", "")
        elif t.get("type") == "English" and not title_en:
            title_en = t.get("title", "")
        elif t.get("type") == "Synonym":
            synonyms.append(t.get("title", ""))
    if not title_en:
        title_en = raw.get("title", "")

    aired = raw.get("aired", {}) or {}
    prop = aired.get("prop", {}) or {}
    from_prop = prop.get("from", {}) or {}
    to_prop = prop.get("to", {}) or {}
    year = raw.get("year") or from_prop.get("year")

    def _build_date(p: dict) -> str | None:
        y, m, d = p.get("year"), p.get("month"), p.get("day")
        if not y:
            return None
        if m and d:
            return f"{y:04d}-{m:02d}-{d:02d}"
        if m:
            return f"{y:04d}-{m:02d}"
        return str(y)

    genres = [g["name"] for g in raw.get("genres", []) if g.get("name")]

    return BronzeAnime(
        id=f"mal:{mal_id}",
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        season=raw.get("season"),
        episodes=raw.get("episodes"),
        mal_id=mal_id,
        score=raw.get("score"),
        format=raw.get("type"),
        status=raw.get("status"),
        start_date=_build_date(from_prop),
        end_date=_build_date(to_prop),
        genres=genres,
        synonyms=synonyms,
    )


def parse_staff_data(
    staff_list: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
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
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role(pos),
                    source="mal",
                )
            )
    return persons, credits


async def fetch_top_anime_credits(
    n_anime: int = 50, type_filter: str = "tv"
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    client = JikanClient()
    all_anime, all_persons, all_credits = [], [], []
    seen: set[str] = set()
    fetched = 0

    try:
        pages_needed = (n_anime + 24) // 25
        for page in range(1, pages_needed + 1):
            if fetched >= n_anime:
                break
            log.info("fetching_top_anime", source="mal", page=page)
            resp = await client.get_top_anime(
                page=page, limit=25, type_filter=type_filter
            )
            for raw_anime in resp.get("data", []):
                if fetched >= n_anime:
                    break
                anime = parse_anime_data(raw_anime)
                all_anime.append(anime)
                fetched += 1
                log.info(
                    "fetching_staff",
                    source="mal",
                    progress=f"{fetched}/{n_anime}",
                    title=anime.display_title,
                )
                try:
                    staff = await client.get_anime_staff(anime.mal_id)
                    persons, credits = parse_staff_data(staff, anime.id)
                    for p in persons:
                        if p.id not in seen:
                            all_persons.append(p)
                            seen.add(p.id)
                    all_credits.extend(credits)
                    log.info(
                        "staff_fetched",
                        source="mal",
                        item_count=len(credits),
                        staff=len(persons),
                        credits=len(credits),
                    )
                except Exception as e:
                    log.error(
                        "staff_fetch_failed",
                        source="mal",
                        anime_id=anime.id,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
    finally:
        await client.close()

    log.info(
        "fetch_complete",
        source="mal",
        item_count=len(all_credits),
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    count: int = typer.Option(
        50, "--count", "-n", help="number of anime to fetch (0=all)"
    ),
    type_filter: str = typer.Option(
        "tv", "--type", help="anime type (blank=all types)"
    ),
    resume: bool = typer.Option(
        True, "--resume/--no-resume", help="resume from checkpoint"
    ),
    checkpoint_interval: int = typer.Option(
        50, "--checkpoint-interval", help="checkpoint save interval (anime count)"
    ),
    fetch_all: bool = typer.Option(False, "--all", help="fetch all anime (equivalent to count=0)"),
) -> None:
    """Collect credit data from MAL (Jikan API)."""
    from src.infra.log import setup_logging
    from src.scrapers.bronze_writer import BronzeWriter

    setup_logging()
    log_path = configure_file_logging("mal")
    log.info("mal_scrape_command_start", log_file=str(log_path), count=count)

    # --all flag: fetch all anime
    if fetch_all:
        count = 0

    # Load checkpoint if resuming
    start_index = 0
    start_page = 1
    if resume:
        checkpoint = _load_checkpoint(CHECKPOINT_FILE)
        if checkpoint:
            start_page = checkpoint.get("last_fetched_page", 1)
            start_index = checkpoint.get("last_fetched_index", 0)
            log.info(
                "checkpoint_loaded",
                last_fetched_page=start_page,
                last_fetched_index=start_index,
                total_anime=checkpoint.get("total_anime", 0),
                total_persons=checkpoint.get("total_persons", 0),
                total_credits=checkpoint.get("total_credits", 0),
                timestamp=checkpoint.get("timestamp"),
            )

    async def _fetch_and_save() -> None:
        """Fetch anime credits incrementally with checkpoint support."""
        client = JikanClient()
        seen: set[str] = set()
        total_anime = 0
        total_persons = 0
        total_credits = 0
        fetched = 0
        current_page = start_page

        anime_bw = BronzeWriter("mal", table="anime")
        persons_bw = BronzeWriter("mal", table="persons")
        credits_bw = BronzeWriter("mal", table="credits")

        # existing_mal_ids dedup check is omitted; ETL (integrate) handles dedup
        existing_mal_ids: set[int] = set()

        is_fetching_all = count == 0
        log.info(
            "mal_fetch_start",
            fetch_all=is_fetching_all,
            count=count if count > 0 else "unlimited",
            start_page=current_page,
        )

        try:
            while True:
                # all-anime mode: use /anime endpoint
                if is_fetching_all:
                    log.info("fetching_all_anime", source="mal", page=current_page)
                    resp = await client.get_all_anime(page=current_page, limit=25)
                else:
                    # top-N popular anime: use /top/anime endpoint
                    pages_needed = (count + 24) // 25
                    if current_page > pages_needed:
                        break
                    log.info("fetching_top_anime", source="mal", page=current_page)
                    resp = await client.get_top_anime(
                        page=current_page,
                        limit=25,
                        type_filter=type_filter,
                    )

                anime_data = resp.get("data", [])
                if not anime_data:
                    log.info("mal_no_more_data", page=current_page)
                    break

                for raw_anime in anime_data:
                    if not is_fetching_all and fetched >= count:
                        break

                    anime = parse_anime_data(raw_anime)
                    fetched += 1

                    # Skip already-processed anime on resume
                    if fetched <= start_index:
                        continue

                    if anime.mal_id and anime.mal_id in existing_mal_ids:
                        continue

                    log.info(
                        "fetching_staff",
                        source="mal",
                        progress=f"{fetched}"
                        if is_fetching_all
                        else f"{fetched}/{count}",
                        title=anime.display_title,
                    )
                    anime_bw.append(anime.model_dump(mode="json"))
                    total_anime += 1

                    try:
                        staff = await client.get_anime_staff(anime.mal_id)
                        persons, credits = parse_staff_data(staff, anime.id)
                        for p in persons:
                            if p.id not in seen:
                                persons_bw.append(p.model_dump(mode="json"))
                                seen.add(p.id)
                                total_persons += 1
                        for c in credits:
                            credits_bw.append(c.model_dump(mode="json"))
                            total_credits += 1
                        log.info(
                            "staff_fetched",
                            source="mal",
                            staff=len(persons),
                            credits=len(credits),
                        )
                    except Exception as e:
                        log.error(
                            "staff_fetch_failed",
                            source="mal",
                            anime_id=anime.id,
                            error=str(e),
                        )

                    # Flush checkpoint every N anime
                    if fetched % checkpoint_interval == 0:
                        anime_bw.flush()
                        persons_bw.flush()
                        credits_bw.flush()
                        _save_checkpoint(
                            CHECKPOINT_FILE,
                            {
                                "last_fetched_page": current_page,
                                "last_fetched_index": fetched,
                                "total_anime": total_anime,
                                "total_persons": total_persons,
                                "total_credits": total_credits,
                                "timestamp": datetime.now(
                                    tz=timezone.utc
                                ).isoformat(),
                            },
                        )
                        log.info(
                            "checkpoint_saved", page=current_page, fetched=fetched
                        )

                if not is_fetching_all and fetched >= count:
                    break

                current_page += 1

        finally:
            await client.close()
            anime_bw.flush()
            persons_bw.flush()
            credits_bw.flush()

        # Delete checkpoint on successful completion
        _delete_checkpoint(CHECKPOINT_FILE)
        log.info(
            "bronze_parquet_written",
            source="mal",
            anime=total_anime,
            persons=total_persons,
            credits=total_credits,
        )

    asyncio.run(_fetch_and_save())


if __name__ == "__main__":
    app()
