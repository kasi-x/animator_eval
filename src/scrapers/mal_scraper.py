"""MyAnimeList クレジットデータ収集 (Jikan API v4 経由).

httpx + structlog + typer で構成。
レート制限: 3 requests/second, 60 requests/minute。
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

BASE_URL = "https://api.jikan.moe/v4"
REQUEST_INTERVAL = 0.4

app = typer.Typer()

CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "mal_checkpoint.json"


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
                    log.warning(
                        "rate_limited",
                        source="mal",
                        retry_after_seconds=retry_after,
                        url=url,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as e:
                log.warning(
                    "request_failed",
                    source="mal",
                    attempt=attempt + 1,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
        from src.scrapers.exceptions import EndpointUnreachableError

        raise EndpointUnreachableError(
            f"Failed to fetch {url} after 5 attempts",
            source="mal",
            url=url,
        )

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

    async def get_all_anime(
        self, page: int = 1, limit: int = 25
    ) -> dict:
        """全アニメをページネーション取得 (order_by=mal_id で安定順序)."""
        params: dict = {"page": page, "limit": limit, "order_by": "mal_id", "sort": "asc"}
        return await self.get("/anime", params=params)


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
    count: int = typer.Option(50, "--count", "-n", help="取得するアニメ数 (0=全アニメ)"),
    type_filter: str = typer.Option("tv", "--type", help="アニメタイプ (空欄=全タイプ)"),
    resume: bool = typer.Option(
        True, "--resume/--no-resume", help="チェックポイントから再開する"
    ),
    checkpoint_interval: int = typer.Option(
        50, "--checkpoint-interval", help="チェックポイント保存間隔 (アニメ数)"
    ),
    fetch_all: bool = typer.Option(
        False, "--all", help="全アニメを取得 (count=0相当)"
    ),
) -> None:
    """MAL (Jikan API) からクレジットデータを収集する."""
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

    # --all フラグで全アニメを取得
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

        try:
            with db_connection() as conn:
                init_db(conn)

                # Load existing MAL IDs to avoid duplicates
                cursor = conn.execute("SELECT mal_id FROM anime WHERE mal_id IS NOT NULL")
                existing_mal_ids = {row[0] for row in cursor.fetchall()}
                log.info("loaded_existing_mal_ids", count=len(existing_mal_ids))

                is_fetching_all = count == 0
                log.info(
                    "mal_fetch_start",
                    fetch_all=is_fetching_all,
                    count=count if count > 0 else "unlimited",
                    start_page=current_page,
                )

                while True:
                    # 全アニメ取得モード: /anime エンドポイント使用
                    if is_fetching_all:
                        log.info("fetching_all_anime", source="mal", page=current_page)
                        resp = await client.get_all_anime(page=current_page, limit=25)
                    else:
                        # 人気アニメTop N: /top/anime エンドポイント使用
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

                        # Skip anime that already exist in DB to avoid UNIQUE constraint violation
                        if anime.mal_id and anime.mal_id in existing_mal_ids:
                            log.info(
                                "skipping_existing_anime",
                                source="mal",
                                mal_id=anime.mal_id,
                                title=anime.display_title,
                            )
                            continue

                        log.info(
                            "fetching_staff",
                            source="mal",
                            progress=f"{fetched}" if is_fetching_all else f"{fetched}/{count}",
                            title=anime.display_title,
                        )
                        upsert_anime(conn, anime)
                        total_anime += 1

                        try:
                            staff = await client.get_anime_staff(anime.mal_id)
                            persons, credits = parse_staff_data(staff, anime.id)
                            for p in persons:
                                if p.id not in seen:
                                    upsert_person(conn, p)
                                    seen.add(p.id)
                                    total_persons += 1
                            for c in credits:
                                insert_credit(conn, c)
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

                        # Save checkpoint every N anime
                        if fetched % checkpoint_interval == 0:
                            conn.commit()
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
                            log.info("checkpoint_saved", page=current_page, fetched=fetched)

                    if not is_fetching_all and fetched >= count:
                        break

                    current_page += 1

                conn.commit()
                update_data_source(conn, "mal", total_credits)

        finally:
            await client.close()

        # Delete checkpoint on successful completion
        _delete_checkpoint(CHECKPOINT_FILE)
        log.info(
            "saved_to_db",
            source="mal",
            anime=total_anime,
            persons=total_persons,
            credits=total_credits,
        )

    asyncio.run(_fetch_and_save())


if __name__ == "__main__":
    app()
