"""AniList GraphQL API によるスタッフクレジット収集.

httpx (async) + structlog + typer で構成。
レート制限: 90 requests/minute (未認証) / より高い (認証済み)。
"""

import asyncio
import os
import time

import httpx
import structlog
import typer

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

ANILIST_URL = "https://graphql.anilist.co"
REQUEST_INTERVAL = 0.7

ANIME_STAFF_QUERY = """
query ($id: Int, $staffPage: Int, $staffPerPage: Int, $charPage: Int, $charPerPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    title { romaji english native }
    seasonYear
    season
    episodes
    averageScore
    coverImage { large extraLarge medium }
    bannerImage
    description
    format
    status
    startDate { year month day }
    endDate { year month day }
    duration
    source
    genres
    tags { name rank }
    popularity
    favourites
    studios { nodes { name } }
    staff(page: $staffPage, perPage: $staffPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native alternative }
          image { large medium }
          dateOfBirth { year month day }
          age
          gender
          yearsActive
          homeTown
          bloodType
          description
          favourites
          siteUrl
        }
      }
    }
    characters(page: $charPage, perPage: $charPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        voiceActors(language: JAPANESE) {
          id
          name { full native alternative }
          image { large medium }
          dateOfBirth { year month day }
          age
          gender
          yearsActive
          homeTown
          bloodType
          description
          favourites
          siteUrl
        }
      }
    }
  }
}
"""

TOP_ANIME_QUERY = """
query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    media(type: ANIME, sort: $sort) {
      id
      title { romaji english native }
      seasonYear
      season
      episodes
      averageScore
      coverImage { large extraLarge medium }
      bannerImage
      description
      format
      status
      startDate { year month day }
      endDate { year month day }
      duration
      source
      genres
      tags { name rank }
      popularity
      favourites
      studios { nodes { name } }
    }
  }
}
"""

app = typer.Typer()


# Batch save helper functions (must be defined before main())
def save_anime_batch_to_database(conn, anime_batch):
    """Save a batch of anime to database."""
    from src.database import upsert_anime
    for anime in anime_batch:
        upsert_anime(conn, anime)


def save_persons_batch_to_database(conn, persons_batch):
    """Save a batch of persons to database."""
    from src.database import upsert_person
    for person in persons_batch:
        upsert_person(conn, person)


def save_credits_batch_to_database(conn, credits_batch):
    """Save a batch of credits to database."""
    from src.database import insert_credit
    for credit in credits_batch:
        insert_credit(conn, credit)


class AniListClient:
    """AniList GraphQL 非同期クライアント (認証サポート)."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        # Load authentication token from environment
        self._access_token = os.getenv("ANILIST_ACCESS_TOKEN")
        if self._access_token:
            log.info("anilist_token_loaded", will_attempt_auth=True)
        else:
            log.info("anilist_no_token", will_use_unauthenticated=True)
        # Create client without default headers (auth added per-request in query())
        self._client = httpx.AsyncClient(timeout=60.0)

        # Rate limit tracking
        self.requests_remaining = None
        self.rate_limit_reset_at = None

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
                # Build headers with auth token if available (per AniList docs)
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                if self._access_token:
                    headers["Authorization"] = f"Bearer {self._access_token}"

                resp = await self._client.post(
                    ANILIST_URL,
                    json={"query": query, "variables": variables},
                    headers=headers,
                )

                # Extract rate limit info from response headers
                if "X-RateLimit-Remaining" in resp.headers:
                    self.requests_remaining = int(resp.headers.get("X-RateLimit-Remaining", -1))
                    self.rate_limit_reset_at = int(resp.headers.get("X-RateLimit-Reset", 0))

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("rate_limited", retry_after=retry_after, requests_remaining=self.requests_remaining)
                    await asyncio.sleep(retry_after)
                    continue

                # Handle invalid token: fall back to unauthenticated
                if resp.status_code in (400, 401) and self._access_token:
                    try:
                        data = resp.json()
                        errors = data.get("errors", [])
                        # Log detailed error info
                        log.warning("auth_error_response",
                                  status=resp.status_code,
                                  errors=errors,
                                  token_format="JWT" if "." in self._access_token else "non-JWT")
                        # Check if error is token-related
                        error_str = str(errors).lower()
                        if "token" in error_str or "auth" in error_str or "invalid" in error_str:
                            log.warning("invalid_token_disabling", errors=errors, fallback="unauthenticated")
                            self._access_token = None  # Disable token for future requests
                            continue  # Retry without token
                    except Exception as parse_error:
                        log.warning("failed_to_parse_error_response", error=str(parse_error))

                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    log.warning("graphql_errors", errors=data["errors"])
                return data.get("data", {})
            except httpx.HTTPError as e:
                log.warning("request_failed", attempt=attempt + 1, error=str(e))
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
        from src.scrapers.exceptions import EndpointUnreachableError

        raise EndpointUnreachableError(
            "Failed to query AniList after 5 attempts",
            source="anilist",
            url=ANILIST_URL,
        )

    async def get_top_anime(self, page: int = 1, per_page: int = 50, sort: list = None) -> dict:
        if sort is None:
            sort = ["POPULARITY_DESC"]  # Default: popular first
        return await self.query(TOP_ANIME_QUERY, {"page": page, "perPage": per_page, "sort": sort})

    async def get_anime_staff(
        self, anilist_id: int, staff_page: int = 1, staff_per_page: int = 25,
        char_page: int = 1, char_per_page: int = 25
    ) -> dict:
        return await self.query(
            ANIME_STAFF_QUERY,
            {
                "id": anilist_id,
                "staffPage": staff_page,
                "staffPerPage": staff_per_page,
                "charPage": char_page,
                "charPerPage": char_per_page,
            },
        )


def parse_anilist_anime(raw: dict) -> Anime:
    """Parse comprehensive anime data from AniList API response."""
    anilist_id = raw["id"]
    title = raw.get("title", {})
    season_map = {"WINTER": "winter", "SPRING": "spring", "SUMMER": "summer", "FALL": "fall"}
    avg = raw.get("averageScore")

    # Parse cover images
    cover = raw.get("coverImage", {})
    cover_large = cover.get("large")
    cover_extra_large = cover.get("extraLarge")
    cover_medium = cover.get("medium")
    banner = raw.get("bannerImage")

    # Parse dates (handle None values from API)
    start_date_obj = raw.get("startDate", {})
    end_date_obj = raw.get("endDate", {})
    start_date = None
    end_date = None
    if start_date_obj and start_date_obj.get("year"):
        year = start_date_obj.get("year")
        month = start_date_obj.get("month") or 1
        day = start_date_obj.get("day") or 1
        start_date = f"{year}-{month:02d}-{day:02d}"
    if end_date_obj and end_date_obj.get("year"):
        year = end_date_obj.get("year")
        month = end_date_obj.get("month") or 1
        day = end_date_obj.get("day") or 1
        end_date = f"{year}-{month:02d}-{day:02d}"

    # Parse studios
    studios_data = raw.get("studios", {}).get("nodes", [])
    studios = [s.get("name") for s in studios_data if s.get("name")]

    # Parse tags (limit to top 10 by rank)
    tags_data = raw.get("tags", [])
    tags = [{"name": t.get("name"), "rank": t.get("rank")} for t in tags_data if t.get("name")]
    tags = sorted(tags, key=lambda x: x.get("rank", 0), reverse=True)[:10]

    return Anime(
        id=f"anilist:{anilist_id}",
        title_ja=title.get("native") or "",
        title_en=title.get("english") or title.get("romaji") or "",
        year=raw.get("seasonYear"),
        season=season_map.get(raw.get("season", ""), None),
        episodes=raw.get("episodes"),
        anilist_id=anilist_id,
        score=avg / 10.0 if avg else None,
        # Images
        cover_large=cover_large,
        cover_extra_large=cover_extra_large,
        cover_medium=cover_medium,
        banner=banner,
        # Details
        description=raw.get("description"),
        format=raw.get("format"),
        status=raw.get("status"),
        start_date=start_date,
        end_date=end_date,
        duration=raw.get("duration"),
        source=raw.get("source"),
        # Classification
        genres=raw.get("genres", []),
        tags=tags,
        # Popularity
        popularity_rank=raw.get("popularity"),
        favourites=raw.get("favourites"),
        # Studios
        studios=studios,
    )


def parse_anilist_staff(
    staff_edges: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    """Parse comprehensive staff/person data from AniList API response."""
    persons = []
    credits = []
    for edge in staff_edges:
        node = edge.get("node", {})
        anilist_person_id = node.get("id")
        if not anilist_person_id:
            continue
        person_id = f"anilist:p{anilist_person_id}"
        name = node.get("name", {})

        # Parse aliases (alternative names)
        aliases = []
        alternative_names = name.get("alternative", [])
        if alternative_names:
            # Filter out None/empty and deduplicate
            aliases = list(set(a for a in alternative_names if a))

        # Parse images
        image = node.get("image", {})
        image_large = image.get("large")
        image_medium = image.get("medium")

        # Parse date of birth (handle None values)
        dob_obj = node.get("dateOfBirth", {})
        date_of_birth = None
        if dob_obj and dob_obj.get("year"):
            year = dob_obj.get("year")
            month = dob_obj.get("month") or 1
            day = dob_obj.get("day") or 1
            date_of_birth = f"{year}-{month:02d}-{day:02d}"

        # Parse years active
        years_active_raw = node.get("yearsActive", [])
        years_active = [y for y in years_active_raw if y] if years_active_raw else []

        persons.append(
            Person(
                id=person_id,
                name_ja=name.get("native") or "",
                name_en=name.get("full") or "",
                aliases=aliases,
                anilist_id=anilist_person_id,
                # Images
                image_large=image_large,
                image_medium=image_medium,
                # Profile
                date_of_birth=date_of_birth,
                age=node.get("age"),
                gender=node.get("gender"),
                years_active=years_active,
                hometown=node.get("homeTown"),
                blood_type=node.get("bloodType"),
                description=node.get("description"),
                # Popularity
                favourites=node.get("favourites"),
                # Links
                site_url=node.get("siteUrl"),
            )
        )
        role = parse_role(edge.get("role", ""))
        credits.append(
            Credit(person_id=person_id, anime_id=anime_id, role=role, source="anilist")
        )
    return persons, credits


def parse_anilist_voice_actors(
    character_edges: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    """Parse voice actor data from AniList character edges."""
    persons = []
    credits = []
    seen_vas = set()

    if not character_edges:
        return persons, credits

    for edge in character_edges:
        voice_actors = edge.get("voiceActors", [])
        if not voice_actors:
            continue
        for va in voice_actors:
            anilist_person_id = va.get("id")
            if not anilist_person_id or anilist_person_id in seen_vas:
                continue

            seen_vas.add(anilist_person_id)
            person_id = f"anilist:p{anilist_person_id}"
            name = va.get("name", {})

            # Parse aliases
            aliases = []
            alternative_names = name.get("alternative", [])
            if alternative_names:
                aliases = list(set(a for a in alternative_names if a))

            # Parse images
            image = va.get("image", {})
            image_large = image.get("large")
            image_medium = image.get("medium")

            # Parse date of birth (handle None values)
            dob_obj = va.get("dateOfBirth", {})
            date_of_birth = None
            if dob_obj and dob_obj.get("year"):
                year = dob_obj.get("year")
                month = dob_obj.get("month") or 1
                day = dob_obj.get("day") or 1
                date_of_birth = f"{year}-{month:02d}-{day:02d}"

            # Parse years active
            years_active_raw = va.get("yearsActive", [])
            years_active = [y for y in years_active_raw if y] if years_active_raw else []

            persons.append(
                Person(
                    id=person_id,
                    name_ja=name.get("native") or "",
                    name_en=name.get("full") or "",
                    aliases=aliases,
                    anilist_id=anilist_person_id,
                    # Images
                    image_large=image_large,
                    image_medium=image_medium,
                    # Profile
                    date_of_birth=date_of_birth,
                    age=va.get("age"),
                    gender=va.get("gender"),
                    years_active=years_active,
                    hometown=va.get("homeTown"),
                    blood_type=va.get("bloodType"),
                    description=va.get("description"),
                    # Popularity
                    favourites=va.get("favourites"),
                    # Links
                    site_url=va.get("siteUrl"),
                )
            )

            # Voice actors don't have a standard "role" field, so we'll use a generic "voice actor" role
            # This will be mapped to Role.OTHER in parse_role, but we'll use the Japanese term
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role("voice actor"),  # Will map to OTHER
                    source="anilist",
                )
            )

    return persons, credits


async def fetch_top_anime_credits(
    n_anime: int = 50,
    show_progress: bool = True,
) -> tuple[list[Anime], list[Person], list[Credit]]:
    """Fetch anime credits with optional rich progress visualization."""
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn
    from rich.table import Table
    import time as time_module

    client = AniListClient()
    all_anime: list[Anime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []
    seen_person_ids: set[str] = set()

    start_time = time_module.time()
    console = Console()

    try:
        pages_needed = (n_anime + 49) // 50
        anime_ids: list[tuple[int, str]] = []

        if show_progress:
            # Phase 1: Fetch anime list
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                list_task = progress.add_task(
                    "[cyan]アニメリスト取得中...", total=pages_needed
                )

                for page in range(1, pages_needed + 1):
                    resp = await client.get_top_anime(page=page, per_page=50)
                    page_data = resp.get("Page", {})
                    for raw in page_data.get("media", []):
                        if len(anime_ids) >= n_anime:
                            break
                        anime = parse_anilist_anime(raw)
                        all_anime.append(anime)
                        anime_ids.append((anime.anilist_id, anime.id))
                    progress.update(list_task, advance=1)

            console.print(f"✅ アニメリスト取得完了: {len(anime_ids)}件\n")

            # Phase 2: Fetch staff info with live dashboard
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                staff_task = progress.add_task(
                    "[green]スタッフ情報取得中...", total=len(anime_ids)
                )

                for i, (anilist_id, anime_id) in enumerate(anime_ids):
                    # Update progress with current anime
                    current_anime = all_anime[i]
                    title = current_anime.title_ja or current_anime.title_en or anime_id
                    progress.update(
                        staff_task,
                        description=f"[green]{title[:40]}..."
                    )

                    try:
                        # Fetch both staff and characters optimized
                        staff_page = 1
                        char_page = 1
                        has_more_staff = True
                        has_more_chars = True

                        while has_more_staff or has_more_chars:
                            resp = await client.get_anime_staff(
                                anilist_id,
                                staff_page=staff_page if has_more_staff else 1,
                                staff_per_page=25 if has_more_staff else 1,
                                char_page=char_page if has_more_chars else 1,
                                char_per_page=25 if has_more_chars else 1
                            )
                            media = resp.get("Media", {})

                            if has_more_staff:
                                staff = media.get("staff", {})
                                edges = staff.get("edges", [])
                                if edges:
                                    persons, credits = parse_anilist_staff(edges, anime_id)
                                    for p in persons:
                                        if p.id not in seen_person_ids:
                                            all_persons.append(p)
                                            seen_person_ids.add(p.id)
                                    all_credits.extend(credits)
                                has_more_staff = staff.get("pageInfo", {}).get("hasNextPage", False)
                                if has_more_staff:
                                    staff_page += 1

                            if has_more_chars:
                                characters = media.get("characters", {})
                                char_edges = characters.get("edges", [])
                                if char_edges:
                                    va_persons, va_credits = parse_anilist_voice_actors(char_edges, anime_id)
                                    for p in va_persons:
                                        if p.id not in seen_person_ids:
                                            all_persons.append(p)
                                            seen_person_ids.add(p.id)
                                    all_credits.extend(va_credits)
                                has_more_chars = characters.get("pageInfo", {}).get("hasNextPage", False)
                                if has_more_chars:
                                    char_page += 1

                    except Exception as e:
                        log.error("staff_fetch_failed", anime_id=anime_id, error=str(e))

                    progress.update(staff_task, advance=1)

                    # Show stats every 100 anime
                    if (i + 1) % 100 == 0:
                        elapsed = time_module.time() - start_time
                        rate = (i + 1) / elapsed
                        eta = (len(anime_ids) - i - 1) / rate if rate > 0 else 0
                        console.print(
                            f"  📊 [{i+1}/{len(anime_ids)}] "
                            f"人物: {len(all_persons):,} / "
                            f"クレジット: {len(all_credits):,} / "
                            f"速度: {rate:.1f}件/秒 / "
                            f"残り: {int(eta//60)}分"
                        )
        else:
            # Original non-visual mode
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
                    # Fetch both staff and characters optimized
                    staff_page = 1
                    char_page = 1
                    has_more_staff = True
                    has_more_chars = True

                    while has_more_staff or has_more_chars:
                        resp = await client.get_anime_staff(
                            anilist_id,
                            staff_page=staff_page if has_more_staff else 1,
                            staff_per_page=25 if has_more_staff else 1,
                            char_page=char_page if has_more_chars else 1,
                            char_per_page=25 if has_more_chars else 1
                        )
                        media = resp.get("Media", {})

                        if has_more_staff:
                            staff = media.get("staff", {})
                            edges = staff.get("edges", [])
                            if edges:
                                persons, credits = parse_anilist_staff(edges, anime_id)
                                for p in persons:
                                    if p.id not in seen_person_ids:
                                        all_persons.append(p)
                                        seen_person_ids.add(p.id)
                                all_credits.extend(credits)
                            has_more_staff = staff.get("pageInfo", {}).get("hasNextPage", False)
                            if has_more_staff:
                                staff_page += 1

                        if has_more_chars:
                            characters = media.get("characters", {})
                            char_edges = characters.get("edges", [])
                            if char_edges:
                                va_persons, va_credits = parse_anilist_voice_actors(char_edges, anime_id)
                                for p in va_persons:
                                    if p.id not in seen_person_ids:
                                        all_persons.append(p)
                                        seen_person_ids.add(p.id)
                                all_credits.extend(va_credits)
                            has_more_chars = characters.get("pageInfo", {}).get("hasNextPage", False)
                            if has_more_chars:
                                char_page += 1

                except Exception as e:
                    log.error("staff_fetch_failed", anime_id=anime_id, error=str(e))

    finally:
        await client.close()

    elapsed_total = time_module.time() - start_time

    if show_progress:
        # Final summary
        console.print("\n" + "="*70)
        console.print("✅ [bold green]取得完了！[/bold green]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("項目", style="cyan")
        table.add_column("件数", justify="right", style="green")

        table.add_row("アニメ", f"{len(all_anime):,}")
        table.add_row("人物", f"{len(all_persons):,}")
        table.add_row("クレジット", f"{len(all_credits):,}")
        table.add_row("所要時間", f"{int(elapsed_total//60)}分{int(elapsed_total%60)}秒")

        console.print(table)
        console.print("="*70 + "\n")

    log.info(
        "fetch_complete",
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
        elapsed_seconds=int(elapsed_total),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    count: int = typer.Option(50, "--count", "-n", help="取得するアニメ数"),
    checkpoint_interval: int = typer.Option(100, "--checkpoint", help="チェックポイント間隔"),
    resume: bool = typer.Option(False, "--resume", help="前回から再開"),
    log_level: str = typer.Option("error", "--log-level", help="ログレベル (debug/info/warning/error)"),
    skip_existing_persons: bool = typer.Option(True, "--skip-existing-persons/--update-all-persons", help="既存人物をスキップ（高速化）"),
    update: bool = typer.Option(False, "--update", "-u", help="アニメリストを更新取得（キャッシュを使わない、放映中/新規のみ更新）"),
    reverse: bool = typer.Option(False, "--reverse", "-r", help="古い順で取得（デフォルト: 新しい順）"),
) -> None:
    """AniList からクレジットデータを収集する (チェックポイント機能付き)."""
    import json
    from pathlib import Path
    from src.database import get_connection, init_db, update_data_source, upsert_anime, upsert_person, get_all_person_ids
    from src.log import setup_logging
    from rich.console import Console

    # Setup logging with specified level
    setup_logging()

    # Configure structlog level filter
    import structlog
    level_map = {
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
    }
    min_level = level_map.get(log_level.lower(), 40)

    # Add level filter to structlog processors
    def level_filter(logger, method_name, event_dict):
        level_value = {
            "debug": 10,
            "info": 20,
            "warning": 30,
            "error": 40,
        }.get(method_name, 20)
        if level_value < min_level:
            raise structlog.DropEvent
        return event_dict

    structlog.configure(
        processors=[
            level_filter,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.dev.ConsoleRenderer(),
        ]
    )
    console = Console()

    # Load .env file (centralized)
    from src.utils.config import load_dotenv_if_exists
    load_dotenv_if_exists()

    # Cache files
    checkpoint_file = Path(__file__).parent.parent.parent / "data" / "anilist_checkpoint.json"
    anime_list_cache_file = Path(__file__).parent.parent.parent / "data" / "anilist_anime_list_cache.json"
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    anime_list_cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Initialize download queue
    from src.utils.download_queue import DownloadQueue
    download_queue = DownloadQueue()

    # Load checkpoint if resuming
    start_index = 0
    fetched_ids = set()
    if resume and checkpoint_file.exists():
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
            start_index = checkpoint.get("last_index", 0)
            fetched_ids = set(checkpoint.get("fetched_ids", []))

            # Display checkpoint recovery message
            console.print()
            console.print(Rule("[bold bright_yellow]チェックポイント復旧[/bold bright_yellow]", style="bright_yellow"))
            checkpoint_table = Table(show_header=False, box=None, padding=(0, 2))
            checkpoint_table.add_row(
                "[bright_yellow]前回の進捗[/bright_yellow]",
                f"[bold bright_green]{start_index:,}[/bold bright_green]件処理済み"
            )
            checkpoint_table.add_row(
                "[bright_yellow]今回の開始位置[/bright_yellow]",
                f"[bold bright_cyan]{start_index + 1:,}[/bold bright_cyan]件目から"
            )
            checkpoint_table.add_row(
                "[bright_yellow]タイムスタンプ[/bright_yellow]",
                f"[dim]{checkpoint.get('timestamp', 'N/A')}[/dim]"
            )
            console.print(Panel(checkpoint_table, border_style="bright_yellow", padding=(1, 2)))
            console.print()

            log.info("checkpoint_loaded", start_index=start_index, fetched_count=len(fetched_ids))

    # Fetch with incremental saving
    async def fetch_with_checkpoints():
        """Execute scraping with checkpoint-based incremental saving."""
        from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, MofNCompleteColumn
        from rich.panel import Panel
        from rich.table import Table
        from rich.rule import Rule
        from rich.align import Align
        from rich.syntax import Syntax
        import time as time_module
        from src.scrapers.image_downloader import download_person_images, download_anime_images

        # === Helper Functions (Nested for Clarity) ===

        def load_existing_person_ids_from_database(conn):
            """Load existing person IDs to skip re-fetching."""
            if not skip_existing_persons:
                return set()

            existing_ids = get_all_person_ids(conn)
            if existing_ids:
                console.print()
                console.print(Panel(
                    f"[bold bright_blue]💾 既存データベースから {len(existing_ids):,}件の人物を読み込みました[/bold bright_blue]\n[dim]重複を避けるためスキップします[/dim]",
                    border_style="bright_blue",
                    padding=(1, 2)
                ))
                console.print()
            return existing_ids

        def should_skip_person(person_id, existing_ids, seen_ids):
            """Determine if person should be skipped."""
            if person_id in seen_ids:
                return True, person_id in existing_ids
            return False, False

        # --- More Helper Functions ---

        def format_elapsed_time(seconds):
            """Format elapsed time as hours/minutes/seconds string."""
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            if hours > 0:
                return f"{hours}時間{minutes}分{secs}秒"
            return f"{minutes}分{secs}秒"

        def create_phase1_summary_table(anime_count, skipped_count):
            """Create Phase 1 completion summary table."""
            from rich.box import ROUNDED
            summary_table = Table(
                show_header=False,
                box=ROUNDED,
                padding=(0, 2),
                border_style="cyan"
            )
            summary_table.add_row(
                "[bold cyan]📋 アニメリスト取得完了[/bold cyan]",
                f"[bold bright_green]{anime_count}件[/bold bright_green]"
            )
            if skipped_count > 0:
                summary_table.add_row(
                    "[dim]  └ スキップ済み[/dim]",
                    f"[dim]{skipped_count}件[/dim]"
                )
            summary_table.add_row(
                "[bold cyan]📥 フェーズ2で処理[/bold cyan]",
                f"[bold bright_yellow]{anime_count}件[/bold bright_yellow]"
            )
            return summary_table

        def display_phase1_summary(summary_table):
            """Display Phase 1 completion panel."""
            console.print()
            console.print(Rule("[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]", style="cyan"))
            console.print(Panel(
                summary_table,
                title="[bold bright_green]✅ 完了[/bold bright_green]",
                border_style="bright_green",
                padding=(1, 2)
            ))
            console.print()

        def create_final_summary_table(totals, elapsed):
            """Create final completion summary table."""
            from rich.box import ROUNDED

            final_table = Table(
                show_header=True,
                header_style="bold white on green",
                box=ROUNDED,
                border_style="green",
                padding=(0, 1)
            )
            final_table.add_column("🎯 項目", style="cyan", width=25)
            final_table.add_column("📊 件数", justify="right", style="bold green", width=20)

            # Main data
            final_table.add_row("🎬 アニメ作品", f"[bold bright_green]{totals['anime']:,}[/bold bright_green]")
            final_table.add_row("👥 人物（新規）", f"[bold bright_green]{totals['persons']:,}[/bold bright_green]")
            final_table.add_row("  └ 🎤 声優", f"[bright_blue]{totals['voice_actors']:,}[/bright_blue]")

            if totals.get("skipped", 0) > 0:
                final_table.add_row(
                    "  └ ⏭️  スキップ",
                    f"[dim]{totals['skipped']:,}[/dim]"
                )

            final_table.add_row("", "")  # Separator

            final_table.add_row("📝 クレジット", f"[bold bright_green]{totals['credits']:,}[/bold bright_green]")
            final_table.add_row("🖼️  画像ファイル", f"[bold bright_green]{totals['images']:,}[/bold bright_green]")

            if totals.get("errors", 0) > 0:
                final_table.add_row(
                    "❌ エラー",
                    f"[bold red]{totals['errors']}[/bold red]"
                )

            final_table.add_row("", "")  # Separator

            # Performance metrics
            rate = totals['anime'] / elapsed if elapsed > 0 else 0
            final_table.add_row(
                "⏱️  所要時間",
                f"[bold bright_blue]{format_elapsed_time(elapsed)}[/bold bright_blue]"
            )
            final_table.add_row(
                "⚡ 平均速度",
                f"[bold bright_yellow]{rate:.2f} 作品/秒[/bold bright_yellow]"
            )

            return final_table

        def display_final_summary(final_table):
            """Display final completion panel."""
            console.print("\n")
            console.print(Rule("[bold bright_green]データ収集完了[/bold bright_green]", style="bright_green"))
            console.print()
            console.print(Panel(
                final_table,
                title="[bold bright_green]🎉 大成功！[/bold bright_green]",
                border_style="bright_green",
                padding=(1, 2)
            ))
            console.print()
            console.print(Align.center("[bold cyan]✨ チェックポイントファイル削除完了[/bold cyan]"))
            console.print()

        def create_checkpoint_data(index, fetched_ids, totals, timestamp):
            """Create checkpoint data dictionary."""
            return {
                "last_index": index,
                "fetched_ids": list(fetched_ids),
                "total_anime": totals["anime"],
                "total_persons": totals["persons"],
                "total_credits": totals["credits"],
                "total_images": totals["images"],
                "total_voice_actors": totals["voice_actors"],
                "total_errors": totals["errors"],
                "timestamp": timestamp,
            }

        def save_checkpoint(file_path, data):
            """Save checkpoint data to JSON file."""
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)

        def delete_checkpoint_file_if_exists(file_path):
            """Delete checkpoint file upon successful completion."""
            if file_path.exists():
                file_path.unlink()

        def display_checkpoint_panel(checkpoint_num, stats_table):
            """Display checkpoint save panel."""
            console.print("\n")
            console.print(Rule(
                f"[bold bright_yellow]💾 チェックポイント #{checkpoint_num}[/bold bright_yellow]",
                style="bright_yellow"
            ))
            console.print(Panel(
                stats_table,
                title=f"[bold bright_yellow]✅ 保存完了[/bold bright_yellow]",
                border_style="bright_yellow",
                padding=(1, 2)
            ))
            console.print()

        async def fetch_anime_list_from_api(client, count, fetched_ids, use_cache=True, anime_list_cache_file=None):
            """Fetch anime list from API with optional caching and smart updates."""
            # Try to load from cache if not updating
            if use_cache and not update and anime_list_cache_file and anime_list_cache_file.exists():
                try:
                    with open(anime_list_cache_file) as f:
                        cached_data = json.load(f)
                        anime_items = []
                        for item in cached_data.get("anime_list", []):
                            anime_dict = item["anime"]
                            anilist_id = anime_dict["anilist_id"]
                            anime_id = anime_dict["id"]
                            # Reconstruct Anime object
                            anime = Anime(**anime_dict)
                            anime_items.append((anime, anilist_id, anime_id))

                        # Apply reverse sorting if needed
                        if reverse:
                            # Sort by start date (oldest first)
                            anime_items.sort(key=lambda x: x[0].year or 9999)
                        else:
                            # Sort by year descending (newest first)
                            anime_items.sort(key=lambda x: x[0].year or 0, reverse=True)

                        # Filter by fetched_ids
                        anime_ids = [item for item in anime_items if item[2] not in fetched_ids]

                        if len(anime_ids) > 0:
                            console.print()
                            sort_info = " (古い順)" if reverse else ""
                            console.print(Rule(f"[bold cyan]フェーズ1: アニメリスト（キャッシュ使用{sort_info}）[/bold cyan]", style="cyan"))
                            cache_info = Table(show_header=False, box=None, padding=(0, 2))
                            cache_info.add_row("[cyan]キャッシュから読込[/cyan]", f"[bold green]{len(anime_ids)}件[/bold green]")
                            console.print(Panel(cache_info, border_style="cyan", padding=(1, 2)))
                            console.print()
                            return anime_ids
                except Exception as e:
                    log.warning("cache_load_failed", error=str(e))
                    # Fall through to API fetch

            # Fetch from API
            console.print()

            # Load previous cache if updating (for smart filtering)
            prev_cache = {}
            if update and anime_list_cache_file and anime_list_cache_file.exists():
                try:
                    with open(anime_list_cache_file) as f:
                        prev_cache = json.load(f)
                    console.print(Rule("[bold cyan]フェーズ1: アニメリスト更新（放映中・新規のみ）[/bold cyan]", style="cyan"))
                except Exception as e:
                    log.warning("prev_cache_load_failed", error=str(e))
                    console.print(Rule("[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]", style="cyan"))
            else:
                console.print(Rule("[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]", style="cyan"))

            console.print()

            pages_needed = (count + 49) // 50
            anime_ids = []
            all_anime_for_cache = []

            # Build lookup for previous anime status
            prev_anime_status = {}
            if prev_cache:
                for item in prev_cache.get("anime_list", []):
                    anime_id = item["anime"]["id"]
                    prev_anime_status[anime_id] = item.get("status")

            # Sort order: by default newest/most relevant first
            # Can be reversed with --reverse flag
            if reverse:
                sort_order = ["START_DATE_ASC"]  # Old first
            else:
                sort_order = ["POPULARITY_DESC"]  # New/popular first

            with Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[bold cyan]{task.description}"),
                BarColumn(bar_width=40),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as progress:
                list_task = progress.add_task("📋 アニメリスト取得中...", total=pages_needed)

                for page in range(1, pages_needed + 1):
                    resp = await client.get_top_anime(page=page, per_page=50, sort=sort_order)
                    page_data = resp.get("Page", {})

                    for raw in page_data.get("media", []):
                        if len(all_anime_for_cache) >= count:
                            break
                        anime = parse_anilist_anime(raw)
                        current_status = raw.get("status")

                        # Store for caching (all anime)
                        all_anime_for_cache.append({
                            "anime": anime.model_dump(),
                            "status": current_status,
                            "fetched_at": time_module.time()
                        })

                        # Smart filtering for --update mode
                        if update and prev_cache:
                            prev_status = prev_anime_status.get(anime.id)

                            # Include if: new anime OR (was/is airing)
                            is_new = anime.id not in prev_anime_status
                            was_airing = prev_status in ("CURRENTLY_AIRING", None)
                            is_airing = current_status == "CURRENTLY_AIRING"

                            if is_new or was_airing or is_airing:
                                if anime.id not in fetched_ids:
                                    anime_ids.append((anime, anime.anilist_id, anime.id))
                        else:
                            # Normal mode: include all
                            if anime.id not in fetched_ids:
                                anime_ids.append((anime, anime.anilist_id, anime.id))

                    # Update progress with rate limit info
                    rate_info = ""
                    if client.requests_remaining is not None:
                        rate_info = f" [dim]| API: {client.requests_remaining}件残[/dim]"
                    progress.update(
                        list_task,
                        description=f"📋 アニメリスト取得中 ({page}/{pages_needed}){rate_info}",
                        advance=1
                    )

            # Display update mode info
            if update and prev_cache and len(all_anime_for_cache) > 0:
                total_checked = len(all_anime_for_cache)
                console.print()
                update_info = Table(show_header=False, box=None, padding=(0, 2))
                update_info.add_row("[cyan]チェック対象[/cyan]", f"[dim]{total_checked}件[/dim]")
                update_info.add_row("[cyan]処理対象（放映中・新規）[/cyan]", f"[bold yellow]{len(anime_ids)}件[/bold yellow]")
                console.print(Panel(update_info, border_style="cyan", padding=(1, 2)))
                console.print()

            # Save to cache
            if anime_list_cache_file:
                try:
                    cache_data = {
                        "count": len(all_anime_for_cache),
                        "fetched_at": time_module.time(),
                        "anime_list": all_anime_for_cache
                    }
                    with open(anime_list_cache_file, "w") as f:
                        json.dump(cache_data, f, indent=2, default=str)
                    log.info("anime_list_cached", count=len(all_anime_for_cache), file=str(anime_list_cache_file))
                except Exception as e:
                    log.warning("cache_save_failed", error=str(e))

            return anime_ids

        async def fetch_and_process_single_anime(
            client, anime, anilist_id, anime_id,
            existing_person_ids, seen_person_ids
        ):
            """Fetch and process staff/characters for a single anime."""
            batch_persons = []
            batch_credits = []
            batch_va_count = 0
            skipped_count = 0

            try:
                # Fetch both staff and characters
                staff_page = 1
                char_page = 1
                has_more_staff = True
                has_more_chars = True

                while has_more_staff or has_more_chars:
                    resp = await client.get_anime_staff(
                        anilist_id,
                        staff_page=staff_page if has_more_staff else 1,
                        staff_per_page=25 if has_more_staff else 1,
                        char_page=char_page if has_more_chars else 1,
                        char_per_page=25 if has_more_chars else 1
                    )
                    media = resp.get("Media", {})

                    # Process staff
                    if has_more_staff:
                        staff = media.get("staff", {})
                        edges = staff.get("edges", [])
                        if edges:
                            persons, credits = parse_anilist_staff(edges, anime_id)
                            for p in persons:
                                if p.id not in seen_person_ids:
                                    if p.id in existing_person_ids:
                                        skipped_count += 1
                                    else:
                                        batch_persons.append(p)
                                    seen_person_ids.add(p.id)
                            batch_credits.extend(credits)

                        has_more_staff = staff.get("pageInfo", {}).get("hasNextPage", False)
                        if has_more_staff:
                            staff_page += 1

                    # Process characters
                    if has_more_chars:
                        characters = media.get("characters", {})
                        char_edges = characters.get("edges", [])
                        if char_edges:
                            va_persons, va_credits = parse_anilist_voice_actors(char_edges, anime_id)
                            batch_va_count += len(va_persons)
                            for p in va_persons:
                                if p.id not in seen_person_ids:
                                    if p.id in existing_person_ids:
                                        skipped_count += 1
                                    else:
                                        batch_persons.append(p)
                                    seen_person_ids.add(p.id)
                            batch_credits.extend(va_credits)

                        has_more_chars = characters.get("pageInfo", {}).get("hasNextPage", False)
                        if has_more_chars:
                            char_page += 1

            except Exception as e:
                log.error("staff_fetch_failed", anime_id=anime_id, error=str(e))
                return batch_persons, batch_credits, batch_va_count, skipped_count, True

            return batch_persons, batch_credits, batch_va_count, skipped_count, False

        async def download_images_for_batches(batch_persons, batch_anime, conn, queue=None):
            """Queue images for async download (non-blocking).

            Instead of downloading synchronously, we queue items for later processing.
            This keeps the scraping pipeline fast.
            """
            if queue is None:
                return 0

            queued_count = 0

            # Queue person images
            for p in batch_persons:
                if p.image_large or p.image_medium:
                    queue.add_person(p.id, p.image_large, p.image_medium)
                    queued_count += 1

            # Queue anime images
            for a in batch_anime:
                if a.cover_large or a.cover_extra_large or a.banner:
                    queue.add_anime(a.id, a.cover_large, a.cover_extra_large, a.banner)
                    queued_count += 1

            return queued_count

        # === Main Execution ===

        # Initialize state
        client = AniListClient()
        totals = {
            "anime": 0,
            "persons": 0,
            "credits": 0,
            "images": 0,
            "voice_actors": 0,
            "errors": 0,
            "skipped": 0,
        }
        seen_person_ids = set()
        start_time = time_module.time()

        try:
            # ===== PHASE 1: Fetch Anime List =====
            anime_ids = await fetch_anime_list_from_api(
                client, count, fetched_ids,
                use_cache=True,
                anime_list_cache_file=anime_list_cache_file
            )

            # Display Phase 1 summary
            summary_table = create_phase1_summary_table(len(anime_ids), len(fetched_ids))
            display_phase1_summary(summary_table)

            # ===== PHASE 2: Fetch Staff & Characters =====
            conn = get_connection()
            init_db(conn)
            existing_person_ids = load_existing_person_ids_from_database(conn)

            console.print()
            console.print(Rule("[bold green]フェーズ2: スタッフ情報取得 & データベース保存[/bold green]", style="green"))
            console.print()

            # Initialize batches for checkpoint saving
            batch_anime = []
            batch_persons = []
            batch_credits = []
            batch_va_count = 0
            total_persons_fetched = 0
            total_credits_fetched = 0
            total_persons_skipped = 0

            # Track last 3 anime skip percentages for trend analysis
            skip_ratio_history = []  # Will store tuples of (anime_title, skip_pct)

            with Progress(
                SpinnerColumn(style="green"),
                TextColumn("[bold green]{task.description}"),
                BarColumn(bar_width=50, complete_style="bright_green", finished_style="bold bright_green"),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console,
                expand=False
            ) as progress:
                staff_task = progress.add_task("[green]📥 アニメ処理中...", total=len(anime_ids))
                person_task = progress.add_task("[blue]👥 スタッフ取得中...", total=None)  # Indeterminate for people count

                for i, (anime, anilist_id, anime_id) in enumerate(anime_ids, start=start_index):
                    # Update progress with current anime title
                    title = anime.title_ja or anime.title_en or anime_id
                    progress.update(staff_task, description=f"[green]{title[:35]}...")

                    # Fetch and process staff/characters for this anime
                    persons, credits, va_count, skipped, had_error = await fetch_and_process_single_anime(
                        client, anime, anilist_id, anime_id,
                        existing_person_ids, seen_person_ids
                    )

                    # Add to batch
                    batch_anime.append(anime)
                    batch_persons.extend(persons)
                    batch_credits.extend(credits)
                    batch_va_count += va_count
                    totals["skipped"] += skipped
                    total_persons_skipped += skipped

                    # Update person tracking
                    total_persons_fetched += len(persons)
                    total_credits_fetched += len(credits)

                    # Calculate skip percentage for this anime
                    total_staff_in_anime = len(persons) + skipped
                    skip_pct = (skipped / total_staff_in_anime * 100) if total_staff_in_anime > 0 else 0

                    # Keep only last 3 entries
                    skip_ratio_history.append((title, skip_pct))
                    if len(skip_ratio_history) > 3:
                        skip_ratio_history.pop(0)

                    if had_error:
                        totals["errors"] += 1

                    fetched_ids.add(anime_id)
                    progress.update(staff_task, advance=1)

                    # Build trend string for last 3 anime
                    trend_str = ""
                    if skip_ratio_history:
                        trend_values = [f"[{'green' if pct < 30 else 'yellow' if pct < 60 else 'red'}]{pct:.0f}%[/]"
                                       for _, pct in skip_ratio_history]
                        trend_str = f" | 📊 直近3既存率: [{', '.join(trend_values)}]"

                    # Calculate percentages
                    total_persons_all = total_persons_fetched + total_persons_skipped
                    skip_pct = (total_persons_skipped / total_persons_all * 100) if total_persons_all > 0 else 0

                    # Build rate limit info
                    rate_limit_info = ""
                    if client.requests_remaining is not None and client.rate_limit_reset_at is not None:
                        import time as time_mod
                        now = time_mod.time()
                        reset_at = client.rate_limit_reset_at
                        if reset_at > now:
                            remaining_secs = int(reset_at - now)
                            if client.requests_remaining < 10:
                                rate_limit_info = f" | [bold red]⏳ rate limit 残り{remaining_secs}秒[/bold red]"
                            else:
                                rate_limit_info = f" | [dim]API: {client.requests_remaining}件[/dim]"

                    # Update person task with detailed info (comprehensive format)
                    total_staff_collected = total_persons_fetched + total_persons_skipped
                    person_desc = (
                        f"[bold cyan]👥 スタッフ個人の情報収集中({total_persons_fetched:,}/{total_staff_collected:,}, "
                        f"{total_persons_skipped:,}は取得済み)[/bold cyan]"
                        f"{trend_str}"
                        f"{rate_limit_info}"
                    )
                    progress.update(person_task, description=person_desc)

                    # Save current anime to database immediately (per-anime saving)
                    save_anime_batch_to_database(conn, [anime])
                    save_persons_batch_to_database(conn, persons)
                    save_credits_batch_to_database(conn, credits)
                    conn.commit()

                    # Queue images for async download (non-blocking)
                    images_queued = await download_images_for_batches(persons, [anime], conn, download_queue)

                    # Update totals
                    totals["anime"] += 1
                    totals["persons"] += len(persons)
                    totals["credits"] += len(credits)
                    totals["voice_actors"] += va_count
                    totals["images"] += images_queued

                    # Save checkpoint file every N anime (for crash recovery)
                    if (i + 1) % checkpoint_interval == 0 or (i + 1) == len(anime_ids):
                        checkpoint_data = create_checkpoint_data(
                            i + 1, fetched_ids, totals, time_module.time()
                        )
                        save_checkpoint(checkpoint_file, checkpoint_data)

            # Finalize database
            update_data_source(conn, "anilist", totals["credits"])
            conn.commit()
            conn.close()

            # Delete checkpoint file on completion
            delete_checkpoint_file_if_exists(checkpoint_file)

            # Display final summary
            elapsed = time_module.time() - start_time
            final_table = create_final_summary_table(totals, elapsed)
            display_final_summary(final_table)

            # Display download queue info
            queue_size = download_queue.count()
            if queue_size > 0:
                console.print()
                console.print(Rule("[bold bright_magenta]📥 画像ダウンロードキュー[/bold bright_magenta]", style="bright_magenta"))
                queue_info = Table(show_header=False, box=None, padding=(0, 2))
                queue_info.add_row(
                    "[bright_magenta]キュー内の画像[/bright_magenta]",
                    f"[bold yellow]{queue_size}件[/bold yellow]"
                )
                queue_info.add_row(
                    "[dim]処理方法[/dim]",
                    "[dim]pixi run process-downloads[/dim]"
                )
                console.print(Panel(queue_info, border_style="bright_magenta", padding=(1, 2)))
                console.print()

        finally:
            await client.close()

    asyncio.run(fetch_with_checkpoints())


@app.command()
def process_downloads(
    log_level: str = typer.Option("error", "--log-level", help="ログレベル (debug/info/warning/error)"),
) -> None:
    """Process pending image downloads from queue."""
    import asyncio
    from src.log import setup_logging
    from src.utils.download_queue import DownloadQueue
    from src.database import get_connection, upsert_person, upsert_anime
    from src.scrapers.image_downloader import download_person_images, download_anime_images
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
    from rich.rule import Rule

    # Setup
    setup_logging()
    console = Console()

    # Load queue
    queue = DownloadQueue()
    if queue.is_empty():
        console.print("[dim]✅ ダウンロードキューは空です[/dim]")
        return

    console.print()
    console.print(Rule("[bold cyan]画像ダウンロードキュー処理[/bold cyan]", style="cyan"))
    console.print()

    # Get database connection
    conn = get_connection()

    async def process_queue():
        """Process all queued downloads."""
        persons = queue.get_persons()
        anime = queue.get_anime()

        total_items = len(persons) + len(anime)
        processed = 0

        with Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=50),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console
        ) as progress:
            task = progress.add_task("🖼️  ダウンロード中...", total=total_items)

            # Download person images
            if persons:
                person_data = [
                    (p.item_id, p.url_large, p.url_medium)
                    for p in persons
                ]
                try:
                    person_paths = await download_person_images(person_data, show_progress=False)
                    # Update database
                    for person_id, paths in person_paths.items():
                        from src.models import Person
                        # Fetch person from DB and update paths
                        # This is simplified - in reality you'd query the DB
                        queue.remove_item(person_id)
                        processed += 1
                        progress.update(task, advance=1)
                except Exception as e:
                    log.error("person_download_failed", error=str(e))

            # Download anime images
            if anime:
                anime_data = [
                    (a.item_id, a.url_medium, a.url_large, a.url_banner)
                    for a in anime
                ]
                try:
                    anime_paths = await download_anime_images(anime_data, show_progress=False)
                    # Update database
                    for anime_id, paths in anime_paths.items():
                        queue.remove_item(anime_id)
                        processed += 1
                        progress.update(task, advance=1)
                except Exception as e:
                    log.error("anime_download_failed", error=str(e))

        # Display summary
        console.print()
        summary = Table(show_header=False, box=None, padding=(0, 2))
        summary.add_row("[cyan]ダウンロード完了[/cyan]", f"[bold green]{processed}件[/bold green]")
        summary.add_row("[cyan]残りキュー[/cyan]", f"[dim]{queue.count()}件[/dim]")
        console.print(Panel(summary, border_style="cyan", padding=(1, 2)))
        console.print()

    # Run async processing
    asyncio.run(process_queue())
    conn.close()


if __name__ == "__main__":
    # When called directly without a command, default to 'main'
    import sys
    if len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1].startswith("--")):
        # No command specified, or starts with --option, so prepend 'main'
        sys.argv.insert(1, "main")
    app()
