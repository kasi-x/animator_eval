"""AniList GraphQL API staff credit collection.

Built with httpx (async) + structlog + typer.
Rate limit: 90 requests/minute (unauthenticated) / higher (authenticated).
"""

import asyncio
import datetime as dt
import time

import httpx
import structlog
import typer
from dotenv import find_dotenv, dotenv_values

from src.runtime.models import (
    BronzeAnime,
    AnimeRelation,
    AnimeStudio,
    Credit,
    Person,
    Studio,
    parse_role,
)
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.hash_utils import hash_anime_data
from src.scrapers.parsers.anilist import (  # noqa: F401
    parse_anilist_person,
    parse_anilist_anime,
    parse_anilist_staff,
    parse_anilist_voice_actors,
    parse_anilist_characters,
    parse_anilist_studios,
    parse_anilist_relations,
)
from src.scrapers.cli_common import (
    DelayOpt,
    ForceOpt,
    LimitOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.progress import progress_enabled as _progress_enabled
from src.scrapers.http_client import RetryingHttpClient
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL

_env = dotenv_values(find_dotenv())

log = structlog.get_logger()

ANILIST_URL = "https://graphql.anilist.co"
# AniList official: 90 req/min (per header), but in degraded state as of 2026-02
# Effective ~18-19 req/window → burst mode: hit rapidly then back off on 429
# Reference: https://docs.anilist.co/guide/rate-limiting
REQUEST_INTERVAL = 0.1  # burst mode: minimum interval, auto-backoff on 429

from src.scrapers.queries.anilist import (  # noqa: E402
    ANIME_STAFF_QUERY,
    ANIME_STAFF_MINIMAL_QUERY,
    PERSON_DETAILS_QUERY,
    TOP_ANIME_QUERY,
)

app = typer.Typer()


# Batch save helper functions (must be defined before main())
def get_anime_ids_to_skip_since(since_str: str) -> set[str]:
    """Get anime IDs fetched after since_dt from SILVER.anime.

    Returns: set of anime IDs to skip (already fetched recently)
    """
    import duckdb
    from pathlib import Path

    try:
        since_dt = dt.datetime.fromisoformat(since_str)
        silver_path = Path("result/silver.duckdb")
        if not silver_path.exists():
            log.warning("since_mode_no_silver", since=since_str)
            return set()

        conn = duckdb.connect(str(silver_path), read_only=True)
        result = conn.execute(
            "SELECT id FROM anime WHERE fetched_at >= ? ORDER BY fetched_at DESC",
            [since_dt]
        ).fetchall()
        conn.close()

        skipped_ids = {row[0] for row in result}
        log.info("since_mode_loaded", since=since_str, skipped_count=len(skipped_ids))
        return skipped_ids
    except Exception as e:
        log.error("since_mode_error", since=since_str, error=str(e))
        return set()


def save_anime_batch_to_bronze(anime_bw, anime_batch):
    """Save a batch of anime to BRONZE parquet with hash tracking."""
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    for anime in anime_batch:
        anime_dict = anime.model_dump(mode="json")
        # Add fetched_at and content_hash for diff detection
        anime_dict["fetched_at"] = now
        anime_dict["content_hash"] = hash_anime_data(anime_dict)
        anime_bw.append(anime_dict)


def save_studios_to_bronze(studios_bw, anime_studios_bw, studios, anime_studios):
    """Save studios and anime-studio relationships to BRONZE parquet."""
    for studio in studios:
        studios_bw.append(studio.model_dump(mode="json"))
    for anime_studio in anime_studios:
        anime_studios_bw.append(anime_studio.model_dump(mode="json"))


def save_relations_to_bronze(relations_bw, relations):
    """Save anime relations to BRONZE parquet."""
    for relation in relations:
        relations_bw.append(relation.model_dump(mode="json"))


def save_persons_batch_to_bronze(persons_bw, persons_batch):
    """Save a batch of persons to BRONZE parquet."""
    for person in persons_batch:
        persons_bw.append(person.model_dump(mode="json"))


def _make_rate_limit_text(cl):
    """Generate an API consumption bar (used/max)."""
    from rich.text import Text

    if cl.rate_limit_max is None and cl.requests_remaining is None:
        return Text("")
    # Header says 90 but effective is ~20 (degraded + burst limiter)
    header_limit = cl.rate_limit_max or 90
    remaining = (
        cl.requests_remaining if cl.requests_remaining is not None else header_limit
    )
    used = header_limit - remaining
    bar_width = 30
    filled = int(bar_width * used / header_limit) if header_limit > 0 else 0
    empty = bar_width - filled
    if used > 60:
        color = "bold red"
    elif used > 30:
        color = "yellow"
    else:
        color = "green"
    bar_str = "█" * filled + "░" * empty
    return Text(f"🔋 API: [{bar_str}] {used}/{header_limit} (burst)", style=color)


def save_credits_batch_to_bronze(credits_bw, credits_batch):
    """Save a batch of credits to BRONZE parquet."""
    for credit in credits_batch:
        credits_bw.append(credit.model_dump(mode="json"))


def save_characters_to_bronze(characters_bw, characters):
    """Save characters to BRONZE parquet."""
    for char in characters:
        characters_bw.append(char.model_dump(mode="json"))


def save_cva_to_bronze(cva_bw, cva_list):
    """Save character-voice-actor mappings to BRONZE parquet."""
    for cva in cva_list:
        cva_bw.append(cva.model_dump(mode="json"))


class AniListClient:
    """Async AniList GraphQL client (with authentication support)."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        # Load authentication token from .env
        self._access_token = _env.get("ANILIST_ACCESS_TOKEN")
        if self._access_token:
            log.info("anilist_token_loaded", will_attempt_auth=True)
        else:
            log.info("anilist_no_token", will_use_unauthenticated=True)
        # Create RetryingHttpClient (handles retries + rate limit backoff)
        self._client = RetryingHttpClient(source="anilist", delay=REQUEST_INTERVAL, timeout=60.0)

        # Rate limit tracking
        self.requests_remaining = None
        self.rate_limit_reset_at = None
        self.rate_limit_max = None  # X-RateLimit-Limit (90=authenticated, 30=unauthenticated)
        self._auth_reported = False
        self._requests_since_reset = 0  # request count since last reset
        self._requests_total = 0  # total request count since process start
        self._last_remaining_before_429 = None  # X-RateLimit-Remaining just before 429
        # Callback: called with remaining seconds during rate limit wait
        # signature: on_rate_limit(remaining_secs: int | None)  None=wait ended
        self.on_rate_limit: callable | None = None

    async def close(self) -> None:
        await self._client.close()

    async def verify_auth(self) -> bool:
        """Verify token validity and sync the remaining rate-limit window quota.

        Returns:
            True if authenticated (rate_limit >= 90), False otherwise.
        """
        try:
            test_query = "query { Viewer { id } }"
            await self._rate_limit()
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            if self._access_token:
                headers["Authorization"] = f"Bearer {self._access_token}"
            resp = await self._client.post(
                ANILIST_URL,
                json={"query": test_query, "variables": {}},
                headers=headers,
            )
            self._requests_total += 1

            limit_val = resp.headers.get("X-RateLimit-Limit")
            if limit_val:
                self.rate_limit_max = int(limit_val)
                self.requests_remaining = int(
                    resp.headers.get("X-RateLimit-Remaining", -1)
                )
                reset_at = resp.headers.get("X-RateLimit-Reset")
                if reset_at:
                    self.rate_limit_reset_at = int(reset_at)

                # Estimate carry-over consumption from the previous run using the API window remaining
                already_used = self.rate_limit_max - self.requests_remaining
                if already_used > 1:  # 1 = this verify_auth call itself
                    carry_over = already_used - 1
                    log.info(
                        "rate_limit_window_carry_over",
                        source="anilist",
                        remaining=self.requests_remaining,
                        limit=self.rate_limit_max,
                        carry_over_from_previous=carry_over,
                    )
                    # Wait for reset when little quota remains
                    if self.requests_remaining < 10 and self.rate_limit_reset_at:
                        wait_secs = max(0, self.rate_limit_reset_at - time.time())
                        if wait_secs > 0:
                            log.info(
                                "waiting_for_rate_limit_reset",
                                source="anilist",
                                remaining=self.requests_remaining,
                                wait_seconds=int(wait_secs) + 1,
                            )
                            await asyncio.sleep(wait_secs + 1)
                            self.requests_remaining = self.rate_limit_max
                            self._requests_since_reset = 0

            # Viewer query succeeds only with valid token
            data = resp.json()
            viewer = data.get("data", {}).get("Viewer")
            if viewer:
                log.info(
                    "auth_verified",
                    source="anilist",
                    user_id=viewer.get("id"),
                    rate_limit_header=self.rate_limit_max,
                    mode="burst (~18 req + 60s wait)",
                    requests_remaining=self.requests_remaining,
                )
                return True
            else:
                if self._access_token:
                    log.warning(
                        "auth_failed",
                        source="anilist",
                        rate_limit=self.rate_limit_max,
                        requests_remaining=self.requests_remaining,
                        hint="Token is invalid or expired. Reissue at https://anilist.co/settings/developer",
                    )
                return False
        except Exception as e:
            log.warning("auth_verify_error", source="anilist", error=str(e))
            return False

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    async def query(self, query: str, variables: dict) -> dict:
        cache_key = {"query": query, "variables": variables}
        cached = load_cached_json("anilist/graphql", cache_key)
        if cached is not None:
            return cached

        await self._rate_limit()

        # Build headers with auth token if available
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"

        # Context to capture rate limit headers from the request
        rate_limit_context: dict = {}

        try:
            resp = await self._client.post(
                ANILIST_URL,
                json={"query": query, "variables": variables},
                headers=headers,
                on_rate_limit=self.on_rate_limit,
                rate_limit_context=rate_limit_context,
            )
            self._requests_since_reset += 1
            self._requests_total += 1

            # Update internal rate limit tracking from context
            if rate_limit_context.get("remaining") is not None:
                self.requests_remaining = rate_limit_context["remaining"]
                self._last_remaining_before_429 = self.requests_remaining
                self.rate_limit_reset_at = rate_limit_context.get("reset_at", 0)
                limit_val = rate_limit_context.get("limit")
                if limit_val and self.rate_limit_max is None:
                    self.rate_limit_max = limit_val
                    if self.rate_limit_max >= 90:
                        log.info(
                            "auth_confirmed",
                            source="anilist",
                            rate_limit=self.rate_limit_max,
                            status="authenticated",
                        )
                    elif self._access_token:
                        log.warning(
                            "auth_token_ineffective",
                            source="anilist",
                            rate_limit=self.rate_limit_max,
                            expected=90,
                            hint="Token may be invalid or expired. Reissue at https://anilist.co/settings/developer",
                        )

            # Handle invalid token: fall back to unauthenticated
            if resp.status_code in (400, 401) and self._access_token:
                try:
                    data = resp.json()
                    errors = data.get("errors", [])
                    log.warning(
                        "auth_error_response",
                        source="anilist",
                        status=resp.status_code,
                        errors=errors,
                        token_format="JWT"
                        if "." in self._access_token
                        else "non-JWT",
                    )
                    error_str = str(errors).lower()
                    if (
                        "token" in error_str
                        or "auth" in error_str
                        or "invalid" in error_str
                    ):
                        log.warning(
                            "invalid_token_disabling",
                            source="anilist",
                            errors=errors,
                            fallback="unauthenticated",
                        )
                        self._access_token = None
                        # Retry without token
                        headers.pop("Authorization", None)
                        resp = await self._client.post(
                            ANILIST_URL,
                            json={"query": query, "variables": variables},
                            headers=headers,
                            on_rate_limit=self.on_rate_limit,
                            rate_limit_context=rate_limit_context,
                        )
                except Exception as parse_error:
                    log.warning(
                        "failed_to_parse_error_response",
                        source="anilist",
                        error_type=type(parse_error).__name__,
                        error_message=str(parse_error),
                    )

            # 404 = resource does not exist → return None
            if resp.status_code == 404:
                log.warning(
                    "resource_not_found",
                    source="anilist",
                    status=404,
                    variables=variables,
                )
                return None

            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                log.warning(
                    "graphql_errors",
                    source="anilist",
                    errors=data["errors"],
                    variables=variables,
                )
            result = data.get("data", {})
            save_cached_json("anilist/graphql", cache_key, result)
            return result

        except httpx.HTTPError as e:
            from src.scrapers.exceptions import EndpointUnreachableError
            raise EndpointUnreachableError(
                f"Failed to query AniList: {e}",
                source="anilist",
                url=ANILIST_URL,
            ) from e

    async def get_top_anime(
        self, page: int = 1, per_page: int = 50, sort: list = None
    ) -> dict:
        if sort is None:
            sort = ["POPULARITY_DESC"]  # Default: popular first
        return await self.query(
            TOP_ANIME_QUERY, {"page": page, "perPage": per_page, "sort": sort}
        )

    async def get_anime_staff(
        self,
        anilist_id: int,
        staff_page: int = 1,
        staff_per_page: int = 25,
        char_page: int = 1,
        char_per_page: int = 25,
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

    async def get_anime_staff_minimal(
        self,
        anilist_id: int,
        staff_page: int = 1,
        staff_per_page: int = 25,
        char_page: int = 1,
        char_per_page: int = 25,
    ) -> dict:
        """Fetch minimal staff list (IDs and roles only, no person details)."""
        return await self.query(
            ANIME_STAFF_MINIMAL_QUERY,
            {
                "id": anilist_id,
                "staffPage": staff_page,
                "staffPerPage": staff_per_page,
                "charPage": char_page,
                "charPerPage": char_per_page,
            },
        )

    async def get_person_details(self, staff_id: int) -> dict:
        """Fetch full person details for a given staff ID."""
        return await self.query(
            PERSON_DETAILS_QUERY,
            {"id": staff_id},
        )

async def fetch_top_anime_credits(
    n_anime: int = 50,
    show_progress: bool = True,
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    """Fetch anime credits with optional rich progress visualization."""
    from rich.console import Console
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeRemainingColumn,
        TimeElapsedColumn,
        MofNCompleteColumn,
    )
    from rich.table import Table
    import time as time_module

    client = AniListClient()
    all_anime: list[BronzeAnime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []
    seen_person_ids: set[str] = set()

    start_time = time_module.time()
    console = Console()

    await client.verify_auth()

    try:
        pages_needed = (n_anime + 49) // 50
        anime_ids: list[tuple[int, str]] = []

        if show_progress:
            from rich.live import Live
            from rich.console import Group
            from rich.text import Text

            # Phase 1: Fetch anime list
            bar_p1 = Progress(
                SpinnerColumn(style="cyan"),
                BarColumn(
                    bar_width=40,
                    complete_style="bright_cyan",
                    finished_style="bold bright_cyan",
                ),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
            )
            task_p1 = bar_p1.add_task("", total=pages_needed)
            status_p1 = Text("📋 Fetching anime list...", style="bold cyan")

            def _make_p1_group():
                parts = [status_p1, bar_p1]
                rl = _make_rate_limit_text(client)
                if str(rl):
                    parts.append(rl)
                return Group(*parts)

            with Live(
                _make_p1_group(), console=console, refresh_per_second=4
            ) as live_p1:
                for page in range(1, pages_needed + 1):
                    resp = await client.get_top_anime(page=page, per_page=50)
                    page_data = resp.get("Page", {})
                    for raw in page_data.get("media", []):
                        if len(anime_ids) >= n_anime:
                            break
                        anime = parse_anilist_anime(raw)
                        all_anime.append(anime)
                        anime_ids.append((anime.anilist_id, anime.id))
                    bar_p1.update(task_p1, advance=1)
                    status_p1 = Text(
                        f"📋 Fetching anime list ({page}/{pages_needed})",
                        style="bold cyan",
                    )
                    live_p1.update(_make_p1_group())

            console.print(f"✅ Anime list complete: {len(anime_ids)} titles\n")

            # Phase 2: Fetch staff info with live dashboard
            bar_p2 = Progress(
                SpinnerColumn(style="green"),
                BarColumn(
                    bar_width=40,
                    complete_style="bright_green",
                    finished_style="bold bright_green",
                ),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            task_p2 = bar_p2.add_task("", total=len(anime_ids))
            status_p2 = Text("📋 Fetching staff info...", style="bold green")

            def _make_p2_group():
                parts = [status_p2, bar_p2]
                rl = _make_rate_limit_text(client)
                if str(rl):
                    parts.append(rl)
                return Group(*parts)

            with Live(
                _make_p2_group(), console=console, refresh_per_second=4
            ) as live_p2:
                for i, (anilist_id, anime_id) in enumerate(anime_ids):
                    current_anime = all_anime[i]
                    title = current_anime.title_ja or current_anime.title_en or anime_id
                    status_p2 = Text(f"📋 {title[:40]}", style="bold green")
                    live_p2.update(_make_p2_group())

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
                                char_per_page=25 if has_more_chars else 1,
                            )
                            media = resp.get("Media", {})

                            if has_more_staff:
                                staff = media.get("staff", {})
                                edges = staff.get("edges", [])
                                if edges:
                                    persons, credits = parse_anilist_staff(
                                        edges, anime_id
                                    )
                                    for p in persons:
                                        if p.id not in seen_person_ids:
                                            all_persons.append(p)
                                            seen_person_ids.add(p.id)
                                    all_credits.extend(credits)
                                has_more_staff = staff.get("pageInfo", {}).get(
                                    "hasNextPage", False
                                )
                                if has_more_staff:
                                    staff_page += 1

                            if has_more_chars:
                                characters = media.get("characters", {})
                                char_edges = characters.get("edges", [])
                                if char_edges:
                                    va_persons, va_credits = parse_anilist_voice_actors(
                                        char_edges, anime_id
                                    )
                                    for p in va_persons:
                                        if p.id not in seen_person_ids:
                                            all_persons.append(p)
                                            seen_person_ids.add(p.id)
                                    all_credits.extend(va_credits)
                                has_more_chars = characters.get("pageInfo", {}).get(
                                    "hasNextPage", False
                                )
                                if has_more_chars:
                                    char_page += 1

                    except Exception as e:
                        log.error(
                            "staff_fetch_failed",
                            source="anilist",
                            anime_id=anime_id,
                            error_type=type(e).__name__,
                            error_message=str(e),
                        )

                    bar_p2.update(task_p2, advance=1)
                    live_p2.update(_make_p2_group())
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
                log.info(
                    "fetching_staff",
                    progress=f"{i + 1}/{len(anime_ids)}",
                    anime_id=anime_id,
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
                            char_per_page=25 if has_more_chars else 1,
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
                            has_more_staff = staff.get("pageInfo", {}).get(
                                "hasNextPage", False
                            )
                            if has_more_staff:
                                staff_page += 1

                        if has_more_chars:
                            characters = media.get("characters", {})
                            char_edges = characters.get("edges", [])
                            if char_edges:
                                va_persons, va_credits = parse_anilist_voice_actors(
                                    char_edges, anime_id
                                )
                                for p in va_persons:
                                    if p.id not in seen_person_ids:
                                        all_persons.append(p)
                                        seen_person_ids.add(p.id)
                                all_credits.extend(va_credits)
                            has_more_chars = characters.get("pageInfo", {}).get(
                                "hasNextPage", False
                            )
                            if has_more_chars:
                                char_page += 1

                except Exception as e:
                    log.error(
                        "staff_fetch_failed",
                        source="anilist",
                        anime_id=anime_id,
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )

    finally:
        await client.close()

    elapsed_total = time_module.time() - start_time

    if show_progress:
        # Final summary
        console.print("\n" + "=" * 70)
        console.print("✅ [bold green]取得完了！[/bold green]\n")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("項目", style="cyan")
        table.add_column("件数", justify="right", style="green")

        table.add_row("アニメ", f"{len(all_anime):,}")
        table.add_row("人物", f"{len(all_persons):,}")
        table.add_row("クレジット", f"{len(all_credits):,}")
        table.add_row(
            "所要時間", f"{int(elapsed_total // 60)}分{int(elapsed_total % 60)}秒"
        )

        console.print(table)
        console.print("=" * 70 + "\n")

    log.info(
        "fetch_complete",
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
        elapsed_seconds=int(elapsed_total),
    )
    return all_anime, all_persons, all_credits


async def _load_anime_ids(
    client,
    fetched_ids,
    console,
    update,
    reverse,
    anime_list_cache_file,
    fetch_anime_list_from_api,
    create_phase1_summary_table,
    display_phase1_summary,
):
    """Phase 1: Fetch anime list and return anime IDs.

    Fetches the anime list (from cache or API), displays the Phase 1 summary,
    and shows the authentication status.

    Returns:
        list of (Anime, anilist_id, anime_id) tuples.
    """
    anime_ids = await fetch_anime_list_from_api(
        client,
        fetched_ids,
        use_cache=True,
        anime_list_cache_file=anime_list_cache_file,
    )

    summary_table = create_phase1_summary_table(len(anime_ids), len(fetched_ids))
    display_phase1_summary(summary_table)

    # 認証状況を表示
    if client.rate_limit_max is not None and client.rate_limit_max >= 90:
        console.print(
            f"[green]🔑 認証済み (rate limit: {client.rate_limit_max} req/min)[/green]"
        )
    else:
        limit = client.rate_limit_max or 30
        console.print(f"[yellow]⚠️ 未認証 (rate limit: {limit} req/min)[/yellow]")

    return anime_ids


async def _fetch_staff_phase(
    client,
    anime_bw,
    credits_bw,
    studios_bw,
    anime_studios_bw,
    relations_bw,
    characters_bw,
    cva_bw,
    anime_ids,
    totals,
    fetched_ids,
    download_queue,
    checkpoint_interval,
    checkpoint_file,
    fetch_staff_ids_for_anime,
    create_checkpoint_data,
    save_checkpoint,
    time_module,
    studios_pending=None,
    relations_pending=None,
    since_ids=None,
    show_progress: bool = True,
):
    """Phase 2A: Fetch staff lists (credits and person IDs) for each anime.

    Iterates over all anime, fetches minimal staff data, saves anime/credits
    to the database, and handles checkpointing.

    Args:
        since_ids: Set of anime IDs already fetched (skip them) or None.

    Returns:
        set of all person IDs that need to be fetched in Phase 2B.
    """
    from src.scrapers.progress import scrape_progress as _scrape_progress

    since_ids = since_ids or set()
    all_person_ids_to_fetch = set()

    with _scrape_progress(
        total=len(anime_ids),
        description="anilist staff phase",
        enabled=show_progress,
    ) as p:
        for loop_idx, (anime, anilist_id, anime_id) in enumerate(anime_ids):
            if anime_id in since_ids:
                continue

            credits, person_ids, va_count, characters, cva_list, had_error = await fetch_staff_ids_for_anime(
                client, anilist_id, anime_id
            )

            all_person_ids_to_fetch.update(person_ids)
            if had_error:
                totals["errors"] += 1

            save_anime_batch_to_bronze(anime_bw, [anime])
            if studios_pending and anime_id in studios_pending:
                studios, anime_studio_edges = studios_pending.pop(anime_id)
                save_studios_to_bronze(studios_bw, anime_studios_bw, studios, anime_studio_edges)
            if relations_pending and anime_id in relations_pending:
                save_relations_to_bronze(relations_bw, relations_pending.pop(anime_id))
            save_credits_batch_to_bronze(credits_bw, credits)
            save_characters_to_bronze(characters_bw, characters)
            save_cva_to_bronze(cva_bw, cva_list)

            if anime.cover_large or anime.cover_extra_large or anime.banner:
                download_queue.add_anime(
                    anime.id, anime.cover_large, anime.cover_extra_large, anime.banner
                )
                totals["images"] += 1

            fetched_ids.add(anime_id)
            totals["anime"] += 1
            totals["credits"] += len(credits)
            totals["voice_actors"] += va_count
            totals["characters"] += len(characters)
            p.advance()

            if (loop_idx + 1) % checkpoint_interval == 0:
                anime_bw.flush()
                credits_bw.flush()
                studios_bw.flush()
                anime_studios_bw.flush()
                relations_bw.flush()
                characters_bw.flush()
                cva_bw.flush()
                cp = Checkpoint(checkpoint_file)
                cp.data.update(create_checkpoint_data(
                    loop_idx + 1, fetched_ids, totals, time_module.time()
                ))
                cp.save(stamp_time=False)
                p.log("anilist_staff_checkpoint", done=loop_idx + 1, total=len(anime_ids))

        cp = Checkpoint(checkpoint_file)
        cp.data.update(create_checkpoint_data(
            len(anime_ids), fetched_ids, totals, time_module.time()
        ))
        cp.save(stamp_time=False)

    return all_person_ids_to_fetch


async def _fetch_person_details_phase(
    client,
    persons_bw,
    ids_to_fetch,
    totals,
    download_queue,
    show_progress: bool = True,
):
    """Phase 2B: Fetch detailed person information for each person ID.

    Iterates over person IDs, fetches full details from the API,
    saves to the database in batches, and queues image downloads.
    """
    if not ids_to_fetch:
        return

    from src.scrapers.progress import scrape_progress as _scrape_progress

    person_batch = []
    skipped_count = 0

    with _scrape_progress(
        total=len(ids_to_fetch),
        description="anilist person phase",
        enabled=show_progress,
    ) as p:
        for idx, person_id in enumerate(ids_to_fetch, 1):
            try:
                resp = await client.get_person_details(person_id)
                if resp is None:
                    skipped_count += 1
                    log.info(
                        "person_skipped_not_found",
                        source="anilist",
                        person_id=person_id,
                    )
                else:
                    staff = resp.get("Staff")
                    if staff:
                        person = parse_anilist_person(staff)
                        person_batch.append(person)
                        totals["persons"] += 1

                        if person.image_large or person.image_medium:
                            download_queue.add_person(
                                person.id, person.image_large, person.image_medium
                            )
                            totals["images"] += 1

            except Exception as e:
                log.warning(
                    "person_fetch_failed",
                    source="anilist",
                    person_id=person_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )

            if len(person_batch) >= 3:
                save_persons_batch_to_bronze(persons_bw, person_batch)
                person_batch.clear()

            p.advance()

        if person_batch:
            save_persons_batch_to_bronze(persons_bw, person_batch)
        persons_bw.flush()

    if skipped_count:
        log.info("anilist_persons_skipped", count=skipped_count)


@app.command()
def run(
    checkpoint_interval: int = typer.Option(
        SCRAPE_CHECKPOINT_INTERVAL, "--checkpoint", help="チェックポイント間隔"
    ),
    force: ForceOpt = False,
    resume: ResumeOpt = True,
    limit: LimitOpt = 0,
    delay: DelayOpt = 0.0,
    log_level: str = typer.Option(
        "error", "--log-level", help="ログレベル (debug/info/warning/error)"
    ),
    skip_existing_persons: bool = typer.Option(
        True,
        "--skip-existing-persons/--update-all-persons",
        help="既存人物をスキップ（高速化）",
    ),
    update: bool = typer.Option(
        False,
        "--update",
        "-u",
        help="アニメリストを更新取得（キャッシュを使わない、放映中/新規のみ更新）",
    ),
    reverse: bool = typer.Option(
        False, "--reverse", "-r", help="古い順で取得（デフォルト: 新しい順）"
    ),
    recent: bool = typer.Option(
        False, "--recent", help="放映開始日が新しい順で取得（新作アニメの追加に使用）"
    ),
    since: str = typer.Option(
        None,
        "--since",
        help="ISO形式の日時以降のみ差分更新 (YYYY-MM-DDTHH:MM:SS、SILVER.anime のfetched_at基準)",
    ),
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """AniList からクレジットデータを収集する (チェックポイント機能付き)."""
    import json
    from pathlib import Path
    from src.infra.logging import setup_logging
    from rich.console import Console
    from src.scrapers.logging_utils import configure_file_logging

    # Setup logging with specified level
    setup_logging()
    log_path = configure_file_logging("anilist")
    log.info("anilist_command_start", log_file=str(log_path))

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
    show_progress = _progress_enabled(resolve_progress_enabled(quiet, progress))

    # Cache files
    checkpoint_file = (
        Path(__file__).parent.parent.parent / "data" / "anilist_checkpoint.json"
    )
    anime_list_cache_file = (
        Path(__file__).parent.parent.parent / "data" / "anilist_anime_list_cache.json"
    )
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    anime_list_cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Initialize download queue
    from src.utils.download_queue import DownloadQueue

    download_queue = DownloadQueue()

    # Import Rich components for checkpoint display
    from rich.rule import Rule
    from rich.panel import Panel
    from rich.table import Table

    # Fetched IDs are tracked via checkpoint JSON only
    fetched_ids = set()

    # Load checkpoint - auto-resume if exists (unless --force is specified)
    start_index = 0
    checkpoint_exists = checkpoint_file.exists()

    # デフォルト: チェックポイント存在時は自動で続きから始める
    # --force フラグで最初から始められる
    if checkpoint_exists and not force:
        from src.scrapers.checkpoint import Checkpoint
        cp = Checkpoint(checkpoint_file)
        start_index = cp.get("last_index", 0)
        fetched_ids.update(cp.get("fetched_ids", []))

        # Display checkpoint recovery message
        console.print()
        console.print(
            Rule(
                "[bold bright_yellow]チェックポイント復旧[/bold bright_yellow]",
                style="bright_yellow",
            )
        )
        checkpoint_table = Table(show_header=False, box=None, padding=(0, 2))
        checkpoint_table.add_row(
            "[bright_yellow]前回の進捗[/bright_yellow]",
            f"[bold bright_green]{start_index:,}[/bold bright_green]件処理済み",
        )
        checkpoint_table.add_row(
            "[bright_yellow]今回の開始位置[/bright_yellow]",
            f"[bold bright_cyan]{start_index + 1:,}[/bold bright_cyan]件目から",
        )
        checkpoint_table.add_row(
            "[bright_yellow]タイムスタンプ[/bright_yellow]",
            f"[dim]{cp.get('timestamp', 'N/A')}[/dim]",
        )
        console.print(
            Panel(checkpoint_table, border_style="bright_yellow", padding=(1, 2))
        )
        console.print()

        log.info(
            "checkpoint_loaded",
            start_index=start_index,
            fetched_count=len(fetched_ids),
        )

    # Fetch with incremental saving
    async def fetch_with_checkpoints(since_ids: set[str] | None = None):
        """Execute scraping with checkpoint-based incremental saving.

        Args:
            since_ids: Set of anime IDs already fetched (skip them)
        """
        since_ids = since_ids or set()
        from rich.progress import (
            Progress,
            SpinnerColumn,
            BarColumn,
            TextColumn,
            TimeElapsedColumn,
            MofNCompleteColumn,
        )
        from rich.panel import Panel
        from rich.table import Table
        from rich.rule import Rule
        from rich.align import Align
        import time as time_module
        from src.scrapers.checkpoint import Checkpoint

        # === Helper Functions (Nested for Clarity) ===

        def load_existing_person_ids_from_database():
            """No-op: ETL handles deduplication; return empty set."""
            return set()

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
                show_header=False, box=ROUNDED, padding=(0, 2), border_style="cyan"
            )
            summary_table.add_row(
                "[bold cyan]📋 アニメリスト取得完了[/bold cyan]",
                f"[bold bright_green]{anime_count}件[/bold bright_green]",
            )
            if skipped_count > 0:
                summary_table.add_row(
                    "[dim]  └ スキップ済み[/dim]", f"[dim]{skipped_count}件[/dim]"
                )
            to_process = anime_count - skipped_count
            summary_table.add_row(
                "[bold cyan]📥 フェーズ2で処理[/bold cyan]",
                f"[bold bright_yellow]{to_process}件[/bold bright_yellow]",
            )
            return summary_table

        def display_phase1_summary(summary_table):
            """Display Phase 1 completion panel."""
            console.print()
            console.print(
                Rule("[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]", style="cyan")
            )
            console.print(
                Panel(
                    summary_table,
                    title="[bold bright_green]✅ 完了[/bold bright_green]",
                    border_style="bright_green",
                    padding=(1, 2),
                )
            )
            console.print()

        def create_final_summary_table(totals, elapsed):
            """Create final completion summary table."""
            from rich.box import ROUNDED

            final_table = Table(
                show_header=True,
                header_style="bold white on green",
                box=ROUNDED,
                border_style="green",
                padding=(0, 1),
            )
            final_table.add_column("🎯 項目", style="cyan", width=25)
            final_table.add_column(
                "📊 件数", justify="right", style="bold green", width=20
            )

            # Main data
            final_table.add_row(
                "🎬 アニメ作品",
                f"[bold bright_green]{totals['anime']:,}[/bold bright_green]",
            )
            final_table.add_row(
                "👥 人物（新規）",
                f"[bold bright_green]{totals['persons']:,}[/bold bright_green]",
            )
            final_table.add_row(
                "  └ 🎤 声優", f"[bright_blue]{totals['voice_actors']:,}[/bright_blue]"
            )
            final_table.add_row(
                "🎭 キャラクター", f"[bright_blue]{totals.get('characters', 0):,}[/bright_blue]"
            )

            if totals.get("skipped", 0) > 0:
                final_table.add_row(
                    "  └ ⏭️  スキップ", f"[dim]{totals['skipped']:,}[/dim]"
                )

            final_table.add_row("", "")  # Separator

            final_table.add_row(
                "📝 クレジット",
                f"[bold bright_green]{totals['credits']:,}[/bold bright_green]",
            )
            final_table.add_row(
                "🖼️  画像ファイル",
                f"[bold bright_green]{totals['images']:,}[/bold bright_green]",
            )

            if totals.get("errors", 0) > 0:
                final_table.add_row(
                    "❌ エラー", f"[bold red]{totals['errors']}[/bold red]"
                )

            final_table.add_row("", "")  # Separator

            # Performance metrics
            rate = totals["anime"] / elapsed if elapsed > 0 else 0
            final_table.add_row(
                "⏱️  所要時間",
                f"[bold bright_blue]{format_elapsed_time(elapsed)}[/bold bright_blue]",
            )
            final_table.add_row(
                "⚡ 平均速度",
                f"[bold bright_yellow]{rate:.2f} 作品/秒[/bold bright_yellow]",
            )

            return final_table

        def display_final_summary(final_table):
            """Display final completion panel."""
            console.print("\n")
            console.print(
                Rule(
                    "[bold bright_green]データ収集完了[/bold bright_green]",
                    style="bright_green",
                )
            )
            console.print()
            console.print(
                Panel(
                    final_table,
                    title="[bold bright_green]🎉 大成功！[/bold bright_green]",
                    border_style="bright_green",
                    padding=(1, 2),
                )
            )
            console.print()
            console.print(
                Align.center(
                    "[bold cyan]✨ チェックポイントファイル削除完了[/bold cyan]"
                )
            )
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
                "total_characters": totals.get("characters", 0),
                "total_errors": totals["errors"],
                "timestamp": timestamp,
            }


        def display_checkpoint_panel(checkpoint_num, stats_table):
            """Display checkpoint save panel."""
            console.print("\n")
            console.print(
                Rule(
                    f"[bold bright_yellow]💾 チェックポイント #{checkpoint_num}[/bold bright_yellow]",
                    style="bright_yellow",
                )
            )
            console.print(
                Panel(
                    stats_table,
                    title="[bold bright_yellow]✅ 保存完了[/bold bright_yellow]",
                    border_style="bright_yellow",
                    padding=(1, 2),
                )
            )
            console.print()

        async def fetch_anime_list_from_api(
            client, fetched_ids, use_cache=True, anime_list_cache_file=None
        ):
            """Fetch anime list from API with optional caching and smart updates."""
            # Try to load from cache if not updating
            if (
                use_cache
                and not update
                and anime_list_cache_file
                and anime_list_cache_file.exists()
            ):
                try:
                    with open(anime_list_cache_file) as f:
                        cached_data = json.load(f)
                        anime_items = []
                        for item in cached_data.get("anime_list", []):
                            anime_dict = item["anime"]
                            anilist_id = anime_dict["anilist_id"]
                            anime_id = anime_dict["id"]
                            # Reconstruct Anime object
                            anime = BronzeAnime(**anime_dict)
                            anime_items.append((anime, anilist_id, anime_id))

                        # Apply reverse sorting if needed
                        if reverse:
                            # Sort by start date (oldest first)
                            anime_items.sort(key=lambda x: x[0].year or 9999)
                        else:
                            # Sort by year descending (newest first)
                            anime_items.sort(key=lambda x: x[0].year or 0, reverse=True)

                        # Filter by fetched_ids
                        anime_ids = [
                            item for item in anime_items if item[2] not in fetched_ids
                        ]

                        if len(anime_ids) > 0:
                            console.print()
                            sort_info = " (古い順)" if reverse else ""
                            console.print(
                                Rule(
                                    f"[bold cyan]フェーズ1: アニメリスト（キャッシュ使用{sort_info}）[/bold cyan]",
                                    style="cyan",
                                )
                            )
                            cache_info = Table(
                                show_header=False, box=None, padding=(0, 2)
                            )
                            cache_info.add_row(
                                "[cyan]キャッシュから読込[/cyan]",
                                f"[bold green]{len(anime_ids)}件[/bold green]",
                            )
                            console.print(
                                Panel(cache_info, border_style="cyan", padding=(1, 2))
                            )
                            console.print()
                            return anime_ids
                except Exception as e:
                    log.warning(
                        "cache_load_failed",
                        source="anilist",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                    # Fall through to API fetch

            # Fetch from API
            console.print()

            # Load previous cache if updating (for smart filtering)
            prev_cache = {}
            if update and anime_list_cache_file and anime_list_cache_file.exists():
                try:
                    with open(anime_list_cache_file) as f:
                        prev_cache = json.load(f)
                    console.print(
                        Rule(
                            "[bold cyan]フェーズ1: アニメリスト更新（放映中・新規のみ）[/bold cyan]",
                            style="cyan",
                        )
                    )
                except Exception as e:
                    log.warning(
                        "prev_cache_load_failed",
                        source="anilist",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                    console.print(
                        Rule(
                            "[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]",
                            style="cyan",
                        )
                    )
            else:
                console.print(
                    Rule(
                        "[bold cyan]フェーズ1: アニメリスト取得[/bold cyan]",
                        style="cyan",
                    )
                )

            console.print()

            anime_ids = []
            all_anime_for_cache = []

            # Build lookup for previous anime status
            prev_anime_status = {}
            if prev_cache:
                for item in prev_cache.get("anime_list", []):
                    anime_id = item["anime"]["id"]
                    prev_anime_status[anime_id] = item.get("status")

            if recent:
                sort_order = ["START_DATE_DESC"]
            elif reverse:
                sort_order = ["START_DATE_ASC"]
            else:
                sort_order = ["POPULARITY_DESC"]

            from rich.live import Live
            from rich.console import Group
            from rich.text import Text

            bar1 = Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[cyan]{task.description}[/cyan]"),
                TimeElapsedColumn(),
            )
            task1 = bar1.add_task("📋 アニメリスト取得中...", total=None)
            status_line_1 = Text("📋 アニメリスト取得中...", style="bold cyan")

            def _make_1_group():
                parts = [status_line_1, bar1]
                rl = _make_rate_limit_text(client)
                if str(rl):
                    parts.append(rl)
                return Group(*parts)

            page = 1
            has_next_page = True
            with Live(_make_1_group(), console=console, refresh_per_second=4) as live1:
                while has_next_page:
                    resp = await client.get_top_anime(
                        page=page, per_page=50, sort=sort_order
                    )
                    page_data = resp.get("Page", {})
                    has_next_page = page_data.get("pageInfo", {}).get("hasNextPage", False)

                    for raw in page_data.get("media", []):
                        anime = parse_anilist_anime(raw)
                        current_status = raw.get("status")

                        # Parse studios and relations from raw data
                        parsed_studios, parsed_anime_studios = parse_anilist_studios(
                            raw, anime.id
                        )
                        if parsed_studios:
                            studios_pending[anime.id] = (
                                parsed_studios,
                                parsed_anime_studios,
                            )
                        parsed_relations = parse_anilist_relations(raw, anime.id)
                        if parsed_relations:
                            relations_pending[anime.id] = parsed_relations

                        # Store for caching (all anime)
                        all_anime_for_cache.append(
                            {
                                "anime": anime.model_dump(),
                                "status": current_status,
                                "fetched_at": time_module.time(),
                            }
                        )

                        # Smart filtering for --update mode
                        if update and prev_cache:
                            prev_status = prev_anime_status.get(anime.id)

                            # Include if: new anime OR (was/is airing)
                            is_new = anime.id not in prev_anime_status
                            was_airing = prev_status in ("CURRENTLY_AIRING", None)
                            is_airing = current_status == "CURRENTLY_AIRING"

                            if is_new or was_airing or is_airing:
                                if anime.id not in fetched_ids:
                                    anime_ids.append(
                                        (anime, anime.anilist_id, anime.id)
                                    )
                        else:
                            # Normal mode: include all
                            if anime.id not in fetched_ids:
                                anime_ids.append((anime, anime.anilist_id, anime.id))

                    desc = f"📋 アニメリスト取得中 ({page}ページ, {len(all_anime_for_cache)}件)"
                    bar1.update(task1, description=desc)
                    status_line_1 = Text(desc, style="bold cyan")
                    live1.update(_make_1_group())
                    page += 1

            # Display update mode info
            if update and prev_cache and len(all_anime_for_cache) > 0:
                total_checked = len(all_anime_for_cache)
                console.print()
                update_info = Table(show_header=False, box=None, padding=(0, 2))
                update_info.add_row(
                    "[cyan]チェック対象[/cyan]", f"[dim]{total_checked}件[/dim]"
                )
                update_info.add_row(
                    "[cyan]処理対象（放映中・新規）[/cyan]",
                    f"[bold yellow]{len(anime_ids)}件[/bold yellow]",
                )
                console.print(Panel(update_info, border_style="cyan", padding=(1, 2)))
                console.print()

            # Save to cache
            if anime_list_cache_file:
                try:
                    cache_data = {
                        "count": len(all_anime_for_cache),
                        "fetched_at": time_module.time(),
                        "anime_list": all_anime_for_cache,
                    }
                    with open(anime_list_cache_file, "w") as f:
                        json.dump(cache_data, f, indent=2, default=str)
                    log.info(
                        "anime_list_cached",
                        count=len(all_anime_for_cache),
                        file=str(anime_list_cache_file),
                    )
                except Exception as e:
                    log.warning(
                        "cache_save_failed",
                        source="anilist",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )

            return anime_ids

        async def fetch_staff_ids_for_anime(client, anilist_id, anime_id):
            """Phase 2A: アニメ1件のスタッフID・ロール一覧を取得（最小クエリ）.

            Returns: (credits, person_ids, va_count, characters, cva_list, had_error)
            """
            credits = []
            person_ids = set()
            va_count = 0
            all_characters = []
            all_cva_list = []
            seen_char_ids: set = set()

            try:
                staff_page = 1
                char_page = 1
                has_more_staff = True
                has_more_chars = True

                while has_more_staff or has_more_chars:
                    resp = await client.get_anime_staff_minimal(
                        anilist_id,
                        staff_page=staff_page if has_more_staff else 1,
                        staff_per_page=25 if has_more_staff else 1,
                        char_page=char_page if has_more_chars else 1,
                        char_per_page=25 if has_more_chars else 1,
                    )
                    media = resp.get("Media", {})

                    if has_more_staff:
                        staff = media.get("staff", {})
                        for edge in staff.get("edges", []):
                            staff_id = edge.get("node", {}).get("id")
                            role = edge.get("role")
                            if staff_id and role:
                                person_ids.add(staff_id)
                                credits.append(
                                    Credit(
                                        person_id=f"anilist:p{staff_id}",
                                        anime_id=anime_id,
                                        role=parse_role(role),
                                        raw_role=role,  # 元のロール文字列を保存
                                        source="anilist",
                                    )
                                )
                        has_more_staff = staff.get("pageInfo", {}).get(
                            "hasNextPage", False
                        )
                        if has_more_staff:
                            staff_page += 1

                    if has_more_chars:
                        characters_obj = media.get("characters", {})
                        char_edges = characters_obj.get("edges", [])
                        page_chars, page_cvas = parse_anilist_characters(char_edges, anime_id)
                        for char in page_chars:
                            if char.anilist_id not in seen_char_ids:
                                seen_char_ids.add(char.anilist_id)
                                all_characters.append(char)
                        all_cva_list.extend(page_cvas)
                        for char_edge in char_edges:
                            for va in char_edge.get("voiceActors") or []:
                                va_id = va.get("id")
                                if va_id:
                                    person_ids.add(va_id)
                                    va_count += 1
                                    credits.append(
                                        Credit(
                                            person_id=f"anilist:p{va_id}",
                                            anime_id=anime_id,
                                            role=parse_role("Voice Actor"),
                                            raw_role="Voice Actor",
                                            source="anilist",
                                        )
                                    )
                        has_more_chars = characters_obj.get("pageInfo", {}).get(
                            "hasNextPage", False
                        )
                        if has_more_chars:
                            char_page += 1

            except Exception as e:
                log.error(
                    "staff_list_fetch_failed",
                    source="anilist",
                    anime_id=anime_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                return credits, person_ids, va_count, all_characters, all_cva_list, True

            return credits, person_ids, va_count, all_characters, all_cva_list, False

        # === Main Execution ===

        # Studios and relations parsed in Phase 1 (keyed by anime_id)
        studios_pending: dict[str, tuple[list[Studio], list[AnimeStudio]]] = {}
        relations_pending: dict[str, list[AnimeRelation]] = {}

        client = AniListClient()
        await client.verify_auth()
        totals = {
            "anime": 0,
            "persons": 0,
            "credits": 0,
            "images": 0,
            "voice_actors": 0,
            "characters": 0,
            "errors": 0,
            "skipped": 0,
        }
        start_time = time_module.time()

        try:
            # ===== PHASE 1: Fetch Anime List =====
            anime_ids = await _load_anime_ids(
                client,
                fetched_ids,
                console,
                update,
                reverse,
                anime_list_cache_file,
                fetch_anime_list_from_api,
                create_phase1_summary_table,
                display_phase1_summary,
            )

            # ===== PHASE 2A: スタッフリスト収集 (クレジットとID) =====
            from src.scrapers.bronze_writer import BronzeWriter, BronzeWriterGroup

            with BronzeWriterGroup(
                "anilist",
                tables=["anime", "credits", "studios", "anime_studios", "relations", "characters", "character_voice_actors"],
            ) as g:
                anime_bw = g["anime"]
                credits_bw = g["credits"]
                studios_bw = g["studios"]
                anime_studios_bw = g["anime_studios"]
                relations_bw = g["relations"]
                characters_bw = g["characters"]
                cva_bw = g["character_voice_actors"]

                all_person_ids_to_fetch = await _fetch_staff_phase(
                    client,
                    anime_bw,
                    credits_bw,
                    studios_bw,
                    anime_studios_bw,
                    relations_bw,
                    characters_bw,
                    cva_bw,
                    anime_ids,
                    totals,
                    fetched_ids,
                    download_queue,
                    checkpoint_interval,
                    checkpoint_file,
                    fetch_staff_ids_for_anime,
                    create_checkpoint_data,
                    save_checkpoint,
                    time_module,
                    studios_pending=studios_pending,
                    relations_pending=relations_pending,
                    since_ids=since_ids,
                    show_progress=show_progress,
                )

            # Phase 2A サマリ
            ids_to_fetch = sorted(all_person_ids_to_fetch)
            total_unique = len(all_person_ids_to_fetch)

            console.print()
            console.print(
                f"[cyan]✅ フェーズ2A完了: {totals['anime']}件のアニメ → "
                f"{totals['credits']:,}クレジット, "
                f"{total_unique:,}人 (フェーズ2Bで取得)[/cyan]"
            )

            # ===== PHASE 2B: 個人情報詳細取得 =====
            with BronzeWriter("anilist", table="persons") as persons_bw:
                await _fetch_person_details_phase(
                    client,
                    persons_bw,
                    ids_to_fetch,
                    totals,
                    download_queue,
                    show_progress=show_progress,
                )

            # Delete checkpoint file on completion
            cp = Checkpoint(checkpoint_file)
            cp.delete()

            # Display final summary
            elapsed = time_module.time() - start_time
            final_table = create_final_summary_table(totals, elapsed)
            display_final_summary(final_table)

            # Display download queue info
            queue_size = download_queue.count()
            if queue_size > 0:
                console.print()
                console.print(
                    Rule(
                        "[bold bright_magenta]📥 画像ダウンロードキュー[/bold bright_magenta]",
                        style="bright_magenta",
                    )
                )
                queue_info = Table(show_header=False, box=None, padding=(0, 2))
                queue_info.add_row(
                    "[bright_magenta]キュー内の画像[/bright_magenta]",
                    f"[bold yellow]{queue_size}件[/bold yellow]",
                )
                queue_info.add_row(
                    "[dim]処理方法[/dim]", "[dim]pixi run process-downloads[/dim]"
                )
                console.print(
                    Panel(queue_info, border_style="bright_magenta", padding=(1, 2))
                )
                console.print()

        finally:
            await client.close()

    # Handle --since mode: skip anime already fetched
    since_ids = set()
    if since:
        since_ids = get_anime_ids_to_skip_since(since)
        console.print(
            f"[cyan]✓ --since mode: {len(since_ids)} anime to skip[/cyan]"
        )

    asyncio.run(fetch_with_checkpoints(since_ids))


@app.command("fetch-persons")
def fetch_persons(
    log_level: str = typer.Option(
        "error", "--log-level", help="ログレベル (debug/info/warning/error)"
    ),
) -> None:
    """Bronze クレジットに含まれる未取得の個人情報を取得する."""
    import asyncio
    import time as time_module
    from src.infra.logging import setup_logging
    from src.scrapers.logging_utils import configure_file_logging

    setup_logging(log_level)
    log_path = configure_file_logging("anilist")
    log.info("anilist_fetch_persons_command_start", log_file=str(log_path))

    async def _run():
        from rich.console import Console
        from rich.rule import Rule

        import pyarrow.dataset as ds

        from src.scrapers.bronze_writer import DEFAULT_BRONZE_ROOT, BronzeWriter

        console = Console()

        # Read anilist person IDs from bronze credits parquet
        credits_path = DEFAULT_BRONZE_ROOT / "source=anilist" / "table=credits"
        if not credits_path.exists():
            console.print("[yellow]Bronze credits not found — run 'run' first.[/yellow]")
            return

        credits_ds = ds.dataset(credits_path, format="parquet")
        tbl = credits_ds.to_table(columns=["person_id"])
        all_credit_person_ids = set(
            pid for pid in tbl.column("person_id").to_pylist() if pid is not None
        )

        # Also read already-fetched persons from bronze
        persons_path = DEFAULT_BRONZE_ROOT / "source=anilist" / "table=persons"
        existing_ids: set[str] = set()
        if persons_path.exists():
            p_ds = ds.dataset(persons_path, format="parquet")
            p_tbl = p_ds.to_table(columns=["id"])
            existing_ids = set(
                pid for pid in p_tbl.column("id").to_pylist() if pid is not None
            )

        # Collect missing AniList person IDs
        missing_ids = set()
        for pid in all_credit_person_ids:
            if pid not in existing_ids and pid.startswith("anilist:p"):
                try:
                    missing_ids.add(int(pid.removeprefix("anilist:p")))
                except ValueError:
                    pass

        ids_to_fetch = sorted(missing_ids)

        console.print()
        console.print(
            Rule(
                "[bold magenta]個人情報取得 (Bronze クレジットから)[/bold magenta]",
                style="magenta",
            )
        )
        console.print(
            f"  クレジット内の人物: [bold]{len(all_credit_person_ids):,}[/bold]人"
        )
        console.print(f"  取得済み:           [dim]{len(existing_ids):,}[/dim]人")
        console.print(
            f"  未取得:             [bold yellow]{len(ids_to_fetch):,}[/bold yellow]人"
        )
        console.print()

        if not ids_to_fetch:
            console.print("[green]全員取得済みです。[/green]")
            return

        console.print(
            f"[dim]🔑 トークン: {'あり' if _env.get('ANILIST_ACCESS_TOKEN') else 'なし'}[/dim]"
        )

        from src.utils.download_queue import DownloadQueue

        client = AniListClient()
        await client.verify_auth()
        download_queue = DownloadQueue()
        persons_bw = BronzeWriter("anilist", table="persons")
        person_batch = []
        fetched = 0
        errors = 0
        start_time = time_module.time()

        skipped_count = 0

        try:
            from src.scrapers.progress import scrape_progress as _scrape_progress

            with _scrape_progress(
                total=len(ids_to_fetch),
                description="anilist fetch-persons",
            ) as p:
                for idx, person_id in enumerate(ids_to_fetch, 1):
                    try:
                        resp = await client.get_person_details(person_id)
                        if resp is None:
                            skipped_count += 1
                            log.info(
                                "person_skipped_not_found",
                                source="anilist",
                                person_id=person_id,
                            )
                        else:
                            staff = resp.get("Staff")
                            if staff:
                                person = parse_anilist_person(staff)
                                person_batch.append(person)
                                fetched += 1

                                if person.image_large or person.image_medium:
                                    download_queue.add_person(
                                        person.id,
                                        person.image_large,
                                        person.image_medium,
                                    )
                    except Exception as e:
                        log.warning(
                            "person_fetch_failed",
                            source="anilist",
                            person_id=person_id,
                            error_type=type(e).__name__,
                            error_message=str(e),
                        )
                        errors += 1

                    if len(person_batch) >= 3:
                        save_persons_batch_to_bronze(persons_bw, person_batch)
                        person_batch.clear()

                    p.advance()

                if person_batch:
                    save_persons_batch_to_bronze(persons_bw, person_batch)
                persons_bw.flush()

            if skipped_count:
                log.info("anilist_persons_skipped", count=skipped_count)

        finally:
            await client.close()
            persons_bw.flush()
            persons_bw.compact()

        elapsed = time_module.time() - start_time
        console.print()
        console.print(
            f"[green]✅ 完了: {fetched:,}人取得, {errors}件エラー ({elapsed:.1f}秒)[/green]"
        )

        queue_size = download_queue.count()
        if queue_size > 0:
            console.print(
                f"[dim]📥 画像キュー: {queue_size}件 → task download-images で取得[/dim]"
            )

    asyncio.run(_run())


@app.command()
def process_downloads(
    log_level: str = typer.Option(
        "error", "--log-level", help="ログレベル (debug/info/warning/error)"
    ),
) -> None:
    """Process pending image downloads from queue."""
    import asyncio
    from src.infra.logging import setup_logging
    from src.scrapers.logging_utils import configure_file_logging
    from src.utils.download_queue import DownloadQueue
    from src.scrapers.image_downloader import (
        download_person_images,
        download_anime_images,
    )
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
    from rich.rule import Rule

    # Setup
    setup_logging()
    log_path = configure_file_logging("anilist")
    log.info("anilist_process_downloads_command_start", log_file=str(log_path))
    console = Console()

    # Load queue
    queue = DownloadQueue()
    if queue.is_empty():
        console.print("[dim]✅ ダウンロードキューは空です[/dim]")
        return

    console.print()
    console.print(
        Rule("[bold cyan]画像ダウンロードキュー処理[/bold cyan]", style="cyan")
    )
    console.print()

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
            console=console,
        ) as progress:
            task = progress.add_task("🖼️  ダウンロード中...", total=total_items)

            # Download person images
            if persons:
                person_data = [(p.item_id, p.url_large, p.url_medium) for p in persons]
                try:
                    person_paths = await download_person_images(
                        person_data, show_progress=False
                    )
                    # Update database
                    for person_id, paths in person_paths.items():
                        # Fetch person from DB and update paths
                        # This is simplified - in reality you'd query the DB
                        queue.remove_item(person_id)
                        processed += 1
                        progress.update(task, advance=1)
                except Exception as e:
                    log.error(
                        "person_download_failed",
                        source="anilist",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )

            # Download anime images
            if anime:
                anime_data = [
                    (a.item_id, a.url_medium, a.url_large, a.url_banner) for a in anime
                ]
                try:
                    anime_paths = await download_anime_images(
                        anime_data, show_progress=False
                    )
                    # Update database
                    for anime_id, paths in anime_paths.items():
                        queue.remove_item(anime_id)
                        processed += 1
                        progress.update(task, advance=1)
                except Exception as e:
                    log.error(
                        "anime_download_failed",
                        source="anilist",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )

        # Display summary
        console.print()
        summary = Table(show_header=False, box=None, padding=(0, 2))
        summary.add_row(
            "[cyan]ダウンロード完了[/cyan]", f"[bold green]{processed}件[/bold green]"
        )
        summary.add_row("[cyan]残りキュー[/cyan]", f"[dim]{queue.count()}件[/dim]")
        console.print(Panel(summary, border_style="cyan", padding=(1, 2)))
        console.print()

    # Run async processing
    asyncio.run(process_queue())


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1].startswith("--")):
        sys.argv.insert(1, "run")
    app()
