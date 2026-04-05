"""AniList GraphQL API によるスタッフクレジット収集.

httpx (async) + structlog + typer で構成。
レート制限: 90 requests/minute (未認証) / より高い (認証済み)。
"""

import asyncio
import time

import httpx
import structlog
import typer
from dotenv import find_dotenv, dotenv_values

from src.models import (
    Anime,
    AnimeRelation,
    AnimeStudio,
    Character,
    CharacterVoiceActor,
    Credit,
    Person,
    Studio,
    parse_role,
)
from src.utils.episode_parser import parse_episodes
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL

_env = dotenv_values(find_dotenv())

log = structlog.get_logger()

ANILIST_URL = "https://graphql.anilist.co"
# AniList公式: 90 req/min（ヘッダー表示）だが、2026-02時点で degraded state
# 実効 ~18-19 req/window → バースト方式: 高速で叩いて429が来たら待つ
# 参考: https://docs.anilist.co/guide/rate-limiting
REQUEST_INTERVAL = 0.1  # バースト方式: 最小間隔で叩き、429で自動待機

ANIME_STAFF_QUERY = """
query ($id: Int, $staffPage: Int, $staffPerPage: Int, $charPage: Int, $charPerPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    idMal
    title { romaji english native }
    seasonYear
    season
    episodes
    averageScore
    meanScore
    coverImage { large extraLarge medium }
    bannerImage
    description
    format
    status
    startDate { year month day }
    endDate { year month day }
    duration
    source
    countryOfOrigin
    isLicensed
    isAdult
    hashtag
    siteUrl
    genres
    synonyms
    tags { name rank }
    popularity
    favourites
    trailer { id site thumbnail }
    studios { edges { isMain node { id name isAnimationStudio favourites siteUrl } } }
    relations { edges { relationType node { id title { romaji } format } } }
    externalLinks { url site type }
    rankings { rank type format year season allTime context }
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
        node {
          id
          name { full native alternative }
          image { large medium }
          description
          gender
          dateOfBirth { year month day }
          age
          bloodType
          favourites
          siteUrl
        }
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

ANIME_STAFF_MINIMAL_QUERY = """
query ($id: Int, $staffPage: Int, $staffPerPage: Int, $charPage: Int, $charPerPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    staff(page: $staffPage, perPage: $staffPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node { id }
      }
    }
    characters(page: $charPage, perPage: $charPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native alternative }
          image { large medium }
          description
          gender
          dateOfBirth { year month day }
          age
          bloodType
          favourites
          siteUrl
        }
        voiceActors(language: JAPANESE) { id }
      }
    }
  }
}
"""

PERSON_DETAILS_QUERY = """
query ($id: Int) {
  Staff(id: $id) {
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
"""

TOP_ANIME_QUERY = """
query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    media(type: ANIME, sort: $sort) {
      id
      idMal
      title { romaji english native }
      seasonYear
      season
      episodes
      averageScore
      meanScore
      coverImage { large extraLarge medium }
      bannerImage
      description
      format
      status
      startDate { year month day }
      endDate { year month day }
      duration
      source
      countryOfOrigin
      isLicensed
      isAdult
      hashtag
      siteUrl
      genres
      synonyms
      tags { name rank }
      popularity
      favourites
      trailer { id site thumbnail }
      studios { edges { isMain node { id name isAnimationStudio favourites siteUrl } } }
      relations { edges { relationType node { id title { romaji } format } } }
      externalLinks { url site type }
      rankings { rank type format year season allTime context }
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


def save_studios_to_database(conn, studios, anime_studios):
    """Save studios and anime-studio relationships to database."""
    from src.database import upsert_studio, insert_anime_studio

    for studio in studios:
        upsert_studio(conn, studio)
    for anime_studio in anime_studios:
        insert_anime_studio(conn, anime_studio)


def save_relations_to_database(conn, relations):
    """Save anime relations to database."""
    from src.database import insert_anime_relation

    for relation in relations:
        insert_anime_relation(conn, relation)


def save_persons_batch_to_database(conn, persons_batch):
    """Save a batch of persons to database."""
    from src.database import upsert_person

    for person in persons_batch:
        upsert_person(conn, person)


def _make_rate_limit_text(cl):
    """API消費量バーを生成 (used/max)."""
    from rich.text import Text

    if cl.rate_limit_max is None and cl.requests_remaining is None:
        return Text("")
    # ヘッダーは90と言うが実際は~20 (degraded + burst limiter)
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


def save_credits_batch_to_database(conn, credits_batch):
    """Save a batch of credits to database."""
    from src.database import insert_credit

    for credit in credits_batch:
        insert_credit(conn, credit)


class AniListClient:
    """AniList GraphQL 非同期クライアント (認証サポート)."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        # Load authentication token from .env
        self._access_token = _env.get("ANILIST_ACCESS_TOKEN")
        if self._access_token:
            log.info("anilist_token_loaded", will_attempt_auth=True)
        else:
            log.info("anilist_no_token", will_use_unauthenticated=True)
        # Create client without default headers (auth added per-request in query())
        self._client = httpx.AsyncClient(timeout=60.0)

        # Rate limit tracking
        self.requests_remaining = None
        self.rate_limit_reset_at = None
        self.rate_limit_max = None  # X-RateLimit-Limit (90=認証済み, 30=未認証)
        self._auth_reported = False
        self._requests_since_reset = 0  # 直近のリセットからのリクエスト数
        self._requests_total = 0  # プロセス起動からの総リクエスト数
        self._last_remaining_before_429 = None  # 429直前のX-RateLimit-Remaining
        # Callback: called with remaining seconds during rate limit wait
        # signature: on_rate_limit(remaining_secs: int | None)  None=wait ended
        self.on_rate_limit: callable | None = None

    async def close(self) -> None:
        await self._client.aclose()

    async def verify_auth(self) -> bool:
        """トークンの有効性を検証し、rate limitウィンドウの残量を同期する。

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

                # API側のウィンドウ残量から、前回実行分の消費を推定
                already_used = self.rate_limit_max - self.requests_remaining
                if already_used > 1:  # 1 = 今のverify_auth自身
                    carry_over = already_used - 1
                    log.info(
                        "rate_limit_window_carry_over",
                        source="anilist",
                        remaining=self.requests_remaining,
                        limit=self.rate_limit_max,
                        carry_over_from_previous=carry_over,
                    )
                    # ウィンドウの残りが少ない場合、リセットまで待つ
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
                        hint="トークンが無効・期限切れです。https://anilist.co/settings/developer で再取得してください",
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
                self._requests_since_reset += 1
                self._requests_total += 1

                if resp.status_code == 429:
                    # 429時はX-RateLimit-*ヘッダーが来ないことがある
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    limit = (
                        self.rate_limit_max
                        or int(resp.headers.get("X-RateLimit-Limit", 0))
                        or None
                    )
                    # 429レスポンス自体のヘッダーも確認
                    remaining_in_429 = resp.headers.get("X-RateLimit-Remaining")
                    limit_in_429 = resp.headers.get("X-RateLimit-Limit")
                    log.warning(
                        "rate_limited",
                        source="anilist",
                        retry_after_seconds=retry_after,
                        requests_since_reset=self._requests_since_reset,
                        requests_total=self._requests_total,
                        last_known_remaining=self._last_remaining_before_429,
                        header_remaining=remaining_in_429,
                        header_limit=limit_in_429,
                        rate_limit_max=limit,
                        authenticated=bool(self._access_token),
                        note="limit < 90: トークンが無効か未認証の可能性"
                        if limit and limit < 90
                        else None,
                    )
                    self.requests_remaining = 0
                    self._requests_since_reset = 0
                    self._last_remaining_before_429 = None
                    # X-RateLimit-Resetがあればそちらを優先（API側の正確なリセット時刻）
                    reset_header = resp.headers.get("X-RateLimit-Reset")
                    if reset_header:
                        reset_at = int(reset_header)
                        wait_seconds = (
                            max(reset_at - time.time(), retry_after) + 1
                        )  # +1秒余裕
                    else:
                        wait_seconds = retry_after + 1
                    self.rate_limit_reset_at = time.time() + wait_seconds
                    log.info(
                        "rate_limit_waiting",
                        source="anilist",
                        wait_seconds=int(wait_seconds),
                        reset_header=reset_header,
                    )
                    # 0.5秒刻みでコールバックを呼びながら待機
                    remaining = wait_seconds
                    while remaining > 0:
                        if self.on_rate_limit:
                            self.on_rate_limit(int(remaining))
                        await asyncio.sleep(0.5)
                        remaining -= 0.5
                    if self.on_rate_limit:
                        self.on_rate_limit(None)  # 待機終了
                    # 待機後にプローブで実際の残量を確認
                    try:
                        probe_resp = await self._client.post(
                            ANILIST_URL,
                            json={
                                "query": "query { SiteStatistics { anime { pageInfo { total } } } }",
                                "variables": {},
                            },
                            headers=headers,
                        )
                        probe_remaining = probe_resp.headers.get(
                            "X-RateLimit-Remaining"
                        )
                        probe_limit = probe_resp.headers.get("X-RateLimit-Limit")
                        if probe_remaining is not None:
                            self.requests_remaining = int(probe_remaining)
                        else:
                            self.requests_remaining = self.rate_limit_max or 90
                        log.info(
                            "rate_limit_probe_after_wait",
                            source="anilist",
                            probe_status=probe_resp.status_code,
                            probe_remaining=probe_remaining,
                            probe_limit=probe_limit,
                        )
                        if probe_resp.status_code == 429:
                            # まだリセットされていない → さらに待つ
                            extra_wait = (
                                int(probe_resp.headers.get("Retry-After", 30)) + 1
                            )
                            log.warning(
                                "rate_limit_not_yet_reset",
                                source="anilist",
                                extra_wait_seconds=extra_wait,
                            )
                            await asyncio.sleep(extra_wait)
                    except Exception:
                        self.requests_remaining = self.rate_limit_max or 90
                    continue

                # 正常レスポンスからrate limit情報を取得
                if "X-RateLimit-Remaining" in resp.headers:
                    self.requests_remaining = int(
                        resp.headers.get("X-RateLimit-Remaining", -1)
                    )
                    self._last_remaining_before_429 = self.requests_remaining
                    self.rate_limit_reset_at = int(
                        resp.headers.get("X-RateLimit-Reset", 0)
                    )
                    limit_val = resp.headers.get("X-RateLimit-Limit")
                    if limit_val and self.rate_limit_max is None:
                        self.rate_limit_max = int(limit_val)
                        if self.rate_limit_max >= 90:
                            log.info(
                                "auth_confirmed",
                                source="anilist",
                                rate_limit=self.rate_limit_max,
                                status="認証済み",
                            )
                        elif self._access_token:
                            log.warning(
                                "auth_token_ineffective",
                                source="anilist",
                                rate_limit=self.rate_limit_max,
                                expected=90,
                                hint="トークンが無効・期限切れの可能性。https://anilist.co/settings/developer で再取得してください",
                            )

                # Handle invalid token: fall back to unauthenticated
                if resp.status_code in (400, 401) and self._access_token:
                    try:
                        data = resp.json()
                        errors = data.get("errors", [])
                        # Log detailed error info
                        log.warning(
                            "auth_error_response",
                            source="anilist",
                            status=resp.status_code,
                            errors=errors,
                            token_format="JWT"
                            if "." in self._access_token
                            else "non-JWT",
                        )
                        # Check if error is token-related
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
                            self._access_token = (
                                None  # Disable token for future requests
                            )
                            continue  # Retry without token
                    except Exception as parse_error:
                        log.warning(
                            "failed_to_parse_error_response",
                            source="anilist",
                            error_type=type(parse_error).__name__,
                            error_message=str(parse_error),
                        )

                # 404 = 存在しないリソース → リトライ不要、Noneを返す
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
                return data.get("data", {})
            except httpx.HTTPError as e:
                log.warning(
                    "request_failed",
                    source="anilist",
                    attempt=attempt + 1,
                    variables=variables,
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                if attempt < 4:
                    await asyncio.sleep(2 ** (attempt + 1))
        from src.scrapers.exceptions import EndpointUnreachableError

        raise EndpointUnreachableError(
            "Failed to query AniList after 5 attempts",
            source="anilist",
            url=ANILIST_URL,
        )

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


def parse_anilist_person(staff: dict) -> Person:
    """Parse individual person details from PERSON_DETAILS_QUERY response."""
    anilist_person_id = staff.get("id")
    if not anilist_person_id:
        raise ValueError("Person ID is required")

    person_id = f"anilist:p{anilist_person_id}"
    name = staff.get("name", {})

    # Parse aliases (alternative names)
    aliases = []
    alternative_names = name.get("alternative", [])
    if alternative_names:
        aliases = list(set(a for a in alternative_names if a))

    # Parse images
    image = staff.get("image", {})
    image_large = image.get("large")
    image_medium = image.get("medium")

    # Parse date of birth
    dob_obj = staff.get("dateOfBirth", {})
    date_of_birth = None
    if dob_obj and dob_obj.get("year"):
        year = dob_obj.get("year")
        month = dob_obj.get("month") or 1
        day = dob_obj.get("day") or 1
        date_of_birth = f"{year}-{month:02d}-{day:02d}"

    # Parse years active
    years_active_raw = staff.get("yearsActive", [])
    years_active = [y for y in years_active_raw if y] if years_active_raw else []

    return Person(
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
        age=staff.get("age"),
        gender=staff.get("gender"),
        years_active=years_active,
        hometown=staff.get("homeTown"),
        blood_type=staff.get("bloodType"),
        description=staff.get("description"),
        # Popularity
        favourites=staff.get("favourites"),
        # Links
        site_url=staff.get("siteUrl"),
    )


def parse_anilist_anime(raw: dict) -> Anime:
    """Parse comprehensive anime data from AniList API response."""
    import json as _json

    anilist_id = raw["id"]
    title = raw.get("title", {})
    season_map = {
        "WINTER": "winter",
        "SPRING": "spring",
        "SUMMER": "summer",
        "FALL": "fall",
    }
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

    # Parse studios (handle both edge-based and node-based formats)
    studios_obj = raw.get("studios", {})
    studios_edges = studios_obj.get("edges", [])
    if studios_edges:
        studios = [
            e.get("node", {}).get("name")
            for e in studios_edges
            if e.get("node", {}).get("name")
        ]
    else:
        studios_nodes = studios_obj.get("nodes", [])
        studios = [s.get("name") for s in studios_nodes if s.get("name")]

    # Parse tags (limit to top 10 by rank)
    tags_data = raw.get("tags", [])
    tags = [
        {"name": t.get("name"), "rank": t.get("rank")}
        for t in tags_data
        if t.get("name")
    ]
    tags = sorted(tags, key=lambda x: x.get("rank", 0), reverse=True)[:10]

    # Parse trailer
    trailer_obj = raw.get("trailer") or {}
    trailer_url = None
    trailer_site = trailer_obj.get("site")
    trailer_id = trailer_obj.get("id")
    if trailer_id and trailer_site:
        if trailer_site == "youtube":
            trailer_url = f"https://www.youtube.com/watch?v={trailer_id}"
        elif trailer_site == "dailymotion":
            trailer_url = f"https://www.dailymotion.com/video/{trailer_id}"
        else:
            trailer_url = trailer_id

    # Parse relations (compact format)
    relations_json = None
    relations_data = raw.get("relations", {}).get("edges", [])
    if relations_data:
        relations = []
        for edge in relations_data:
            node = edge.get("node", {})
            if node.get("id"):
                relations.append(
                    {
                        "id": node["id"],
                        "type": edge.get("relationType"),
                        "title": (node.get("title") or {}).get("romaji"),
                        "format": node.get("format"),
                    }
                )
        if relations:
            relations_json = _json.dumps(relations, ensure_ascii=False)

    # Parse external links (compact format)
    external_links_json = None
    external_links_data = raw.get("externalLinks") or []
    if external_links_data:
        links = []
        for link in external_links_data:
            if link.get("url"):
                links.append(
                    {
                        "url": link["url"],
                        "site": link.get("site"),
                        "type": link.get("type"),
                    }
                )
        if links:
            external_links_json = _json.dumps(links, ensure_ascii=False)

    # Parse rankings (compact format)
    rankings_json = None
    rankings_data = raw.get("rankings") or []
    if rankings_data:
        rankings = []
        for r in rankings_data:
            rankings.append(
                {
                    "rank": r.get("rank"),
                    "type": r.get("type"),
                    "format": r.get("format"),
                    "year": r.get("year"),
                    "season": r.get("season"),
                    "allTime": r.get("allTime"),
                    "context": r.get("context"),
                }
            )
        if rankings:
            rankings_json = _json.dumps(rankings, ensure_ascii=False)

    return Anime(
        id=f"anilist:{anilist_id}",
        title_ja=title.get("native") or "",
        title_en=title.get("english") or title.get("romaji") or "",
        year=raw.get("seasonYear"),
        season=season_map.get(raw.get("season", ""), None),
        episodes=raw.get("episodes"),
        mal_id=raw.get("idMal"),
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
        mean_score=raw.get("meanScore"),
        # Studios
        studios=studios,
        # Extended metadata
        synonyms=raw.get("synonyms") or [],
        country_of_origin=raw.get("countryOfOrigin"),
        is_licensed=raw.get("isLicensed"),
        is_adult=raw.get("isAdult"),
        hashtag=raw.get("hashtag"),
        site_url=raw.get("siteUrl"),
        trailer_url=trailer_url,
        trailer_site=trailer_site,
        relations_json=relations_json,
        external_links_json=external_links_json,
        rankings_json=rankings_json,
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
        raw_role_str = edge.get("role", "")
        role = parse_role(raw_role_str)

        episodes = parse_episodes(raw_role_str)
        if episodes:
            # Create one Credit per episode
            for ep in sorted(episodes):
                credits.append(
                    Credit(
                        person_id=person_id,
                        anime_id=anime_id,
                        role=role,
                        raw_role=raw_role_str,
                        episode=ep,
                        source="anilist",
                    )
                )
        else:
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=role,
                    raw_role=raw_role_str,
                    source="anilist",
                )
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
            years_active = (
                [y for y in years_active_raw if y] if years_active_raw else []
            )

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

            # Voice actors don't have a standard "role" field, so we use a dedicated voice_actor role
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role("voice actor"),  # Will map to Role.VOICE_ACTOR
                    raw_role="Voice Actor",  # 元のロール文字列
                    source="anilist",
                )
            )

    return persons, credits


def parse_anilist_characters(
    character_edges: list[dict], anime_id: str
) -> tuple[list[Character], list[CharacterVoiceActor]]:
    """Parse character data and character-VA mappings from AniList character edges."""
    characters = []
    cva_list = []
    seen_chars = set()

    if not character_edges:
        return characters, cva_list

    for edge in character_edges:
        char_node = edge.get("node") or {}
        anilist_char_id = char_node.get("id")
        if not anilist_char_id:
            continue

        character_role = edge.get("role", "")  # MAIN, SUPPORTING, BACKGROUND

        # Parse character (deduplicate)
        if anilist_char_id not in seen_chars:
            seen_chars.add(anilist_char_id)
            char_id = f"anilist:c{anilist_char_id}"
            name = char_node.get("name", {})

            aliases = []
            alt_names = name.get("alternative", [])
            if alt_names:
                aliases = list(set(a for a in alt_names if a))

            image = char_node.get("image", {})

            # Parse date of birth
            dob_obj = char_node.get("dateOfBirth") or {}
            date_of_birth = None
            if dob_obj.get("year"):
                y = dob_obj["year"]
                m = dob_obj.get("month") or 1
                d = dob_obj.get("day") or 1
                date_of_birth = f"{y}-{m:02d}-{d:02d}"

            characters.append(
                Character(
                    id=char_id,
                    name_ja=name.get("native") or "",
                    name_en=name.get("full") or "",
                    aliases=aliases,
                    anilist_id=anilist_char_id,
                    image_large=image.get("large"),
                    image_medium=image.get("medium"),
                    description=char_node.get("description"),
                    gender=char_node.get("gender"),
                    date_of_birth=date_of_birth,
                    age=char_node.get("age"),
                    blood_type=char_node.get("bloodType"),
                    favourites=char_node.get("favourites"),
                    site_url=char_node.get("siteUrl"),
                )
            )

        # Parse character-VA mappings
        char_id = f"anilist:c{anilist_char_id}"
        for va in edge.get("voiceActors") or []:
            va_id = va.get("id")
            if va_id:
                cva_list.append(
                    CharacterVoiceActor(
                        character_id=char_id,
                        person_id=f"anilist:p{va_id}",
                        anime_id=anime_id,
                        character_role=character_role,
                        source="anilist",
                    )
                )

    return characters, cva_list


def parse_anilist_studios(
    raw: dict, anime_id: str
) -> tuple[list[Studio], list[AnimeStudio]]:
    """Parse studio data from AniList Media response.

    Returns (studios, anime_studio_edges).
    """
    studios = []
    anime_studios = []
    seen = set()

    studios_obj = raw.get("studios", {})
    edges = studios_obj.get("edges", [])
    if not edges:
        return studios, anime_studios

    for edge in edges:
        node = edge.get("node") or {}
        anilist_studio_id = node.get("id")
        if not anilist_studio_id:
            continue

        studio_id = f"anilist:s{anilist_studio_id}"
        is_main = edge.get("isMain", False)

        if anilist_studio_id not in seen:
            seen.add(anilist_studio_id)
            studios.append(
                Studio(
                    id=studio_id,
                    name=node.get("name") or "",
                    anilist_id=anilist_studio_id,
                    is_animation_studio=node.get("isAnimationStudio"),
                    favourites=node.get("favourites"),
                    site_url=node.get("siteUrl"),
                )
            )

        anime_studios.append(
            AnimeStudio(
                anime_id=anime_id,
                studio_id=studio_id,
                is_main=is_main,
            )
        )

    return studios, anime_studios


def parse_anilist_relations(raw: dict, anime_id: str) -> list[AnimeRelation]:
    """Parse relation edges from AniList Media response.

    Extracts SEQUEL, PREQUEL, SIDE_STORY, PARENT, etc. links between anime.
    """
    relations = []
    relations_obj = raw.get("relations", {})
    edges = relations_obj.get("edges", [])
    if not edges:
        return relations

    for edge in edges:
        node = edge.get("node") or {}
        node_id = node.get("id")
        if not node_id:
            continue

        title_obj = node.get("title") or {}
        relations.append(
            AnimeRelation(
                anime_id=anime_id,
                related_anime_id=f"anilist:{node_id}",
                relation_type=edge.get("relationType", ""),
                related_title=title_obj.get("romaji", ""),
                related_format=node.get("format"),
            )
        )

    return relations


async def fetch_top_anime_credits(
    n_anime: int = 50,
    show_progress: bool = True,
) -> tuple[list[Anime], list[Person], list[Credit]]:
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
    all_anime: list[Anime] = []
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
            status_p1 = Text("📋 アニメリスト取得中...", style="bold cyan")

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
                        f"📋 アニメリスト取得中 ({page}/{pages_needed})",
                        style="bold cyan",
                    )
                    live_p1.update(_make_p1_group())

            console.print(f"✅ アニメリスト取得完了: {len(anime_ids)}件\n")

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
            status_p2 = Text("📋 スタッフ情報取得中...", style="bold green")

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
    count,
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
        count,
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
    conn,
    anime_ids,
    totals,
    fetched_ids,
    console,
    download_queue,
    checkpoint_interval,
    checkpoint_file,
    fetch_staff_ids_for_anime,
    create_checkpoint_data,
    save_checkpoint,
    Progress,
    SpinnerColumn,
    BarColumn,
    MofNCompleteColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    time_module,
    studios_pending=None,
    relations_pending=None,
):
    """Phase 2A: Fetch staff lists (credits and person IDs) for each anime.

    Iterates over all anime, fetches minimal staff data, saves anime/credits
    to the database, and handles checkpointing.

    Returns:
        set of all person IDs that need to be fetched in Phase 2B.
    """
    from rich.rule import Rule
    from rich.live import Live
    from rich.console import Group
    from rich.text import Text

    console.print()
    console.print(
        Rule("[bold cyan]フェーズ2A: スタッフリスト収集[/bold cyan]", style="cyan")
    )
    console.print()

    all_person_ids_to_fetch = set()

    bar2a = Progress(
        SpinnerColumn(style="cyan"),
        BarColumn(
            bar_width=50,
            complete_style="bright_cyan",
            finished_style="bold bright_cyan",
        ),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )
    task2a = bar2a.add_task("", total=len(anime_ids))
    title_line = Text("📋 ...", style="bold cyan")
    wait_line_2a = Text("")

    def _make_2a_group():
        parts = [title_line, bar2a]
        rl = _make_rate_limit_text(client)
        if str(rl):
            parts.append(rl)
        if str(wait_line_2a):
            parts.append(wait_line_2a)
        return Group(*parts)

    live_2a = None  # Live への参照（コールバックから更新用）

    def _on_rate_limit_2a(secs):
        nonlocal wait_line_2a
        if secs is None:
            wait_line_2a = Text("")
        else:
            wait_line_2a = Text(f"⏳ 現在、待機中（あと{secs}秒）", style="bold red")
        if live_2a:
            live_2a.update(_make_2a_group())

    client.on_rate_limit = _on_rate_limit_2a

    with Live(_make_2a_group(), console=console, refresh_per_second=4) as live:
        live_2a = live
        for loop_idx, (anime, anilist_id, anime_id) in enumerate(anime_ids):
            title = anime.title_ja or anime.title_en or anime_id
            title_line = Text(f"📋 {title}", style="bold cyan")
            live.update(_make_2a_group())

            credits, person_ids, va_count, had_error = await fetch_staff_ids_for_anime(
                client, anilist_id, anime_id
            )

            all_person_ids_to_fetch.update(person_ids)
            if had_error:
                totals["errors"] += 1

            save_anime_batch_to_database(conn, [anime])
            if studios_pending and anime_id in studios_pending:
                studios, anime_studio_edges = studios_pending.pop(anime_id)
                save_studios_to_database(conn, studios, anime_studio_edges)
            if relations_pending and anime_id in relations_pending:
                save_relations_to_database(conn, relations_pending.pop(anime_id))
            save_credits_batch_to_database(conn, credits)
            conn.commit()

            if anime.cover_large or anime.cover_extra_large or anime.banner:
                download_queue.add_anime(
                    anime.id, anime.cover_large, anime.cover_extra_large, anime.banner
                )
                totals["images"] += 1

            fetched_ids.add(anime_id)
            totals["anime"] += 1
            totals["credits"] += len(credits)
            totals["voice_actors"] += va_count
            bar2a.update(task2a, advance=1)

            if (loop_idx + 1) % checkpoint_interval == 0:
                save_checkpoint(
                    checkpoint_file,
                    create_checkpoint_data(
                        loop_idx + 1, fetched_ids, totals, time_module.time()
                    ),
                )

        save_checkpoint(
            checkpoint_file,
            create_checkpoint_data(
                len(anime_ids), fetched_ids, totals, time_module.time()
            ),
        )

    return all_person_ids_to_fetch


async def _fetch_person_details_phase(
    client,
    conn,
    ids_to_fetch,
    totals,
    console,
    download_queue,
    Progress,
    SpinnerColumn,
    BarColumn,
    MofNCompleteColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
):
    """Phase 2B: Fetch detailed person information for each person ID.

    Iterates over person IDs, fetches full details from the API,
    saves to the database in batches, and queues image downloads.
    """
    if not ids_to_fetch:
        return

    from rich.rule import Rule
    from rich.live import Live
    from rich.console import Group
    from rich.text import Text
    from src.database import mark_person_unfetchable

    console.print()
    console.print(
        Rule(
            "[bold magenta]フェーズ2B: 個人情報詳細取得[/bold magenta]", style="magenta"
        )
    )
    console.print()

    person_batch = []

    bar2b = Progress(
        SpinnerColumn(style="magenta"),
        BarColumn(
            bar_width=50,
            complete_style="bright_magenta",
            finished_style="bold bright_magenta",
        ),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )
    task2b = bar2b.add_task("", total=len(ids_to_fetch))
    status_line = Text(
        f"👤 個人情報取得中 (0/{len(ids_to_fetch):,})", style="bold magenta"
    )
    wait_line_2b = Text("")

    def _make_2b_group():
        parts = [status_line, bar2b]
        rl = _make_rate_limit_text(client)
        if str(rl):
            parts.append(rl)
        if str(wait_line_2b):
            parts.append(wait_line_2b)
        return Group(*parts)

    live_2b = None

    def _on_rate_limit_2b(secs):
        nonlocal wait_line_2b
        if secs is None:
            wait_line_2b = Text("")
        else:
            wait_line_2b = Text(f"⏳ 現在、待機中（あと{secs}秒）", style="bold red")
        if live_2b:
            live_2b.update(_make_2b_group())

    client.on_rate_limit = _on_rate_limit_2b
    skipped_count = 0

    with Live(_make_2b_group(), console=console, refresh_per_second=4) as live:
        live_2b = live
        for idx, person_id in enumerate(ids_to_fetch, 1):
            try:
                resp = await client.get_person_details(person_id)
                if resp is None:
                    # 404等でアクセス不可 → DBに記録してスキップ
                    skipped_count += 1
                    mark_person_unfetchable(conn, person_id, status="not_found")
                    conn.commit()
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
                save_persons_batch_to_database(conn, person_batch)
                conn.commit()
                person_batch.clear()

            bar2b.update(task2b, advance=1)
            skip_info = f" [N/A: {skipped_count}]" if skipped_count else ""
            status_line = Text(
                f"👤 個人情報取得中 ({idx:,}/{len(ids_to_fetch):,}){skip_info}",
                style="bold magenta",
            )
            live.update(_make_2b_group())

        if person_batch:
            save_persons_batch_to_database(conn, person_batch)
            conn.commit()

    if skipped_count:
        console.print(
            f"[yellow]⚠️ {skipped_count}人がAPI上で見つかりませんでした（削除済み等）[/yellow]"
        )


@app.command()
def main(
    count: int = typer.Option(50, "--count", "-n", help="取得するアニメ数"),
    checkpoint_interval: int = typer.Option(
        SCRAPE_CHECKPOINT_INTERVAL, "--checkpoint", help="チェックポイント間隔"
    ),
    force_restart: bool = typer.Option(
        False, "--force-restart", help="チェックポイントを無視して最初から始める"
    ),
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
) -> None:
    """AniList からクレジットデータを収集する (チェックポイント機能付き)."""
    import json
    from pathlib import Path
    from src.database import (
        get_connection,
        init_db,
        update_data_source,
        get_all_person_ids,
    )
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

    # DB に既存のアニメIDを取得（中断後の再開でも重複スキップ）
    fetched_ids = set()
    if not force_restart:
        try:
            from src.database import get_connection as _gc, init_db as _idb

            _conn = _gc()
            _idb(_conn)
            rows = _conn.execute("SELECT id FROM anime").fetchall()
            fetched_ids = {row["id"] for row in rows}
            _conn.close()
            if fetched_ids:
                console.print(
                    f"[dim]💾 DB内の既存アニメ: {len(fetched_ids):,}件（スキップ対象）[/dim]"
                )
        except Exception:
            pass

    # Load checkpoint - auto-resume if exists (unless --force-restart is specified)
    start_index = 0
    checkpoint_exists = checkpoint_file.exists()

    # デフォルト: チェックポイント存在時は自動で続きから始める
    # --force-restart フラグで最初から始められる
    if checkpoint_exists and not force_restart:
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
            start_index = checkpoint.get("last_index", 0)
            fetched_ids.update(checkpoint.get("fetched_ids", []))

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
                f"[dim]{checkpoint.get('timestamp', 'N/A')}[/dim]",
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
    async def fetch_with_checkpoints():
        """Execute scraping with checkpoint-based incremental saving."""
        from rich.progress import (
            Progress,
            SpinnerColumn,
            BarColumn,
            TextColumn,
            TimeRemainingColumn,
            TimeElapsedColumn,
            MofNCompleteColumn,
        )
        from rich.panel import Panel
        from rich.table import Table
        from rich.rule import Rule
        from rich.align import Align
        import time as time_module

        # === Helper Functions (Nested for Clarity) ===

        def load_existing_person_ids_from_database(conn):
            """Load existing person IDs to skip re-fetching."""
            if not skip_existing_persons:
                return set()

            existing_ids = get_all_person_ids(conn)
            if existing_ids:
                console.print()
                console.print(
                    Panel(
                        f"[bold bright_blue]💾 既存データベースから {len(existing_ids):,}件の人物を読み込みました[/bold bright_blue]\n[dim]重複を避けるためスキップします[/dim]",
                        border_style="bright_blue",
                        padding=(1, 2),
                    )
                )
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
            client, count, fetched_ids, use_cache=True, anime_list_cache_file=None
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
            if recent:
                sort_order = ["START_DATE_DESC"]  # New first (for adding recent anime)
            elif reverse:
                sort_order = ["START_DATE_ASC"]  # Old first
            else:
                sort_order = ["POPULARITY_DESC"]  # New/popular first

            from rich.live import Live
            from rich.console import Group
            from rich.text import Text

            bar1 = Progress(
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
            task1 = bar1.add_task("", total=pages_needed)
            status_line_1 = Text("📋 アニメリスト取得中...", style="bold cyan")

            def _make_1_group():
                parts = [status_line_1, bar1]
                rl = _make_rate_limit_text(client)
                if str(rl):
                    parts.append(rl)
                return Group(*parts)

            with Live(_make_1_group(), console=console, refresh_per_second=4) as live1:
                for page in range(1, pages_needed + 1):
                    resp = await client.get_top_anime(
                        page=page, per_page=50, sort=sort_order
                    )
                    page_data = resp.get("Page", {})

                    for raw in page_data.get("media", []):
                        if len(all_anime_for_cache) >= count:
                            break
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

                    bar1.update(task1, advance=1)
                    status_line_1 = Text(
                        f"📋 アニメリスト取得中 ({page}/{pages_needed})",
                        style="bold cyan",
                    )
                    live1.update(_make_1_group())

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

            Returns: (credits, person_ids, va_count, had_error)
            """
            credits = []
            person_ids = set()
            va_count = 0

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
                        characters = media.get("characters", {})
                        for char_edge in characters.get("edges", []):
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
                                            raw_role="Voice Actor",  # 元のロール文字列
                                            source="anilist",
                                        )
                                    )
                        has_more_chars = characters.get("pageInfo", {}).get(
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
                return credits, person_ids, va_count, True

            return credits, person_ids, va_count, False

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
            "errors": 0,
            "skipped": 0,
        }
        start_time = time_module.time()

        try:
            # ===== PHASE 1: Fetch Anime List =====
            anime_ids = await _load_anime_ids(
                client,
                count,
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
            conn = get_connection()
            init_db(conn)
            existing_person_ids = load_existing_person_ids_from_database(conn)

            all_person_ids_to_fetch = await _fetch_staff_phase(
                client,
                conn,
                anime_ids,
                totals,
                fetched_ids,
                console,
                download_queue,
                checkpoint_interval,
                checkpoint_file,
                fetch_staff_ids_for_anime,
                create_checkpoint_data,
                save_checkpoint,
                Progress,
                SpinnerColumn,
                BarColumn,
                MofNCompleteColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
                time_module,
                studios_pending=studios_pending,
                relations_pending=relations_pending,
            )

            # Phase 2A サマリ
            # DB にある person と取得不可IDを除外して、本当に取得が必要な ID だけにする
            from src.database import get_unfetchable_person_ids

            unfetchable_ids = get_unfetchable_person_ids(conn)
            ids_to_fetch = sorted(
                all_person_ids_to_fetch
                - {
                    int(pid.removeprefix("anilist:p"))
                    for pid in existing_person_ids
                    if pid.startswith("anilist:p")
                }
                - unfetchable_ids
            )
            total_unique = len(all_person_ids_to_fetch)
            skipped_existing = (
                total_unique
                - len(ids_to_fetch)
                - len(unfetchable_ids & all_person_ids_to_fetch)
            )
            skipped_unfetchable = len(unfetchable_ids & all_person_ids_to_fetch)

            console.print()
            skip_parts = []
            if skipped_existing:
                skip_parts.append(f"{skipped_existing:,}人は取得済み")
            if skipped_unfetchable:
                skip_parts.append(f"{skipped_unfetchable:,}人はN/A")
            skip_text = ", ".join(skip_parts)
            console.print(
                f"[cyan]✅ フェーズ2A完了: {totals['anime']}件のアニメ → "
                f"{totals['credits']:,}クレジット, "
                f"{total_unique:,}人 (うち{skip_text} → "
                f"[bold]{len(ids_to_fetch):,}人を取得[/bold])[/cyan]"
            )

            # ===== PHASE 2B: 個人情報詳細取得 =====
            await _fetch_person_details_phase(
                client,
                conn,
                ids_to_fetch,
                totals,
                console,
                download_queue,
                Progress,
                SpinnerColumn,
                BarColumn,
                MofNCompleteColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )

            totals["skipped"] = skipped_existing

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

    asyncio.run(fetch_with_checkpoints())


@app.command("fetch-persons")
def fetch_persons(
    log_level: str = typer.Option(
        "error", "--log-level", help="ログレベル (debug/info/warning/error)"
    ),
) -> None:
    """DBのクレジットに含まれる未取得の個人情報を取得する."""
    import asyncio
    import time as time_module
    from src.database import get_connection, init_db, get_all_person_ids
    from src.log import setup_logging

    setup_logging(log_level)

    async def _run():
        from rich.console import Console
        from rich.rule import Rule
        from rich.live import Live
        from rich.console import Group
        from rich.text import Text
        from rich.progress import (
            Progress,
            SpinnerColumn,
            BarColumn,
            MofNCompleteColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        console = Console()
        conn = get_connection()
        init_db(conn)

        # credits テーブルから全 person_id を取得
        rows = conn.execute("SELECT DISTINCT person_id FROM credits").fetchall()
        all_credit_person_ids = {row["person_id"] for row in rows}

        # 既に persons テーブルに存在する ID
        existing_ids = get_all_person_ids(conn)

        # 取得不可と記録済みの ID
        from src.database import get_unfetchable_person_ids

        unfetchable_ids = get_unfetchable_person_ids(conn)

        # 未取得の AniList person ID を抽出（取得不可を除外）
        missing_ids = set()
        for pid in all_credit_person_ids:
            if pid not in existing_ids and pid.startswith("anilist:p"):
                try:
                    anilist_id = int(pid.removeprefix("anilist:p"))
                    if anilist_id not in unfetchable_ids:
                        missing_ids.add(anilist_id)
                except ValueError:
                    pass

        ids_to_fetch = sorted(missing_ids)

        console.print()
        console.print(
            Rule(
                "[bold magenta]個人情報取得 (DBのクレジットから)[/bold magenta]",
                style="magenta",
            )
        )
        console.print(
            f"  クレジット内の人物: [bold]{len(all_credit_person_ids):,}[/bold]人"
        )
        console.print(f"  取得済み:           [dim]{len(existing_ids):,}[/dim]人")
        if unfetchable_ids:
            console.print(
                f"  N/A (取得不可):     [dim]{len(unfetchable_ids):,}[/dim]人"
            )
        console.print(
            f"  未取得:             [bold yellow]{len(ids_to_fetch):,}[/bold yellow]人"
        )
        console.print()

        if not ids_to_fetch:
            console.print("[green]全員取得済みです。[/green]")
            conn.close()
            return

        console.print(
            f"[dim]🔑 トークン: {'あり' if _env.get('ANILIST_ACCESS_TOKEN') else 'なし'}[/dim]"
        )

        from src.utils.download_queue import DownloadQueue

        client = AniListClient()
        await client.verify_auth()
        download_queue = DownloadQueue()
        person_batch = []
        fetched = 0
        errors = 0
        start_time = time_module.time()

        try:
            bar = Progress(
                SpinnerColumn(style="magenta"),
                BarColumn(
                    bar_width=50,
                    complete_style="bright_magenta",
                    finished_style="bold bright_magenta",
                ),
                MofNCompleteColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            )
            task = bar.add_task("", total=len(ids_to_fetch))
            status_line = Text(
                f"👤 個人情報取得中 (0/{len(ids_to_fetch):,})", style="bold magenta"
            )
            wait_line = Text("")

            skipped_count = 0

            def _make_group():
                parts = [status_line, bar]
                rl = _make_rate_limit_text(client)
                if str(rl):
                    parts.append(rl)
                if str(wait_line):
                    parts.append(wait_line)
                return Group(*parts)

            live_ref = None

            def _on_rate_limit(secs):
                nonlocal wait_line
                if secs is None:
                    wait_line = Text("")
                else:
                    wait_line = Text(
                        f"⏳ 現在、待機中（あと{secs}秒）", style="bold red"
                    )
                if live_ref:
                    live_ref.update(_make_group())

            client.on_rate_limit = _on_rate_limit

            with Live(_make_group(), console=console, refresh_per_second=4) as live:
                live_ref = live
                for idx, person_id in enumerate(ids_to_fetch, 1):
                    try:
                        resp = await client.get_person_details(person_id)
                        if resp is None:
                            skipped_count += 1
                            from src.database import mark_person_unfetchable

                            mark_person_unfetchable(conn, person_id, status="not_found")
                            conn.commit()
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
                        save_persons_batch_to_database(conn, person_batch)
                        conn.commit()
                        person_batch.clear()

                    bar.update(task, advance=1)
                    skip_info = f" [N/A: {skipped_count}]" if skipped_count else ""
                    status_line = Text(
                        f"👤 個人情報取得中 ({idx:,}/{len(ids_to_fetch):,}){skip_info}",
                        style="bold magenta",
                    )
                    live.update(_make_group())

                if person_batch:
                    save_persons_batch_to_database(conn, person_batch)
                    conn.commit()

            if skipped_count:
                console.print(
                    f"[yellow]⚠️ {skipped_count}人がAPI上で見つかりませんでした（削除済み等）[/yellow]"
                )

        finally:
            await client.close()
            conn.close()

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
    from src.log import setup_logging
    from src.utils.download_queue import DownloadQueue
    from src.database import get_connection
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
    conn.close()


if __name__ == "__main__":
    # When called directly without a command, default to 'main'
    import sys

    if len(sys.argv) == 1 or (len(sys.argv) > 1 and sys.argv[1].startswith("--")):
        # No command specified, or starts with --option, so prepend 'main'
        sys.argv.insert(1, "main")
    app()
