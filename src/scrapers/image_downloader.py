"""画像ダウンローダー - 人物・作品の画像を非同期でダウンロード."""

import asyncio
import hashlib
from pathlib import Path
from typing import Optional

import httpx
import structlog
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn

log = structlog.get_logger()

# 画像保存先ディレクトリ
IMAGES_DIR = Path(__file__).parent.parent.parent / "data" / "images"
PERSON_IMAGES_DIR = IMAGES_DIR / "persons"
ANIME_IMAGES_DIR = IMAGES_DIR / "anime"

# レート制限（画像ダウンロードは緩め）
DOWNLOAD_DELAY = 0.1  # 秒


MAX_RETRIES = 3
MIN_IMAGE_SIZE = 1024  # 1KB — smaller is likely corrupt/placeholder


async def download_image(
    client: httpx.AsyncClient,
    url: str,
    save_dir: Path,
    filename: Optional[str] = None,
) -> Optional[str]:
    """画像をダウンロードして保存 (リトライ + コンテンツ検証付き).

    Args:
        client: httpx AsyncClient
        url: 画像URL
        save_dir: 保存先ディレクトリ
        filename: ファイル名（Noneの場合はURLからハッシュ生成）

    Returns:
        保存したファイルパス（相対パス）、失敗時はNone
    """
    if not url:
        return None

    try:
        # URLからファイル名を生成（衝突回避のためハッシュ使用）
        if filename is None:
            url_hash = hashlib.md5(url.encode()).hexdigest()
            ext = Path(url).suffix or ".jpg"
            filename = f"{url_hash}{ext}"

        save_path = save_dir / filename
        save_dir.mkdir(parents=True, exist_ok=True)

        # 既にダウンロード済みならスキップ
        if save_path.exists():
            return str(save_path.relative_to(IMAGES_DIR.parent))

        # ダウンロード (リトライ付き)
        for attempt in range(1, MAX_RETRIES + 1):
            await asyncio.sleep(DOWNLOAD_DELAY)
            try:
                resp = await client.get(url, timeout=30.0, follow_redirects=True)
            except httpx.HTTPError as e:
                if attempt < MAX_RETRIES:
                    wait = DOWNLOAD_DELAY * (2 ** attempt)
                    log.warning("image_download_retry", url=url, attempt=attempt, wait=wait, error=str(e))
                    await asyncio.sleep(wait)
                    continue
                log.warning("image_download_error", url=url, error=str(e))
                return None

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 5))
                log.warning("image_rate_limited", url=url, retry_after=retry_after)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(retry_after)
                    continue
                return None

            if resp.status_code != 200:
                if attempt < MAX_RETRIES:
                    wait = DOWNLOAD_DELAY * (2 ** attempt)
                    log.warning("image_download_retry", url=url, attempt=attempt, status=resp.status_code)
                    await asyncio.sleep(wait)
                    continue
                log.warning("image_download_failed", url=url, status=resp.status_code)
                return None

            # Content validation
            content_type = resp.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                log.warning("image_invalid_content_type", url=url, content_type=content_type)
                return None

            if len(resp.content) < MIN_IMAGE_SIZE:
                log.warning("image_too_small", url=url, size=len(resp.content), min_size=MIN_IMAGE_SIZE)
                return None

            save_path.write_bytes(resp.content)
            return str(save_path.relative_to(IMAGES_DIR.parent))

        return None  # Should not reach here, but safety net

    except Exception as e:
        log.warning("image_download_error", url=url, error=str(e))
        return None


async def download_person_images(
    persons: list[tuple[str, str, str]],  # [(person_id, image_large, image_medium)]
    show_progress: bool = True,
) -> dict[str, dict[str, Optional[str]]]:
    """人物の画像を一括ダウンロード.

    Args:
        persons: [(person_id, image_large_url, image_medium_url)] のリスト
        show_progress: プログレスバー表示

    Returns:
        {person_id: {"large": path, "medium": path}} の辞書
    """
    results = {}

    async with httpx.AsyncClient(timeout=60.0) as client:
        if show_progress:
            from rich.console import Console
            console = Console()

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                console=console
            ) as progress:
                task = progress.add_task(
                    "[cyan]人物画像ダウンロード中...",
                    total=len(persons)
                )

                for person_id, img_large, img_medium in persons:
                    person_dir = PERSON_IMAGES_DIR / person_id.replace(":", "_")

                    large_path = None
                    medium_path = None

                    if img_large:
                        large_path = await download_image(
                            client, img_large, person_dir, "large.png"
                        )

                    if img_medium:
                        medium_path = await download_image(
                            client, img_medium, person_dir, "medium.png"
                        )

                    results[person_id] = {
                        "large": large_path,
                        "medium": medium_path,
                    }

                    progress.update(task, advance=1)
        else:
            # No progress bar
            for person_id, img_large, img_medium in persons:
                person_dir = PERSON_IMAGES_DIR / person_id.replace(":", "_")

                large_path = None
                medium_path = None

                if img_large:
                    large_path = await download_image(
                        client, img_large, person_dir, "large.png"
                    )

                if img_medium:
                    medium_path = await download_image(
                        client, img_medium, person_dir, "medium.png"
                    )

                results[person_id] = {
                    "large": large_path,
                    "medium": medium_path,
                }

    log.info("person_images_downloaded", count=len(results))
    return results


async def download_anime_images(
    anime_list: list[tuple[str, str, str, str]],  # [(anime_id, cover_large, cover_extra_large, banner)]
    show_progress: bool = True,
) -> dict[str, dict[str, Optional[str]]]:
    """アニメ作品の画像を一括ダウンロード.

    Args:
        anime_list: [(anime_id, cover_large, cover_extra_large, banner)] のリスト
        show_progress: プログレスバー表示

    Returns:
        {anime_id: {"cover_large": path, "banner": path}} の辞書
    """
    results = {}

    async with httpx.AsyncClient(timeout=60.0) as client:
        if show_progress:
            from rich.console import Console
            console = Console()

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                console=console
            ) as progress:
                task = progress.add_task(
                    "[green]作品画像ダウンロード中...",
                    total=len(anime_list)
                )

                for anime_id, cover_large, cover_xl, banner in anime_list:
                    anime_dir = ANIME_IMAGES_DIR / anime_id.replace(":", "_")

                    cover_path = None
                    banner_path = None

                    # 最高画質を優先
                    best_cover = cover_xl or cover_large
                    if best_cover:
                        cover_path = await download_image(
                            client, best_cover, anime_dir, "cover.jpg"
                        )

                    if banner:
                        banner_path = await download_image(
                            client, banner, anime_dir, "banner.jpg"
                        )

                    results[anime_id] = {
                        "cover_large": cover_path,
                        "banner": banner_path,
                    }

                    progress.update(task, advance=1)
        else:
            # No progress bar
            for anime_id, cover_large, cover_xl, banner in anime_list:
                anime_dir = ANIME_IMAGES_DIR / anime_id.replace(":", "_")

                cover_path = None
                banner_path = None

                best_cover = cover_xl or cover_large
                if best_cover:
                    cover_path = await download_image(
                        client, best_cover, anime_dir, "cover.jpg"
                    )

                if banner:
                    banner_path = await download_image(
                        client, banner, anime_dir, "banner.jpg"
                    )

                results[anime_id] = {
                    "cover_large": cover_path,
                    "banner": banner_path,
                }

    log.info("anime_images_downloaded", count=len(results))
    return results
