#!/usr/bin/env python3
"""失敗したアニメの再スクレイピング.

ログファイルから失敗したアニメIDを抽出し、再処理する。

Usage:
    # ログファイルから自動抽出して再処理
    PYTHONPATH=. pixi run python scripts/retry_failed_anime.py

    # 特定のアニメIDを指定
    PYTHONPATH=. pixi run python scripts/retry_failed_anime.py --anime-ids 179970 186737 192378

    # ログファイルを指定
    PYTHONPATH=. pixi run python scripts/retry_failed_anime.py --log-file path/to/scraper.log
"""

import asyncio
import re
import sys
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.update_anime_credits import fetch_and_update_anime
from src.database import get_connection, init_db
from src.scrapers.anilist_scraper import AniListClient

logger = structlog.get_logger()
console = Console()


def extract_failed_anime_ids_from_log(log_file: Path | None = None) -> list[str]:
    """ログファイルから失敗したアニメIDを抽出する.

    Args:
        log_file: ログファイルパス（Noneの場合は複数候補を自動検索）

    Returns:
        失敗したアニメIDのリスト（重複なし、ソート済み）
    """
    failed_ids = set()

    # ログファイル候補
    if log_file:
        candidates = [log_file]
    else:
        candidates = [
            Path("scraper.log"),
            Path("logs/scraper.log"),
            Path("result/scraper.log"),
            Path("data/scraper.log"),
        ]

    # エラーパターン: staff_list_fetch_failed で anime_id を抽出
    pattern = re.compile(r"staff_list_fetch_failed.*anime_id=(anilist:\d+)")

    found_any = False
    for candidate in candidates:
        if not candidate.exists():
            continue

        found_any = True
        console.print(f"[dim]📄 Scanning log: {candidate}[/dim]")

        with open(candidate) as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    anime_id = match.group(1)
                    failed_ids.add(anime_id)

    if not found_any:
        console.print("[yellow]⚠️  No log files found in standard locations[/yellow]")
        console.print("[dim]Searched: scraper.log, logs/scraper.log, result/scraper.log, data/scraper.log[/dim]")

    return sorted(failed_ids)


def extract_anilist_id(anime_id: str) -> int:
    """anime_id から AniList ID を抽出.

    Args:
        anime_id: "anilist:179970" 形式

    Returns:
        AniList ID（整数）
    """
    if anime_id.startswith("anilist:"):
        return int(anime_id.split(":")[1])
    return int(anime_id)


async def retry_failed_anime(
    anime_ids: list[str],
    *,
    verbose: bool = True,
) -> dict:
    """失敗したアニメを再スクレイピング.

    Args:
        anime_ids: 再処理するアニメIDのリスト
        verbose: 詳細なエラーログを表示

    Returns:
        処理結果の統計
    """
    if not anime_ids:
        console.print("[yellow]⚠️  No anime IDs to retry[/yellow]")
        return {"total": 0, "success": 0, "failed": 0, "failed_ids": [], "errors": []}

    console.print()
    console.print(Panel.fit(
        f"[bold cyan]🔄 Retrying {len(anime_ids)} Failed Anime[/bold cyan]",
        border_style="cyan"
    ))
    console.print()

    # AniListクライアント
    client = AniListClient()

    # 統計
    stats = {
        "total": len(anime_ids),
        "success": 0,
        "failed": 0,
        "failed_ids": [],
        "errors": [],  # エラー詳細を保存
        "credits_added": 0,
        "persons_added": 0,
        "characters_added": 0,
        "studios_added": 0,
        "characters_added": 0,
    }

    # 進捗表示
    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console
    ) as progress:
        retry_task = progress.add_task("🔄 Retrying failed anime...", total=len(anime_ids))

        for i, anime_id in enumerate(anime_ids, 1):
            anilist_id = extract_anilist_id(anime_id)

            try:
                # 再スクレイピング（update_anime_credits.py の関数シグネチャに合わせる）
                result = await fetch_and_update_anime(
                    client=client,
                    anime_id=anilist_id,  # 整数で渡す
                )

                # 結果判定
                success = result.get("success", False)
                if success:
                    stats["success"] += 1
                    stats["credits_added"] += result.get("credits", 0)
                    stats["persons_added"] += result.get("persons", 0)
                    stats["characters_added"] += result.get("characters", 0)
                    stats["studios_added"] += result.get("studios", 0)
                else:
                    stats["failed"] += 1
                    stats["failed_ids"].append(anime_id)
                    error_msg = result.get("error", "Unknown error")
                    stats["errors"].append({
                        "anime_id": anime_id,
                        "error": error_msg
                    })
                    if verbose:
                        console.print(f"[red]❌ {anime_id}: {error_msg}[/red]")

                # 進捗更新
                rate_info = ""
                if client.requests_remaining is not None:
                    rate_info = f" [dim]| API: {client.requests_remaining} remaining[/dim]"

                progress.update(
                    retry_task,
                    description=f"🔄 Retrying ({i}/{len(anime_ids)}){rate_info}",
                    advance=1
                )

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(
                    "retry_failed",
                    anime_id=anime_id,
                    error_type=type(e).__name__,
                    error_message=str(e)
                )
                stats["failed"] += 1
                stats["failed_ids"].append(anime_id)
                stats["errors"].append({
                    "anime_id": anime_id,
                    "error": error_msg
                })
                if verbose:
                    console.print(f"[red]❌ {anime_id}: {error_msg}[/red]")
                progress.advance(retry_task)

    return stats


def display_results(stats: dict) -> None:
    """処理結果を表示."""
    console.print()
    console.print("─" * 80)
    console.print()

    # サマリーテーブル
    summary_table = Table(show_header=False, box=None, padding=(0, 2))
    summary_table.add_row(
        "[bold cyan]Total Retried[/bold cyan]",
        f"[bold]{stats['total']}[/bold]"
    )
    summary_table.add_row(
        "[green]✅ Success[/green]",
        f"[bold green]{stats['success']}[/bold green]"
    )
    if stats['failed'] > 0:
        summary_table.add_row(
            "[red]❌ Failed[/red]",
            f"[bold red]{stats['failed']}[/bold red]"
        )

    console.print(Panel(
        summary_table,
        title="[bold]Retry Results[/bold]",
        border_style="cyan"
    ))

    # 追加データ統計
    if stats['success'] > 0:
        console.print()
        data_table = Table(show_header=False, box=None, padding=(0, 2))
        data_table.add_row("[cyan]Credits Added[/cyan]", f"[dim]{stats['credits_added']:,}[/dim]")
        data_table.add_row("[cyan]Persons Added[/cyan]", f"[dim]{stats['persons_added']:,}[/dim]")
        data_table.add_row("[cyan]Characters Added[/cyan]", f"[dim]{stats['characters_added']:,}[/dim]")
        data_table.add_row("[cyan]Studios Added[/cyan]", f"[dim]{stats['studios_added']:,}[/dim]")
        console.print(Panel(data_table, border_style="cyan"))

    # エラー詳細（失敗した場合）
    if stats.get('errors'):
        console.print()
        console.print("[bold red]Error Details:[/bold red]")
        console.print()

        # エラーテーブル
        error_table = Table(show_header=True, box=None, padding=(0, 1))
        error_table.add_column("Anime ID", style="cyan")
        error_table.add_column("Error", style="red")

        for error_info in stats['errors']:
            anime_id = error_info['anime_id']
            error_msg = error_info['error']
            # エラーメッセージを70文字で切る
            if len(error_msg) > 70:
                error_msg = error_msg[:67] + "..."
            error_table.add_row(anime_id, error_msg)

        console.print(error_table)

    # 失敗リスト（簡易版）
    if stats['failed_ids'] and not stats.get('errors'):
        console.print()
        console.print("[bold red]Still Failed:[/bold red]")
        for anime_id in stats['failed_ids']:
            console.print(f"  [dim]• {anime_id}[/dim]")

    console.print()


def main(
    anime_ids: list[str] = typer.Argument(None, help="Anime IDs to retry (e.g., 179970 186737)"),
    log_file: Path | None = typer.Option(None, "--log-file", "-l", help="Log file to extract failed IDs from"),
    verbose: bool = typer.Option(True, "--verbose/--quiet", help="Show detailed error messages"),
) -> None:
    """失敗したアニメを再スクレイピング.

    Examples:
        # ログから自動抽出して再処理
        PYTHONPATH=. pixi run python scripts/retry_failed_anime.py

        # 特定のアニメIDを指定
        PYTHONPATH=. pixi run python scripts/retry_failed_anime.py 179970 186737 192378

        # ログファイルを指定
        PYTHONPATH=. pixi run python scripts/retry_failed_anime.py --log-file scraper.log
    """
    # アニメID取得
    if anime_ids:
        # コマンドライン引数から
        target_ids = [f"anilist:{aid}" if not aid.startswith("anilist:") else aid for aid in anime_ids]
        console.print(f"[cyan]📋 Target: {len(target_ids)} anime IDs from arguments[/cyan]")
    else:
        # ログファイルから抽出
        target_ids = extract_failed_anime_ids_from_log(log_file)
        if not target_ids:
            console.print("[yellow]⚠️  No failed anime IDs found in logs[/yellow]")
            console.print("[dim]Hint: Specify anime IDs manually or check log file location[/dim]")
            raise typer.Exit(1)

        console.print(f"[cyan]📋 Found {len(target_ids)} failed anime IDs in logs[/cyan]")

    # IDリスト表示
    console.print()
    console.print("[bold]Anime IDs to retry:[/bold]")
    for anime_id in target_ids:
        console.print(f"  [dim]• {anime_id}[/dim]")

    # 再処理実行
    stats = asyncio.run(retry_failed_anime(
        target_ids,
        verbose=verbose,
    ))

    # 結果表示
    display_results(stats)

    # 終了コード
    if stats["failed"] > 0:
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(main)
