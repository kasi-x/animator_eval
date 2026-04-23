#!/usr/bin/env python3
"""スクレイピング失敗の修復 — 実装モジュール.

fix_scraping_failures.sh から呼ばれる。直接実行も可。

Usage:
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py status
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py retry-no-credits
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py backfill
    PYTHONPATH=. pixi run python scripts/fix_scraping_failures_impl.py detect-deleted
"""

import asyncio
import sys
import time
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.update_anime_credits import fetch_and_update_anime
from src.db import (
    get_connection,
    init_db,
    insert_anime_relation,
    insert_anime_studio,
    insert_character_voice_actor,
    upsert_anime,
    upsert_character,
    upsert_studio,
)
from src.scrapers.anilist_scraper import (
    AniListClient,
    parse_anilist_anime,
    parse_anilist_characters,
    parse_anilist_relations,
    parse_anilist_studios,
)

logger = structlog.get_logger()
console = Console()
app = typer.Typer()

# チェックポイントファイル
CHECKPOINT_DIR = Path("data")
RETRY_CHECKPOINT = CHECKPOINT_DIR / "fix_retry_checkpoint.json"
BACKFILL_CHECKPOINT = CHECKPOINT_DIR / "fix_backfill_checkpoint.json"


def _load_checkpoint(path: Path) -> set[int]:
    """チェックポイントから処理済みIDを読む."""
    import json

    if path.exists():
        with open(path) as f:
            data = json.load(f)
        return set(data.get("completed_ids", []))
    return set()


def _save_checkpoint(path: Path, completed_ids: set[int]) -> None:
    """チェックポイントを保存する."""
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"completed_ids": sorted(completed_ids)}, f)


# ──────────────────────────────────────
# status: DB状態表示
# ──────────────────────────────────────


@app.command()
def status():
    """DB状態を表示."""
    conn = get_connection()
    init_db(conn)

    counts = {}
    for table in ["anime", "persons", "credits", "studios", "anime_studios",
                   "characters", "character_voice_actors", "anime_relations"]:
        counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608

    # クレジットなしアニメ — AniListにスタッフデータが未登録の作品
    no_credits_total = conn.execute("""
        SELECT COUNT(*) FROM anime a LEFT JOIN credits c ON a.id = c.anime_id
        WHERE c.id IS NULL
    """).fetchone()[0]

    # メタデータ欠損
    null_country = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE country_of_origin IS NULL"
    ).fetchone()[0]
    null_site = conn.execute(
        "SELECT COUNT(*) FROM anime WHERE site_url IS NULL"
    ).fetchone()[0]

    conn.close()

    # テーブル表示
    tbl = Table(show_header=True, header_style="bold cyan", padding=(0, 2))
    tbl.add_column("Item", style="cyan")
    tbl.add_column("Count", justify="right")
    tbl.add_column("Status", style="dim")

    tbl.add_row("Anime", f"{counts['anime']:,}", "")
    tbl.add_row("Persons", f"{counts['persons']:,}", "")
    tbl.add_row("Credits", f"{counts['credits']:,}", "")
    tbl.add_row("─" * 30, "─" * 10, "─" * 15)

    # 問題箇所
    status_ok = "[green]OK[/green]"
    status_bad = "[red]NEEDS FIX[/red]"

    tbl.add_row(
        "Credits missing (no staff on AniList)",
        f"{no_credits_total:,}",
        "[dim]N/A[/dim]",
    )
    tbl.add_row("Studios (normalized)", f"{counts['studios']:,}",
                status_ok if counts['studios'] > 0 else status_bad)
    tbl.add_row("Anime-Studios links", f"{counts['anime_studios']:,}",
                status_ok if counts['anime_studios'] > 0 else status_bad)
    tbl.add_row("Characters", f"{counts['characters']:,}",
                status_ok if counts['characters'] > 0 else status_bad)
    tbl.add_row("Voice Actor links", f"{counts['character_voice_actors']:,}",
                status_ok if counts['character_voice_actors'] > 0 else status_bad)
    tbl.add_row("Anime Relations", f"{counts['anime_relations']:,}",
                status_ok if counts['anime_relations'] > 0 else status_bad)
    tbl.add_row("─" * 30, "─" * 10, "─" * 15)
    tbl.add_row("Metadata: country_of_origin NULL", f"{null_country:,}",
                status_ok if null_country < 100 else status_bad)
    tbl.add_row("Metadata: site_url NULL", f"{null_site:,}",
                status_ok if null_site < 100 else status_bad)

    console.print(Panel(tbl, title="[bold]DB Health Check[/bold]", border_style="cyan"))


# ──────────────────────────────────────
# retry-no-credits: クレジット取得失敗の再スクレイプ
# ──────────────────────────────────────


@app.command("retry-no-credits")
def retry_no_credits():
    """クレジットが0件のアニメについて確認.

    AniListにスタッフデータが未登録の作品がほとんどのため、
    APIリトライは行わず状況を表示するのみ。
    別データソース（MAL、MediaArts等）での補完を推奨。
    """
    conn = get_connection()
    init_db(conn)

    no_credits = conn.execute("""
        SELECT COUNT(*) FROM anime a LEFT JOIN credits c ON a.id = c.anime_id
        WHERE c.id IS NULL
    """).fetchone()[0]

    # format別の内訳
    rows = conn.execute("""
        SELECT a.format, COUNT(*) as cnt
        FROM anime a LEFT JOIN credits c ON a.id = c.anime_id
        WHERE c.id IS NULL
        GROUP BY a.format ORDER BY cnt DESC
    """).fetchall()
    conn.close()

    if no_credits == 0:
        console.print("[green]✅ クレジット欠損なし[/green]")
        return

    tbl = Table(show_header=True, header_style="bold cyan", padding=(0, 2))
    tbl.add_column("Format")
    tbl.add_column("Count", justify="right")
    for row in rows:
        tbl.add_row(row["format"] or "NULL", f"{row['cnt']:,}")
    tbl.add_row("─" * 15, "─" * 8)
    tbl.add_row("[bold]Total[/bold]", f"[bold]{no_credits:,}[/bold]")

    console.print(Panel(
        tbl,
        title="[bold]Credits Missing (No Staff on AniList)[/bold]",
        border_style="yellow",
    ))
    console.print()
    console.print("[yellow]これらはAniListにスタッフデータが未登録の作品です。[/yellow]")
    console.print("[yellow]MAL / MediaArts DB 等の別データソースでの補完を推奨します。[/yellow]")


# ──────────────────────────────────────
# backfill: 正規化テーブル + メタデータ補完
# ──────────────────────────────────────


@app.command()
def backfill():
    """全アニメの正規化テーブル（studios/characters/relations）とメタデータを補完.

    軽量モード: アニメ1件につき1 APIリクエストのみ。
    メタデータ + studios + relations + characters(1ページ目)を取得。
    既存クレジットには触らない。
    """
    conn = get_connection()
    init_db(conn)

    rows = conn.execute("""
        SELECT a.anilist_id FROM anime a
        WHERE a.anilist_id IS NOT NULL
          AND (
            a.country_of_origin IS NULL
            OR a.site_url IS NULL
            OR NOT EXISTS (
                SELECT 1 FROM anime_studios ast WHERE ast.anime_id = a.id
            )
          )
        ORDER BY a.anilist_id
    """).fetchall()
    conn.close()

    target_ids = [row["anilist_id"] for row in rows]
    if not target_ids:
        console.print("[green]✅ メタデータ/正規化テーブル補完不要 — スキップ[/green]")
        return

    # チェックポイントから再開
    completed = _load_checkpoint(BACKFILL_CHECKPOINT)
    remaining = [aid for aid in target_ids if aid not in completed]

    if not remaining:
        console.print(f"[green]✅ 全 {len(target_ids)} 件が処理済み[/green]")
        BACKFILL_CHECKPOINT.unlink(missing_ok=True)
        return

    console.print(f"[cyan]対象: {len(remaining)} 件[/cyan]"
                  + (f" [dim](前回の続き: {len(completed)} 件完了済み)[/dim]" if completed else ""))
    console.print(
        "[dim]※ 軽量モード: 1件1リクエストでメタデータ + studios + characters + relations を補完[/dim]"
    )
    console.print(
        "[dim]  （既存クレジットには触りません）[/dim]"
    )

    stats = asyncio.run(_batch_backfill_light(remaining, completed, BACKFILL_CHECKPOINT))
    _display_batch_results(stats, "Backfill Results")


# ──────────────────────────────────────
# detect-deleted: 404アニメの検出
# ──────────────────────────────────────


@app.command("detect-deleted")
def detect_deleted():
    """AniList APIで404を返すアニメ（クレジットなし + year=NULL）を検出."""
    conn = get_connection()
    init_db(conn)

    # year=NULL かつクレジットなし — AniListにスタッフデータがない
    # この中から実際に404(削除済み)のものを見つける
    # 1705件全部をAPIで叩くのは重いので、サンプリングチェック
    rows = conn.execute("""
        SELECT a.anilist_id, a.title_ja, a.title_en, a.format
        FROM anime a LEFT JOIN credits c ON a.id = c.anime_id
        WHERE c.id IS NULL AND a.year IS NULL AND a.anilist_id IS NOT NULL
        ORDER BY a.anilist_id
    """).fetchall()
    conn.close()

    target_ids = [(row["anilist_id"], row["title_ja"] or row["title_en"] or "?", row["format"]) for row in rows]
    if not target_ids:
        console.print("[green]✅ 対象なし[/green]")
        return

    console.print(f"[cyan]チェック対象: {len(target_ids)} 件 (year=NULL, no credits)[/cyan]")
    console.print("[dim]AniList APIで存在確認します（404 = 削除済み、empty = スタッフ未登録）[/dim]")
    console.print()

    stats = asyncio.run(_detect_deleted_anime(target_ids))

    # 結果表示
    console.print()
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_row("[cyan]Checked[/cyan]", f"{stats['checked']:,}")
    tbl.add_row("[red]Deleted (404)[/red]", f"{stats['deleted']:,}")
    tbl.add_row("[yellow]No staff data[/yellow]", f"{stats['no_staff']:,}")
    tbl.add_row("[green]Has staff (recovered)[/green]", f"{stats['recovered']:,}")
    tbl.add_row("[red]Errors[/red]", f"{stats['errors']:,}")
    console.print(Panel(tbl, title="[bold]Detection Results[/bold]", border_style="cyan"))

    if stats["deleted_ids"]:
        console.print()
        console.print(f"[bold red]Deleted anime IDs ({len(stats['deleted_ids'])}):[/bold red]")
        for aid, title in stats["deleted_ids"][:20]:
            console.print(f"  [dim]• anilist:{aid} — {title}[/dim]")
        if len(stats["deleted_ids"]) > 20:
            console.print(f"  [dim]  ... and {len(stats['deleted_ids']) - 20} more[/dim]")


# ──────────────────────────────────────
# 共通処理
# ──────────────────────────────────────


# メタデータ + studios + characters(1ページ目) + relations を1リクエストで取得
# staff のページネーションは不要（クレジットは触らない）
BACKFILL_QUERY = """
query ($id: Int) {
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
    characters(page: 1, perPage: 25) {
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

# キャラクター追加ページ取得用の軽量クエリ
BACKFILL_CHARS_QUERY = """
query ($id: Int, $page: Int) {
  Media(id: $id, type: ANIME) {
    characters(page: $page, perPage: 25) {
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


async def _batch_backfill_light(
    anime_ids: list[int],
    completed: set[int],
    checkpoint_path: Path,
    checkpoint_interval: int = 50,
) -> dict:
    """軽量backfill: 1アニメ = 1リクエスト（メタデータ+studios+chars+relations）.

    既存クレジットには触らない。
    """
    client = AniListClient()
    conn = get_connection()
    init_db(conn)

    stats = {
        "total": len(anime_ids),
        "success": 0,
        "empty": 0,
        "error": 0,
        "error_details": [],
    }
    start_time = time.time()

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing...", total=len(anime_ids))

        for i, anilist_id in enumerate(anime_ids):
            anime_id_str = f"anilist:{anilist_id}"
            try:
                resp = await client.query(BACKFILL_QUERY, {"id": anilist_id})
                media = resp.get("Media")

                if not media:
                    stats["empty"] += 1
                else:
                    # 1. アニメメタデータ更新
                    anime_obj = parse_anilist_anime(media)
                    upsert_anime(conn, anime_obj)

                    # 2. Studios
                    studios, anime_studio_edges = parse_anilist_studios(media, anime_id_str)
                    for studio in studios:
                        upsert_studio(conn, studio)
                    for edge in anime_studio_edges:
                        insert_anime_studio(conn, edge)

                    # 3. Relations
                    relations = parse_anilist_relations(media, anime_id_str)
                    for rel in relations:
                        insert_anime_relation(conn, rel)

                    # 4. Characters — 全ページ取得
                    seen_chars: set[str] = set()
                    seen_cva: set[tuple[str, str, str]] = set()

                    char_edges = media.get("characters", {}).get("edges", [])
                    has_more = media.get("characters", {}).get("pageInfo", {}).get("hasNextPage", False)
                    char_page = 1

                    while True:
                        if char_edges:
                            chars, cvas = parse_anilist_characters(char_edges, anime_id_str)
                            for ch in chars:
                                if ch.id not in seen_chars:
                                    seen_chars.add(ch.id)
                                    upsert_character(conn, ch)
                            for cva in cvas:
                                cva_key = (cva.character_id, cva.person_id, cva.anime_id)
                                if cva_key not in seen_cva:
                                    seen_cva.add(cva_key)
                                    insert_character_voice_actor(conn, cva)

                        if not has_more:
                            break

                        # 次ページ取得
                        char_page += 1
                        try:
                            resp2 = await client.query(
                                BACKFILL_CHARS_QUERY,
                                {"id": anilist_id, "page": char_page},
                            )
                            media2 = resp2.get("Media", {})
                            char_edges = media2.get("characters", {}).get("edges", [])
                            has_more = media2.get("characters", {}).get("pageInfo", {}).get("hasNextPage", False)
                        except Exception:
                            break  # キャラ追加ページ失敗はベストエフォート

                    stats["success"] += 1

            except Exception as e:
                stats["error"] += 1
                stats["error_details"].append({
                    "anilist_id": anilist_id,
                    "error": f"{type(e).__name__}: {e}",
                })

            completed.add(anilist_id)

            # チェックポイント + DB commit
            if (i + 1) % checkpoint_interval == 0:
                conn.commit()
                _save_checkpoint(checkpoint_path, completed)

            # 進捗更新
            rate_info = ""
            if client.requests_remaining is not None:
                rate_info = f" | API: {client.requests_remaining}"
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            progress.update(
                task,
                description=f"[{stats['success']}✅ {stats['empty']}⏭️  {stats['error']}❌]{rate_info} ({rate:.1f}/s)",
                advance=1,
            )

    conn.commit()
    conn.close()

    # チェックポイント処理
    if stats["error"] == 0:
        checkpoint_path.unlink(missing_ok=True)
    else:
        _save_checkpoint(checkpoint_path, completed)

    await client.close()
    stats["elapsed"] = time.time() - start_time
    return stats


async def _batch_rescrape(
    anime_ids: list[int],
    completed: set[int],
    checkpoint_path: Path,
    checkpoint_interval: int = 10,
) -> dict:
    """バッチ再スクレイプ（チェックポイント付き）."""
    client = AniListClient()
    stats = {
        "total": len(anime_ids),
        "success": 0,
        "empty": 0,
        "error": 0,
        "error_details": [],
    }
    start_time = time.time()

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing...", total=len(anime_ids))

        for i, anilist_id in enumerate(anime_ids):
            try:
                result = await fetch_and_update_anime(client, anilist_id)
                result_status = result.get("status", "error")

                if result_status == "success":
                    stats["success"] += 1
                elif result_status == "empty":
                    stats["empty"] += 1
                else:
                    stats["error"] += 1
                    stats["error_details"].append({
                        "anilist_id": anilist_id,
                        "status": result_status,
                    })

            except Exception as e:
                stats["error"] += 1
                stats["error_details"].append({
                    "anilist_id": anilist_id,
                    "error": f"{type(e).__name__}: {e}",
                })

            completed.add(anilist_id)

            # チェックポイント保存
            if (i + 1) % checkpoint_interval == 0:
                _save_checkpoint(checkpoint_path, completed)

            # 進捗更新
            rate_info = ""
            if client.requests_remaining is not None:
                rate_info = f" | API: {client.requests_remaining}"
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            progress.update(
                task,
                description=f"[{stats['success']}✅ {stats['empty']}⏭️  {stats['error']}❌]{rate_info} ({rate:.1f}/s)",
                advance=1,
            )

    # 最終チェックポイント → 完了なら削除
    if stats["error"] == 0:
        checkpoint_path.unlink(missing_ok=True)
    else:
        _save_checkpoint(checkpoint_path, completed)

    await client.close()

    stats["elapsed"] = time.time() - start_time
    return stats


async def _detect_deleted_anime(
    targets: list[tuple[int, str, str]],
) -> dict:
    """404アニメを検出して、見つかったものは再スクレイプ."""
    client = AniListClient()
    stats = {
        "checked": 0,
        "deleted": 0,
        "no_staff": 0,
        "recovered": 0,
        "errors": 0,
        "deleted_ids": [],
    }

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Checking...", total=len(targets))

        for anilist_id, title, fmt in targets:
            stats["checked"] += 1
            try:
                # 軽量クエリでまずMedia存在確認 + staff有無チェック
                resp = await client.query(
                    """
                    query ($id: Int) {
                      Media(id: $id, type: ANIME) {
                        id
                        staff(page: 1, perPage: 1) {
                          pageInfo { total }
                        }
                      }
                    }
                    """,
                    {"id": anilist_id},
                )
                media = resp.get("Media")
                if media is None:
                    # APIがMedia=nullを返す場合（存在しない or 削除済み）
                    stats["deleted"] += 1
                    stats["deleted_ids"].append((anilist_id, title))
                else:
                    staff_total = media.get("staff", {}).get("pageInfo", {}).get("total", 0)
                    if staff_total == 0:
                        stats["no_staff"] += 1
                    else:
                        # スタッフはいるのにクレジット未取得 → 再スクレイプ
                        stats["recovered"] += 1
                        try:
                            await fetch_and_update_anime(client, anilist_id)
                        except Exception:
                            pass  # ベストエフォート

            except Exception as e:
                err_str = str(e)
                if "404" in err_str or "Not Found" in err_str:
                    stats["deleted"] += 1
                    stats["deleted_ids"].append((anilist_id, title))
                else:
                    stats["errors"] += 1

            rate_info = ""
            if client.requests_remaining is not None:
                rate_info = f" | API: {client.requests_remaining}"
            progress.update(
                task,
                description=f"[{stats['deleted']}🗑️  {stats['no_staff']}⏭️  {stats['recovered']}✅]{rate_info}",
                advance=1,
            )

    await client.close()
    return stats


def _display_batch_results(stats: dict, title: str) -> None:
    """バッチ処理結果を表示."""
    console.print()
    tbl = Table(show_header=False, box=None, padding=(0, 2))
    tbl.add_row("[cyan]Total[/cyan]", f"{stats['total']:,}")
    tbl.add_row("[green]✅ Success[/green]", f"[bold green]{stats['success']:,}[/bold green]")
    tbl.add_row("[yellow]⏭️  Empty (no staff)[/yellow]", f"{stats['empty']:,}")
    tbl.add_row("[red]❌ Error[/red]", f"[bold red]{stats['error']:,}[/bold red]")
    elapsed = stats.get("elapsed", 0)
    if elapsed > 0:
        tbl.add_row("[dim]Elapsed[/dim]", f"[dim]{elapsed / 60:.1f} min[/dim]")
        rate = stats["total"] / elapsed
        tbl.add_row("[dim]Rate[/dim]", f"[dim]{rate:.1f} anime/s[/dim]")

    console.print(Panel(tbl, title=f"[bold]{title}[/bold]", border_style="cyan"))

    # エラー詳細
    errors = stats.get("error_details", [])
    if errors:
        console.print()
        console.print(f"[bold red]Errors ({len(errors)}):[/bold red]")
        for err in errors[:20]:
            aid = err.get("anilist_id", "?")
            msg = err.get("error", err.get("status", "unknown"))
            console.print(f"  [dim]• anilist:{aid} — {msg}[/dim]")
        if len(errors) > 20:
            console.print(f"  [dim]  ... and {len(errors) - 20} more[/dim]")

    # チェックポイントの案内
    if stats["error"] > 0:
        console.print()
        console.print("[yellow]💡 チェックポイントが保存されました。再実行すると続きから再開します。[/yellow]")


if __name__ == "__main__":
    app()
