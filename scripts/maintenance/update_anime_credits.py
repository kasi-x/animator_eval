"""特定のアニメのクレジットを再スクレイプして、改善されたロール分類で更新する.

改善点:
- AniListClient を再利用（永続TCP接続、5回リトライ+指数バックオフ）
- マッハ方式レート制限（0.1秒間隔、429来たらRetry-Afterだけ待つ）
- 双方向ページネーション（staff+characters を1リクエストで進行）
- チェックポイント（アニメ1件完了ごとに保存、自動再開）
- アトミックDB更新（全クレジット取得完了後に DELETE→INSERT）
- 重複排除（person_id, anime_id, raw_role のセット）
- アニメ詳細情報も同時取得・DB更新

Usage:
    PYTHONPATH=. pixi run python scripts/update_anime_credits.py --anime-id 16498
    PYTHONPATH=. pixi run python scripts/update_anime_credits.py --limit 5
    PYTHONPATH=. pixi run python scripts/update_anime_credits.py --limit 50 --resume
"""

import argparse
import asyncio
import json
from pathlib import Path

import structlog

from src.db import (
    get_connection,
    insert_anime_relation,
    insert_anime_studio,
    insert_character_voice_actor,
    insert_credit,
    upsert_anime,
    upsert_character,
    upsert_studio,
)
from src.runtime.models import AnimeRelation, AnimeStudio, Character, CharacterVoiceActor, Credit, Studio, parse_role
from src.scrapers.anilist_scraper import (
    ANIME_STAFF_MINIMAL_QUERY,
    AniListClient,
    parse_anilist_anime,
    parse_anilist_characters,
    parse_anilist_relations,
    parse_anilist_studios,
)

logger = structlog.get_logger()

PER_PAGE = 25
CHECKPOINT_FILE = Path(__file__).parent.parent / "data" / "update_credits_checkpoint.json"

# アニメ詳細情報 + 最小スタッフ/声優データの複合クエリ
# 初回リクエストでアニメメタデータも取得する
ANIME_WITH_STAFF_QUERY = """
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


def _parse_credits_from_media(media: dict, anime_id_str: str) -> list[Credit]:
    """スタッフ+声優クレジットを抽出して重複排除（raw_roleベース）."""
    credits: list[Credit] = []
    seen: set[tuple[str, str, str]] = set()

    # スタッフクレジット
    for edge in media.get("staff", {}).get("edges", []):
        node = edge.get("node", {})
        person_id_raw = node.get("id")
        raw_role_str = edge.get("role", "")
        if not person_id_raw or not raw_role_str:
            continue
        person_id = f"anilist:p{person_id_raw}"
        role = parse_role(raw_role_str)
        key = (person_id, anime_id_str, raw_role_str)
        if key not in seen:
            seen.add(key)
            credits.append(Credit(
                person_id=person_id,
                anime_id=anime_id_str,
                role=role,
                raw_role=raw_role_str,
                source="anilist",
            ))

    # 声優クレジット
    for edge in media.get("characters", {}).get("edges", []):
        for va in edge.get("voiceActors") or []:
            va_id = va.get("id")
            if not va_id:
                continue
            person_id = f"anilist:p{va_id}"
            role = parse_role("Voice Actor")
            key = (person_id, anime_id_str, "Voice Actor")
            if key not in seen:
                seen.add(key)
                credits.append(Credit(
                    person_id=person_id,
                    anime_id=anime_id_str,
                    role=role,
                    raw_role="Voice Actor",
                    source="anilist",
                ))

    return credits


async def fetch_all_credits(
    client: AniListClient, anime_id: int
) -> tuple[dict | None, list[Credit], list[Character], list[CharacterVoiceActor], list[Studio], list[AnimeStudio], list[AnimeRelation]]:
    """アニメ1件の全クレジット+アニメ情報+キャラクター+スタジオ+関連作品をページネーションで取得.

    双方向ページネーション: staff と characters を同時に進行し、
    片方が完了したら perPage=1 でスキップ。

    Returns:
        (anime_raw_data, credits, characters, character_voice_actors, studios, anime_studios, relations)
    """
    anime_id_str = f"anilist:{anime_id}"
    all_credits: list[Credit] = []
    all_characters: list[Character] = []
    all_cva: list[CharacterVoiceActor] = []
    all_studios: list[Studio] = []
    all_anime_studios: list[AnimeStudio] = []
    all_relations: list[AnimeRelation] = []
    seen_credits: set[tuple[str, str, str]] = set()
    seen_chars: set[str] = set()
    seen_cva: set[tuple[str, str, str]] = set()
    anime_raw: dict | None = None

    staff_page = 1
    char_page = 1
    has_more_staff = True
    has_more_chars = True
    is_first_page = True

    while has_more_staff or has_more_chars:
        # 初回はアニメ詳細情報付きクエリ、2回目以降は最小クエリ
        query = ANIME_WITH_STAFF_QUERY if is_first_page else ANIME_STAFF_MINIMAL_QUERY
        resp = await client.query(
            query,
            {
                "id": anime_id,
                "staffPage": staff_page if has_more_staff else 1,
                "staffPerPage": PER_PAGE if has_more_staff else 1,
                "charPage": char_page if has_more_chars else 1,
                "charPerPage": PER_PAGE if has_more_chars else 1,
            },
        )
        media = resp.get("Media", {})
        if not media:
            break

        # 初回レスポンスからアニメ情報+スタジオを保持
        if is_first_page:
            anime_raw = media
            is_first_page = False
            # スタジオ・関連作品は初回レスポンスのみ（ページネーション不要）
            studios, anime_studio_edges = parse_anilist_studios(media, anime_id_str)
            all_studios = studios
            all_anime_studios = anime_studio_edges
            all_relations = parse_anilist_relations(media, anime_id_str)

        # Parse credits from this page (with dedup by raw_role)
        page_credits = _parse_credits_from_media(media, anime_id_str)
        for c in page_credits:
            key = (c.person_id, c.anime_id, c.raw_role or "")
            if key not in seen_credits:
                seen_credits.add(key)
                all_credits.append(c)

        # Parse characters and character-VA mappings
        char_edges = media.get("characters", {}).get("edges", [])
        if char_edges:
            chars, cvas = parse_anilist_characters(char_edges, anime_id_str)
            for ch in chars:
                if ch.id not in seen_chars:
                    seen_chars.add(ch.id)
                    all_characters.append(ch)
            for cva in cvas:
                cva_key = (cva.character_id, cva.person_id, cva.anime_id)
                if cva_key not in seen_cva:
                    seen_cva.add(cva_key)
                    all_cva.append(cva)

        # Advance staff pagination
        if has_more_staff:
            has_more_staff = media.get("staff", {}).get("pageInfo", {}).get("hasNextPage", False)
            if has_more_staff:
                staff_page += 1

        # Advance character pagination
        if has_more_chars:
            has_more_chars = media.get("characters", {}).get("pageInfo", {}).get("hasNextPage", False)
            if has_more_chars:
                char_page += 1

    return anime_raw, all_credits, all_characters, all_cva, all_studios, all_anime_studios, all_relations


def atomic_update_credits(conn, anime_id_str: str, new_credits: list[Credit]) -> int:
    """アトミックにクレジットを更新: DELETE → バッチINSERT を1トランザクションで.

    Returns:
        更新前のクレジット数
    """
    old_count = conn.execute(
        "SELECT COUNT(*) FROM credits WHERE anime_id = ?", (anime_id_str,)
    ).fetchone()[0]

    conn.execute("DELETE FROM credits WHERE anime_id = ?", (anime_id_str,))
    for credit in new_credits:
        insert_credit(conn, credit)
    conn.commit()

    return old_count


def load_checkpoint() -> dict:
    """チェックポイントを読み込む."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {}


def save_checkpoint(completed_anime_ids: list[int], last_index: int) -> None:
    """チェックポイントを保存する."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "completed_anime_ids": completed_anime_ids,
            "last_index": last_index,
        }, f)


def delete_checkpoint() -> None:
    """チェックポイントを削除する."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


async def fetch_and_update_anime(client: AniListClient, anime_id: int) -> dict:
    """アニメ1件のクレジット+アニメ情報+キャラクターを取得してアトミックにDB更新."""
    anime_id_str = f"anilist:{anime_id}"

    try:
        anime_raw, all_credits, all_characters, all_cva, all_studios, all_anime_studios, all_relations = await fetch_all_credits(client, anime_id)

        if not all_credits:
            logger.warning("no_credits_found", anime_id=anime_id)
            return {"anime_id": anime_id, "status": "empty", "old_count": 0, "new_count": 0}

        # アトミックDB更新（全データ取得完了後）
        conn = get_connection()

        # アニメ情報を更新
        title = ""
        if anime_raw:
            anime_obj = parse_anilist_anime(anime_raw)
            upsert_anime(conn, anime_obj)
            title = anime_obj.title_ja or anime_obj.title_en or ""

        old_count = atomic_update_credits(conn, anime_id_str, all_credits)

        # スタジオ + アニメ×スタジオ マッピングを保存
        for studio in all_studios:
            upsert_studio(conn, studio)
        for anime_studio in all_anime_studios:
            insert_anime_studio(conn, anime_studio)

        # 関連作品を保存
        for relation in all_relations:
            insert_anime_relation(conn, relation)

        # キャラクター + キャラクター×VA マッピングを保存
        for ch in all_characters:
            upsert_character(conn, ch)
        for cva in all_cva:
            insert_character_voice_actor(conn, cva)
        conn.commit()
        conn.close()

        logger.info("anime_updated", anime_id=anime_id, title=title,
                     old_count=old_count, new_count=len(all_credits))

        return {
            "anime_id": anime_id,
            "title": title,
            "status": "success",
            "old_count": old_count,
            "new_count": len(all_credits),
        }

    except Exception as e:
        logger.error("scrape_failed", anime_id=anime_id, error=str(e))
        return {"anime_id": anime_id, "status": "error", "old_count": 0, "new_count": 0}


async def update_top_anime(limit: int, resume: bool = False):
    """クレジット数が多いアニメから順に再スクレイプ."""
    conn = get_connection()
    top_anime = conn.execute(
        """SELECT anime_id FROM credits
           GROUP BY anime_id ORDER BY COUNT(*) DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()

    anime_list = []
    for (anime_id_str,) in top_anime:
        try:
            anilist_id = int(anime_id_str.split(":")[1])
            anime_list.append(anilist_id)
        except (ValueError, IndexError):
            logger.warning("skip_non_anilist", anime_id=anime_id_str)

    # チェックポイントから再開
    start_index = 0
    completed_ids: list[int] = []
    if resume:
        checkpoint = load_checkpoint()
        if checkpoint:
            completed_ids = checkpoint.get("completed_anime_ids", [])
            start_index = checkpoint.get("last_index", 0)
            logger.info("resuming_from_checkpoint",
                        completed=len(completed_ids), start_index=start_index)
            print(f"チェックポイントから再開: {start_index}/{len(anime_list)}件完了済み")

    client = AniListClient()
    results = []

    try:
        for i in range(start_index, len(anime_list)):
            anilist_id = anime_list[i]

            if anilist_id in completed_ids:
                continue

            result = await fetch_and_update_anime(client, anilist_id)
            results.append(result)

            if result["status"] == "success":
                completed_ids.append(anilist_id)

            # チェックポイント保存（毎回）
            save_checkpoint(completed_ids, i + 1)

            # 進捗表示
            done = i + 1 - start_index
            total = len(anime_list) - start_index
            status_icon = "✓" if result["status"] == "success" else "✗"
            title = result.get("title", "")
            title_display = f" {title}" if title else ""
            print(
                f"  [{done}/{total}] {status_icon} anilist:{anilist_id}{title_display}"
                f"  {result.get('old_count', 0)} → {result.get('new_count', 0)}件"
            )
    finally:
        await client.close()

    # 全件完了したらチェックポイント削除
    if start_index + len(results) >= len(anime_list):
        delete_checkpoint()

    return results


def main():
    parser = argparse.ArgumentParser(
        description="特定のアニメのクレジットを再スクレイプして更新（高速・堅牢版）"
    )
    parser.add_argument("--anime-id", type=int, help="AniList anime ID")
    parser.add_argument("--limit", type=int, help="上位N件のアニメを更新")
    parser.add_argument("--resume", action="store_true", help="チェックポイントから再開")
    args = parser.parse_args()

    if args.anime_id:
        client = AniListClient()

        async def _run():
            try:
                return await fetch_and_update_anime(client, args.anime_id)
            finally:
                await client.close()

        result = asyncio.run(_run())
        if result["status"] == "success":
            title = result.get("title", "")
            print(f"\n更新完了: anilist:{result['anime_id']} {title}")
            print(f"  Before: {result['old_count']}件")
            print(f"  After:  {result['new_count']}件")
        elif result["status"] == "empty":
            print(f"\nクレジットなし: anime_id={result['anime_id']}")
        else:
            print(f"\nエラー: anime_id={result['anime_id']}")

    elif args.limit:
        results = asyncio.run(update_top_anime(args.limit, resume=args.resume))
        success = [r for r in results if r.get("status") == "success"]
        total_old = sum(r.get("old_count", 0) for r in success)
        total_new = sum(r.get("new_count", 0) for r in success)
        print(f"\n{len(success)}/{len(results)}件のアニメを更新")
        print(f"  Total Before: {total_old:,}件")
        print(f"  Total After:  {total_new:,}件")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
