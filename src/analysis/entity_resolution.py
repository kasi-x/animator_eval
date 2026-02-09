"""名寄せ処理 — 同一人物の異表記を統合する.

名寄せの精度は法的要件（信用毀損リスク）に直結するため、
保守的なマッチング（高精度・低再現率）を採用する。

false positive（別人を同一人物と判定）は絶対に避ける。
false negative（同一人物を見逃す）は許容する。
"""

import re
import unicodedata
from collections import defaultdict

import structlog

from src.models import Person

logger = structlog.get_logger()


def normalize_name(name: str) -> str:
    """名前文字列を正規化する.

    - NFKC正規化（全角→半角、互換文字統一）
    - 空白統一
    - 敬称除去
    - 小文字化（英語名）
    """
    if not name:
        return ""

    # NFKC正規化
    name = unicodedata.normalize("NFKC", name)

    # 空白の統一
    name = re.sub(r"\s+", " ", name).strip()

    # 敬称除去
    honorifics = ["さん", "先生", "氏", "様", "san", "sensei"]
    for h in honorifics:
        name = re.sub(rf"\s*{re.escape(h)}$", "", name)

    # 英語名は小文字化
    if all(ord(c) < 128 or c.isspace() for c in name):
        name = name.lower()

    return name


def _is_kanji(char: str) -> bool:
    """漢字かどうかを判定."""
    cp = ord(char)
    return (
        (0x4E00 <= cp <= 0x9FFF)  # CJK統合漢字
        or (0x3400 <= cp <= 0x4DBF)  # CJK統合漢字拡張A
        or (0x20000 <= cp <= 0x2A6DF)  # CJK統合漢字拡張B
    )


def _is_japanese_name(name: str) -> bool:
    """日本語の名前かどうかを判定."""
    return any(_is_kanji(c) or ("\u3040" <= c <= "\u309F") or ("\u30A0" <= c <= "\u30FF") for c in name)


def exact_match_cluster(persons: list[Person]) -> dict[str, str]:
    """完全一致による名寄せ（最も保守的）.

    正規化後の名前が完全一致する場合のみ統合する。
    Returns: {person_id: canonical_id}
    """
    name_groups: dict[str, list[str]] = defaultdict(list)

    for p in persons:
        for name in [p.name_ja, p.name_en] + p.aliases:
            normalized = normalize_name(name)
            if normalized:
                name_groups[normalized].append(p.id)

    # 同一名のグループを統合
    canonical_map: dict[str, str] = {}

    for _name, ids in name_groups.items():
        if len(ids) < 2:
            continue
        unique_ids = list(dict.fromkeys(ids))
        canonical = unique_ids[0]  # 最初に見つかったIDを正規とする
        for pid in unique_ids[1:]:
            if pid not in canonical_map:
                canonical_map[pid] = canonical
                logger.info("entity_merged", source=pid, canonical=canonical, strategy="exact_match")

    return canonical_map


def _normalize_romaji(name: str) -> str:
    """ローマ字名を正規化する.

    - 小文字化
    - ハイフン・アポストロフィ除去
    - 長音記号統一 (ō→o, ū→u, etc.)
    - 名前の構成要素をソート（語順の違いを吸収）
    """
    if not name:
        return ""

    name = name.lower().strip()

    # 記号除去
    name = name.replace("-", "").replace("'", "").replace("'", "")

    # 長音マクロン除去
    macron_map = str.maketrans("āēīōūÅĒĪŌŪ", "aeiouaeiou")
    name = name.translate(macron_map)

    # 名前パーツをソート（"yamada taro" == "taro yamada"）
    parts = sorted(name.split())
    return " ".join(parts)


def romaji_match(persons: list[Person]) -> dict[str, str]:
    """ローマ字名の正規化比較による名寄せ.

    英語名(name_en)のローマ字表記を正規化して比較する。
    名前の語順違い（姓名の入れ替え）を吸収する。
    十分な長さの名前のみを対象とする（短い名前は曖昧性が高い）。
    """
    MIN_NAME_LENGTH = 5  # "Ai Li" のような短い名前は除外

    # ソース別に分類
    mal_persons: dict[str, Person] = {}
    anilist_persons: dict[str, Person] = {}

    for p in persons:
        if p.id.startswith("mal:"):
            mal_persons[p.id] = p
        elif p.id.startswith("anilist:"):
            anilist_persons[p.id] = p

    # MAL の正規化ローマ字インデックス
    mal_romaji_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in mal_persons.items():
        if p.name_en and len(p.name_en) >= MIN_NAME_LENGTH:
            normalized = _normalize_romaji(p.name_en)
            if normalized:
                mal_romaji_index[normalized].append(pid)

    canonical_map: dict[str, str] = {}

    for anilist_pid, p in anilist_persons.items():
        if not p.name_en or len(p.name_en) < MIN_NAME_LENGTH:
            continue
        normalized = _normalize_romaji(p.name_en)
        if not normalized:
            continue
        if normalized in mal_romaji_index:
            mal_ids = mal_romaji_index[normalized]
            if len(mal_ids) == 1 and anilist_pid not in canonical_map:
                canonical_map[anilist_pid] = mal_ids[0]
                logger.info(
                    "entity_merged",
                    source=anilist_pid,
                    canonical=mal_ids[0],
                    strategy="romaji_match",
                    name=normalized,
                )

    return canonical_map


def cross_source_match(persons: list[Person]) -> dict[str, str]:
    """異なるデータソース間の名寄せ.

    MAL と AniList の人物を名前の完全一致で統合する。
    """
    # ソース別に分類
    mal_persons: dict[str, Person] = {}
    anilist_persons: dict[str, Person] = {}

    for p in persons:
        if p.id.startswith("mal:"):
            mal_persons[p.id] = p
        elif p.id.startswith("anilist:"):
            anilist_persons[p.id] = p

    # 正規化名 → person_id のインデックス
    mal_name_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in mal_persons.items():
        for name in [p.name_ja, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and len(n) >= 2:
                mal_name_index[n].append(pid)

    canonical_map: dict[str, str] = {}

    for anilist_pid, p in anilist_persons.items():
        for name in [p.name_ja, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and n in mal_name_index:
                mal_ids = mal_name_index[n]
                if len(mal_ids) == 1:
                    # 一意にマッチ
                    canonical_map[anilist_pid] = mal_ids[0]
                    logger.info(
                        "entity_merged",
                        source=anilist_pid,
                        canonical=mal_ids[0],
                        strategy="cross_source",
                        name=n,
                    )
                    break
                else:
                    # 曖昧 — 安全のためスキップ
                    logger.debug(
                        "ambiguous_cross_source_match",
                        name=n,
                        candidates=mal_ids,
                    )

    return canonical_map


def resolve_all(persons: list[Person]) -> dict[str, str]:
    """全名寄せ処理を実行する.

    Returns: {person_id: canonical_id}
    未統合の人物はマッピングに含まれない。
    """
    # Step 1: 完全一致
    exact = exact_match_cluster(persons)

    # Step 2: クロスソースマッチ（完全一致）
    cross = cross_source_match(persons)

    # Step 3: ローマ字マッチ（既にマッチ済みのものは除外）
    already_matched = set(exact) | set(cross)
    remaining = [p for p in persons if p.id not in already_matched]
    romaji = romaji_match(remaining)

    # 統合
    merged = {**exact, **cross, **romaji}
    logger.info(
        "entity_resolution_complete",
        total_merges=len(merged),
        exact_merges=len(exact),
        cross_source_merges=len(cross),
        romaji_merges=len(romaji),
    )
    return merged
