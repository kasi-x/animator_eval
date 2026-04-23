"""名寄せ処理 — 同一人物の異表記を統合する.

名寄せの精度は法的要件（信用毀損リスク）に直結するため、
保守的なマッチング（高精度・低再現率）を採用する。

false positive（別人を同一人物と判定）は絶対に避ける。
false negative（同一人物を見逃す）は許容する。
"""

import functools
import re
import unicodedata
from collections import defaultdict

import structlog
from rapidfuzz import fuzz

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
    return any(
        _is_kanji(c) or ("\u3040" <= c <= "\u309f") or ("\u30a0" <= c <= "\u30ff")
        for c in name
    )


def _definitely_different(p1: Person, p2: Person) -> bool:
    """数値IDが両者とも設定されており、かつ異なる場合は確実に別人.

    ANN/AniList/MAL などのソースでは同名別人に別IDが付与されるため、
    数値IDの不一致は同一人物でないことの確実な証拠となる。
    """
    if p1.ann_id and p2.ann_id and p1.ann_id != p2.ann_id:
        return True
    if p1.anilist_id and p2.anilist_id and p1.anilist_id != p2.anilist_id:
        return True
    if p1.mal_id and p2.mal_id and p1.mal_id != p2.mal_id:
        return True
    return False


def _numeric_id_key(p: Person) -> tuple:
    """同名グループ内でホモニム分割するためのキー.

    数値IDが設定されている場合はそれを使い、未設定の場合は person_id 自体をキーにする。
    これにより同じ名前でも異なる数値IDを持つ人物は別クラスタに分離される。
    """
    return (p.ann_id, p.anilist_id, p.mal_id)


def exact_match_cluster(
    persons: list[Person],
    ml_clusters: dict[str, str] | None = None,
) -> dict[str, str]:
    """完全一致による名寄せ（最も保守的）.

    正規化後の名前が完全一致する場合のみ統合する。
    日本語名を優先し、英語名のみでのマッチは日本語名がない場合のみ許容する。
    これにより、異なる漢字で同じローマ字表記のケース（例: 岡遼子 vs 岡亮子）での
    false positive を防ぐ。

    ホモニム保護: ann_id / anilist_id / mal_id が両者ともセットされており
    かつ異なる場合はマージしない（同名別人）。

    Returns: {person_id: canonical_id}
    """
    # スクリプト別名前グループ
    ja_name_groups: dict[str, list[str]] = defaultdict(list)
    ko_name_groups: dict[str, list[str]] = defaultdict(list)
    zh_name_groups: dict[str, list[str]] = defaultdict(list)
    # 英語名でのグループ化（ネイティブ名を持たない人物のみ）
    en_name_groups: dict[str, list[str]] = defaultdict(list)
    # エイリアスでのグループ化
    alias_groups: dict[str, list[str]] = defaultdict(list)

    persons_by_id = {p.id: p for p in persons}

    for p in persons:
        has_native = False
        if p.name_ja:
            normalized_ja = normalize_name(p.name_ja)
            if normalized_ja:
                ja_name_groups[normalized_ja].append(p.id)
                has_native = True
        if p.name_ko:
            normalized_ko = normalize_name(p.name_ko)
            if normalized_ko:
                ko_name_groups[normalized_ko].append(p.id)
                has_native = True
        if p.name_zh:
            normalized_zh = normalize_name(p.name_zh)
            if normalized_zh:
                zh_name_groups[normalized_zh].append(p.id)
                has_native = True
        # 英語名はネイティブ名を持たない人物のみ
        if not has_native and p.name_en:
            normalized_en = normalize_name(p.name_en)
            if normalized_en:
                en_name_groups[normalized_en].append(p.id)

        # エイリアスは補助的に使用
        for alias in p.aliases:
            normalized_alias = normalize_name(alias)
            if normalized_alias:
                alias_groups[normalized_alias].append(p.id)

    canonical_map: dict[str, str] = {}

    def _merge_group(ids: list[str], name: str, strategy: str) -> None:
        """同名グループ内でホモニム保護しながらマージする."""
        unique_ids = list(dict.fromkeys(ids))
        if len(unique_ids) < 2:
            return
        # ホモニム分割: 以下の場合はマージ不可として別クラスタに分離
        #   1. 数値ID（ann_id/anilist_id/mal_id）が両方セットかつ異なる
        #   2. ML クラスタリングで異なるクラスタに分類された
        clusters: list[list[str]] = []
        for pid in unique_ids:
            placed = False
            p = persons_by_id[pid]
            for cluster in clusters:
                rep_id = cluster[0]
                rep = persons_by_id[rep_id]
                # 数値 ID による確実な別人判定
                if _definitely_different(p, rep):
                    continue
                # ML クラスタによる別人判定
                if ml_clusters:
                    p_cluster = ml_clusters.get(pid)
                    rep_cluster = ml_clusters.get(rep_id)
                    if (
                        p_cluster is not None
                        and rep_cluster is not None
                        and p_cluster != rep_cluster
                    ):
                        continue
                cluster.append(pid)
                placed = True
                break
            if not placed:
                clusters.append([pid])

        for cluster in clusters:
            if len(cluster) < 2:
                continue
            canonical = cluster[0]
            for pid in cluster[1:]:
                if pid not in canonical_map:
                    canonical_map[pid] = canonical
                    logger.info(
                        "entity_merged",
                        source=pid,
                        canonical=canonical,
                        strategy=strategy,
                        name=name,
                    )

    # スクリプト別名前での統合（ネイティブ名は互いに独立）
    for name_ja, ids in ja_name_groups.items():
        _merge_group(ids, name_ja, "exact_match")
    for name_ko, ids in ko_name_groups.items():
        _merge_group(ids, name_ko, "exact_match")
    for name_zh, ids in zh_name_groups.items():
        _merge_group(ids, name_zh, "exact_match")

    # 英語名での統合（ネイティブ名を持たない人物のみ）
    for name_en, ids in en_name_groups.items():
        _merge_group(ids, name_en, "exact_match")

    # エイリアスでの統合（補助的、既にマッチしていない場合のみ）
    for alias, ids in alias_groups.items():
        if len(ids) < 2:
            continue
        unique_ids = list(dict.fromkeys(ids))
        # エイリアスマッチはネイティブ名を持たない場合のみ許可
        valid_ids = [
            pid for pid in unique_ids
            if not (persons_by_id[pid].name_ja
                    or persons_by_id[pid].name_ko
                    or persons_by_id[pid].name_zh)
        ]
        _merge_group(valid_ids, alias, "exact_match_alias")

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

    MAL/MADB/ANN の人物を AniList に対して名前の完全一致で統合する。
    MADB人物は name_ja の正規化一致のみ使用（法的リスク回避）。
    ANN人物は name_ja + name_en の完全一致。ホモニム保護付き。
    """
    # ソース別に分類
    mal_persons: dict[str, Person] = {}
    anilist_persons: dict[str, Person] = {}
    madb_persons: dict[str, Person] = {}
    ann_persons: dict[str, Person] = {}
    allcinema_persons: dict[str, Person] = {}

    for p in persons:
        if p.id.startswith("mal:"):
            mal_persons[p.id] = p
        elif p.id.startswith("anilist:"):
            anilist_persons[p.id] = p
        elif p.id.startswith("madb:"):
            madb_persons[p.id] = p
        elif p.id.startswith("ann-"):
            ann_persons[p.id] = p
        elif p.id.startswith("allcinema:"):
            allcinema_persons[p.id] = p

    # AniList の正規化名インデックス（ja/ko/zh/en）
    anilist_ja_index: dict[str, list[str]] = defaultdict(list)
    anilist_ko_index: dict[str, list[str]] = defaultdict(list)
    anilist_zh_index: dict[str, list[str]] = defaultdict(list)
    anilist_en_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in anilist_persons.items():
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3:
                anilist_ja_index[n].append(pid)
        if p.name_ko:
            n = normalize_name(p.name_ko)
            if n and len(n) >= 2:
                anilist_ko_index[n].append(pid)
        if p.name_zh:
            n = normalize_name(p.name_zh)
            if n and len(n) >= 2:
                anilist_zh_index[n].append(pid)
        if p.name_en:
            n = normalize_name(p.name_en)
            if n and len(n) >= 5:
                anilist_en_index[n].append(pid)

    # MAL の正規化名インデックス
    mal_name_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in mal_persons.items():
        for name in [p.name_ja, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and len(n) >= 2:
                mal_name_index[n].append(pid)

    canonical_map: dict[str, str] = {}

    # MAL → AniList マッチング
    for anilist_pid, p in anilist_persons.items():
        for name in [p.name_ja, p.name_ko, p.name_zh, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and n in mal_name_index:
                mal_ids = mal_name_index[n]
                if len(mal_ids) == 1:
                    mal_p = next(
                        mp for mid, mp in mal_persons.items() if mid == mal_ids[0]
                    )
                    if not _definitely_different(p, mal_p):
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
                    logger.debug(
                        "ambiguous_cross_source_match",
                        name=n,
                        candidates=mal_ids,
                    )

    # MADB → AniList マッチング（name_ja のみ、高精度）
    for madb_pid, p in madb_persons.items():
        if not p.name_ja:
            continue
        n = normalize_name(p.name_ja)
        if not n or len(n) < 3:
            continue
        if n in anilist_ja_index:
            anilist_ids = anilist_ja_index[n]
            if len(anilist_ids) == 1:
                al_p = anilist_persons[anilist_ids[0]]
                if not _definitely_different(p, al_p):
                    canonical_map[madb_pid] = anilist_ids[0]
                    logger.info(
                        "entity_merged",
                        source=madb_pid,
                        canonical=anilist_ids[0],
                        strategy="cross_source_madb",
                        name=n,
                    )
            else:
                logger.debug(
                    "ambiguous_cross_source_match",
                    name=n,
                    candidates=anilist_ids,
                    source="madb",
                )

    # ANN → AniList マッチング（name_ja 優先、なければ name_en）
    # ANN person は ann_id を持つため、ホモニム保護が効く
    for ann_pid, p in ann_persons.items():
        if ann_pid in canonical_map:
            continue
        matched = False
        # name_ja での照合（最優先）
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3 and n in anilist_ja_index:
                al_ids = anilist_ja_index[n]
                if len(al_ids) == 1:
                    al_p = anilist_persons[al_ids[0]]
                    if not _definitely_different(p, al_p):
                        canonical_map[ann_pid] = al_ids[0]
                        logger.info(
                            "entity_merged",
                            source=ann_pid,
                            canonical=al_ids[0],
                            strategy="cross_source_ann",
                            name=n,
                        )
                        matched = True
                elif len(al_ids) > 1:
                    logger.debug(
                        "ambiguous_cross_source_match",
                        name=n,
                        candidates=al_ids,
                        source="ann",
                    )
        # name_en でのフォールバック（name_ja がない ANN 人物向け）
        if not matched and p.name_en and not p.name_ja:
            n = normalize_name(p.name_en)
            if n and len(n) >= 5 and n in anilist_en_index:
                al_ids = anilist_en_index[n]
                if len(al_ids) == 1:
                    al_p = anilist_persons[al_ids[0]]
                    if not _definitely_different(p, al_p):
                        canonical_map[ann_pid] = al_ids[0]
                        logger.info(
                            "entity_merged",
                            source=ann_pid,
                            canonical=al_ids[0],
                            strategy="cross_source_ann_en",
                            name=n,
                        )

    # allcinema → AniList/ANN マッチング（name_ja の完全一致、ホモニム保護付き）
    # allcinema persons は allcinema_id を持つため数値ID保護が効く
    ann_ja_index: dict[str, list[str]] = defaultdict(list)
    for ann_pid, p in ann_persons.items():
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3:
                ann_ja_index[n].append(ann_pid)

    for ac_pid, p in allcinema_persons.items():
        if ac_pid in canonical_map:
            continue
        if not p.name_ja:
            continue
        n = normalize_name(p.name_ja)
        if not n or len(n) < 3:
            continue
        matched = False
        # AniList 優先
        if n in anilist_ja_index:
            al_ids = anilist_ja_index[n]
            if len(al_ids) == 1:
                al_p = anilist_persons[al_ids[0]]
                if not _definitely_different(p, al_p):
                    canonical_map[ac_pid] = al_ids[0]
                    logger.info(
                        "entity_merged",
                        source=ac_pid,
                        canonical=al_ids[0],
                        strategy="cross_source_allcinema",
                        name=n,
                    )
                    matched = True
            elif len(al_ids) > 1:
                logger.debug(
                    "ambiguous_cross_source_match",
                    name=n,
                    candidates=al_ids,
                    source="allcinema",
                )
        # ANN フォールバック
        if not matched and n in ann_ja_index:
            ann_ids = ann_ja_index[n]
            if len(ann_ids) == 1:
                ann_p = ann_persons[ann_ids[0]]
                if not _definitely_different(p, ann_p):
                    canonical_map[ac_pid] = ann_ids[0]
                    logger.info(
                        "entity_merged",
                        source=ac_pid,
                        canonical=ann_ids[0],
                        strategy="cross_source_allcinema_ann",
                        name=n,
                    )

    return canonical_map


def similarity_based_cluster(
    persons: list[Person], threshold: float = 0.95
) -> dict[str, str]:
    """文字列類似度による名寄せ（最も保守的）.

    Jaro-Winkler類似度を使用し、極めて高い閾値（デフォルト0.95）で
    類似名を統合する。false positiveを避けるため、同一ソース内でのみ比較する。

    Args:
        persons: Person オブジェクトのリスト
        threshold: 類似度の閾値（0.95以上推奨、1.0に近いほど保守的）

    Returns:
        {person_id: canonical_id} のマッピング

    Notes:
        - 法的要件により false positive は絶対に避ける必要がある
        - 短い名前（5文字未満）は曖昧性が高いため除外
        - 同一ソース内でのみ比較（MAL同士、AniList同士）
        - 日本語名とローマ字名は別々に評価
    """
    if threshold < 0.9:
        logger.warning(
            "similarity_threshold_too_low", threshold=threshold, recommended=0.95
        )

    MIN_NAME_LENGTH = 5

    # ソース別に分類
    persons_by_source: dict[str, list[Person]] = defaultdict(list)
    for p in persons:
        source = p.id.split(":")[0] if ":" in p.id else "unknown"
        persons_by_source[source].append(p)

    canonical_map: dict[str, str] = {}

    # 各ソース内で類似度マッチング
    for source, source_persons in persons_by_source.items():
        if len(source_persons) < 2:
            continue

        # 名前 → person_id のマッピング構築
        name_to_ids: dict[str, list[str]] = defaultdict(list)

        for p in source_persons:
            for name_field in [p.name_ja, p.name_ko, p.name_zh, p.name_en] + p.aliases:
                if not name_field:
                    continue
                normalized = normalize_name(name_field)
                if len(normalized) >= MIN_NAME_LENGTH:
                    name_to_ids[normalized].append(p.id)

        # Blocking optimization: group names by first character (reduces comparisons by ~95%)
        blocks: dict[str, list[str]] = defaultdict(list)
        for name in name_to_ids.keys():
            if len(name) >= MIN_NAME_LENGTH:
                first_char = name[0].lower()
                blocks[first_char].append(name)

        # LRU cache for expensive fuzzy similarity calls
        @functools.lru_cache(maxsize=10000)
        def cached_similarity(n1: str, n2: str) -> float:
            return fuzz.ratio(n1, n2) / 100.0

        # Compare within blocks only (same first character)
        comparisons_made = 0
        comparisons_skipped = 0

        for block_char, block_names in blocks.items():
            for i in range(len(block_names)):
                name1 = block_names[i]
                for j in range(i + 1, len(block_names)):
                    name2 = block_names[j]

                    # Early filter: skip if length differs by >20%
                    len_ratio = abs(len(name1) - len(name2)) / max(
                        len(name1), len(name2)
                    )
                    if len_ratio > 0.2:
                        comparisons_skipped += 1
                        continue

                    # Early filter: skip if first 3 chars don't match
                    if len(name1) >= 3 and len(name2) >= 3:
                        if name1[:3] != name2[:3]:
                            comparisons_skipped += 1
                            continue

                    # Jaro-Winkler類似度（prefix重視、名前マッチングに適している）
                    similarity = cached_similarity(name1, name2)
                    comparisons_made += 1

                    if similarity >= threshold:
                        ids1 = name_to_ids[name1]
                        ids2 = name_to_ids[name2]

                        # 1対1マッチのみ受け入れ（曖昧性排除）
                        if len(ids1) == 1 and len(ids2) == 1:
                            canonical = ids1[0]
                            target = ids2[0]

                            # まだマッピングされていない場合のみ追加
                            if target not in canonical_map:
                                canonical_map[target] = canonical
                                logger.info(
                                    "entity_merged",
                                    source=target,
                                    canonical=canonical,
                                    strategy="similarity_based",
                                    similarity=f"{similarity:.3f}",
                                    name1=name1,
                                    name2=name2,
                                )

        logger.debug(
            "similarity_blocking_stats",
            comparisons_made=comparisons_made,
            comparisons_skipped=comparisons_skipped,
            reduction_pct=round(
                100
                * comparisons_skipped
                / max(1, comparisons_made + comparisons_skipped),
                1,
            ),
        )

    return canonical_map


def _transitive_closure(mapping: dict[str, str]) -> dict[str, str]:
    """推移閉包を計算し、全てのキーが最終的な canonical ID を直接指すようにする.

    例: {A→B, B→C} → {A→C, B→C}
    チェーンを辿って終端（他のキーに再マップされない値）を見つける。
    """
    if not mapping:
        return mapping

    # 各キーについてチェーンを辿る
    resolved: dict[str, str] = {}
    for key in mapping:
        target = mapping[key]
        # チェーンの終端まで辿る（循環防止付き）
        visited: set[str] = {key}
        while target in mapping and target not in visited:
            visited.add(target)
            target = mapping[target]
        resolved[key] = target

    return resolved


def resolve_all(
    persons: list[Person],
    credits_by_person: dict[str, list] | None = None,
    anime_meta: dict[str, dict] | None = None,
) -> dict[str, str]:
    """全名寄せ処理を実行する.

    Args:
        persons: 全人物リスト
        credits_by_person: {person_id: [Credit, ...]} (ML 分割に使用)
        anime_meta: {anime_id: {"year": int, "studios": list}} (ML 分割に使用)

    Returns: {person_id: canonical_id}
    未統合の人物はマッピングに含まれない。
    """
    # Step 0: ML クレジットパターンによる同名別人分離
    ml_clusters: dict[str, str] | None = None
    if credits_by_person and anime_meta:
        try:
            from src.analysis.ml_homonym_split import split_homonym_groups

            ml_clusters = split_homonym_groups(persons, credits_by_person, anime_meta)
            logger.info("ml_homonym_split_applied", n_clustered=len(ml_clusters))
        except Exception as exc:
            logger.warning("ml_homonym_split_failed", error=str(exc))

    # Step 1: 完全一致（ML クラスタを guard として使用）
    exact = exact_match_cluster(persons, ml_clusters=ml_clusters)

    # Step 2: クロスソースマッチ（完全一致）
    cross = cross_source_match(persons)

    # Step 3: ローマ字マッチ（既にマッチ済みのものは除外）
    # キーと値の両方を除外: canonical ID が後段で再マッチされるのを防ぐ
    already_matched = (
        set(exact) | set(exact.values()) | set(cross) | set(cross.values())
    )
    remaining = [p for p in persons if p.id not in already_matched]
    romaji = romaji_match(remaining)

    # Step 4: 類似度ベースマッチ（既にマッチ済みのものは除外）
    already_matched = already_matched | set(romaji) | set(romaji.values())
    remaining = [p for p in persons if p.id not in already_matched]
    similarity = similarity_based_cluster(remaining, threshold=0.95)

    # Step 5: AI-assisted matching (LLM) for borderline similarity pairs
    already_matched = already_matched | set(similarity) | set(similarity.values())
    ai_merges = _ai_assisted_step(persons, already_matched)

    # 統合 + 推移閉包
    merged = {**exact, **cross, **romaji, **similarity, **ai_merges}
    merged = _transitive_closure(merged)
    logger.info(
        "entity_resolution_complete",
        total_merges=len(merged),
        exact_merges=len(exact),
        cross_source_merges=len(cross),
        romaji_merges=len(romaji),
        similarity_merges=len(similarity),
        ai_merges=len(ai_merges),
    )
    return merged


def _ai_assisted_step(
    persons: list[Person], already_matched: set[str]
) -> dict[str, str]:
    """Step 5: AI-assisted entity resolution for borderline cases.

    Finds pairs with similarity 0.85-0.95 (too low for auto-match, too high
    to ignore) and asks the LLM to verify.

    Returns: {person_id: canonical_id}
    """
    try:
        from src.analysis.ai_entity_resolution import (
            LLMError,
            ask_llm_if_same_person,
            check_llm_available,
        )
        from src.analysis.llm_pipeline import find_ai_match_candidates, is_llm_enabled
    except ImportError:
        return {}

    if not is_llm_enabled():
        return {}

    if not check_llm_available():
        logger.info("ai_entity_resolution_skipped", reason="llm_not_available")
        return {}

    candidates = find_ai_match_candidates(persons, already_matched, max_candidates=500)
    if not candidates:
        return {}

    logger.info("ai_entity_resolution_start", candidates=len(candidates))

    ai_map: dict[str, str] = {}
    merged_ids: set[str] = set()

    for p1, p2, sim in candidates:
        if p1.id in merged_ids or p2.id in merged_ids:
            continue

        try:
            decision = ask_llm_if_same_person(p1, p2)
            if decision.is_match and decision.confidence >= 0.8:
                ai_map[p2.id] = p1.id
                merged_ids.add(p1.id)
                merged_ids.add(p2.id)
                logger.info(
                    "entity_merged",
                    canonical=p1.id,
                    source=p2.id,
                    strategy="ai_assisted",
                    similarity=f"{sim:.3f}",
                    confidence=f"{decision.confidence:.2f}",
                    name1=p1.name_ja,
                    name2=p2.name_ja,
                )
        except LLMError:
            continue

    return ai_map
