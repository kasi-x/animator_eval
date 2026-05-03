# Task: anime 拡張列 cross-source 化 (Resolved 機能の一部前倒し)

**ID**: `22_silver_coverage/05_anime_extras_cross_source`
**Priority**: 🟡
**Estimated changes**: 約 +300 / -30 lines, 4-5 files
**Requires senior judgment**: yes (cross-source ロジックは将来 Resolved 層と整合性必要)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

22/03 audit で発覚した anime 拡張列の cross-source 化:

- `anime.country_of_origin` 96.5% NULL (anilist 由来 19,915 行のみ埋まる)
- `anime.synonyms` 96.5% NULL (同上)
- `anime.description` 96.7% NULL (anilist 18,679 + mal 13,852 待機)
- `anime.external_links_json` 97.9% NULL (anilist 11,583 待機)

anime 562K のうち anilist 由来は 3.5% のみ。残りに **同 anime の anilist データを join で適用** する経路を追加。

---

## Hard constraints

- **H1**: 各列に score 系列流入禁止、display は `display_*` prefix
- **H3**: entity_resolution 不変、本タスクは **既存 cross-source mapping (`anilist_id` / `mal_id` 等)** ベースの copy のみ
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 22/03 audit 結果確認
- [ ] silver/animetor backup

---

## 設計

### cross-source mapping 経路

SILVER `anime` 表は ID prefix で source 別行 (`anilist:a123` / `mal:a123` / `bgm:s456`)。同 anime を別 source 行に紐付ける既存 mapping:

- `anime.anilist_id` 列 (各 source 行が AniList ID を持つ場合)
- `anime.mal_id` 列 (同 MAL ID)
- 既存 `meta_entity_resolution_audit` の anime merge 履歴

### copy 戦略

```sql
-- anilist 行から非 anilist 行へ拡張列 copy
UPDATE anime AS dst
SET 
    country_of_origin = src.country_of_origin,
    synonyms = src.synonyms,
    description = COALESCE(dst.description, src.description),
    external_links_json = src.external_links_json
FROM anime AS src
WHERE dst.anilist_id IS NOT NULL
  AND src.anilist_id = dst.anilist_id
  AND src.id LIKE 'anilist:a%'
  AND dst.id NOT LIKE 'anilist:a%'
  AND dst.country_of_origin IS NULL
```

各 source 行で `anilist_id` を持つもの → anilist 行から拡張列を copy。

注記: これは **5層設計の Resolved 層機能の一部** を Conformed 層内で実装する形になる。Phase 2 (Resolved 層) 完成時には、この cross-source copy ロジックを削除して Resolved 層から取得する形に移行する。

---

## Files to modify

| File | 内容 |
|------|---------|
| `src/etl/cross_source_copy/__init__.py` | 新規パッケージ init |
| `src/etl/cross_source_copy/anime_extras.py` | `copy_from_anilist(conn) -> dict` (各列の copy 件数) |
| `tests/test_etl/test_cross_source_copy.py` | 単体テスト |
| `src/etl/integrate_duckdb.py` | dispatcher で `cross_source_copy.anime_extras.copy_from_anilist()` 呼出 |

## Files to NOT touch

- `src/analysis/entity_resolution.py` (H3)

---

## Steps

### Step 1: 既存 mapping 列確認

```bash
duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE anilist_id IS NOT NULL) AS with_anilist_id,
  COUNT(*) FILTER (WHERE mal_id IS NOT NULL) AS with_mal_id,
  COUNT(*) AS total
FROM anime
"
```

cross-source mapping 列の充足度確認。低ければ本カード Stop (Resolved 層待ち)。

### Step 2: copy_from_anilist 実装

新規 `src/etl/cross_source_copy/anime_extras.py`:
```python
def copy_from_anilist(conn) -> dict[str, int]:
    """non-anilist anime rows に anilist 行から拡張列を COALESCE copy。
    Returns {"country_copied": N, "synonyms_copied": N, ...}"""
```

冪等性 (既に値ある場合は skip)。

### Step 3: テスト

合成 fixture (anilist 行 + mal 行で anilist_id 一致) で copy 動作確認。

### Step 4: 実行 + 結果確認

```bash
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -c "
import duckdb
from src.etl.cross_source_copy.anime_extras import copy_from_anilist
conn = duckdb.connect('result/silver.duckdb')
print(copy_from_anilist(conn))
"
duckdb result/silver.duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE country_of_origin IS NOT NULL) AS country_filled,
  COUNT(*) FILTER (WHERE synonyms IS NOT NULL) AS synonyms_filled,
  COUNT(*) AS total
FROM anime
"
```

期待: country / synonyms / description / external_links_json の NULL 率改善。

### Step 5: integrate_duckdb dispatcher 統合

各 silver_loader 完了後に cross_source_copy 呼出。

### Step 6: ドキュメント注記

`src/etl/cross_source_copy/__init__.py` に「これは Resolved 層 (Phase 2) で代替予定」を記載。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_cross_source_copy.py
pixi run python -m src.etl.audit.silver_column_coverage
# 期待: anime.country_of_origin 96.5% → 50% 程度に改善
```

---

## Stop-if conditions

- [ ] cross-source mapping 列が SILVER に十分入っていない (anilist_id < 30K) → 本カード意義薄、Resolved 層実装待ち
- [ ] 既存テスト破壊

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/integrate_duckdb.py
rm -rf src/etl/cross_source_copy/
rm tests/test_etl/test_cross_source_copy.py
```

---

## Completion signal

- [ ] anime.country_of_origin / synonyms / description NULL 率大幅改善
- [ ] 22/03 audit 再実行で該当列の severity 格下げ
- [ ] DONE: `22_silver_coverage/05_anime_extras_cross_source`

## 成果物保全プロトコル
完了後: commit + bundle `/tmp/agent-bundles/22-05-anime-extras.bundle` 作成、報告に bundle path + SHA。
