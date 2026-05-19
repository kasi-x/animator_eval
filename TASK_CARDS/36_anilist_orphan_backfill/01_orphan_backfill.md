# Task: AniList orphan persons backfill

**ID**: `36_anilist_orphan_backfill/01_orphan_backfill`
**Priority**: 🟡 Medium
**Estimated changes**: 約 +250 / -10 lines, 3 files (新規 1)
**Requires senior judgment**: yes (rate limit 戦略 + checkpoint 設計)
**Blocks**: `15_extension_reports/02_o1_gender_ceiling` (§15 gender 70% 達成)、`35_data_quality_backfill/01` の hometown→nationality gain 2 倍化
**Blocked by**: なし

---

## Goal

`conformed.persons` の anilist orphan ~90K (credits 由来 id-only) に Staff GraphQL fetch でメタ情報 (gender / hometown / dateOfBirth / image / yearsActive / primaryOccupations) を追加し、`§15` の gender enrichment 閾値を達成する。

---

## Hard constraints

- H1: anime.score 影響なし (本 backfill は person metadata のみ)
- H3: entity resolution 不変 (本 backfill は BRONZE 追記 + integrate 再走のみ、merge logic 触らない)
- Rate limit 遵守: AniList GraphQL は 90 req/min (実測 ~80 安全側) → checkpoint で resumable 設計必須
- 既存 staff_id (7,528 件) には触らない (`INSERT OR IGNORE` + content_hash 比較で重複回避)

---

## Background

`conformed.persons WHERE id LIKE 'anilist:%'` 内訳:

| カテゴリ | id 例 | row 数 | gender | hometown | 来源 |
|---------|------|-------:|-------:|---------:|------|
| Staff 取得済 | `anilist:s12345` | ~7,528 | 5,894 (78.3%) | 大半 | Staff query 本人ページ |
| credits orphan | `anilist:s99999` (id-only) | ~90,000 | 0 | 0 | credits の staff/voiceActor edge から自動生成 |

orphan の特徴:
- 同じ `anilist:s{id}` prefix だが metadata 列が空文字
- name_en は credits 経由で入っている可能性あり (Staff edge の `name.userPreferred`)
- 完全な詳細 (description / image / yearsActive / primaryOccupations / dateOfBirth / homeTown / gender) は Staff GraphQL 経由必須

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/anilist_scraper.py` | 新規 sub-command `orphan-backfill` 追加: orphan id list 抽出 → batch Staff fetch → BRONZE 追記 |
| `src/etl/integrate_duckdb.py` | (変更不要) re-integrate で BRONZE 追記分が conformed に流入 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parse_anilist*` | parse ロジックは既存 `parse_anilist_person` を再利用 |
| `src/etl/resolved/resolve_persons.py` | 本 backfill は conformed 更新まで、resolved 再走は別 step |

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/maintenance/backfill_anilist_orphan_persons.py` | エントリポイント。orphan id list を `conformed.persons` から SELECT → AniList Staff query batch fetch → BRONZE table=persons に追記 |
| `tests/scrapers/test_anilist_orphan_backfill.py` | smoke test (httpx mock で `get_person_details` 経路、parse 結果が BRONZE に書込まれること検証) |

---

## Steps

### Step 1: orphan id 抽出

```python
def list_orphan_ids(conn) -> list[int]:
    """conformed.persons から anilist orphan id (metadata 空) を抽出。"""
    rows = conn.execute("""
        SELECT CAST(SUBSTRING(id, LENGTH('anilist:s')+1) AS INTEGER) AS aid
        FROM conformed.persons
        WHERE id LIKE 'anilist:s%'
          AND (gender IS NULL OR gender = '')
          AND (hometown IS NULL OR hometown = '')
          AND (description IS NULL OR description = '')
        ORDER BY aid
    """).fetchall()
    return [r[0] for r in rows]
```

### Step 2: checkpoint 設計

- `data/anilist/orphan_backfill_checkpoint.json` に `{last_processed_id: int, fetched_count: int, failed_ids: [int]}`
- 1000 件単位 で flush
- `--resume` で last_processed_id から再開
- `--max-ids N` で部分実行可

### Step 3: rate limit + retry

- 既存 `client.get_person_details(id)` を流用
- `asyncio.Semaphore(1)` + interval 0.75 秒 (= 80 req/min)
- 429 / 5xx は exponential backoff、3 回 retry 後 failed_ids に追加
- BRONZE write は既存 `save_persons_batch_to_bronze()` を再利用

### Step 4: re-integrate

```bash
pixi run python -m src.etl.integrate_duckdb
```

conformed.persons の anilist 集合の gender / hometown / nationality / その他 metadata が増える。

### Step 5: tests

```python
def test_orphan_id_extraction(test_conn):
    # 既存 Staff (id=1) + orphan (id=2) を仕込み
    # list_orphan_ids が [2] のみ返すこと
    ...

def test_fetch_and_write(monkeypatch, tmp_bronze):
    # get_person_details が固定 dict を返すよう mock
    # backfill 実行で BRONZE parquet に 1 row 追記、metadata 列 populated
    ...
```

---

## Verification

```bash
# 1. unit + scrape smoke
pixi run lint
pixi run test-scoped tests/scrapers/test_anilist_orphan_backfill.py \
                     tests/unit/test_name_utils.py

# 2. dry-run (10 件のみ)
pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py --max-ids 10

# 3. BRONZE parquet 追記確認
pixi run python -c "
import duckdb
c = duckdb.connect(':memory:')
r = c.execute(\"\"\"
    SELECT COUNT(*) FROM read_parquet(
        'result/bronze/source=anilist/table=persons/date=*/*.parquet',
        hive_partitioning=true, union_by_name=true
    ) WHERE gender IS NOT NULL OR hometown IS NOT NULL
\"\"\").fetchone()
print(r)
"

# 4. 完全実行 (放置 ~16 時間、checkpoint 使用)
nohup pixi run python scripts/maintenance/backfill_anilist_orphan_persons.py \
    --resume > logs/orphan_backfill.log 2>&1 &

# 5. 再 integrate → 充足率確認
pixi run python -m src.etl.integrate_duckdb
pixi run python -c "
import duckdb
c = duckdb.connect('result/animetor.duckdb', read_only=True)
r = c.execute(\"SELECT COUNT(*) FILTER (WHERE gender IS NOT NULL AND gender != ''), COUNT(*) FROM conformed.persons WHERE id LIKE 'anilist:%'\").fetchone()
print(f'anilist gender: {r[0]}/{r[1]} = {100*r[0]/r[1]:.1f}%')
"
```

---

## Success criteria

- [ ] `data/anilist/orphan_backfill_checkpoint.json` 出力 (resumable)
- [ ] BRONZE `source=anilist/table=persons/date=YYYY-MM-DD/*.parquet` に追記 row >= 50K (10K 強 = 部分実行でも妥協可)
- [ ] re-integrate 後 `conformed.persons` anilist gender 充足率 >= 30% (現 6.0% → 大幅改善)
- [ ] §15 全体 null 率 < 70% 達成 (現 80.9% → 60-65%)
- [ ] failed_ids 比率 < 10% (rate limit / 削除済 ID 除外)

---

## Stop-if

- 429 連発 (rate limit 検出後 5 分超復帰しない) → 即停止 + interval 拡大
- AniList GraphQL schema 変更で `get_person_details` が None 連発 → schema 確認後再開
- BRONZE parquet 書込で disk 不足

---

## Rollback

```bash
# 新規 BRONZE date partition のみ削除
TODAY=$(date +%Y-%m-%d)
rm -rf result/bronze/source=anilist/table=persons/date=$TODAY/
git checkout src/scrapers/anilist_scraper.py
```

---

## 関連

- `TODO.md §12.1` (起票根拠)
- `TASK_CARDS/15_extension_reports/02_o1_gender_ceiling` (本 backfill 完了で再起動可)
- `TASK_CARDS/35_data_quality_backfill/01_nationality_backfill` (hometown 充足で nationality も連鎖増加)
- `src/scrapers/anilist_scraper.py:1560-1620` (既存 person phase loop、再利用元)
