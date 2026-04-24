# Task: bangumi persons jsonlines → BRONZE parquet (relation 参照 id のみ)

**ID**: `08_bangumi_scraper/04_person_detail`
**Priority**: 🟠
**Estimated changes**: 約 +120 lines, 1 file 新規
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `03_subject_relations`

---

## Goal

`data/bangumi/dump/latest/person.jsonlines` → `src_bangumi_persons` parquet。ただし anime subject に一切登場しない person は除外 (`subject_persons.person_id` + `person_characters.person_id` の和集合で filter)。

---

## Hard constraints

- H3 entity resolution 不変 (AniList/MAL/ANN 側への統合は別タスク)
- infobox (list-of-dict with 中文 key) は **raw JSON 文字列** で保存、展開しない
- 死亡/誕生日等の PII 相当 column も raw 保存 (bangumi 公開情報なので法的問題なし、ただし report 層で露出は `REPORT_PHILOSOPHY.md` 従う)

---

## Pre-conditions

- [ ] `03_subject_relations` 完了
- [ ] `result/bronze/source=bangumi/table=subject_persons/**/*.parquet` 存在
- [ ] `result/bronze/source=bangumi/table=person_characters/**/*.parquet` 存在

---

## Step 0: person jsonlines 構造確認

```bash
head -1 data/bangumi/dump/latest/person.jsonlines | python -m json.tool
# 期待 key: id, name, type (1=個人,2=公司,3=組合), career, infobox, summary, img, last_modified, ...
```

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/migrate_bangumi_persons_to_parquet.py` | filter + parquet 変換 |

---

## Steps

### Step 1: 参照 person_id 集合取得

```python
import duckdb
con = duckdb.connect()
referenced_ids = {r[0] for r in con.execute("""
    SELECT DISTINCT person_id FROM read_parquet('result/bronze/source=bangumi/table=subject_persons/**/*.parquet')
    UNION
    SELECT DISTINCT person_id FROM read_parquet('result/bronze/source=bangumi/table=person_characters/**/*.parquet')
""").fetchall()}
```

### Step 2: jsonlines stream filter → parquet

- infobox / career は json.dumps して string column に
- type=2/3 (会社/組合) も保存 OK。filter は referenced_ids のみ
- img URL はそのまま保存 (DL はしない、後タスク)

### Step 3: 出力

```
result/bronze/source=bangumi/table=persons/date=<release>/part-0.parquet
```

---

## Verification

```bash
pixi run python scripts/migrate_bangumi_persons_to_parquet.py

pixi run python -c "
import duckdb
con = duckdb.connect()
n_all = con.execute(\"SELECT count(*) FROM read_parquet('data/bangumi/dump/latest/person.jsonlines')\").fetchone()[0] if False else None
# jsonlines 直読は不可、wc -l で代替
"
wc -l data/bangumi/dump/latest/person.jsonlines
pixi run python -c "
import duckdb
con = duckdb.connect()
n = con.execute(\"SELECT count(*) FROM read_parquet('result/bronze/source=bangumi/table=persons/**/*.parquet')\").fetchone()[0]
print('filtered persons:', n)
# referenced_ids 集合サイズと一致すること
"

pixi run lint
```

---

## Stop-if conditions

- [ ] filtered 行数が referenced_ids 集合サイズより大きい → filter 誤作動
- [ ] filtered 行数が 5000 未満 → 集合取得 SQL 誤り
- [ ] infobox 保存で parquet schema エラー → json.dumps 漏れ

---

## Rollback

```bash
git checkout scripts/
rm -rf result/bronze/source=bangumi/table=persons/date=<今回>/
```

---

## Completion signal

- [ ] referenced_ids = parquet row count
- [ ] DuckDB から read_parquet 可能
- [ ] DONE 記録
