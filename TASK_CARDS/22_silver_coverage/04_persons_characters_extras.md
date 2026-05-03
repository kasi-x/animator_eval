# Task: persons + characters missing 列の取込改善

**ID**: `22_silver_coverage/04_persons_characters_extras`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -50 lines, 6-8 files
**Requires senior judgment**: yes (列追加範囲)
**Blocks**: 15/02 O1 gender_ceiling 復活 (gender 取込次第)
**Blocked by**: なし

---

## Goal

22/03 audit で発覚した persons / characters の CRITICAL gap を取込改善:

- `persons.gender` 95.8% NULL (anilist BRONZE 5,894 + bangumi 12,646 待機)
- `persons.description` 100% NULL (anilist 4,266 + bangumi 12,056)
- `persons.image_large` / `hometown` 100% NULL
- `characters.date_of_birth` 99.2% NULL (BRONZE 1,125 件)
- `characters.gender` (現状確認要)

各 source loader の INSERT/UPDATE 文 + integrate_duckdb の SQL template に欠落列を追加。

---

## Hard constraints

- **H1**: 各列に display 系列なし (構造的属性のみ)
- **H3**: entity_resolution 不変
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 22/03 audit 結果確認: `result/audit/silver_column_coverage.md` (CRITICAL Top 5)
- [ ] silver/animetor backup
- [ ] `pixi run test` baseline pass

---

## 修正対象

### `persons` 表

| 列 | 現 NULL 率 | BRONZE 待機 | 対応 |
|----|----|----|----|
| `gender` | 95.8% | anilist 5,894 / bangumi 12,646 | `_PERSONS_SQL_TMPL` に gender 列追加 |
| `description` | 100% | anilist 4,266 / bangumi 12,056 | description 列追加 |
| `image_large` | 100% (推定) | (要調査) | image 列追加 |
| `hometown` / `country_of_origin` | 100% (推定) | anilist (`homeTown` 列) | hometown 列追加 |
| `years_active` | (要確認) | anilist | yearsActive 列追加 |
| `primary_occupations` | (要確認) | anilist | JSON 配列 |

### `characters` 表

| 列 | 現 NULL 率 | BRONZE 待機 | 対応 |
|----|----|----|----|
| `date_of_birth` | 99.2% | 1,125 | loader に列追加 |
| `gender` | (要確認) | anilist + bangumi | 同様 |
| `blood_type` | (要確認) | bangumi | 同様 |
| `description` | (要確認) | anilist + bangumi | 同様 |

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/integrate_duckdb.py` | `_PERSONS_SQL_TMPL` / `_CHARACTERS_SQL_TMPL` に欠落列追加 (anilist 由来は INSERT 時) |
| `src/etl/silver_loaders/anilist.py` | persons / characters 拡張列の UPDATE/INSERT 経路追加 |
| `src/etl/silver_loaders/bangumi.py` | gender / blood_type / birth_year 等を persons / characters UPDATE に追加 |
| `src/etl/silver_loaders/mal.py` | (該当列あれば) |
| `src/db/schema.py` | persons / characters に欠落列追加 (末尾追記) |
| `tests/test_etl/test_silver_*.py` | 各 loader 回帰テスト追加 |

## Files to NOT touch

- `src/analysis/io/silver_reader.py` (本タスクは loader 側)
- `src/analysis/entity_resolution.py` (H3)
- 23/01 Phase 1 が rename 中の files との競合は merge 段階で解決

---

## Steps

### Step 1: BRONZE 側の列確認

```bash
duckdb -c "DESCRIBE SELECT * FROM read_parquet('result/bronze/source=anilist/table=persons/date=*/*.parquet', union_by_name=true) LIMIT 0" | grep -iE "gender|description|image|hometown|year|occupation"
duckdb -c "DESCRIBE SELECT * FROM read_parquet('result/bronze/source=bangumi/table=persons/date=*/*.parquet', union_by_name=true) LIMIT 0" | grep -iE "gender|description|image|blood|birth"
```

### Step 2: schema.py に列追加 (末尾)

`persons` 拡張セクションに `gender VARCHAR / description TEXT / image_large TEXT / hometown VARCHAR / years_active TEXT` 等。

### Step 3: 各 loader の SQL 修正

- `integrate_duckdb._PERSONS_SQL_TMPL`: anilist persons INSERT 時に gender / description / image_large / hometown / years_active 追加
- `silver_loaders/bangumi.py`: persons UPDATE で gender / blood_type / birth_year (NULL-safe COALESCE)
- `silver_loaders/anilist.py`: characters extras UPDATE
- `silver_loaders/bangumi.py`: characters extras UPDATE

### Step 4: 再 ETL + 確認

```bash
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -m src.etl.integrate_duckdb
duckdb result/silver.duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE gender IS NOT NULL) AS gender_filled,
  COUNT(*) FILTER (WHERE description IS NOT NULL) AS desc_filled,
  COUNT(*) FILTER (WHERE image_large IS NOT NULL) AS image_filled,
  COUNT(*) FILTER (WHERE hometown IS NOT NULL) AS hometown_filled,
  COUNT(*) AS total
FROM persons
"
```

期待:
- gender filled: 5,894 + 12,646 = 18,540 (NULL 95.8% → 93.2% 程度に改善)
- description filled: 16,322 (100% → ~94%)

### Step 5: 22/03 audit 再実行

```bash
pixi run python -m src.etl.audit.silver_column_coverage
```

CRITICAL gap が CRITICAL から外れたこと確認。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_anilist.py tests/test_etl/test_silver_bangumi.py tests/test_etl/test_silver_mal.py
duckdb result/silver.duckdb -c "SELECT COUNT(*) FILTER (WHERE gender IS NOT NULL) FROM persons"
```

---

## Stop-if conditions

- [ ] BRONZE 側に該当列が存在しない (parser 未対応)
- [ ] 既存テスト破壊
- [ ] schema migration で既存 row に NULL fill 制約違反

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/silver_loaders/ src/etl/integrate_duckdb.py src/db/schema.py tests/test_etl/
```

---

## Completion signal

- [ ] persons.gender NULL 率 95.8% → 93%以下 (実際の数値次第)
- [ ] persons.description / image / hometown 大幅改善
- [ ] characters.date_of_birth NULL 率 99.2% → 99%以下
- [ ] 22/03 audit 再実行で CRITICAL Top 5 から該当列が外れる
- [ ] DONE: `22_silver_coverage/04_persons_characters_extras`

## 成果物保全プロトコル
完了後: commit + bundle `/tmp/agent-bundles/22-04-persons-extras.bundle` 作成、報告に bundle path + SHA。
