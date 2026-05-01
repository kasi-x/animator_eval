# Task: credits within-source 重複削除 (668K rows)

**ID**: `20_silver_quality/02_credits_within_source_dedup`
**Priority**: 🟠
**Estimated changes**: 約 +200 / -10 lines, 2 files
**Requires senior judgment**: yes (重複定義の妥当性)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

18/01 dedup audit で検出された credits の within-source 重複 668,347 件 (12.6%) を SQL ベースで安全削除する。

重複 = 同 evidence_source 内で `(person_id, anime_id, role, episode)` がすべて同じ行。

---

## Hard constraints

- **H1**: 削除判定に `anime.score` 不使用
- **H3**: entity_resolution 不変
- **H4**: `evidence_source` 維持
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `result/audit/silver_dedup_credits.csv` 存在
- [ ] silver.duckdb backup
- [ ] `pixi run test` baseline pass

---

## 重複削除戦略

### 方針 A: 完全重複のみ削除

```sql
WITH duplicates AS (
    SELECT person_id, anime_id, role, evidence_source, episode,
           ROW_NUMBER() OVER (
               PARTITION BY person_id, anime_id, role, evidence_source, episode
               ORDER BY rowid
           ) AS rn
    FROM credits
)
DELETE FROM credits WHERE rowid IN (
    SELECT rowid FROM duplicates WHERE rn > 1
)
```

ROW_NUMBER で 1 行残し、他削除。

### 方針 B: より厳格

`raw_role` も含めて完全一致を要求 (raw_role 違えば別とする)。

→ A 推奨。raw_role 違いは role 正規化前のバラつきで、SILVER では同 role になる。

### Pre-flight check

削除対象の sample を 10 行抽出してログ出力:
```sql
SELECT * FROM duplicates WHERE rn > 1 LIMIT 10
```

人手チェック後に実行。

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/dedup/credits_within_source.py` | `dedup(conn, dry_run=False) -> dict` |
| `tests/test_etl/test_dedup_credits.py` | 合成 fixture で重複削除検証 (idempotent / source 別 / role 別) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/etl/silver_loaders/*` | 既存 ETL 不変 |

---

## Steps

### Step 1: 重複統計取得

```bash
duckdb result/silver.duckdb -c "
WITH dup AS (
    SELECT person_id, anime_id, role, evidence_source, episode,
           ROW_NUMBER() OVER (PARTITION BY person_id, anime_id, role, evidence_source, episode ORDER BY rowid) AS rn
    FROM credits
)
SELECT evidence_source, COUNT(*) FILTER (WHERE rn > 1) AS dup_to_delete
FROM dup GROUP BY 1 ORDER BY 2 DESC
"
```

source 別の削除件数を把握。

### Step 2: dedup 関数実装

`credits_within_source.py`:
```python
def dedup(conn, dry_run=False) -> dict[str, int]:
    """Returns {"before": n1, "after": n2, "deleted_per_source": {...}}"""
```

### Step 3: テスト

合成 credits 表 (重複 / 非重複混在) で動作検証。`idempotent` (2 回実行で 2 回目は 0 削除) も確認。

### Step 4: dry-run + 実行

```bash
# Dry-run
pixi run python -c "
import duckdb
from src.etl.dedup.credits_within_source import dedup
conn = duckdb.connect('result/silver.duckdb')
print(dedup(conn, dry_run=True))
"

# Backup + actual
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -c "...(dry_run=False)..."
```

### Step 5: 18/01 audit 再実行

```bash
pixi run python -c "
import duckdb
from src.etl.audit.silver_dedup import audit
from pathlib import Path
conn = duckdb.connect('result/silver.duckdb', read_only=True)
print(audit(conn, Path('result/audit')))
"
```

期待: `credits within-source dup` が 0 になっていること。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_dedup_credits.py
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM credits"  # 5,306,015 - 668,347 ≈ 4,637,668 期待
```

---

## Stop-if conditions

- [ ] credits 削除件数が 668,347 を大幅超過 (例: > 1M) → 重複定義過広、Rollback
- [ ] credits row が想定外に減少 → データ破壊、Rollback
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/dedup/credits_within_source.py tests/test_etl/test_dedup_credits.py
```

---

## Completion signal

- [ ] credits row count: 5,306,015 → ~4,637,668
- [ ] 18/01 audit 再実行で credits within-source dup = 0
- [ ] DONE: `20_silver_quality/02_credits_within_source_dedup`
