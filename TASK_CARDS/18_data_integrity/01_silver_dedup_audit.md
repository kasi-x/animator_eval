# Task: SILVER cross-source 重複検出 audit

**ID**: `18_data_integrity/01_silver_dedup_audit`
**Priority**: 🟠
**Estimated changes**: 約 +500 / -0 lines, 4 files
**Requires senior judgment**: yes (重複判定基準)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

credits / persons / anime / studios の cross-source 重複候補を検出し、audit レポートを生成する。**実際の merge は行わない** (H3 で entity_resolution 不変)。

---

## Hard constraints

- **H1**: 検出に `anime.score` 不使用 (構造的属性のみ)
- **H3**: entity_resolution ロジック不変、本タスクは **検出のみ**
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] SILVER 28 表 row count 確認済
- [ ] `pixi run test` baseline pass

---

## 検出戦略

### persons (重複候補)
- ID prefix 解体: `anilist:p123` / `mal:p456` / `bgm:p789` / `ann:p... ` を抽出
- 同一エンティティ候補: 同名 (name_ja exact / romaji NFKC) + 同生年 (差 ≤ 1) + 同役職分布
- 既存 `meta_entity_resolution_audit` で merge 済の組は除外
- 出力: `result/audit/silver_dedup_persons.csv` (candidate_id_a, candidate_id_b, sources, similarity, evidence)

### anime (重複候補)
- ID prefix 解体: `anilist:a` / `mal:a` / `ann:a` / `bgm:s` / `mediaarts:` 等
- 同一作品候補: title (NFKC + lowercase + 句読点除去) + 公開年 (差 ≤ 1) + 形式 (TV/OVA/Movie)
- 出力: `result/audit/silver_dedup_anime.csv`

### studios (重複候補)
- 同名 (NFKC + 略称展開) + 国別
- 出力: `result/audit/silver_dedup_studios.csv`

### credits (重複候補)
- 同 person_id × 同 anime_id × 同 role の重複行 (異なる evidence_source)
- これは実は OK (multi-source 確認用)、ただし同 source 内重複は問題
- 出力: `result/audit/silver_dedup_credits.csv` (within_source_dup_only)

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/audit/__init__.py` | 空 (package init) |
| `src/etl/audit/silver_dedup.py` | `audit(conn, output_dir)` |
| `tests/test_etl/test_silver_dedup.py` | smoke + 各検出関数 unit test |
| `result/audit/silver_dedup_summary.md` | 集計サマリ (本タスク実行で生成) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/etl/silver_loaders/*` | 既存 ETL 不変 |

---

## Steps

### Step 1: 検出関数 4 種実装

`silver_dedup.py`:
- `find_person_dup_candidates(conn) -> DataFrame`
- `find_anime_dup_candidates(conn) -> DataFrame`
- `find_studio_dup_candidates(conn) -> DataFrame`
- `find_credit_within_source_dup(conn) -> DataFrame`

### Step 2: audit() オーケストレータ

```python
def audit(conn, output_dir: Path) -> dict[str, int]:
    """Generate dedup audit reports. Returns per-table candidate counts."""
```

### Step 3: テスト

合成 fixture で各検出関数の precision 確認。

### Step 4: 実行 + レポート

```bash
pixi run python -c "
from pathlib import Path
import duckdb
from src.etl.audit.silver_dedup import audit
conn = duckdb.connect('result/silver.duckdb', read_only=True)
out = Path('result/audit')
out.mkdir(exist_ok=True)
print(audit(conn, out))
"
```

`result/audit/silver_dedup_summary.md` に件数集計 (各 SILVER 表ごと top 20 候補 + 統計)。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_dedup.py
ls result/audit/silver_dedup_*.csv
cat result/audit/silver_dedup_summary.md
```

---

## Stop-if conditions

- [ ] 候補数が異常に多い (> 50% の行が候補) → 検出基準厳格化
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm -rf src/etl/audit/silver_dedup.py tests/test_etl/test_silver_dedup.py
rm -f result/audit/silver_dedup_*.csv result/audit/silver_dedup_summary.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] レポート生成
- [ ] DONE: `18_data_integrity/01_silver_dedup_audit`
