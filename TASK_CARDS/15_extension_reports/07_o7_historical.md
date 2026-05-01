# Task: O7 historical_credit_restoration (失われたクレジット復元)

**ID**: `15_extension_reports/07_o7_historical`
**Priority**: 🟢
**Estimated changes**: 約 +600 / -0 lines, 5 files (schema + ETL + report + claim フロー設計)
**Requires senior judgment**: yes (confidence_tier 設計 / claim フロー)
**Blocks**: なし
**Blocked by**: schema 変更 (`confidence_tier` 列追加) ユーザ承認

---

## Goal

戦前〜80 年代作品の missing credit を多源 fuzzy match で復元し、推定クレジットを別 confidence tier で SILVER に格納。文化庁・NFAJ 向け独立 brief を構築する。

---

## Hard constraints

- **H1**: `anime.score` を復元判定に使わない (構造的整合性のみ)
- **H2**: 「失われた」「不在」表現の能力暗示注意 — `lint_vocab` 拡張要 (横断タスク連携)
- **H3**: entity_resolution ロジック不変 (本カードは新規 confidence_tier 列追加のみ)
- **H4**: `credits.evidence_source` 維持、新行は `evidence_source = 'restoration_estimated'`
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] **schema 変更承認**: `credits.confidence_tier` 列追加 (HIGH / MEDIUM / LOW / RESTORED)
- [ ] BRONZE ANN / mediaarts (madb) / seesaawiki / allcinema 古作品データ存在確認
- [ ] `pixi run test` baseline pass

---

## Method 設計

### multi-source fuzzy match

戦前〜80 年代作品の credit を 4 ソース横断で抽出:
- ANN (Anime News Network)
- mediaarts (madb)
- seesaawiki (旧作品 wiki)
- 1960s-80s 業界誌スキャン (将来)

```
restoration_candidate = {
    anime_id, role, person_name_candidate,
    sources_supporting: [list of source names],
    similarity_score: float,
    cohort_year: int,
    progression_consistency: bool,  # 役職進行が時系列で整合するか
}
```

### 信頼度 tier

| Tier | 条件 |
|------|------|
| HIGH | 既存 entity_resolution 5 段階通過 (現行 SILVER) |
| MEDIUM | 2 source 以上一致 + 役職進行整合 |
| LOW | 1 source のみ + similarity > 0.85 |
| RESTORED | 推定のみ (本カード新設) |

### claim フロー

外部からの訂正受付:
- 個人 / 遺族 / 研究者からの訂正フォーム
- `meta_credit_corrections` テーブル新設 (claim_id, anime_id, person_id, corrected_field, claimer_role, evidence_url, status)
- 訂正承認は別途 review プロセス (本カード範囲外、設計のみ)

---

## Files to create

| File | 内容 |
|------|------|
| `scripts/report_generators/reports/o7_historical.py` | `O7HistoricalRestorationReport(BaseReport)` |
| `src/etl/credit_restoration/multi_source_match.py` | `find_restoration_candidates()` |
| `src/etl/credit_restoration/insert_restored.py` | `insert_restored_credits(conn, candidates, tier)` (`evidence_source = 'restoration_estimated'`) |
| `tests/reports/test_o7_historical.py` | smoke + lint_vocab + method gate |
| `docs/method_notes/o7_restoration_design.md` | confidence_tier / claim フロー仕様 |

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/db/schema.py` | `credits.confidence_tier` 列追加 + `meta_credit_corrections` テーブル新設 + Atlas migration |
| `docs/REPORT_INVENTORY.md` | 末尾に O7 エントリ追加 |
| `scripts/lint_report_vocabulary.py` | 「失われた」「不在」等の文脈条件拡張 (横断タスク連携、本カードでは検討記録のみ) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| 既存 SILVER `credits` 既存行 | RESTORED は新規行として INSERT |

---

## Steps

### Step 1: schema 変更承認 + Atlas migration

ユーザに以下確認:
- `credits.confidence_tier` 列追加 (NULL = HIGH デフォルト)
- `meta_credit_corrections` テーブル新設

承認後:
```bash
# schema.py 編集
# atlas migrate diff
# atlas migrate apply
```

### Step 2: multi_source_match 実装

戦前〜80 年代作品 (anime.year < 1990) を対象に:
- 4 source 横断で同タイトル作品を集約
- 役職別 person_name fuzzy match (rapidfuzz, threshold 0.85)
- 役職進行整合性チェック (`src/analysis/career/role_progression.py` Card 03 完了後利用)

### Step 3: insert_restored_credits

`evidence_source = 'restoration_estimated'` + `confidence_tier = 'RESTORED'` で INSERT。既存行は触らない。

### Step 4: claim フロー設計記録

`docs/method_notes/o7_restoration_design.md` に:
- claim 受付経路 (FastAPI endpoint or Google Form)
- review プロセス (誰が承認するか)
- 訂正履歴の audit trail

実装は別カード化候補。

### Step 5: レポート HTML

- 復元候補一覧 (anime_id × role × person_name × tier)
- 信頼度別 facet
- 訂正履歴 (claim フロー稼働後)
- cohort × source 復元数 sankey

### Step 6: 文化庁 / NFAJ 向け brief

新 audience brief 採否は `x_cross_cutting` で決定。当面は Technical Appendix に格納。

### Step 7: テスト

- 合成 fixture で fuzzy match 動作確認
- `evidence_source = 'restoration_estimated'` 行が既存 SILVER credits を破壊していないこと
- lint_vocab 通過

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. テスト
pixi run test-scoped tests/reports/test_o7_historical.py

# 3. schema 検証
pixi run python -c "
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute('SELECT confidence_tier, COUNT(*) FROM credits GROUP BY 1').fetchall())
"

# 4. invariant
rg 'anime\.score\b' scripts/report_generators/reports/o7_historical.py   # 0 件
rg 'evidence_source' src/etl/credit_restoration/insert_restored.py   # 必ず restoration_estimated
```

---

## Stop-if conditions

- [ ] schema 変更承認得られず → 本カード Stop
- [ ] fuzzy match で false positive 過多 (sample レビューで > 20%) → threshold 引上 / 手動レビュー必須化
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout src/db/schema.py docs/REPORT_INVENTORY.md
rm -rf src/etl/credit_restoration/
rm scripts/report_generators/reports/o7_historical.py
rm tests/reports/test_o7_historical.py
rm docs/method_notes/o7_restoration_design.md
# Atlas migration のロールバックは別途
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] schema migration 適用済
- [ ] DONE: `15_extension_reports/07_o7_historical`

---

## ステークホルダー / 参考文献

- ステークホルダー: 文化庁文化財第二課、NFAJ (国立映画アーカイブ)、国会図書館
- 参考: 米国議会図書館 National Film Registry の credit 復元手法、Library of Congress credit attestation 基準
