# Task: BRONZE→SILVER LOW coverage 13 表の修正

**ID**: `19_silver_postprocess/02_low_coverage_fix`
**Priority**: 🟠
**Estimated changes**: 約 +300 / -50 lines, 4-6 files
**Requires senior judgment**: yes (date partition dedup 戦略)
**Blocks**: なし
**Blocked by**: 18/02 silver_completeness 完了済

---

## Goal

`result/audit/silver_completeness.md` で LOW (<50%) 判定された 13 表 (anilist credits/characters/studios 等) のロード戦略を見直し、PARTIAL (≥50%) 以上に改善する。

---

## Hard constraints

- **H1**: scoring 経路に score / popularity 流入禁止
- **H3**: entity_resolution ロジック不変
- **H4**: `evidence_source` 維持
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `result/audit/silver_completeness.md` 存在 (18/02 で生成済)
- [ ] LOW 判定 13 表のリスト把握
- [ ] `pixi run test` baseline pass

---

## 設計

### LOW 判定の典型パターン (18/02 報告)

> anilist credits/characters/studios show sub-50% — due to dedup across date partitions

つまり:
- BRONZE は **date partition** (`/date=2026-04-25/` 等) で複数日 snapshot を保持
- SILVER 側で `ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC)` で最新行のみ取込
- → BRONZE row count = 全 snapshot の合計、SILVER row count = 最新 snapshot のみ → coverage 50% 未満

これは **正常**な動作。ただし `silver_completeness` 計測側が修正要:

#### Option A: 計測修正

`silver_completeness.py` で BRONZE row count に「最新 partition のみ」or「distinct id のみ」を使う。

#### Option B: SILVER ロード戦略変更

存在しないなら適用しない。Option A が筋。

### LOW の他の原因

partition 関係なく LOW なら:
- BRONZE 列が SILVER 側で dropped
- ID マッピング不一致
- type filter (例: bangumi の type=2 anime のみ)
- NULL ID 行スキップ

### 13 表ごと診断

各表で:
1. `silver_completeness.py` の coverage 計算ロジックが partition 重複考慮しているか
2. 重複考慮しても LOW なら、loader 側で取込率向上

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/audit/silver_completeness.py` | partition-aware coverage 計算 (distinct id ベース) |
| `tests/test_etl/test_silver_completeness.py` | 新ロジック回帰テスト |
| `src/etl/silver_loaders/<source>.py` | 必要なら個別 loader 改修 (LOW 残った表のみ) |
| `result/audit/silver_completeness.md` | 再計測結果 |

## Files to NOT touch

| File | 理由 |
|------|------|
| BRONZE parquet | 改変禁止 |
| `src/analysis/entity_resolution.py` | H3 |

---

## Steps

### Step 1: LOW 13 表の正確なリスト取得

```bash
grep -A 20 "LOW" result/audit/silver_completeness.md
```

### Step 2: partition-aware coverage 計算修正

`silver_completeness.py`:
- BRONZE 側 row count を `COUNT(DISTINCT id)` に変更 (date partition snapshot 重複排除)
- 既存 `COUNT(*)` モードは `--include-snapshots` フラグで残す

### Step 3: 再計測

```bash
pixi run python -m src.etl.audit.silver_completeness
```

LOW → PARTIAL/OK に改善した表数確認。

### Step 4: 残 LOW の個別調査

partition 修正で改善しない表のみ、loader 側を調査。原因に応じて修正:
- type filter で意図的に絞っている → 説明追記 (LOW でなく FILTERED 区分)
- NULL ID → そのまま skip OK
- ID 不一致 → loader バグ修正

### Step 5: 改善後再計測 + 報告

`result/audit/silver_completeness.md` 更新:
- 修正前/修正後の coverage 比較
- 残った LOW (説明付き)

### Step 6: 回帰テスト

`tests/test_etl/test_silver_completeness.py` に partition-aware テスト追加。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_completeness.py
pixi run python -m src.etl.audit.silver_completeness
grep -c "LOW" result/audit/silver_completeness.md  # 修正前 13 → 修正後 < 13 期待
```

---

## Stop-if conditions

- [ ] `silver_completeness.md` 不在 → 18/02 未完了、本カード Stop
- [ ] 計測ロジック修正で他のテスト破壊
- [ ] LOW 残数 が削減ゼロ (= 全部別原因) → 個別調査が長時間化、別カード化推奨

---

## Rollback

```bash
git checkout src/etl/audit/silver_completeness.py tests/test_etl/test_silver_completeness.py src/etl/silver_loaders/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] LOW 13 → < 5 (改善実績)
- [ ] `silver_completeness.md` 更新
- [ ] DONE: `19_silver_postprocess/02_low_coverage_fix`
