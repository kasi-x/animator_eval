# Task: 拡張目的レポート群 横断タスク

**ID**: `15_extension_reports/x_cross_cutting`
**Priority**: 🟡
**Estimated changes**: 約 +200 / -50 lines, 4 files
**Requires senior judgment**: yes (audience 設計判断)
**Blocks**: 15_extension_reports/06_o8 / 07_o7 / 08_o5 (新 audience brief 採否)
**Blocked by**: なし (個別 O カード進行と並行可)

---

## Goal

O1-O8 の brief マッピング確定、新 audience brief 採否、lint_vocab 拡張、method_note template 拡張を一括で処理する。各 O レポートカードと並行進行可。

---

## Hard constraints

- **H2**: lint_vocab 拡張時の禁則語追加は既存テスト破壊しない (誤検出回避)
- **H5**: 既存テスト破壊禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `pixi run test` baseline pass
- [ ] `docs/REPORT_INVENTORY.md` 既存マッピング把握
- [ ] `docs/REPORT_PHILOSOPHY.md` audience 設計原則把握

---

## サブタスク

### X1. brief マッピング確定 (`docs/REPORT_INVENTORY.md`)

各 O レポートの brief 帰属を確定:

| O | レポート | 第一 brief | セカンダリ |
|---|---------|----------|----------|
| O1 | gender_ceiling | Policy | HR |
| O2 | mid_management | HR | Policy |
| O3 | ip_dependency | Business | Policy |
| O4 | foreign_talent | Policy | Business |
| O5 | education_outcome | (新 audience: 教育機関?) | Policy |
| O6 | cross_border | Business | Policy |
| O7 | historical_restoration | (新 audience: 文化財?) | Technical Appendix |
| O8 | soft_power | (新 audience: クールジャパン?) | Business |

`docs/REPORT_INVENTORY.md` に確定マッピング表を追加。

### X2. 新 audience brief 採否

候補:
- 教育機関 (O5)
- 文化財 (O7)
- クールジャパン (O8)

判断基準:
- 既存 3 brief (Policy / HR / Business) に収まらない構造的差異があるか
- ステークホルダー特化用語・表現の必要性
- 維持コスト (ステークホルダーごとに lint vocabulary 個別管理など)

**推奨**: 当面は Technical Appendix + 既存 brief への section 追加で運用。新 audience brief は最低 3 レポート群が確定してから新設 (12 ヶ月後 review)。

`docs/REPORT_PHILOSOPHY.md` に判断記録を追加。

### X3. lint_vocab 拡張

O7 (失われたクレジット復元) で「失われた」「不在」等の表現が能力暗示に近接する可能性。

検討対象 (文脈条件付検出):
- 「失われた」 + 「人材」 → flag
- 「不在」 + 「能力」 → flag
- 「埋もれた」 + 「才能」 → flag
- 「眠っている」 + 「実力」 → flag

実装: `scripts/lint_report_vocabulary.py` に文脈条件パターン追加 (2-gram 検出)。既存テストへの影響を全 O カードで検証。

### X4. method_note template 拡張

`section_builder.method_note_from_lineage()` の手法バリエーション拡張:

新規対応必要な手法:
- Cox 回帰 (O1)
- Mann-Whitney U (O1, O3, O4)
- Kaplan-Meier (O2, O4)
- counterfactual + bootstrap CI (O3)
- community detection / louvain (O6)
- propensity score matching / IPW (O5)
- DID (O1, O7)
- weighted PageRank (O6)

各手法ごとに method note の書式 (前提 / 仮定 / 結果解釈の標準フォーマット) を template 化。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `docs/REPORT_INVENTORY.md` | brief マッピング表追加 |
| `docs/REPORT_PHILOSOPHY.md` | 新 audience brief 判断記録追加 |
| `scripts/lint_report_vocabulary.py` | 文脈条件付禁則パターン追加 |
| `scripts/report_generators/section_builder.py` | method_note_from_lineage() バリエーション拡張 |

## Files to NOT touch

| File | 理由 |
|------|------|
| 各 O カード固有レポート (`o*.py`) | 横断タスクは O カード本体不変 |

---

## Steps

### Step 1: brief マッピング確定

- 上記マッピング表を `docs/REPORT_INVENTORY.md` 末尾に追加
- 既存レポートの brief 帰属とも整合確認

### Step 2: 新 audience 採否判断

- `docs/REPORT_PHILOSOPHY.md` に判断記録 (採否、保留期間、review 条件)

### Step 3: lint_vocab 拡張

- 2-gram 文脈条件パターンを `LINT_PATTERNS` に追加
- 既存全レポートに対し誤検出ゼロ確認

```bash
pixi run python scripts/lint_report_vocabulary.py --files scripts/report_generators/reports/
```

### Step 4: method_note template 拡張

- `section_builder.py` に手法 → 標準テンプレ map を追加
- 各 O カードからの呼出しを想定して引数設計

### Step 5: 全 O カードへの影響テスト

- 既存テスト全 pass 維持
- O カード未実装でも横断タスクは独立完了可能

---

## Verification

```bash
# 1. lint
pixi run lint
pixi run python scripts/lint_report_vocabulary.py

# 2. 既存テスト
pixi run test-scoped tests/test_lint_vocab.py tests/test_section_builder.py

# 3. ドキュメント整合
grep -A 20 "brief マッピング" docs/REPORT_INVENTORY.md
grep -A 10 "新 audience" docs/REPORT_PHILOSOPHY.md
```

---

## Stop-if conditions

- [ ] lint_vocab 拡張で既存レポート誤検出 → パターン緩和 / 文脈条件強化
- [ ] method_note template の引数設計が既存呼出しを破壊
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout docs/REPORT_INVENTORY.md docs/REPORT_PHILOSOPHY.md \
             scripts/lint_report_vocabulary.py scripts/report_generators/section_builder.py
```

---

## Completion signal

- [ ] X1-X4 全 サブタスク pass
- [ ] DONE: `15_extension_reports/x_cross_cutting`

---

## 関連

- `15_extension_reports/06_o8_soft_power`: 新 audience (クールジャパン) 採否
- `15_extension_reports/07_o7_historical`: 新 audience (文化財) 採否、lint_vocab 拡張
- `15_extension_reports/08_o5_education`: 新 audience (教育機関) 採否
