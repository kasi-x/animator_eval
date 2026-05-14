# Task: 3 brief (Policy / HR / Business) の section 構成 labor-first 改訂

**ID**: `34_report_rebuild/02_brief_restructure`
**Priority**: 🟠
**Estimated changes**: +400 / -200 lines, 3 files
**Requires senior judgment**: yes (audience 設定)
**Blocks**: 政策アクセス / pilot 接触
**Blocked by**: `34_report_rebuild/01_audit_existing_reports`

---

## Goal

3 audience brief の構成を labor-first スタンスに合わせて改訂。
- HR brief は元々スタジオ HR 向け → labor-first スタンスでは「労働者向け B2C」または「労組 HR 部門向け」へ転換
- Business brief は元々投資家向け → 「アニメーター個人・労組」「労働者寄り顧客 (出版・労務支援)」 向けに改訂
- Policy brief は強化 (政策担当 2 ページ短縮版を別途作成、`33_policy/01`)

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **政策** | Policy brief の影響力強化 |
| **Business** | B2C 顧客向け brief (個別アニメーター / 労組) として再構成 |
| **Publication** | brief 群を replication package の一部として添付可能 |

---

## 改訂内容

### Policy brief (拡張)

**Before**: 政策担当向けだが学術寄り長文
**After**:
- 冒頭に STANCE 段落 (labor-first 明示)
- 各 finding に「労働者保護に資する根拠」「機会格差是正の根拠」を明示
- 政策推奨セクション強化 (クレジット記載ガイドライン化 / 機会格差是正補助金 / 中堅枯渇対策)
- 議連向け接続 (`33_policy/02_giin_renkei_protocol` で再利用)

### HR brief (転換)

**Before**: スタジオ HR 向け (採用判断・評価支援含む語感)
**After**: 「アニメーター個人 + 労組」向けに rebrand:
- タイトル: 「Workers Brief」(or 個人向け SaaS の核)
- section: 自分の構造的位置 / 同位置の cohort 比較 / クレジット公表率 / 報酬交渉用 fact sheet
- スタジオ HR 向け要素は削除 (採用判断・パフォーマンス評価語句)

### Business brief (転換)

**Before**: 投資家向け (過小評価人材・新興チーム発掘)
**After**: 「労働者寄り business 観察」へ転換:
- 過小評価人材 → opportunity_residual の構造観察 (採用提案ではなく機会格差検出)
- 新興チーム → 中堅育成パイプライン強度 (`26/03`) の観察
- ホワイトスペース → 人材流動性の構造観察
- 投資家向け終わりに労働者観点の caveat block 必須

---

## Files

| File | 変更 |
|------|------|
| `scripts/generate_briefs_v2.py` | brief タイトル / section 順序改訂 |
| `scripts/report_generators/briefs/<各brief>.py` | section 内容書き換え |
| `docs/REPORT_INVENTORY.md` | brief tier 表更新 |

---

## Pre-conditions

- [ ] STANCE.md 公開 (済)
- [ ] 既存 brief の audit 完了 (`34/01`)

---

## Stop-if

- 「HR brief 転換」が既存ユーザー (もしいれば) に大きな影響 → naming 変更のみ・内容は段階移行

---

## Verification

```bash
pixi run python scripts/generate_briefs_v2.py
# 出力 HTML 確認
pixi run python scripts/report_generators/lint_vocab.py result/html/*_brief.html
# 全 brief で 0 violations
```
