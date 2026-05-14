# Task: スタートアップ形態の整理 (法人形態 / co-founder / IP)

**ID**: `31_business/01_startup_form`
**Priority**: 🟠
**Estimated changes**: doc-only
**Requires senior judgment**: yes (税務 / 法務)
**Blocks**: funding_plan / B2C SaaS launch
**Blocked by**: なし

---

## Goal

ユーザーが本プロジェクトを startup として運営している実態を整理し、
法人形態・co-founder 構造・IP 所有関係を明文化。
publication / business / 政策 経路と整合する形に。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | funding 交渉時の前提整理 |
| **Publication** | conflict of interest declaration の正確性 (`29_legal/02_ethics_review`) |
| **政策** | 政策担当者への自プロジェクト説明時の整合 |

---

## 確認すべき項目 (ユーザーへのヒアリング)

### 法人形態

- 既に法人化済? (合同会社 / 株式会社 / 一般社団 / 個人事業 / 任意団体)
- 法人化済なら設立日・登記事項
- 未だなら、いつ・どの形態で法人化予定か

### Co-founder / 従業員

- co-founder あり? いるなら役割分担
- 従業員 / 業務委託あり?
- 本プロジェクトの code / data の貢献者は誰か

### IP 所有関係

- 本プロジェクトの code / data は誰の所有か (ユーザー個人 / 法人 / 大学)
- 大学側 IP rules との関係 (院生在籍中の発明・成果の帰属)
- publication の著作権は誰に帰属するか
- OSS 化する場合のライセンス決定権

### 本業との関係

- ユーザーの本業 (社会人) と本プロジェクトの利害相反確認
- 本業側の副業規定確認
- 本業時間 / 本プロジェクト時間の境界

### 本プロジェクトの位置付け

- startup の主力 product か、複数 work の一つか
- 他 product / business との resource 共有

---

## Steps

### Step 1: ユーザー回答ヒアリング

上記項目を埋めるヒアリング (フォーム化推奨)。
本人答えにくい論点 (IP / 本業) は別 session で。

### Step 2: 整理 doc 作成

`docs/startup_context.md` (機密度高、git 管理外推奨 / private repo) で整理。

```markdown
# Startup Context

## Legal entity
- 形態: ...
- 設立: ...

## Founders / team
- ユーザー: ...
- co-founder: ...

## IP ownership
- code: ...
- data: ...
- publication: ...

## 本業との関係
- 副業規定: OK / NG / 要承認
- 利害相反: なし / あり (詳細)
```

### Step 3: ethics statement / STANCE 更新

`29_legal/02_ethics_review_for_publication.md` の COI 章に反映。
`STANCE.md §7` を整理結果に基づき改訂。

---

## Pre-conditions

- [ ] STANCE.md §7 ドラフト完成 (済)
- [ ] ユーザーのヒアリング枠確保

---

## Stop-if

- 大学 IP rules で本プロジェクト成果が大学帰属 → publication / business 戦略全面再考必要

---

## 機密性注意

本タスクで生成される `docs/startup_context.md` は機密度高。
公開リポジトリにコミットしない。`.gitignore` 追加または private repo 化。
