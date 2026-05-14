# Task: 査読誌投稿時の ethics statement / 自主審査

**ID**: `29_legal/02_ethics_review_for_publication`
**Priority**: 🟠
**Estimated changes**: +200 / -0 lines, 1-2 files (ethics statement template + 投稿時 checklist)
**Requires senior judgment**: yes
**Blocks**: 最初の査読誌投稿
**Blocked by**: なし (法律家 review と並行可能)

---

## Goal

Labour Economics / J. Cultural Economics / Applied Network Science 等の高位 venue は
ethics statement / data ethics declaration を要求。投稿時に block されないよう事前準備。

---

## 経路別価値

publication 経路の必須通過点。

---

## 想定論点

### IRB 相当の自主審査

- 大学 IRB は「学位論文と独立」だと適用外の可能性 → 自主審査でカバー
- 公開クレジット集約は人間対象研究 (human subjects research) 該当性 → 多くの venue では「既公開 secondary data」として扱える
- ただし person-level scoring を出版する点は注意 (de-identification していない)

### Ethics statement に書くこと

- データ source とライセンス (各 source の TOS / robots.txt 遵守状況)
- 個人特定可能性 (person-level の公開 score を伴う旨を openly 開示)
- opt-out 機構の記述
- conflict of interest (本プロジェクトはユーザー個人 / startup として運営 = COI 潜在)
- replication 方針 (`32_publication/01_replication_snapshot_exception` 連動)

### venue 別要件確認

- Labour Economics: data availability statement 強制
- J. Cultural Economics: ethics declaration 強制
- Applied Network Science: data + code 公開推奨
- RIETI DP: 内部 review のみ

---

## Steps

### Step 1: 各 venue の guidelines 確認

```
- Labour Economics → submission guidelines の "Ethics" セクション
- J. Cultural Economics → 同上
- Applied Network Science → 同上
- 日本経済学会 大会発表 → 規程確認
```

### Step 2: テンプレート作成

`docs/templates/ethics_statement.md`:

```markdown
# Ethics Statement (template)

## Data sources
[各 source とライセンス・取得方法]

## Subject identifiability
本研究は個人クレジット情報を集約し、person-level の構造的指標を出版する。
de-identification は行わない。理由: ...

## Opt-out
本プロジェクトは on-request 削除機構を提供する。詳細: [URL]

## Conflict of interest
著者は本プロジェクトを startup として運営している。本論文は ...

## Replication
[snapshot policy]
```

### Step 3: 投稿時 checklist 化

`docs/templates/submission_checklist.md` で venue ごとに ethics 要件をチェック。

---

## Pre-conditions

- [ ] STANCE.md §3 (個人情報・公開設計) 完成
- [ ] `32_publication/00_paper_strategy.md` で最初の投稿先確定

---

## Stop-if

- 主要 venue が「全 person-level data の anonymization 必須」と判定 → 戦略再考、subset anonymized 投稿 / preprint 経路に切替
