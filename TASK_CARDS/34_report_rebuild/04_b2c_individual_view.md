# Task: 個別 person view (B2C) の labor-first 設計

**ID**: `34_report_rebuild/04_b2c_individual_view`
**Priority**: 🟡
**Estimated changes**: 設計のみ (実装は `27/03` + `31/02` 連動)
**Requires senior judgment**: yes
**Blocks**: B2C launch
**Blocked by**: `27_methodology/03_iv_xai`、`31_business/02_b2c_design`、`29_legal/03_optout_mechanism`

---

## Goal

個別アニメーター本人が自分のページを見た時の体験を labor-first 設計。
「自分の構造的位置を確認 → 報酬交渉に使う」が中核 use case。

---

## 想定 UX

### 訪問者: 本人

```
1. 個人 portfolio 表示 (公開クレジット集約)
2. 構造的位置 percentile (cohort 内、CI 付き)
3. IV 5 成分の透明分解 (`27/03`)
4. 「報酬交渉用 fact sheet」PDF download ボタン
5. opt-out ボタン
```

### 訪問者: 第三者

```
1. 個人 portfolio 表示
2. 集計指標は表示するが、序列化的な順位表示はしない
3. 「この人物のクレジットを公表しているスタジオ」(プロジェクトのスタンスを反映)
4. 本人連絡先 (本人提供時のみ)
```

### 訪問者: スタジオ HR

```
labor-first スタンス: 通常閲覧と同じ第三者ビュー。HR 向け特殊機能 (採用判断 / 比較ランキング) は提供しない。
```

---

## 設計原則

- **本人が自分を確認できる** = 中核体験
- **第三者は集計のみ** = 個人を直接序列化させない
- **採用判断・人事評価支援は提供しない** (STANCE §1.2)
- **報酬交渉根拠の出力** = 主要 deliverable

---

## Pre-conditions

- [ ] `27_methodology/03_iv_xai` 完了
- [ ] `29_legal/03_optout_mechanism` 完了
- [ ] `31_business/02_b2c_design` 確定

---

## Stop-if

- 法務 review で「第三者の閲覧不可」と判定 → portfolio を本人認証下のみに縮退
