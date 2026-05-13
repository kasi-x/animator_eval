# B2C Individual Person View Design — Labor-First UX

**Status**: Design Document (pre-implementation)  
**Target Audiences**: Individual animators, directors, production staff (本人向け)  
**Related Tasks**: `TASK_CARDS/34_report_rebuild/04_b2c_individual_view`, `27/03` (IV API), `29/03` (opt-out), `31/02` (B2C SaaS)  
**Last Updated**: 2026-05-13

---

## 0. Executive Summary

本文書は、個別アニメーター本人が自分の構造的位置を確認し、報酬交渉に活用できる B2C 個人ページの UX 設計を定める。中核価値は **「自分の協業ネットワークとスコア成分を透明に分解 → PDF として報酬交渉の根拠に活用」** にある。

**労働者寄りスタンス（STANCE.md §1）**: 本人には最大限の透明性を提供し、第三者には集計指標のみを表示。採用判断・人事評価支援機能は提供しない。

---

## 1. Audience Personas (本人たち)

個別ページの訪問者は以下 4 セグメント。各々の情報ニーズは異なる：

### 1.1 現役アニメーター（動画・作画・原画）

**Motivation**: 
- 自分がこれまで作った作品のリスト確認
- 「同じデビュー時期の人と比べて、自分はどの位置？」
- 報酬交渉時「これまでの協業規模」を示す根拠

**Key page sections**:
- クレジット履歴 (Timeline)
- コホート内ペンシル (cohort percentile with CI)
- 協業相手の可視化 (ego graph)
- PDF export ボタン

---

### 1.2 引退／転職後の元アニメーター

**Motivation**:
- キャリア棚卸し
- スキルの説得根拠化（新職への転職時、フリーランス営業時）
- 「当時のネットワークが今の価値」を数値化

**Key page sections**:
- 完全なクレジット集約
- ネットワーク成長の時間軸表示
- 最後の活動からの経過年数
- Export / SNS share ボタン

---

### 1.3 学生・志望者

**Motivation**:
- 「プロになるのに必要な協業数」「初期キャリアの進行速度」を理解
- ロールモデル探索

**Key page sections**:
- デビュー年からの時系列発展
- 初期5年のマイルストーン
- 相手の役職構成の多様性

---

### 1.4 スタジオ HR（採用担当）

**Explicit policy**: HR 向け特殊機能はない。通常の第三者ビューと同一。  
→ 「この人を採用すべき」判断を支援するツールではなく、「この人の過去の貢献可視性」を見せるのみ。

---

## 2. Information Architecture — 層構造化

個人ページは以下 5 層（下から積み上げ）で構成。上層ほど解釈が強い（REPORT_PHILOSOPHY.md）：

| 層 | 内容 | 例 | スコープ |
|----|------|-----|---------|
| **L1: 構造的事実層** | 公開クレジット集約 | 年別作品数、役職分布、スタジオリスト | 客観的事実のみ |
| **L2: ネットワーク位置層** | 協業密度・中心性 | 協業相手数、avg path length、共作相手一覧 | グラフ理論的指標 |
| **L3: スコア成分層** | IV 5 成分分解 | person_fe, birank, studio_exposure, awcc, patronage（各々 contrib%, cohort pctl） | 信頼区間付き |
| **L4: コホート比較層** | 同期比較（NOT global rank） | 「同デビュー年代・同役職」内でのペンシル | cohort relative only |
| **L5: 解釈層** | 本人解釈ガイド | 「person_fe が高い = 規模の大きい作品に繰り返し呼ばれている」 | 前置詞明示（「あなたは...」禁止） |

**階層横断ルール**:
- L1–L3 は全訪問者に表示（ただし L3 は本人認証後のみ ci_lower/upper 表示）
- L4 は本人のみ表示
- L5 は展開可能な collapsible セクション（読み飛ばし可能）

---

## 3. Page Wireframe (Text-Based, 5 Sections)

```
┌─────────────────────────────────────────────────────────────────┐
│ HEADER                                                          │
│ [Profile Image]  [Name JA] / [Name EN]  [Primary Role]        │
│ Debut Year: YYYY  Latest activity: YYYY  Total works: NNN     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SECTION 1: CREDIT PORTFOLIO (Timeline)                         │
│                                                                 │
│ FINDING: 年別作品数チャート (timeline view)                    │
│ - X軸: 年, Y軸: 작픔수                                          │
│ - hover で該当年の作品一覧展開                                 │
│ - 各作品タイルには [source badge (anilist/mal/ann)]            │
│                                                                 │
│ TABLE: 直近 20 作品リスト                                       │
│ Col: [Year] [Title] [Studio] [Role] [Episodes]               │
│                                                                 │
│ INTERPRETATION (Collapsible):                                 │
│ 「この活動パターンが示すもの」解説 (1段落)                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SECTION 2: NETWORK POSITION (Cohort Percentile + CI)          │
│                                                                 │
│ FINDING: 「あなたのデビュー年代・役職グループでのランキング」 │
│ - 指標: birank, person_fe, patronage, awcc (各々)             │
│ - 表示フォーマット:                                            │
│   [Bar chart, 0–100 percentile]                               │
│   ├─ Your position: PP th percentile                          │
│   ├─ 95% CI: [LL–UU]                                          │
│   ├─ Cohort size: NNN                                         │
│   └─ Cohort definition: Debut 20YY–20ZZ, [Role Group]        │
│                                                                 │
│ INTERPRETATION (Collapsible):                                 │
│ - 「percentile が高い = ??」(必ず前置詞明示)                 │
│ - 「CI が広い理由」 (sample size, measurement precision 言及) │
│ - 代替視点: 「このスコアは何を測定しないか」1 例挙げる       │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SECTION 3: IV TRANSPARENT DECOMPOSITION (5 Components)        │
│                                                                 │
│ FINDING: 統合スコア (IV) の 5 成分分解                        │
│                                                                 │
│ [Stacked bar / Radial chart showing 5 components]              │
│ ├─ person_fe       : X.XX  (YY.Z% contrib, PPth pctl in cohort) │
│ ├─ birank          : X.XX  (YY.Z% contrib, PPth pctl in cohort) │
│ ├─ studio_exposure : X.XX  (YY.Z% contrib, PPth pctl in cohort) │
│ ├─ awcc            : X.XX  (YY.Z% contrib, PPth pctl in cohort) │
│ └─ patronage       : X.XX  (YY.Z% contrib, PPth pctl in cohort) │
│                                                                 │
│ Dormancy multiplier: D = X.XX (last credit year: YYYY)        │
│ Final IV = Σ(λᵢ·component_i) × D = ZZ.ZZ                     │
│                                                                 │
│ [Method note button]: Click for "何を測定しているか"説明       │
│ ├─ 定義・計算式                                                │
│ ├─ 各成分が何を表すか                                         │
│ ├─ CI 計算方法                                                 │
│ └─ 既知の限界 (minimum 3 項目)                                │
│                                                                 │
│ INTERPRETATION (Collapsible):                                 │
│ 「あなたの IV 構成の特徴」解説                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SECTION 4: COLLABORATION NETWORK (Ego Graph)                   │
│                                                                 │
│ FINDING: 協業相手の可視化                                      │
│                                                                 │
│ [Interactive node-link diagram, 1–2 hops]                      │
│ - Center: あなた                                               │
│ - 1-hop: 直接協業相手 (color by role, size by shared projects) │
│ - 2-hop: 協業相手の協業相手 (薄い表示)                         │
│ - Edge weight: 共作数                                           │
│                                                                 │
│ [Table below graph]                                            │
│ Col: [Collaborator name] [Shared works] [Their IV score (link)]│
│                                                                 │
│ INTERPRETATION (Collapsible):                                 │
│ 「このネットワークの構造的意味」                              │
│ - 「協業相手の IV 分布が意味すること」                        │
│ - 「規模の大きい作品への参加 = ?」(機会 ≠ 能力)              │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SECTION 5: COMPENSATION FACT SHEET (PDF Download)             │
│                                                                 │
│ [Green button]: "報酬交渉用ファクトシート（PDF）をダウンロード" │
│                                                                 │
│ PDF Content:                                                   │
│ ├─ 名前、デビュー年                                             │
│ ├─ 過去 3 年の年別作品数、平均制作規模 (staff count)           │
│ ├─ 協業相手数（役職別）                                        │
│ ├─ 業界コホート内のペンシル（本人のみ表示）                  │
│ ├─ 「これらが何を意味するか」の 1 ページ解説                  │
│ └─ Disclaimer (両言語)                                         │
│                                                                 │
│ **含める情報**:                                               │
│ - 構造的事実のみ（協業数、役職進行、プロジェクト規模）         │
│ - 信頼区間（報酬交渉で「根拠の堅牢性」を示唆）               │
│ - 「スコアは何を測定しないか」1 段落                           │
│                                                                 │
│ **含めない情報**:                                             │
│ - Global rank（「業界内順位 Nth」）                            │
│ - 他者との直接比較ランキング                                   │
│ - Hiring recommendation                                        │
│ - 才能評価フレーミング                                         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ FOOTER                                                          │
│                                                                 │
│ [Opt-out button] "このページを削除（Opt-out）"               │
│ → Click → Confirmation → Email verification → 7 day SLA      │
│                                                                 │
│ [Disclaimer] (両言語)                                         │
│ - スコアは「能力評価」ではなく「ネットワーク指標」             │
│ - 採用・報酬判断の単一根拠として使用すべきではない           │
│ - 本プロジェクトはそのような使用の責任を負わない             │
│                                                                 │
│ [Contact] delete@animetor.example / contact form link         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. 27/03 IV Decomposition API 統合点

### 4.1 API Endpoint

`GET /api/persons/{person_id}/iv` (src/routers/persons.py:315–464)

**Response JSON structure**:

```json
{
  "person_id": "person_12345",
  "iv": 3.45,
  "cohort": "debut_2010s_animator_group",
  "cohort_size": 847,
  "percentile_in_cohort": 72,
  "components": {
    "person_fe": {
      "value": 0.82,
      "contrib_pct": 22.3,
      "cohort_pctl": 68
    },
    "birank": {
      "value": 1.12,
      "contrib_pct": 28.5,
      "cohort_pctl": 75
    },
    "studio_exposure": {
      "value": 0.56,
      "contrib_pct": 15.2,
      "cohort_pctl": 45
    },
    "awcc": {
      "value": 0.34,
      "contrib_pct": 9.8,
      "cohort_pctl": 52
    },
    "patronage": {
      "value": 0.61,
      "contrib_pct": 16.4,
      "cohort_pctl": 62
    }
  },
  "dormancy": {
    "D": 0.95,
    "last_credit_year": 2025
  },
  "shapley_fallback": false,
  "method_note": "Equal-lambda decomposition (λ=0.2 per component)...",
  "correlation_diagnostics": {
    "max_abs_r": 0.87,
    "high_corr_pairs": [...]
  },
  "metadata": {
    "disclaimer_ja": "...",
    "disclaimer_en": "...",
    "cohort_definition": "debut_decade × primary_role_group",
    "percentile_scope": "within-cohort only — not a global rank"
  }
}
```

### 4.2 UI Mapping: JSON → Component

| API field | UI component | Display rule |
|-----------|-------------|--------------|
| `components[*].value` | Stacked bar height | Proportional to contribution |
| `components[*].contrib_pct` | % label on bar | Shown inline |
| `components[*].cohort_pctl` | Small badge "P75 in cohort" | Cohort-relative only (NOT global) |
| `dormancy.D` | Multiplier badge | Shown below components |
| `dormancy.last_credit_year` | Tooltip on dormancy D | "Last: 2025" |
| `percentile_in_cohort` | Centered number above components | "You are at Pth percentile in your cohort" |
| `cohort_size` | Parenthetical note | "(cohort n=NNN)" |
| `method_note` | Expandable "Method" button | Click-to-reveal technical details |
| `correlation_diagnostics` | Tech appendix link | "See full correlation matrix (link)" |

### 4.3 Client-Side Rendering (Frontend)

**Framework**: React / Vue.js / plain JS (TBD by `31/02`)

**Key responsibilities**:
1. **Stacked bar chart** (D3.js / Plotly): 5 components + dormancy multiplier
2. **Cohort percentile bar** (0–100, user position highlighted)
3. **Interactivity**: 
   - Hover component → show full name + definition tooltip
   - Click method note → expand technical details in modal
4. **Responsive**: Mobile-optimized (column-wrap on small screens)

---

## 5. 明示的に排除する機能（Forbidden Features）

以下は **絶対に実装しない**。デザイン会議で要望が来た場合、本文書を引用して却下：

### 5.1 Global Ranking Display

- ❌ 「業界内ランク 1,234 位 / 45,678 人」
- ❌ 「Top 10% アニメーター」
- ❌ "Ranked #42 among directors"
- **理由**: STANCE.md §1.2（能力シーケンスへの転化防止）、REPORT_PHILOSOPHY.md §7（個人間優劣含意禁止）
- **代替**: コホート内ペンシル（同期比較、NOT グローバルランク）

### 5.2 Hiring Recommendation Interface

- ❌ 「この人を採用すべき」チェックボックス
- ❌ "Recommended for role X" badge
- ❌ Comparative ranking for HR use
- **理由**: STANCE.md §4.2（HR向け特殊機能提供しない）、TASK_CARDS/04_b2c_individual_view §8（labor-first スタンス）

### 5.3 Talent / Ability Framing

- ❌ "高い実力者" "優秀" "劣る" "能力がある"
- ❌ "Highly skilled" "Talented" "Competent"
- ❌ IV スコア → 「能力スコア」表示
- **理由**: CLAUDE.md Hard Rule H2、REPORT_PHILOSOPHY.md 禁則 (個人優劣含意)

### 5.4 Longitudinal Ranking Comparison

- ❌ "去年は 1,234 位 → 今年は 1,230 位に上昇"
- ❌ "Year-on-year ranking improvement"
- ❌ Rank delta chart
- **理由**: ランク化の含意、測定者の選択依存を隠蔽

### 5.5 Other Persons' Compensation Estimates

- ❌ 「○○さんの推定年収」
- ❌ Salary inference from IV score
- ❌ Pay comparison feature
- **理由**: 推定精度なし、第三者のプライバシー侵害

---

## 6. 明示的に含める機能（Included Features）

### 6.1 Cohort Percentile Display (WITH CI)

**What to show**:
```
┌──────────────────────────────────────────┐
│ あなたの位置（デビュー 2010s, 作画グループ） │
│                                           │
│ Percentile: 72th  [95% CI: 68–76]        │
│ Cohort size: 847                          │
│                                           │
│ ℹ️ 「72 位」ではなく「下位 28% より上」    │
└──────────────────────────────────────────┘
```

**CI の意味**: 測定の不確実性を透明に。報酬交渉で「根拠の堅牢性」を主張。

### 6.2 Collaboration Cluster Visualization

**What to show**:
- Ego graph (自分 + 直接協業相手 + 2-hop)
- Edge color by role (director/animator/sound/etc.)
- Node size by shared project count
- Tooltip: 「○○さんと × 件の共作」

**Label**: 「あなたの協業ネットワーク」（NOT「信頼ネット」—比喩を避ける）

### 6.3 Career Milestones + Timeline

**What to show**:
```
2015 ├─ Debut: TV series "X" (作画 3 ep)
     │
2016 ├─ First studio A
     │
2018 ├─ Joined studio B
     │
2022 ├─ Switched to freelance
     │
2025 └─ Last credit: "Z" (original dir)
```

**Label**: 「あなたのキャリアタイムライン」

### 6.4 Fact Sheet PDF Export (Compensation Use Case)

**Content**:
1. 基本情報（名前、デビュー年）
2. 過去 3 年の活動量（年別作品数、参加話数合計）
3. 協業相手統計（相手数、役職分布）
4. 制作規模分布（avg staff count、median duration）
5. コホート内ペンシル（本人認証後のみ）
6. 「この情報の意味」1 段落解説
7. Disclaimer (JA + EN)

**Design**:
- A4 縦、1.5–2 ページ
- グラフあり（年別チャート 1 図）
- スコアは「信頼区間付き」でのみ掲載
- Logo / 生成日時 / Version記載

---

## 7. Privacy & Opt-Out Integration（29/03）

### 7.1 Opt-Out Flow

```
[Page footer] "このページを削除（Opt-out）" button
   ↓
[Modal] 確認メッセージ + 理由入力フィーム
   ↓
"削除リクエストを送信"
   ↓
Email verification (確認メール) → Link click
   ↓
"削除完了。7 日以内に display から削除されます。"
   ↓
[7 day SLA]
[TASK_CARDS/29_legal/03_optout_mechanism]
```

### 7.2 Post-Deletion State

**Display layer**:
- 個人ページ: 404 "このページは削除されました"
- API `/api/persons/{id}`: 404
- Network ego graph: 相手リストから自動除外

**Mart layer**:
- 次回 pipeline 実行時、該当 person を除外して再計算
- 統計集計 (median, percentile) 自動更新

**Conformed/Source layers**:
- 現状: historical record として保持（法務確認待ち）
- Future: 法務指示で完全削除の可能性

**Audit**:
```sql
INSERT INTO meta_optout_audit (
  person_id_removed, requested_at, verified_at, 
  removed_at, requester_method, sla_met
)
```

---

## 8. Forbidden Vocab Enforcement

本ページには以下の単語を絶対に使用しない。CI linter（`scripts/report_generators/lint_vocab.py`）で enforce:

| JA | EN | Context |
|----|----|----|
| 能力 | ability | ❌ スコアが「能力」を測定しない |
| 実力 | competence | ❌ 低スコア → 実力不足 (false implication) |
| 優秀 | exceptional / superior | ❌ 高スコア → 優秀 (false implication) |
| 劣る | inferior | ❌ 低スコア → 劣る |
| 才能 | talent | ❌ スコア → 才能評価 |
| 技量 | skill | ❌ 協業密度 ≠ 技量の高さ |
| 上位 X% | top X% | ❌ グローバルランキング暗示 |
| ランキング | ranking | ❌ グローバルランク表現 |
| 育成力 | mentoring ability | ❌ 能力評価の含意 |

**替わりに使う言葉**:
- 「構造的位置」「協業密度」「ネットワーク中心性」
- 「データセット上の可視性」「協業相手数」
- 「参加作品の制作規模」「役職進行の多様性」

---

## 9. A/B Testing & Validation Plan

### 9.1 Usability Testing (Pre-launch)

- **Target**: 5–10 アニメーター（全年代カバー）
- **Test format**: 
  1. ユーザーに自分のページを見してもらう
  2. 「何がわかった？」を聞く（自由記述）
  3. 「報酬交渉に使えそう？」Yes/No
  4. 「何が不足？」

- **Success metric**: 8/10 以上が「報酬交渉の根拠に使える」と回答

### 9.2 Legal Review (前提条件)

- [ ] `29_legal/01_data_protection_review` による「第三者表示 OK」確認
- [ ] opt-out SLA 7 日確定
- [ ] Disclaimer 両言語確定

### 9.3 Disclaimer A/B Test

2 variant を用意（実装後）:
- **Variant A**: 現行（§9 参照）
- **Variant B**: 簡潔版（50 字以内）

**Metric**: 「disclaimer を読んだ」ユーザー率、理解度 survey

---

## 10. Dependency Tree

本設計の実装には以下の依存タスクが完了していることが必須：

| Task | Status | Impact |
|------|--------|--------|
| `27/03` (IV XAI decomposition) | 完了 | API endpoint が本設計の L3 を供給 |
| `29/03` (opt-out mechanism) | 待機 | Footer opt-out button 実装、7 day SLA |
| `29/01` (legal review) | 待機 | 第三者表示範囲、cohort percentile OK 確認 |
| `31/02` (B2C SaaS design) | 待機 | Frontend framework, auth, PDF export lib 決定 |
| `lint_vocab.py` update | 待機 | Forbidden vocab の linter 拡張 |

**Go/No-go criteria**:
- すべての待機タスクが「完了」に達したら `31/02` パネルで最終確認 → 実装開始
- 待機中でも「設計文書」は今この時点で fix

---

## 11. Success Metrics

### 11.1 Usage Metrics (B2C launch 後、3 ヶ月)

- DAU (Daily Active Users): 100+
- Page view rate: 1.5+ views per session
- PDF export conversion: 25%+（報酬交渉に使っている実証）

### 11.2 Qualitative Feedback

- ユーザー interview: 「IV 分解 → PDF で雇用主に提示した」事例 ≥ 3 件
- Twitter/note 言及: positive tone ≥ 70%

### 11.3 Legal / Privacy

- Opt-out request SLA 遵守率: 100%
- Data protection review で指摘なし

---

## 12. Known Unknowns & Future Revision Points

### 12.1 Stacked Bar vs. Radial Chart

本文書では「Stacked bar」を示唆したが、5 成分の視覚化は複数案あり：

- Stacked bar（累積、合計が見やすい）
- Radial / Radar chart（成分構成が直感的）
- Sunburst chart（階層強調）

最終選定は `31/02` (frontend design) で実装チーム判断。

### 12.2 Interactivity Scope

「hover で tooltip」と書いたが、実装コスト (D3.js vs. Plotly vs. custom canvas) により変更あり得る。

### 12.3 Mobile Responsiveness

本 wireframe は desktop-first。Mobile 版は `31/02` で separate design 検討。

### 12.4 Internationalization

現在 JA/EN の disclaimer のみ。Future: more languages (ZH / KO) if business expansion

---

## 13. References

- **Foundational**: `CLAUDE.md`, `STANCE.md` (labor-first framing), `REPORT_PHILOSOPHY.md` (v2 method gate)
- **API**: `src/routers/persons.py:315–464` (IV decomposition endpoint)
- **Opt-out**: `TASK_CARDS/29_legal/03_optout_mechanism.md`
- **B2C design**: `TASK_CARDS/31_business/02_b2c_design` (TBD)

---

## 改訂履歴

- **2026-05-13 (v1.0)**: 初版起草。5 層 IA、persona、wireframe、IV API 統合、forbidden features、dependency tree を明文化。本人向け transparency、HR 向け機能排除の原則を確定。

