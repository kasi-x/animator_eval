# Template: Tier C (Monthly Long-Form Article — note Primary)

**Purpose**: Establish thought leadership. Create persistent, searchable reference for journalists, policymakers, and researchers. Deeper analysis than Tier A/B with explicit interpretation section.

**Time to produce**: 60–90 minutes (with figures + citations)  
**Platform**: note (primary), X (thread link), LinkedIn (monthly digest)  
**Frequency**: 1 per month  
**Length**: 1,500–3,000 characters (~500–1,200 words in English equivalent)  

---

## Structure (5 Sections)

| Section | Length | Purpose |
|---------|--------|---------|
| **Title + Subtitle** | 1 line | Specific phenomenon + data-backed framing |
| **Problem** | 50–100 chars | Why this phenomenon matters; scope; audience |
| **Evidence** | 800–1,500 chars | 3–4 findings with 1–2 figures, data cites |
| **Interpretation** | 400–600 chars | Explicitly labeled first-person reading; mechanisms; caveats |
| **What Next?** | 200–300 chars | 3–4 possible responses (no recommendation) |
| **Sources & Notes** | 300–500 chars | Full citations, methodology, limitations, opt-in to feedback |

**Total**: 1,500–3,000 characters

---

## Blank Template

```markdown
# [TITLE: Specific phenomenon + Data-backed claim]

## Problem

[2–3 sentences describing the phenomenon, why it matters, and to whom.]

[Example: "Historically, animators worked with 5–8 different studios per year. 
That number dropped to 2–3. Why? And what does it mean for careers?"]

## Evidence

[Lead with chart/figure 1. Annotate with finding 1.]

### Finding 1: [Narrow label]

[1–2 sentences with data point(s). Include N, CI, time window.]

[Example: "In our network analysis of 450 TV anime (2015–2025), 
studio co-credit partners declined from avg 5.2 to 3.1 per animator 
(95% CI: 2.8–3.4). This represents a 40% contraction."]

### Finding 2: [Narrow label]

[Chart 2. 1–2 sentences with data point.]

[Example: "Production budgets fell 18% over the same period (deflation-adjusted). 
This timing correlation is suggestive but not causal."]

### Finding 3: [Narrow label]

[Chart 3 (optional). 1–2 sentences.]

[Example: "Key frame animators show a 35% drop in studio diversity, 
while In-between animators show 25%. Role-specific effect."]

## Interpretation: [Labeled Phenomenon] & Labor Consequences

**I interpret this as follows** [first-person marker]:

[1–2 paragraphs of structured interpretation]

- Mechanism A: [Explanation]. Evidence: [cite finding X].
- Mechanism B: [Explanation]. Evidence: [cite finding Y].
- Mechanism C: [Alternative]. Evidence: [weaker or correlational].

**Caveats**: These are observational findings. We cannot claim causation without 
[named method: experiment / natural experiment / IV / RDD / event study].

**Labor consequences** [if applicable]:
- For early-career animators: [implication]
- For mid-career: [implication]
- For management/studios: [implication]

[Example: "I interpret the co-credit collapse as production budget pressure. 
With tighter budgets, studios hire fewer people per project, forcing rosters 
to shrink. Causation? Hard to claim without experiment. But we observe: 
(a) budget drop timing aligns with partnership drop, (b) projects with lower 
budgets show stronger consolidation, (c) full-time staff unaffected, only 
on-call collaborators. This is consistent with budget constraint, not 
animator preference."]

## What Next?

[3–4 possible responses, framed neutrally]

**Studios could**: [Response A] because [mechanism]. This might [potential outcome].

**Labor unions might**: [Response B] by [tactic]. This might [outcome].

**Policymakers could**: [Response C] via [mechanism]. This might [outcome].

[Example: "Studios could invest in crew training/mentorship to offset 
consolidation. Unions might negotiate minimum team size clauses. 
Policymakers could mandate public credit disclosure to increase transparency. 
We take no position."]

## Sources, Methods, Caveats

### Data & Sources
- Source A: [full citation]
- Source B: [full citation]
- Entity resolution method: [brief + link to methodology]

### What This Analysis Does & Doesn't Do
- **Does**: Observe co-credit network density changes over time
- **Does not**: Claim causation; attribute to individual behavior; explain causal direction

### Sensitivity
[1–2 key assumptions. What would change if we changed them?]

[Example: "If we exclude TV recap episodes, the partnership drop is 36% 
(vs 40% with recaps included). If we use studio-weighted edges instead 
of individual edges, gini rises to 0.74 (vs 0.72). These shifts don't 
change our interpretation."]

### Feedback & Transparency
Questions, corrections, or alternative readings? Reach out: 
[contact email or form]. We prioritize animator, union, and studio feedback.

---

## Filled-In Example 1: Studio Consolidation & Labor Impact

```markdown
# アニメ制作の『共クレジット関係』はなぜ縮小しているか — 
4 年の業界ネットワーク解析と労働市場への波及

## Problem

アニメ制作の現場では、ひとりのアニメーターが異なるスタジオと協業する機会が急速に減少している。
2015 年の平均は年間 5.2 スタジオだったが、2025 年には 3.1 スタジオへと 40% の縮小を記録している。
この変化は、個人の選択ではなく、業界の構造的な制約の反映かもしれない。

## Evidence

[**チャート 1**: スタジオ共クレジット関係数の推移]

### 発見 1: ネットワーク密度の 40% 縮小

TV アニメ 450 作品（2015–2025 年）の分析から、アニメーターの平均共クレジット相手スタジオ数は 
5.2 スタジオ/年から 3.1 スタジオ/年へと低下した（95% CI: 2.8–3.4）。
この傾向は全役職に共通しており、特定の集団の離職ではなく、業界全体の構造的変化を示唆している。

### 発見 2: 制作予算との時系列相関

同期間における TV アニメの制作予算（デフレータ調整済）は 18% 低下している。
共クレジット関係の縮小時期と予算削減時期がほぼ一致している。
ただし相関は因果関係を示さない。

[**チャート 2**: 制作予算と共クレジット関係数の並行推移]

### 発見 3: 役職別の差異

主要動画師と原画師で分析すると、原画師の共クレジット多様性の低下（35%）が主要動画師（25%）を上回っている。
この役職別パターンは、単なる業界縮小ではなく、制作工程の構造的な変化を示唆している。

## Interpretation: 制作予算圧縮と共クレジット関係の連鎖

**私の解釈は次の通りである**：

制作予算の縮小が、スタジオの雇用可能クルーサイズを直接制限し、結果として 
「その他」協業相手の削減につながっている。以下の観察がこの読み方を支持する：

- **メカニズム A（予算制約）**: 予算当たりのスタッフ数が固定的であれば、予算削減は必然的にクルーを縮小させる。
  我々の観察：フルタイム常勤者数は不変（むしろ増加傾向）だが、 
  アドホック協力者（freelance / 短期契約）が 38% 削減されている。

- **メカニズム B（スケジュール集約）**: 配信サービス向け納期短縮化により、 
  新規スタジオとの協業調整コストが相対的に上昇した可能性。

- **メカニズム C（スタジオ大型化）**: 大手スタジオ内部の人員充足化に伴い、 
  外部アニメーターの必要性が減少。ただし、我々の観察ではこれは二次的。

**注釈**：これらの観察は因果性を主張しない。確定的な因果特定には、 
自然実験（自然災害による予算衝撃など）または企業レベルパネルデータが必要。

**労働市場への波及**：

- **初期キャリア段階**: 複数スタジオでの経験取得機会喪失。師匠選択の自由度低下。
- **中堅段階**: ポートフォリオ多様化の困難。スタジオ依存度上昇。
- **スタジオ経営層**: 雇用交渉力の相対優位性向上。労働者の乗り換えコスト増加。

## What Next?

**スタジオは**: 制作工程の内製化を進める一方、意識的にアニメーターの外部機会を 
保証する制度設計（メンター配置、外部プロジェクト推奨枠など）を導入できる。
結果：初期キャリア支援の強化、人員の長期維持。

**労働組合は**: 団体交渉で「外部協業最小数条項」を提案できる。例：年間 N スタジオ以上との協業。
結果：労働者の選択肢保全、スタジオ間の人材流動性維持。

**政策担当者は**: 制作予算配分ガイドラインで「クレジット公開率」と「多様性」を 
指標化し、公助対象化できる。結果：業界透明性向上、労働標準化の基盤。

---我々は立場を示さない。ただし分配帰結を示す---

## Sources, Methods, Caveats

### Data & Sources
- AniList, MAL, ANN の entity resolution 統合データベース  
  （entity resolution 精度: 94% cross-validation）
- 制作予算: RIETI アニメーション産業調査（公開版）

### Analysis Scope
- **含むもの**: 公開クレジット記録、共クレジット関係、制作スタジオ属性
- **含まないもの**: 視聴者評価、個人の職務能力評価、給与

### 感度分析
予算指標を異なる deflator で調整した場合（CPI vs. 業界物価指数）、 
共クレジット縮小の時期は ±6 ヶ月ずれるが、全体傾向は不変（36–42% 低下幅）。
役職別分析では、原画師が支配的。

### Transparency & Feedback
データまたは解釈に誤りを発見した場合、お知らせください。  
メール：[contact] / フォーム：[form link]  
アニメーター、労組、スタジオ経営からのフィードバック を優先対応します。

---
```

---

## Filled-In Example 2: Gender & Early-Career Network

```markdown
# 『女性アニメーター』の共クレジット多様性が落ちている — 
役職分離と初期キャリア機会格差の構造的観察

## Problem

近年、初期キャリア段階の女性アニメーターが、同じキャリア段階の男性よりも 
少ない数のスタジオと協業する傾向が顕著になっている。
これは個人の選択か、それとも制度的障壁か。

## Evidence

[**チャート 1**: 初期キャリア層（デビュー 2015–2020）の共クレジット多様性（男女別）]

### 発見 1: 女性は平均 3.0 スタジオ、男性は 4.1 スタジオ

初期キャリア層（デビュー 2015–2020、活動期間 2015–2025）の分析から：

- 女性アニメーター: 年間平均 3.0 スタジオ （95% CI: 2.7–3.3, N=278）
- 男性アニメーター: 年間平均 4.1 スタジオ （95% CI: 3.9–4.3, N=612）

差は統計有意（p < 0.01）。ただし、年齢・経験年数で回帰調整すると、 
差は 26% から 18% へと縮小する。

### 発見 2: 役職分布の性別非対称性

女性はアニメーターの 64% が「動画」（In-between）、36% が「原画」（Key frame）。
男性は「動画」40%, 「原画」50%。 

この役職分離が共クレジット多様性の差の 60% を説明する （分散分解）。

[**チャート 2**: 役職別の共クレジット多様性（男女別）]

### 発見 3: 役職分離は「選択」か「割当」か

女性のうち、「原画志向だが動画割当」と申告した比率は 31%。
男性の同比率は 8%。 

ただし、志向と割当の因果方向は観察データからは不明。

## Interpretation: 構造的分離と初期キャリア機会

**私の解釈は次の通りである**：

役職分離は、個人の能力差ではなく、制度的・市場的な分離メカニズムの 
反映と考える。根拠：

- **メカニズム A（採用時選別）**: 動画職は短期契約・entry-level として位置づけられ、 
  採用面で女性比率が高く固定化。男性も女性も、雇用機会の構造的偏在に直面。

- **メカニズム B（経験蓄積の差）**: 動画職では協業相手がスタジオ内部に限定されやすく、 
  外部ネットワーク形成機会が男性より少ない。年数が経っても多様性が伸びない傾向。

- **メカニズム C（本人選択）**: 働き方や家庭事情を理由に、 
  安定性の高い長期契約（一スタジオ専属）を女性が多く選ぶ可能性。 
  ただし、この「選択」自体が限定的な市場機会の中での選択であることに注意。

**結論ではなく開いた問い**: 因果特定には、採用決定時点でのランダム割当、 
または自然実験（新規スタジオ立ち上げなど）の機会が必要。

**労働市場への波及**：

- **共クレジット多様性 → キャリア流動性**: 共クレジット多様性が低いと、 
  単一スタジオ依存度が高まり、離職時の転職先選択肢が狭まる傾向。

- **初期キャリア機会喪失**: 複数スタジオでの経験が初期段階で制限されると、 
  中期キャリアでの「専門選択」の自由度が低下。

- **賃金格差への機構**: 動画職と原画職の給与差（原画 +30–50%）と 
  役職分離が相乗すれば、初期キャリア女性の生涯賃金圧力となる。

## What Next?

**スタジオは**: 採用プロセスの透明化と、「動画→原画」キャリアパスの 
明示的設計を導入できる。例：2 年の動画経験後、希望者は原画研修を受講可能。
結果：初期キャリア女性のキャリアパス拡張。

**労働組合は**: 団体交渉で「役職転換機会保障」と 
「長期多様性指標」（スタジオごとの動画/原画比率目標）を提案できる。

**研究者は**: 採用・配置決定にいかなるバイアスがあるか、 
実験的アプローチ（架空履歴書実験、名前ベース実験）で検証できる。

---

## Sources, Methods, Caveats

### Data & Sources
- 公開クレジットデータ（AniList + MAL entity resolution, N=890 アニメーター）
- キャリア段階定義: debut year から 5 年間を「初期キャリア」と定義
- 役職分類: 原画, 動画, 演出その他 (24 role types)

### 重要な留意点
- これは観察研究であり、採用過程のランダム化実験ではない
- 「女性が動画に多い」= 「女性の能力が低い」ではない（能力フレーム禁止）
- 役職と共クレジット多様性の関係が因果的か選別的かは不明
- 属性変数（年齢, 居住地, 学歴）が不完全なため、過剰調整の可能性

### Feedback
誤読・誤解釈のご指摘をお待ちしています。  
特にスタジオ現場、女性アニメーター、労組からのご意見を優先します。

---
```

---

## Checklist Before Publishing Tier C

```
□ Title is specific (not generic: "Industry Observation" ✓, "Anime Data" ✗)
□ Problem section answers: "Why does this matter?" + "To whom?"
□ Evidence section has 2–3 findings with data (N, CI, time window)
□ Evidence avoids evaluative adjectives (no "great," "terrible")
□ Interpretation section clearly marked ("I interpret...") + first-person
□ Interpretation includes 2–3 mechanisms + caveats ("hard to claim without...")
□ No causal claim without named method (experiment, IV, RDD, etc.)
□ Labor consequences section explicitly frames implications
□ "What Next?" presents 3–4 alternatives without recommendation
□ Caveats section: sensitivity analysis + assumptions + what would change
□ Sources fully cited (APA or Chicago style)
□ Feedback loop enabled (contact form / email)
□ Vocabulary lint passed (pixi run lint-vocab < article.txt)
□ Figure(s) annotated with axes, units, N, time window, CI
□ Alt-text for figures (accessibility)
□ Tone read-aloud: Would a union organizer / policy staffer / animator feel respected?
□ ~1,500–3,000 chars (verify in note editor before publish)
```

---

## How to Adapt This Template

1. **Pick a finding** from completed report or analysis module (e.g., "Collaboration diversity dropped 40% 2018–2025")

2. **Draft Problem section**: Why should I care? For whom? (Studio, animator, policy?)

3. **Gather evidence**:
   - Finding 1: [Metric change with N, CI, time window]
   - Finding 2: [Correlate or mechanism] with data
   - Finding 3: (optional) Role or cohort breakdown

4. **Create 1–2 figures**: Annotate with title, axes, N, year range, CI

5. **Write Interpretation**:
   - Start with "I interpret this as..."
   - List 2–3 mechanisms
   - Name what you cannot claim (causation, direction)
   - List labor/policy implications

6. **Draft "What Next?"**: 3–4 possible responses, neutral framing

7. **Add Caveats**:
   - What assumptions underlie this finding?
   - What would change the conclusion?
   - What data would prove/disprove mechanisms?

8. **Cite sources**: Full author, year, title, link

9. **Lint & review**:
   - `pixi run lint-vocab < article.txt`
   - Read aloud for tone
   - Fact-check data points
   - Ask: "Would an animator feel this disrespects their experience?"

10. **Publish on note**:
    - Use note's markdown editor
    - Upload figures inline
    - Enable comments
    - Link from X + LinkedIn

---

## Typical Workflow

**Day 1** (20 min): Topic selection + outline  
**Day 2** (40 min): Evidence gathering + figures  
**Day 3** (20 min): Interpretation + caveats  
**Day 4** (10 min): Citations + polish  
**Day 5** (5 min): Vocab lint + publish  

---

**Template version**: 1.0 (2026-05-13)  
**Related**: `docs/sns_operations.md` §2.3, §3 (tone rules), `forbidden_vocab.yaml`, `REPORT_PHILOSOPHY.md`
