# PROJECT.md — Animetor Eval

> 「アニメーターは能力でなくネットワーク位置で報酬が決まる」を構造的に可視化し、交渉非対称性を是正するインフラ。

`README.md` は技術概要、`CLAUDE.md` は実装規範、本ファイルは **何のために存在するか・何ができるか・どう続けるか** を扱う。

---

## 1. これを使って何ができるか (ユースケース)

### A. 個人 — 補償交渉

- アニメーター/監督個人が studio・agency と交渉する際の **構造的プロファイル PDF / portfolio URL** を生成
- 提示指標 (全て CI 付き):
  - 過去 N 年の Authority percentile (cohort 同期内ランク)
  - opportunity_residual (作品規模で OLS 残差化した個人寄与)
  - peer_percentile (同役職コホート内位置)
  - consistency (CV)
- 「能力が高い」でなく「業界ネットワークでこの位置にいる」という主張に置換 → 主観評価への退行を防ぐ

### B. 個人 — キャリア探索

- 自分と類似コホートの軌跡 (誰が次役職に上がり、誰が消えたか)
- collaboration recommender: 過去に組んだ監督・作画監督との累積エッジ重みで親和性を出す
- dormancy 検出: 一定期間クレジット途絶 → 復帰タイミングのアラート

### C. スタジオ HR

- 後継候補ピックアップ: 監督・作画監督について「次世代候補」を客観抽出
- 離職リスク予測: visibility loss / opportunity_residual 急落の早期検出
- チーム化学スコア: 既存チーム × 候補者の予測的相性
- 報酬公正性監査: peer_percentile vs 実報酬の乖離
- 出力: HR brief (月次)

### D. 業界政策

- 市場集中度 (監督・作画監督・スタジオの HHI / Top-N share)
- ジェンダー配分 × 役職 × 時系列 (天井効果の検出)
- 流出コホート追跡 (ゲーム / 海外 / 廃業)
- 出力: Policy brief、文化庁・経産省・厚労省・労組向け

### E. 投資・制作

- 過小評価人材検出 (高 Authority かつ大作未参加 → 安い段階で接触)
- 新興チームスコア (Trust 累積急上昇クラスタ)
- ホワイトスペース (役職 × ジャンルで供給薄領域)
- 出力: Business brief、季次

### F. 学術

- 労働経済学のデータセット (AKM 適用、創造産業/日本)
- ネットワーク科学のドメイン特化応用
- 文化生産論の定量化

---

## 2. 何をするか (ロードマップ)

| 段階 | 機能 | 受益者 | KPI |
|-----|------|-------|-----|
| **P1** | 公開 portfolio サイト (person_id 単位、SEO 流入) | 個人 | MAU, claim 率 |
| **P2** | Studio HR brief 月額サブスク | スタジオ管理職 | 契約スタジオ数, ARR |
| **P3** | Industry health dashboard (公開) | 政策・労組・メディア | 引用件数 |
| **P4** | Talent Index licensing | 投資ファンド・制作委員会 | 年間契約金額 |
| **P5** | API (GraphQL, AniList 互換的) | 開発者・研究者 | call 数 |

P1 → P3 を 12 ヶ月、P4 を 12〜24 ヶ月、P5 を最後 (差別化シェアの保護)。

---

## 3. 既存正当化 (強化版)

### 3.1 賃金問題の構造性

アニメーター低賃金は「能力不足」でなく「**ネットワーク位置不足**」で支配的に説明される、という仮説を AKM 分解で実証可能にする:

```
log(production_scale_ij) = θ_i (person FE) + ψ_j (studio FE) + ε_ij
```

- θ_i = work-allocation 効果を除いた純粋な個人寄与
- ψ_j = スタジオに帰属する寄与 (ブランド・案件供給力)
- (θ, ψ) の分散分解で「個人効果」と「スタジオ効果」の比率が出る

これは個人交渉では絶対に出せない数値 → 集合的可視化が前提条件。

### 3.2 anime.score 排除の合理性

- **法的**: 視聴者レビューを報酬基準にすると主観性 / 操作可能性 / 反差別法の disparate impact 議論を呼ぶ。米国 EEOC 基準では facially neutral でも危険判定。
- **経済学的**: 人気は需要側変数。供給側 (作り手) の評価関数に混入させると identification が崩れる。
- **倫理的**: 視聴者人気は人格と無関係。能力評価への流用は categorical mistake。

排除は build-time にコードで強制 (`anime.score` は SILVER 不存在、scoring path 接続不可)。

### 3.3 CI 必須

個人補償の根拠として point estimate のみ提示は overclaim 訴訟リスクと学術非耐性を抱える。SE = σ/√n 計算と method note 出力を全 brief で必須化。

### 3.4 5 段階 entity resolution

誤マッチ = 名誉毀損の構成要件 (実名特定 + 虚偽情報流通)。exact → cross-source → romaji → similarity 0.95 → AI 補助 0.8 の降順保守的解決で defamation リスクを工学的に軽減。

### 3.5 観客別 brief 分離

「同じ事実を異なる観客に異なるフレームで」を意図的に設計。Policy / HR / Business を混在させない:

- 政策レポートに投資家向けの talent pick を混ぜると煽動的になる
- HR レポートに政策議論を混ぜると守秘契約と矛盾する
- 観客分離は中立性の構造的担保

---

## 4. アピールポイント (差別化軸)

### A. Architectural firewall (主観排除のコード化)

3 層 DB (BRONZE/SILVER/GOLD) と vocabulary lint (`ability`/`skill`/`talent` 等の正規表現ブロック) を CI で強制。ポリシー文書でなく **ビルドが落ちる** 。

### B. 24-role taxonomy + 役職進行モデル

「動画 → 原画 → 作画監督 → 監督」等の業界固有キャリアパスを正面から扱う。一般化された labor mobility model にマップする際の bridge 層が独自資産。

### C. Network + econometric ハイブリッド

PageRank / Trust / BiRank (ネットワーク科学) と AKM (労働経済学) の同居。単独では出ない知見が出る:

- 「Authority 高 / θ_i 低」 = 大作経由の過大評価人物
- 「Authority 低 / θ_i 高」 = 過小評価人物 (= 投資・採用の最大の発見ポイント)

### D. Multi-source canonical layer

AniList GraphQL + Jikan + ANN + SeesaaWiki + allcinema + Media Arts Database を canonicalize した SILVER 層。各ソース単独では人物粒度で穴がある (MAL は監督薄、ANN は古作薄、AniList は邦画薄、allcinema は古作邦画厚など)。**カバレッジ × 名寄せ精度** が最も模倣困難な資産。

### E. Method gate を CI で強制

individual 推定には CI、group claim には null model、prediction には holdout の三点を全レポートで satisfy しないとビルド通らない。アカデミックな再現性基準。

### F. Rust 並列 + graceful fallback

Brandes betweenness を rayon 並列化、Python (NetworkX) に fallback。数十万ノードで実用速度を出しつつ、開発体験は Python のまま。

### G. Two-layer evaluation の分離

参照系 (Network Profile) と補償系 (Individual Contribution) を別レイヤで分離。参照系を「相対位置の説明」、補償系を「数値根拠」として切り分けることで法的・倫理的整合性が取りやすい。

---

## 5. 出口戦略

### 5.1 短期 (0〜12 ヶ月) — PMF 探索

| ID | 内容 | 想定単価 | 必要顧客数 | コメント |
|----|------|---------|-----------|---------|
| E1 | Freemium portfolio (個人公開) | 0 円 | 流入 SEO | claim 率と MAU で吸引力検証 |
| E2 | Studio HR brief 月額 | ¥50k〜¥200k/月 | 3〜5 社で PMF | white-label, NDA 込み |
| E3 | 個人有料プロファイル (証明書 PDF + CI 付) | ¥3k 〜 ¥10k/件 | 高単価、低頻度 | 補償交渉直前需要 |

### 5.2 中期 (12〜36 ヶ月) — 収益化

| ID | 内容 | コメント |
|----|------|---------|
| E4 | Talent Index licensing | 投資ファンド・制作委員会、年契約 ¥数百万〜 |
| E5 | Government / Foundation grants | 文化庁、JSPS 科研費、Open Data Charter |
| E6 | API access tier (商用) | データプロバイダ的位置 |

### 5.3 長期 (36+ ヶ月) — Exit / 持続

| ID | 内容 | 想定買い手 / パートナー |
|----|------|-----------------------|
| E7 | Acquisition (業界 DB 系) | anikore, ANN 親会社, allcinema 親会社 |
| E8 | Acquisition (HR / agency) | Akiba Creators, アニメ映像制作労組 |
| E9 | Acquisition (制作データ) | Filmarks, GEM Partners |
| E10 | Acquisition (国際) | Kadokawa (MyAnimeList 経由), AniList Inc., Sony Pictures (Crunchyroll) |
| E11 | Industry-standard reference | 労組合同運営化、業界統計の公的扱い |
| E12 | Open dataset spinoff | academic license 無償 + commercial premium |

### 5.4 出口優先順位 (推定)

1. **E11 (公的 / 労組共同運営)** — mission integrity 最大、利益最小化を許容するなら理想
2. **E10 (国際 acquisition)** — 評価額・レバレッジ最大、ただし mission drift リスク
3. **E9 (制作データ系)** — 隣接事業 synergy、文化的整合
4. **E7 / E8** — 国内専業者買収、データ統合の自然な行き先

---

## 6. リスク / Exit Blocker

| リスク | 内容 | 緩和 |
|-------|------|-----|
| データ ToS | AniList/MAL の二次配布制限 | 生データ再配布せず、分析結果のみ配布 |
| 名誉毀損 | entity resolution 誤マッチ | 5 段階保守的解決、AI 補助 ≥ 0.8、訂正フロー整備 |
| Network effect 不在 | 競合複製コストが低い | データカバレッジ + 方法論論文化で堀を作る |
| 業界政治 | スタジオ批判で取材拒否 | 観客分離 + 中立 framing + 当事者対話チャネル |
| 主観混入 | レビュー指標の誘惑 | architectural firewall + lint で機械防衛 |
| 個人嫌悪 | 「勝手に評価された」苦情 | claim/opt-out フロー、portfolio 自己編集権 |
| 再現性 | 計算結果の検証困難 | meta_lineage + method note + holdout |

---

## 7. 非ゴール (やらないこと)

- **個人能力評価**: スコアは構造的位置の指標。能力・才能の判定はしない
- **視聴者人気の取り込み**: anime.score は display のみ、scoring 不参加 (永続)
- **クローズドソース化**: 中核アルゴリズムは公開、商用差別化はデータ運用とレポート品質
- **個人ブランディング営業**: 個人の高スコア宣伝で広告収益を取らない
- **AI による「主観的評価生成」**: AI 利用は entity resolution / 翻訳 / parsing 補助のみ

---

## 8. 進捗管理

- 未完了: `TODO.md`
- 完了: `DONE.md`
- 設計原則: `CLAUDE.md`
- レポート哲学: `docs/REPORT_PHILOSOPHY.md`
- 計算根拠: `docs/CALCULATION_COMPENDIUM.md`
