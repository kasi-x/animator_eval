# 計算ロジック監査 TODO

本文書は、プロジェクト全体の計算ロジック監査の結果と残課題を管理する。
最終更新: 2026-04-06 (D項目全体監査 — 26/27 件が既に対処済みと確認)

カテゴリ凡例:
- `[実装エラー]` — コードのバグ・不整合。修正が明確
- `[疑念]` — 設計意図は理解できるが、結果の妥当性に疑問
- `[正当化困難]` — マジックナンバーや恣意的選択で根拠が不在
- `[要説明]` — ドキュメント不足。根拠を明記すべき
- `[要調査]` — 実データでの影響を検証してから判断すべき

---

# 進捗サマリ

| カテゴリ | 総数 | 修正済み | 残 | 備考 |
|----------|------|---------|-----|------|
| anime.score 汚染経路 | 16 | **16** | **0** | 全経路クリーン |
| 実装バグ (B01-B16) | 16 | **16** | **0** | 全件修正済み |
| 新規発見バグ (A) | 4 | **4** | **0** | A1-A3, Shapley 全修正済み |
| 新規発見バグ (2次) | 10 | **10** | **0** | C1,H1,M1-M4,L1-L4 全修正済み |
| 設計疑念 (D01-D27) | 27 | **26** | **1** | D25 のみ未対処 |
| テストカバレッジ | — | — | — | pipeline_phases 87%未テスト |

---

# 第1部: 未修正の残課題 (優先度順)

## Priority 1: 設計疑念 — 残り1件

### D25. [疑念] API が生スコアを文脈なしで公開

**File:** `src/api.py:270-300`

`/api/persons` が iv_score, birank 等の生スコアを返すが、「スコアは能力ではなくネットワーク密度を表す」という文脈がない。補償証拠として使う場合は信頼区間も必要（CLAUDE.md 要件）。

**対応案:** 各エンドポイントのレスポンスに `metadata.disclaimer` フィールドを追加。

---

### ~~D-項目全体 (26/27 件): 対処済み確認~~

以下の全項目について 2026-04-06 にコードを精査し、既に実装 or コメントで対処されていることを確認した。

| TAG | 対処内容 | 確認箇所 |
|-----|---------|---------|
| ~~D01~~ | 感度分析実施済み (top-100 は±20%摂動で<5%変化) | `config.py:40-43` |
| ~~D02~~ | OLS回帰でデータ駆動の役職重みを推定 | `contribution_attribution.py:79-114` |
| ~~D03~~ | 幾何平均に変更（二次膨張を回避） | `graph.py:751` |
| ~~D04~~ | decay_rate = 0.5 に統一 | `patronage_dormancy.py` |
| ~~D05~~ | κ 上下限に理論的根拠を文書化 | `akm.py:721` |
| ~~D06~~ | α はmover-stayer回帰から自動推定 | `akm.py:506-511` |
| ~~D07~~ | L2 CV を廃止し PCA PC1 荷重に変更 | `integrated_value.py:203` |
| ~~D08~~ | 処理順序の根拠を文書化（rank-corr > 0.98） | `akm.py:971-980` |
| ~~D09~~ | t検定 p<0.05 を追加 | `akm.py:433` |
| ~~D10~~ | 乗法形式の設計意図を文書化 | `patronage_dormancy.py:297` |
| ~~D11~~ | 3.3x スケーリングは意図的（trio相乗効果） | `synergy_score.py:254-257` |
| ~~D12~~ | ANIMATION_DIRECTOR 包含の理由を文書化 | `role_groups.py:22-27` |
| ~~D13~~ | 除外ではなくサンプリング（最大50人）に変更 | `compatibility.py:106` |
| ~~D14~~ | 「独立軸でない」旨を文書化 | `individual_contribution.py:160-164` |
| ~~D15/D21~~ | mover比率10%閾値の根拠（Card-Heining-Kline） | `akm.py:891-896` |
| ~~D16~~ | 同時性バイアスを文書化（ラグ実装は検討中） | `expected_ability.py:83` |
| ~~D17~~ | 監督が patronage=0 になる構造的理由を文書化 | `patronage_dormancy.py:86` |
| ~~D18~~ | 循環でなく一方向フロー (BiRank→IV) であることを文書化 | `core_scoring.py:99-104` |
| ~~D19~~ | bisect_right でタイは上位パーセンタイルを付与（文書化済み） | `post_processing.py:30` |
| ~~D20~~ | unique_studios を機会プロキシとして使う根拠を文書化 | `individual_contribution.py:134` |
| ~~D22~~ | seed=42 を固定 | `graph_construction.py:49,56,60` |
| ~~D23~~ | スレッド安全化 (main thread で実行) | `analysis_modules.py` |
| ~~D24~~ | CVを廃止し PCA に変更（ターゲットリーク解消） | `integrated_value.py:97-114` |
| ~~D26~~ | VOICE_ACTOR を `non_production` として登録済み | `role_groups.py:166` |
| ~~D27~~ | anime.score を使用しない実装に変更済み | `anime_value.py:82-95` |

---

## Priority 2: CI/統計の改善 — 全件対処済み

### ~~CI-1. 同次分散仮定~~ → 修正済み (per-person clustered SE に変更)

### ~~CI-2. IV 正規化パラメータの意味的不整合~~ → 確認済み (component_mean はrawの平均を正しく保存)

**確認箇所:** `integrated_value.py:150-153` — コメントに明記済み。`component_mean` は `x_mean_raw`（正規化前の平均）を保存しており、増分更新時も `(x - mean) / std` で正しく正規化できる。

---

## Priority 3: テストカバレッジ

### T01. pipeline_phases/ のユニットテスト (13/15 ファイルが未テスト)

特に重要:
- Phase 5 `core_scoring.py` — 補償根拠の中核
- Phase 8 `post_processing.py` — パーセンタイル・CI 計算
- Phase 9 `analysis_modules.py` — ThreadPoolExecutor 並列実行

### T02. patronage_dormancy.py の直接テスト

IVテストではモック使用。実際の dormancy penalty 計算ロジック (指数減衰、猶予期間) が未検証。

### T03. VA パイプライン (7 モジュール全て未テスト)

va_akm, va_integrated_value, va_graph 等。

### T04. generate_all_reports.py (23,447 行、テストなし)

分割してテスト可能にすべき。

---

## Priority 4: コード品質

### Q02. generate_all_reports.py の分割 (23,447 行)

プロジェクト全ソースの 40% が単一ファイル。レビュー・テスト・並列開発が困難。

---

# 第2部: 修正済み項目

## anime.score 汚染 — 全16経路修正済み

| 経路 | モジュール | 修正内容 |
|------|-----------|---------|
| 1 | akm.py | outcome を production_scale に変更 |
| 2 | graph.py | _work_importance から score_mult 削除 |
| 3 | skill.py | OpenSkill を staff_count ランキングに変更 |
| 4 | patronage_dormancy.py | Quality 項を削除 |
| 5 | integrated_value.py | CV ターゲットを PCA PC1 に変更 |
| 6 | temporal_pagerank.py | エッジ重みから score 削除 |
| 7 | individual_contribution.py | independent_value を collab IV に変更 |
| 8 | individual_contribution.py | OLS 統制変数から anime.score 削除 |
| 9 | individual_contribution.py | fallback パスから anime.score 削除 |
| 10 | work_impact.py | score_factor を構造指標に変更 |
| 11 | anime_value.py | 表示用のみに限定 |
| 12 | synergy_score.py | quality_factor をエピソード軌跡ベースに変更 |
| 13 | compatibility.py | person_anime_scores を log(staff_count) に変更 |
| 14 | milestones.py | 表示用のみに限定 |
| 15 | analysis_modules.py | _run_teams の min_score/コメント削除 |
| 16 | anime_prediction.py | staff_count を予測対象に変更 |

## 実装バグ — 全16件 + 新規4件 = 20件修正済み

| TAG | 修正内容 |
|-----|---------|
| B01 STUDIO-EXPOSURE-INCONSISTENCY | Phase 6 で compute_studio_exposure() を使用 |
| B02 IV-RENORMALIZATION-MISSING | component_std/mean を context に保持・伝搬 |
| B03 AKM-STUDIO-MISMATCH | _build_panel で studio_assignments を参照 |
| B04 BIRANK-UPDATE-ORDER | Jacobi 型 (古い p) に修正 |
| B05 CLOSENESS-WEIGHT-INVERTED | distance = 1/weight を追加 |
| B06 OLS-ZERO-DUMMY | n_roles = max(len(roles)-1, 0) |
| B07 INDEPENDENT-VALUE-SELF-INCLUSION | pid 自身も除外 |
| B08 CONFIDENCE-SCALE | パーセンタイル変換後のスコアで CI 計算 |
| B09 BOOTSTRAP-WRONG-TARGET | 解析的 SE (sigma/sqrt(n)) に変更 |
| B10 CONSISTENCY-SCALE-MISMATCH | 統一公式 1.0 - std/ref_scale |
| B11 EPISODE-WEIGHT-JACCARD | 独立性仮定の積に修正、ロール別ヒューリスティック |
| B12 STUDIO-RETENTION-JACCARD | retention = intersection/prev_staff に修正 |
| B13 MILESTONES-GLOBAL-MAX | 個人クレジット内の大規模制作参加に変更 |
| B14 RETROSPECTIVE-NOOP | career_fraction + post_ratio で future_peak 推定 |
| B15 STUDIO-EXPOSURE-DEAD-CODE | akm_result パス整理 |
| B16 MENTORSHIP-YEAR-COUNT | year span 修正 |
| A1 SCORE-HISTORY-SCHEMA | v1 スキーマを現行8カラムに更新 |
| A2 RAW-ROLE-MISSING | entity_resolution で raw_role を保持 |
| A3 MENTORSHIP-THREAD-SAFETY | メインスレッドで lock 下に書き込み |
| B-NEW SHAPLEY-NOOP | marginal contribution 直接呼び出しに簡略化 |

## 2次発見バグ — 10件修正済み

| TAG | 重要度 | 修正内容 |
|-----|--------|---------|
| C1 API-DATA-QUALITY | CRITICAL | api.py stats.get("credits") → "credits_count" (常に0を返していた) |
| H1 ROLE-PARSE-CRASH | HIGH | database.py Role(role_str) の ValueError を catch (未知ロールでクラッシュ) |
| M1 COMP-DUPLICATE-KEY | MEDIUM | compensation_analyzer.py ANIMATION_DIRECTOR 重複エントリ削除 |
| M2 ROLE-FLOW-LEX-SORT | MEDIUM | role_flow.py max() にステージ番号抽出キー追加 |
| M3 API-YEAR-FILTER | MEDIUM | api.py year_from/year_to の is None ガードと明示括弧 |
| M4 STUDIO-ASSIGN-DUP | MEDIUM | AKMResult に studio_assignments を追加、二重計算解消 |
| L1 FROZENSET-DUP | LOW | data_loading.py _ANCHOR_PRODUCTION_ROLES 重複削除 |
| L2 ANIME-VALUE-DIM | LOW | anime_value.py main() dimension="iv_score" → "overall" |
| L3 HARDCODED-PATH | LOW | entity_resolution.py 固定パス → JSON_DIR 使用 |
| L4 SILENT-EXCEPT | LOW | analysis_modules.py _run_bridges に exc_info=True 追加 |

## コード品質 — Q01 lint エラー9件修正済み

- `dml.py` 未使用変数 `n` 削除
- `genre_ecosystem.py` import 位置修正
- `models.py` 重複辞書キー修正 (`"文芸"`, `"music"`, `"記録"`, `"タイミング"`)
- `seesaawiki_scraper.py` 未使用 import 削除
- `test_analysis_coverage.py` 未使用 import 削除
- `test_dml.py` 未使用 import 削除
- `test_synergy_score.py` テスト更新 (episodes ベース quality_factor)

---

# 第3部: 修正ロードマップ

### Phase A: 設計改善 (D項目)

| 順序 | 対象 | 作業量 |
|------|------|--------|
| A1 | D01/D02 — 重み根拠の感度分析・文書化 | 大 |
| ~~A2~~ | ~~D04 — decay_rate 統一~~ | ~~済~~ |
| A3 | D08/~~D09~~ — AKM 処理順序 (D09有意性検定は修正済み) | 中 |
| A4 | D12 — DIRECTOR_ROLES 見直し | 小 |
| A5 | D14/D18 — 循環依存の文書化 or 解消 | 中 |
| A6 | 残り D 項目の文書化 | 中 |

### Phase B: CI/統計改善

| 順序 | 対象 | 作業量 |
|------|------|--------|
| ~~B1~~ | ~~Heteroscedasticity correction~~ | ~~済~~ |
| B2 | IV 正規化パラメータ意味統一 | 中 |

### Phase C: テストカバレッジ

| 順序 | 対象 | 作業量 |
|------|------|--------|
| C1 | Phase 5/8 ユニットテスト | 大 |
| C2 | patronage_dormancy 直接テスト | 中 |
| C3 | VA パイプラインテスト | 中 |

### Phase D: コード品質

| 順序 | 対象 | 作業量 |
|------|------|--------|
| D1 | generate_all_reports.py 分割 | 大 |
