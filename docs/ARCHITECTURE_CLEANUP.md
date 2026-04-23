# ARCHITECTURE_CLEANUP.md

Animetor Eval のレイアウト・命名整理計画 (TODO §5 H-7 + §11)。

作成: 2026-04-24 / ステータス: 計画 (未実装)

## 目的

- `src/` 平置き 15 ファイル解消 → `runtime/ infra/ db/ testing/`
- `src/analysis/` 平置き 70 ファイル解消 → 既存 subpackage + 新規 `domain/ entity/ quality/ reports/ io/`
- `PipelineContext` 完全削除 → typed inputs 統一

## 影響範囲 (先行調査)

- `from src.<flat>` を持つファイル: 235
- `from src.analysis.<flat>` を持つファイル: 332 超 (va 80, network 77, gold_writer 70, scoring 57, silver_reader 42 ...)

## 現状 snapshot (2026-04-24)

### `src/` 直下 (15 ファイル / 8043 行)

| ファイル | 行数 | 分類 |
|---|---:|---|
| `cli.py` | 2304 | runtime |
| `models.py` | 1680 | runtime (Pydantic v2) |
| `api.py` | 994 | runtime |
| `db_rows.py` | 736 | db |
| `report.py` | 572 | runtime |
| `synthetic.py` | 438 | testing |
| `validation.py` | 342 | infra |
| `pipeline.py` | 276 | runtime |
| `websocket_manager.py` | 247 | infra |
| `database.py` | 223 | db (薄いラッパー、§4 DuckDB 移行で再検討) |
| `freshness.py` | 179 | infra |
| `log.py` | 33 | infra |
| `api_validators.py` | 13 | runtime |
| `api_reports.py` | 6 | runtime |
| `__init__.py` | 0 | — |

### `src/analysis/` 既存 subpackage (11 個)

- `attrition/ causal/ gender/ genre/ market/ mentor/ network/ scoring/ studio/ talent/ team/ va/`

### `src/analysis/` 平置き (70 ファイル) — 再分類対象

---

## Phase A: `src/` 平置き解消 (§11.1)

### A.1 ディレクトリ新設

```
src/runtime/
src/infra/
src/testing/
```

(`src/db/` は既存)

### A.2 移動マップ

| 現在 | 移動先 | 備考 |
|---|---|---|
| `src/api.py` | `src/runtime/api.py` | |
| `src/api_reports.py` | `src/runtime/api_reports.py` | 6 行、将来統合候補 |
| `src/api_validators.py` | `src/runtime/api_validators.py` | 13 行 |
| `src/cli.py` | `src/runtime/cli.py` | |
| `src/pipeline.py` | `src/runtime/pipeline.py` | |
| `src/report.py` | `src/runtime/report.py` | |
| `src/models.py` | `src/runtime/models.py` | Pydantic v2。shared だが runtime に近い |
| `src/database.py` | `src/db/dao.py` | 命名揺らぎ解消 (database 重複回避) |
| `src/db_rows.py` | `src/db/rows.py` | |
| `src/log.py` | `src/infra/logging.py` | 命名: stdlib と衝突しない。import `src.infra.logging` |
| `src/websocket_manager.py` | `src/infra/websocket.py` | 命名簡素化 |
| `src/freshness.py` | `src/infra/freshness.py` | |
| `src/validation.py` | `src/infra/validation.py` | pipeline 汎用バリデーション (phase 側 validation.py と別) |
| `src/synthetic.py` | `src/testing/fixtures.py` | §11.1 通りリネーム |

### A.3 実施手順

1. `src/{runtime,infra,testing}/__init__.py` 作成
2. `git mv` で 14 ファイルを機械移動
3. `grep -rln "^from src\.\(api\|cli\|pipeline\|models\|log\|database\|db_rows\|websocket_manager\|freshness\|validation\|synthetic\|report\|api_reports\|api_validators\) " --include="*.py" .` で対象特定
4. `ruff check --fix` + 手動 sed で import 置換:
   ```
   from src.api        → from src.runtime.api
   from src.cli        → from src.runtime.cli
   from src.pipeline   → from src.runtime.pipeline
   from src.report     → from src.runtime.report
   from src.models     → from src.runtime.models
   from src.api_reports    → from src.runtime.api_reports
   from src.api_validators → from src.runtime.api_validators
   from src.database   → from src.db.dao
   from src.db_rows    → from src.db.rows
   from src.log        → from src.infra.logging
   from src.websocket_manager → from src.infra.websocket
   from src.freshness  → from src.infra.freshness
   from src.validation → from src.infra.validation
   from src.synthetic  → from src.testing.fixtures
   ```
5. `CLAUDE.md` Directory Structure 更新
6. `pixi run lint` → `pixi run test-scoped` (影響範囲テスト、下記)
7. 問題なければ `pixi run test` PR 直前実行

### A.4 テストパッチ点 (既存 CLAUDE.md 指摘)

- `src.database.DEFAULT_DB_PATH` → `src.db.dao.DEFAULT_DB_PATH`
- `src.db.init.DEFAULT_DB_PATH` → 変更なし (既存通り)
- `src.pipeline.JSON_DIR` → `src.runtime.pipeline.JSON_DIR`
- `src.analysis.visualize.JSON_DIR` → Phase B 後に `src.analysis.reports.visualize.JSON_DIR`

### A.5 後方互換 shim (オプション)

一気に全 import を書き換えるのが困難なら、`src/api.py` 等に以下:
```python
from src.runtime.api import *  # noqa: F401,F403
from src.runtime.api import __all__  # noqa: F401
```
ただし **推奨は一括置換**。shim は Phase A 完了時に削除。

---

## Phase B: `src/analysis/` 70 ファイルの subpackage 化 (§11.2)

### B.1 分類 (ファイル → subpackage)

#### 既存 subpackage へ追加

**`analysis/scoring/`** (既存: akm, birank, expected_ability, individual_contribution, integrated_value, normalize, pagerank, patronage_dormancy, potential_value)
- 追加: `skill.py`, `influence.py`

**`analysis/network/`** (既存: bridges, circles, community_detection, core_periphery, ego_graph, independent_unit, knowledge_spanners, multilayer, network_density, network_evolution, path_finding, peer_effects, structural_holes, temporal_bridge, temporal_influence, temporal_pagerank, trust, trust_entry)
- 追加: `graph.py`, `graph_rust.py`, `sparse_graph.py`, `collab_diversity.py`, `collaboration_strength.py`, `cooccurrence_groups.py`, `synergy_score.py`

**`analysis/talent/`** (既存: succession, undervalued)
- 追加: `talent_pipeline.py`, `growth.py`, `growth_acceleration.py`, `career.py`, `career_friction.py`, `milestones.py`, `mentorship.py`, `role_flow.py`

**`analysis/team/`** (既存: chemistry, templates)
- 追加: `team_composition.py`

#### 新規 subpackage

**`analysis/entity/`** (新) — entity resolution 群
- `entity_resolution.py`, `ai_entity_resolution.py`, `entity_resolution_eval.py`, `ml_homonym_split.py`

**`analysis/domain/anime/`** (新) — 作品側指標
- `anime_prediction.py`, `anime_stats.py`, `anime_value.py`, `production_analysis.py`, `decade_analysis.py`, `seasonal.py`, `time_series.py`, `transitions.py`, `work_impact.py`

**`analysis/domain/person/`** (新) — 個人側指標 (スコアでなく属性/相対比較)
- `person_parameters.py`, `person_tags.py`, `versatility.py`, `productivity.py`, `recommendation.py`, `similarity.py`, `compatibility.py`, `cohort.py`, `compensation_analyzer.py`, `clusters.py`, `aggregate_stats.py`, `comparison_matrix.py`, `contribution_attribution.py`

**`analysis/quality/`** (新) — 統計品質・検証
- `bias_detector.py`, `confidence.py`, `crossval.py`, `data_quality.py`, `outliers.py`, `robustness.py`, `stability.py`, `uncertainty.py`

**`analysis/reports/`** (新) — 可視化・エクスポート
- `insights_report.py`, `explain.py`, `method_notes.py`, `credit_stats.py`, `credit_stats_html.py`, `visualize.py`, `visualize_interactive.py`, `graphml_export.py`, `neo4j_export.py`, `neo4j_direct.py`

**`analysis/io/`** (新) — IO/永続化/cache
- `duckdb_io.py`, `gold_writer.py`, `silver_reader.py`, `feat_precompute.py`, `calc_cache.py`

#### top-level 残置 (subpackage にしない)

- `protocols.py` (型定義共有)
- `llm_pipeline.py` (横断的 LLM エントリ)
- `__init__.py` (現状の boundary guard 維持)

### B.2 重複確認項目 (§9 残務)

- `similarity.py` (`analysis/domain/person/`) と `recommendation.py` の機能重複確認 → Phase B 中に精査、片方削除 or マージ候補
- `va/akm.py` vs `scoring/akm.py` — VA 版は分離維持 (ドメイン別モデル)

### B.3 移動マップ数値

- 追加 (既存 subpackage): 2 + 7 + 8 + 1 = 18 ファイル
- 新規 subpackage へ: 4 + 9 + 13 + 8 + 10 + 5 = 49 ファイル
- top-level 残置: 3 ファイル (`__init__.py`, `protocols.py`, `llm_pipeline.py`)
- 合計: 18 + 49 + 3 = 70 ✅

### B.4 後方互換戦略

import 影響: `src.analysis.va`(80), `src.analysis.network`(77), `src.analysis.gold_writer`(70), `src.analysis.scoring`(57), `src.analysis.silver_reader`(42) ...

**方針**: `__init__.py` で re-export **せず**、全 import を正式な新パスに書き換える (dead code aggressive 方針 feedback_dead_code_aggressive.md)。

ただし **高頻度 import** (>30 呼出箇所) は一時的に top-level re-export で緩衝:
```python
# src/analysis/__init__.py
from src.analysis.io.gold_writer import (
    gold_connect_write, DEFAULT_GOLD_DB_PATH, GoldReader, GoldWriter,
)
from src.analysis.io.silver_reader import (
    silver_connect, load_anime_silver, load_credits_silver, DEFAULT_SILVER_PATH,
)
```
ただしこれは Phase B 完了後 2 週間以内に削除。

### B.5 実施手順

1. 新規 subpackage の `__init__.py` 作成
2. `git mv` で 49 + 18 = 67 ファイル移動
3. リポジトリ全体で import 一括置換 (置換表は実装者が `Bash` で生成)
4. `ruff check --fix` + pytest scoped 実行
5. 既存 subpackage の `__init__.py` も再 export を整理
6. `CLAUDE.md` Directory Structure 更新

### B.6 検証

- `pixi run lint` 無エラー
- `pixi run test-scoped tests/test_visualize.py tests/test_entity_resolution.py tests/test_network.py tests/test_scoring.py` (代表的カテゴリ)
- PR 直前に `pixi run test` フル

---

## Phase C: §5 H-7 PipelineContext 完全削除

### C.1 ブロック要因 (既に TODO.md §5 に明記)

- VA pipeline が ctx を直接使用 → Hamilton 化済 (`analysis/va/pipeline/`) で大部分解消
- `export_and_viz.py` に 71 箇所の ctx 参照 → pure function 分解必須

### C.2 手順

1. VA pipeline 残存 ctx 参照の洗い出し: `grep -n 'ctx\.' src/analysis/va/pipeline/`
2. `src/pipeline_phases/export_and_viz.py` を ExportSpec registry 経由の pure function 群に分解
   - 各 export 関数は `(scores_df, anime_df, ...) → output_path` の形
   - 現状の 26 ExportSpec を関数シグネチャにハードコードせず、`registry.py` で data 宣言を維持
3. 全 Hamilton node の `ctx: PipelineContext` を明示 typed inputs に変換
   - 例: `def compute_foo(ctx: PipelineContext)` → `def compute_foo(persons: PersonsDF, credits: CreditsDF)`
4. `src/pipeline_phases/context.py` 削除
5. `src/pipeline_phases/__init__.py` の ctx 関連 export 削除
6. テスト: `pixi run test` (Hamilton DAG 全体に影響、フル走行必須)

### C.3 前提条件

- Phase A/B **完了後**に着手 (レイアウトが固まってない状態で大規模リファクタしない)
- DuckDB §4 の残務 (`§4` 全項目) 完了後 — `§4` 4 項目残ってる状態でも着手可だが、context 削除で DuckDB 移行との競合発生に注意

### C.4 リスク

- Hamilton DAG のテストカバレッジが不十分な箇所あり → `pixi run pipeline` E2E 走行で検証
- `PipelineContext.betweenness_cache` 等の最適化 state をどこに持たせるか設計必要 → Hamilton memoization or 専用 cache オブジェクト

---

## 実施順序

```
Phase A (src/ 平置き解消)
  └─ 機械的移動、import 一括置換
  └─ PR 1 本、レビュー容易
      ↓
Phase B (analysis/ 70 → subpackage)
  └─ 既存 11 subpackage + 新規 6 subpackage
  └─ PR を subpackage 単位で分割推奨 (例: B-1 entity/, B-2 io/, B-3 reports/, ...)
  └─ 各 PR は 5-15 ファイル移動 + import 置換
      ↓
Phase C (PipelineContext 削除)
  └─ 設計重い、export_and_viz.py pure function 化が本体
  └─ DuckDB §4 残務と競合管理
```

## 後方互換性ポリシー

- Phase A: 一括置換。shim 使わない (規模適度)
- Phase B: 高頻度 import (>30 箇所) のみ `analysis/__init__.py` で一時 re-export、2 週間後削除
- Phase C: 後方互換不要 (context.py は内部実装)

## 非目標

- テストファイル (`tests/`) の再配置 — `§6.4` で別タスク
- `_v2` suffix 廃止 (§11.3) — `database_v2.py` / `models_v2.py` は既に削除済。`generate_reports_v2.py` → `generate_reports.py` は §8 残務として既に計画済
- `scripts/` 配下の整理 — 本計画の対象外

## 進捗チェックリスト

- [ ] Phase A: src/runtime/ 作成 + api/cli/pipeline/report/models 移動
- [ ] Phase A: src/infra/ 作成 + log/websocket/freshness/validation 移動
- [ ] Phase A: src/db/ に dao.py (旧 database.py) / rows.py (旧 db_rows.py) 統合
- [ ] Phase A: src/testing/ 作成 + fixtures.py (旧 synthetic.py) 移動
- [ ] Phase A: import 一括置換 + test-scoped 緑
- [ ] Phase B-1: entity/ 新設 + 4 ファイル移動
- [ ] Phase B-2: io/ 新設 + 5 ファイル移動 (gold_writer 70 箇所影響で慎重)
- [ ] Phase B-3: quality/ 新設 + 8 ファイル移動
- [ ] Phase B-4: domain/anime/ 新設 + 9 ファイル移動
- [ ] Phase B-5: domain/person/ 新設 + 13 ファイル移動
- [ ] Phase B-6: reports/ 新設 + 10 ファイル移動
- [ ] Phase B-7: 既存 subpackage へ追加 (scoring +2, network +7, talent +8, team +1)
- [ ] Phase B-8: similarity/recommendation 重複精査 (§9)
- [ ] Phase C-1: VA pipeline ctx 残存参照除去
- [ ] Phase C-2: export_and_viz.py pure function 化
- [ ] Phase C-3: Hamilton node 全 typed inputs 変換
- [ ] Phase C-4: context.py 削除 + フルテスト緑

## 関連ドキュメント

- `CLAUDE.md` — Directory Structure (完了時に更新)
- `TODO.md` §5 (Hamilton H-7), §11 (レイアウト整理), §9 (similarity/recommendation 重複)
- `DONE.md` — Phase 1-4 基盤 (参考)
