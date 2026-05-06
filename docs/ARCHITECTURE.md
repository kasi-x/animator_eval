# Animetor Eval — システムアーキテクチャ

このドキュメントはAnimetor Evalの詳細なアーキテクチャ設計を説明します。

## 目次

1. [概要](#概要)
2. [パイプラインアーキテクチャ](#パイプラインアーキテクチャ)
3. [データモデル](#データモデル)
4. [スコアリングアルゴリズム](#スコアリングアルゴリズム)
5. [パフォーマンス最適化](#パフォーマンス最適化)
6. [リアルタイム監視](#リアルタイム監視)
7. [国際化システム](#国際化システム)
8. [API設計](#api設計)
9. [テスト戦略](#テスト戦略)

---

## 概要

Animetor Evalはアニメ業界の制作者を**ネットワーク科学**の手法で評価するシステムです。グラフ理論、PageRankアルゴリズム、ベイズ推定を組み合わせて、クレジットデータから3軸スコア（Authority, Trust, Skill）を計算します。

### 設計原則

1. **データ駆動**: クレジットデータのみを使用、主観的評価を排除
2. **モジュール性**: 10フェーズパイプライン、37+分析モジュール
3. **パフォーマンス**: 並列実行、キャッシング、アルゴリズム最適化
4. **保守性**: 小さな関数（<100行）、明確な責務分離
5. **法的配慮**: 信用毀損リスクを考慮した保守的な名寄せ

### データベース5層モデル (v62+)

Animetor Evalは**5層データアーキテクチャ**を採用しており、データの源泉から分析出力まで各層で責務を厳密に分離します。旧 3 層 (BRONZE/SILVER/GOLD) では SILVER に「集約」と「entity_resolution 後の代表値選抜」が混在し、AKM silent fail (21/01) の原因となりました。5 層化でこれを解消します。

| 層 | 役割 | 物理形式 | 旧呼称 |
|----|------|---------|--------|
| **Raw** | scrape 直後の生 cache | filesystem `data/<source>/raw/` | — |
| **Source** | source 別 parsed parquet | `result/bronze/source=*/` | BRONZE |
| **Conformed** | source 横断 schema 統一、source 並存 | `animetor.duckdb` `conformed` schema | SILVER |
| **Resolved** | entity_resolution 済 canonical 1 row | `result/resolved.duckdb` | (新規) |
| **Mart** | scoring / feature / 統計 | `animetor.duckdb` `mart` schema | GOLD |

#### Raw層（生データ層）
- **形式**: HTML / JSON / XML filesystem cache
- **場所**: `data/<source>/raw/`
- **特性**: scraper の生 cache。不可変・scrape の証拠
- **用途**: 監査、再 parse の source、scrape 証拠保存
- **変更**: scrapers のみ書込。分析コードから完全隔離

#### Source層（source 別構造化層）
- **テーブル**: `src_anilist_anime`, `src_mal_anime`, `src_jvmg_*` 等
- **特性**: 各 source ごとに独立 schema で parsed。集約・統合は行わない (source の**完全性**を保つ)
- **anime.scoreを含む**: 視聴者評価スコアが保持される
- **用途**: 監査、歴史的比較、外部検証のみ
- **読取**: Display lookup ヘルパー経由のみ（UI メタデータ用）
- **制約**: 分析コードから完全隔離
- 旧呼称: BRONZE。物理 path `result/bronze/` は当面維持

#### Conformed層（横断統一層）
- **テーブル**: `conformed.anime`, `conformed.persons`, `conformed.credits`, `conformed.studios` 等
- **特性**: 各 source の同種データを**schema 統一**。ID は `<source>:<id>` prefix で source 並存 (`anilist:a123`)
- **entity_resolution 前**: source 並存のまま。代表値選抜はまだ行わない
- **anime.scoreを除外**: `display_*_<source>` prefix で隔離 (H1)
- **用途**: Source → Resolved の中間層。Conformed の補強タスク (22/01 等)
- **制約**: 分析・スコアリングコードは直接読まない
- 旧呼称: SILVER。物理 file `silver.duckdb` は当面維持、ドキュメント上は Conformed

#### Resolved層（代表値選抜層、新規）
- **テーブル**: `resolved.anime`, `resolved.persons`, `resolved.credits`, `resolved.studios`
- **特性**: entity_resolution 済 canonical 1 row。1 canonical anime / person / studio = 1 row
- **代表値選抜**: source 優先順位リスト + majority vote tie-break
  - 例: `title_ja` = seesaawiki > anilist > mal > mediaarts
- **欠損補填**: source A で空、B にある値を採用
- **meta_resolution_audit**: 各 canonical_id がどの source row 由来かトレース可能
- **用途**: **AKM / 全 scoring の唯一の入力層**
- **アクセス**: `src/analysis/*`, `src/pipeline_phases/*` → Resolved のみを読む
- **ETL**: `src/etl/resolved/` パッケージ経由のみ書込
- 旧 SILVER の anime_studios silent fail (21/01) の根本対策

#### Mart層（分析・統計層）
- **テーブル**: `mart.scores`, `mart.score_history`, `mart.meta_*`, `mart.feat_*`
- **特性**: AKM 結果 (`theta_i` / `psi_j`)、派生 feature、スコア、レポート集計
- **meta_lineageテーブル**: formula_version, ci_method, null_model, holdout_method, inputs_hash, row_count を記録
- **用途**: レポート生成、API 応答、監査証拠
- **アクセス**: レポートジェネレータ、API 層、フロントエンド
- 旧呼称: GOLD。物理 file `gold.duckdb` は当面維持、ドキュメント上は Mart

**重要**: 分析コードは **Resolved 層のみ読取**。Source 層へのアクセスは `src/utils/display_lookup.py` ヘルパー経由のみ（UI 表示用）。これにより、viewer ratings（anime.score）に依存しないスコアリング完全性を保証。

---

### Legacy: データベース3層モデル (v53-v54 まで)

旧 3 層モデルの記述。過去仕様の参照・git history との照合用として保存。

#### BRONZE層（旧 Source 層相当）
- **テーブル**: `src_anilist_anime`, `src_mal_anime`, `src_jvmg_*`
- **用途**: 監査、歴史的比較。Display lookup 経由のみ

#### SILVER層（旧 Conformed + Resolved 合算）
- **テーブル**: `anime`, `anime_external_ids`, `anime_display`, `anime_genres`, `anime_tags`, `credits`, `persons`, `roles`
- **問題**: entity_resolution 前後の行が混在し AKM silent fail (21/01) の原因となった

#### GOLD層（旧 Mart 層相当）
- **テーブル**: `scores`, `score_history`, `meta_*`
- **用途**: レポート生成、API 応答、監査証拠

---

## パイプラインアーキテクチャ

### 全体構成

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Orchestrator                    │
│                     (src/pipeline.py)                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
            ┌─────────────────────────────────┐
            │    PipelineContext (dataclass)   │
            │  - Shared state across phases    │
            │  - Performance monitoring        │
            │  - WebSocket broadcaster         │
            └─────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   Phase 1-3            Phase 4-6             Phase 7-10
 (Data Prep)         (Scoring)            (Analysis & Export)
```

### 10フェーズ詳細

#### Phase 1: Data Loading
**モジュール**: `src/pipeline_phases/data_loading.py`

```python
def load_pipeline_data(context: PipelineContext, conn: Connection) -> None:
    """Load persons, anime, credits from SQLite database."""
```

- SQLiteから全データをメモリに読み込み
- Personモデル、Animeモデル、Creditモデルに変換
- anime_mapを構築（O(1)アクセス用）

**パフォーマンス**: ~0.1-0.5秒（10K persons, 50K credits）

#### Phase 2: Validation
**モジュール**: `src/pipeline_phases/validation.py`

```python
@dataclass
class ValidationResult:
    passed: bool
    errors: list[str]
    warnings: list[str]
```

**検証項目**:
- 参照整合性（orphaned credits, missing anime）
- データ完全性（missing names, invalid years）
- クレジット分布（role imbalance, source coverage）
- 重複検出（duplicate credits）

#### Phase 3: Entity Resolution
**モジュール**: `src/pipeline_phases/entity_resolution.py`

**5段階の名寄せ**:

1. **Exact Match**: 日本語名の完全一致（優先）
2. **Cross-Source**: 異なるソース間でのID一致
3. **Romaji Normalization**: ローマ字の正規化（大文字小文字、スペース）
4. **Similarity-based**: Jaro-Winkler (threshold=0.95)、同一ソース内のみ
5. **AI-assisted** (optional): Ollama + Qwen3、漢字バリアント対応

**False Positive対策**:
```python
# 日本語名を優先、英語名にフォールバックしない
if person.name_ja:
    primary_name = person.name_ja
else:
    primary_name = person.name_en
```

結果: 誤マッチ率 < 0.3%

#### Phase 4: Graph Construction
**モジュール**: `src/pipeline_phases/graph_construction.py`

**3種類のグラフ**:

1. **Person-Anime Graph** (二部グラフ):
   - Nodes: {persons} ∪ {anime}
   - Edges: 参加関係（person → anime）
   - Edge weights: 役職重み × 監督ボーナス

2. **Collaboration Graph**:
   - Nodes: persons
   - Edges: 共通作品での協業
   - Edge weights: 協業回数 × 重要度

3. **Director-Animator Graph**:
   - Nodes: directors ∪ animators
   - Edges: 監督-アニメーター関係
   - Edge weights: 継続起用回数

**最適化**: エッジの事前集約（~1M `has_edge()` 呼び出しを削減）

```python
edge_data = defaultdict(lambda: {"weight": 0.0, "shared_works": 0})
for anime_id, staff in anime_credits.items():
    for pid_a, role_a, w_a in staff:
        for pid_b, role_b, w_b in staff:
            if pid_a >= pid_b:
                continue
            edge_key = (pid_a, pid_b)
            edge_data[edge_key]["weight"] += (w_a + w_b) / 2
            edge_data[edge_key]["shared_works"] += 1

G.add_edges_from((a, b, attrs) for (a, b), attrs in edge_data.items())
```

#### Phase 5: Core Scoring
**モジュール**: `src/pipeline_phases/core_scoring.py`

**Authority (PageRank)**:
```python
PR(u) = (1-d)/N + d * Σ [PR(v) * W(v,u) / L(v)]   for v in B_u
```

- Damping factor: 0.85
- Max iterations: 100
- Convergence: 1e-6

**Trust (Repeat Engagement)**:
```python
trust_score = Σ (role_weight × time_decay × director_prominence)
```

- Time decay: `exp(-λ * years_since)`
- Half-life: 3 years

**Skill (OpenSkill / PlackettLuce)**:
```python
from openskill import Rating
rating = Rating(mu=25.0, sigma=8.33)
```

- Team-based rating system
- Recent works weighted higher
- Convergence after ~10 works

**Normalization**: Min-Max scaling to [0, 100]

#### Phase 6: Supplementary Metrics
**モジュール**: `src/pipeline_phases/supplementary_metrics.py`

**8種のメトリクス**:
1. Decay: 時間減衰スコア
2. Role Profiles: 役職分布
3. Career Stats: キャリア統計
4. Circles: 監督サークル
5. Versatility: 多様性スコア
6. Centrality: 中心性指標
7. Network Density: ネットワーク密度
8. Growth Trends: 成長トレンド

#### Phase 7: Result Assembly
**モジュール**: `src/pipeline_phases/result_assembly.py`

全メトリクスを統合した結果辞書を構築:

```python
result = {
    "person_id": ...,
    "name": ...,
    "iv_score": ...,
    "birank": ...,
    "patronage": ...,
    "person_fe": ...,
    "career": {...},
    "breakdown": {...},
    # ... 30+ fields
}
```

#### Phase 8: Post-Processing
**モジュール**: `src/pipeline_phases/post_processing.py`

- パーセンタイル計算（P10, P25, P50, P75, P90）
- 信頼区間推定
- 安定性スコア（複数実行での分散）

#### Phase 9: Analysis Modules (並列実行)
**モジュール**: `src/pipeline_phases/analysis_modules.py`

**20タスクを並列実行**:

```python
with ThreadPoolExecutor(max_workers=min(32, cpu_count + 4)) as executor:
    futures = {executor.submit(task.function, context): task for task in tasks}
    for future in as_completed(futures):
        task = futures[future]
        try:
            future.result()
        except Exception as e:
            logger.error("task_failed", task=task.name, error=str(e))
```

**タスク例**:
- anime_stats: アニメ品質統計
- collaborations: 協業ペア分析
- outliers: 外れ値検出
- teams: チーム構成パターン
- growth: 成長トレンド
- bridges: ブリッジノード検出
- ... (20 total)

**スレッドセーフ**: `threading.Lock` で共有書き込み保護

#### Phase 10: Export & Visualization
**モジュール**: `src/pipeline_phases/export_and_viz.py`

**26種のエクスポート**（Declarative Registry）:

```python
@dataclass
class ExportSpec:
    filename: str
    data_getter: Callable[[PipelineContext], Any]
    transformer: Callable[[Any], dict | list] | None = None
    condition: Callable[[PipelineContext], bool] | None = None
    log_message: str = "export_saved"
    log_metric_name: str | None = None
```

---

## データモデル

### Pydantic v2 Models

```python
class Person(BaseModel):
    id: str
    name_ja: str = ""
    name_en: str = ""
    aliases: list[str] = Field(default_factory=list)
    mal_id: int | None = None
    anilist_id: int | None = None
    canonical_id: str | None = None

class Anime(BaseModel):
    id: str
    title_ja: str = ""
    title_en: str = ""
    year: int | None = None
    season: str | None = None
    episodes: int | None = None
    score: float | None = None

class Credit(BaseModel):
    person_id: str
    anime_id: str
    role: Role  # Enum
    episode: int = -1
    source: str = ""
```

### SQLite Schema (v4)

```sql
CREATE TABLE persons (
    id TEXT PRIMARY KEY,
    name_ja TEXT DEFAULT '',
    name_en TEXT DEFAULT '',
    aliases TEXT DEFAULT '[]',
    mal_id INTEGER UNIQUE,
    anilist_id INTEGER UNIQUE,
    canonical_id TEXT
);

CREATE TABLE anime (
    id TEXT PRIMARY KEY,
    title_ja TEXT DEFAULT '',
    title_en TEXT DEFAULT '',
    year INTEGER,
    season TEXT,
    episodes INTEGER,
    mal_id INTEGER UNIQUE,
    anilist_id INTEGER UNIQUE,
    score REAL
);

CREATE TABLE credits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id TEXT NOT NULL,
    anime_id TEXT NOT NULL,
    role TEXT NOT NULL,
    episode INTEGER DEFAULT -1,
    source TEXT DEFAULT '',
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (anime_id) REFERENCES anime(id)
);

CREATE TABLE scores (
    person_id TEXT PRIMARY KEY,
    iv_score REAL,
    birank REAL,
    patronage REAL,
    person_fe REAL,
    -- ... 20+ columns
);
```

---

## スコアリングアルゴリズム

### Authority (Weighted PageRank)

**実装**: `src/analysis/pagerank.py`

```python
def weighted_pagerank(
    G: nx.Graph,
    alpha: float = 0.85,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> dict[str, float]:
    """
    Weighted PageRank with personalization.

    PR(u) = (1-α)/N + α * Σ [PR(v) * W(v,u) / L(v)]

    where:
    - α: damping factor (0.85)
    - W(v,u): edge weight from v to u
    - L(v): sum of outgoing edge weights from v
    """
```

**収束条件**: `max(|PR_new - PR_old|) < tol`

### Trust (Repeat Engagement)

**実装**: `src/analysis/trust.py`

```python
trust_score = Σ (role_importance × time_weight × director_boost)

where:
- role_importance: ROLE_IMPORTANCE_MAP[role]
- time_weight: exp(-λ * years_since)  # λ = ln(2)/3
- director_boost: director_pagerank ^ 0.5
```

**最適化**:
- `ROLE_IMPORTANCE_MAP`: モジュールレベル定数（再計算なし）
- `director_work_counts`: 事前計算
- `time_weight`: LRUキャッシュ（`@lru_cache(maxsize=100)`）

### Skill (OpenSkill)

**実装**: `src/analysis/skill.py`

OpenSkillは**Plackett-Luce**モデルのベイズ近似を使用：

```python
from openskill import Rating, rate

# 初期rating
rating = Rating(mu=25.0, sigma=8.33)

# Team-based update
new_ratings = rate([[team1], [team2], ...])
```

**チーム定義**:
- 同じアニメの全スタッフ = 1チーム
- チーム順位 = アニメスコア（MAL/AniList）

---

## パフォーマンス最適化

### Phase 2最適化（2026-02-09実装）

#### 1. Graph Construction (3-5x speedup)

**Before**:
```python
for pid_a, pid_b in combinations(staff, 2):
    if G.has_edge(pid_a, pid_b):  # O(1) but called ~1M times
        G[pid_a][pid_b]["weight"] += weight
    else:
        G.add_edge(pid_a, pid_b, weight=weight)
```

**After**:
```python
edge_data = defaultdict(lambda: {"weight": 0.0})
for pid_a, pid_b in combinations(staff, 2):
    edge_data[(pid_a, pid_b)]["weight"] += weight

G.add_edges_from((a, b, attrs) for (a, b), attrs in edge_data.items())
```

削減: ~1M `has_edge()` 呼び出し → 3-5倍高速化

#### 2. Entity Resolution (10-100x speedup)

**Before**: O(M²) = 50M comparisons

**After**:
- First-character blocking: O(M²/26)
- Length filtering: Skip if `|len(a) - len(b)| / max(len(a), len(b)) > 0.2`
- Prefix check: Skip if `a[:3] != b[:3]`
- LRU cache: `@lru_cache(maxsize=10000)`

削減: 50M → ~2M comparisons → 10-100倍高速化

#### 3. Trust Scoring (40-50% speedup)

**Before**: Dict recreation on every iteration

**After**:
- Hoist `ROLE_IMPORTANCE_MAP` to module level
- Pre-compute `anime_years`, `director_work_counts`
- LRU cache for `time_weight` calculation

### Phase 9並列化（2026-02-10実装）

**Before**: Sequential execution = 0.15s (synthetic data)

**After**: ThreadPoolExecutor with 20 workers = 0.03-0.04s

**Speedup**: 4-6x

**Worker count**: `min(32, cpu_count + 4)` (I/O-bound tasks)

---

## リアルタイム監視

### WebSocketアーキテクチャ

```
┌─────────────┐         WebSocket         ┌──────────────┐
│   Browser   │ ◄─────────────────────── │ FastAPI      │
│             │   ws://host/ws/pipeline   │ WebSocket    │
│ pipeline_   │                            │ Manager      │
│ monitor.html│                            └──────────────┘
└─────────────┘                                    ▲
                                                   │ broadcast_sync
                                                   │
                                            ┌──────┴───────┐
                                            │   Pipeline   │
                                            │  (10 phases) │
                                            └──────────────┘
```

### Message Types

```typescript
type WebSocketMessage =
  | { type: "connection_established"; message: string; timestamp: string }
  | { type: "pipeline_start"; total_phases: number; timestamp: string }
  | { type: "phase_update"; phase: number; phase_name: string; status: "running"; progress: number; timestamp: string }
  | { type: "phase_complete"; phase: number; phase_name: string; duration_ms: number; progress: number; timestamp: string }
  | { type: "phase_error"; phase: number; phase_name: string; error: string; timestamp: string }
  | { type: "pipeline_complete"; total_persons: number; duration_seconds: number; timestamp: string }
```

### Implementation

**Backend** (`src/websocket_manager.py`):

```python
class WebSocketManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def broadcast(self, message: dict):
        """Broadcast to all connected clients."""
        for connection in self.active_connections:
            await connection.send_json(message)

    def broadcast_sync(self, message: dict):
        """Sync wrapper for non-async contexts."""
        asyncio.create_task(self.broadcast(message))
```

**Frontend** (`static/pipeline_monitor_i18n.js`):

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/pipeline');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleWebSocketMessage(data);
};

function handleWebSocketMessage(data) {
    switch (data.type) {
        case 'pipeline_start':
            initPhases();
            break;
        case 'phase_complete':
            updatePhase(data.phase, 'complete', data.duration_ms);
            break;
        // ... other cases
    }
}
```

---

## 国際化システム

### i18n Architecture

```
src/i18n/
├── __init__.py              # I18n class, global instance
└── locales/
    ├── en.json              # English translations
    └── ja.json              # Japanese translations
```

### Translation Structure

```json
{
  "app": {
    "name": "Animetor Eval"
  },
  "cli": {
    "stats": {
      "title": "Database Statistics",
      "total_persons": "Persons"
    }
  },
  "pipeline": {
    "phases": {
      "data_loading": "Data Loading"
    }
  }
}
```

### API Integration

**Backend**:
```python
from src.i18n import t, set_language

set_language("ja")
message = t("cli.stats.title")  # "データベース統計"
```

**Frontend**:
```javascript
// Fetch translations
const response = await fetch('/api/i18n/ja');
const data = await response.json();
translations = data.translations;

// Use translations
const title = translations.cli.stats.title;
```

---

## API設計

### REST API (FastAPI)

**Base URL**: `http://localhost:8000`

**Authentication**: None (local development)

**Rate Limiting**: None (to be added for production)

### Endpoint Patterns

```python
@app.get("/api/persons")
def list_persons(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    sort_by: str = Query("iv_score"),
) -> PaginatedResponse:
    """List all persons with pagination and sorting."""

@app.get("/api/persons/{person_id}")
def get_person(person_id: str) -> dict:
    """Get person profile by ID."""

@app.get("/api/persons/{person_id}/similar")
def get_similar_persons(person_id: str, top_n: int = 10) -> list[dict]:
    """Find similar persons using cosine similarity."""
```

### Response Models

```python
class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    per_page: int
    pages: int

class HealthResponse(BaseModel):
    status: str
    db_exists: bool
    scores_exist: bool
```

---

## テスト戦略

### Test Pyramid

```
                    ┌──────────┐
                    │    E2E   │ 42 tests
                    │  (3%)    │
                ┌───┴──────────┴───┐
                │   Integration    │ 200 tests
                │     (20%)        │
            ┌───┴──────────────────┴───┐
            │        Unit Tests        │ 732 tests
            │         (75%)            │
            └──────────────────────────┘
```

### Test Categories

1. **Unit Tests** (732):
   - Analysis modules: 300+
   - Pipeline phases: 100+
   - Utilities: 200+
   - I18n: 16
   - API endpoints: 65+

2. **Integration Tests** (200):
   - Synthetic pipeline: 24
   - Visualization pipeline: 18
   - CLI commands: 60
   - Database operations: 50+

3. **E2E Tests** (42):
   - Full pipeline execution
   - API server lifecycle
   - WebSocket communication

### Test Infrastructure

**Fixtures** (`tests/conftest.py`):

```python
@pytest.fixture
def populated_db(monkeypatch, tmp_path):
    """Create test DB with synthetic data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    # ... populate with test data
    monkeypatch.setattr("src.database.DEFAULT_DB_PATH", db_path)
    return conn
```

**Monkeypatching**:
```python
# MUST patch at module level where imported
monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_db)
monkeypatch.setattr(src.pipeline, "JSON_DIR", tmp_json)
```

---

## 今後の拡張

### Neo4j移行

現在のSQLiteスキーマはNeo4j互換設計：

```cypher
CREATE (p:Person {id: $id, name_ja: $name_ja})
CREATE (a:Anime {id: $id, title_ja: $title_ja})
CREATE (p)-[:WORKED_ON {role: $role}]->(a)
CREATE (p1)-[:COLLABORATED_WITH {weight: $weight}]->(p2)
```

GraphMLエクスポート機能実装済み（`src/analysis/neo4j_export.py`）

### スケーラビリティ

- **現状**: 10K persons, 50K credits → 10秒
- **目標**: 100K persons, 500K credits → <2分
- **手段**:
  - Dask並列化
  - Redis caching
  - PostgreSQL migration

---

## スキーマ・リファレンス

### Database Markup Language (DBML)

完全なスキーマドキュメント: `docs/schema.dbml`

DBML形式により、以下を可視化できます：
- テーブル関係図（Entity Relationship Diagram）
- 5層アーキテクチャ（Raw / Source / Conformed / Resolved / Mart）
- データ型と制約
- 外部キー関係

**DBMLの閲覧**: [DBDiagram.io](https://dbdiagram.io) にuploadして可視化

### テーブル分類

#### Conformed層 (旧 SILVER、84 tables, 3 categories)
**コア (5 tables)**: `conformed.anime`, `conformed.persons`, `conformed.credits`, `conformed.roles`, `conformed.sources`
**拡張 (5 tables)**: `conformed.anime_external_ids`, `conformed.anime_genres`, `conformed.anime_tags`, `conformed.person_aliases`, `conformed.person_ext_ids`
**分析 (2 tables)**: `conformed.anime_analysis`, `conformed.anime_display`

#### Source層 (旧 BRONZE、13 tables)
**データソース**: `src_anilist_*`, `src_mal_*`, `src_seesaawiki_*`, `src_ann_*`, `src_allcinema_*`, `src_madb_*`
⚠️ Contains anime.score, popularity - NEVER used in scoring formulas

#### Resolved層 (新規)
**canonical (4 tables)**: `resolved.anime`, `resolved.persons`, `resolved.credits`, `resolved.studios`
**監査 (1 table)**: `resolved.meta_resolution_audit` (canonical_id → source row トレース)

#### Mart層 (旧 GOLD、17+ tables)
**スコア (3 tables)**: `mart.scores`, `mart.score_history`, `mart.va_scores`
**メタ (17 tables)**: `mart.meta_*` (formula lineage, policy analysis outputs)
**特徴 (17 tables)**: `mart.feat_*` (clustering features for Phase 9)

### 重要な設計決定

1. **Resolved-onlyスコアリング**: `src/analysis/*` は Resolved 層のみを読取
   - Exception: `src/utils/display_lookup.py` (UI表示用の Source アクセス)
   
2. **Audit Trail**: `meta_lineage` テーブル記録
   - formula_version, ci_method, null_model, holdout_method
   - inputs_hash (入力データ変更検出)
   - git_commit (どのコード版で計算したか)

3. **Confidence Intervals**: 全個人別スコアに必須
   - `scores.confidence_lower`, `scores.confidence_upper`
   - 95% CI (analytical SE = sigma/sqrt(n))

4. **バージョン管理**: SQLiteマイグレーション (v1-v54)
   - Phase 4までに v54スキーマ確定
   - 以降の破壊的変更は v55+

---

## レポート層 / 可視化層 (v3)

設計仕様の完全版: [`docs/REPORT_DESIGN_v3.md`](REPORT_DESIGN_v3.md) / [`docs/VIZ_SYSTEM_v3.md`](VIZ_SYSTEM_v3.md)

### ReportSpec / BriefArc データクラス階層

```
ReportSpec                    # 各レポートが宣言する方法論メタデータ
├── claim                     # 1文の主張 (狭い名前)
├── identifying_assumption    # 主張の成立前提
├── null_model                # 帰無モデル定義
├── method_gate               # CI / 縮約 / holdout / 感度の最低要件
├── sensitivity_grid          # window / threshold 代替選択 (1軸以上)
├── interpretation_guard      # 禁止 framing + 必須代替解釈数
└── data_lineage              # Source → Mart 経路 + meta_lineage table 参照

BriefArc                      # 3 brief の narrative 構造を規定するデータクラス
├── phenomenon                # Section 1: 現象の提示 (評価的形容詞なし)
├── null_contrast             # Section 2: null model との対比
├── interpretive_limit        # Section 3: 解釈の限界と不確実性の明示
└── alternative_view          # Section 4: 代替解釈の併記
```

`ReportSpec` を持たないレポートは Pipeline Phase 5 strict mode でブロックされる (`ci_check_report_spec.py`)。全 45 reports が curated SPEC を保持している (v3.0 完了時点)。

### Phase 5 Strict Mode

v3 から Pipeline Phase 5 (Core Scoring) の出口に SPEC バリデーションゲートを追加。

```
Phase 5: Core Scoring
    └── SPEC gate (ci_check_report_spec.py)
         ├── ReportSpec 存在確認
         ├── method_gate フィールド充足確認 (CI / null_model / holdout)
         └── 未充足 → pipeline 停止 (exit code 1)
```

手動実行:
```bash
pixi run check-report-spec-strict   # 全レポート SPEC 検証
```

### Visualization Layer (src/viz/)

```
src/viz/
├── primitives/          # P1-P11 chart primitive (CI / null overlay / shrinkage badge デフォルト ON)
│   ├── ci_scatter.py    # P1: 点推定 + 誤差バー
│   ├── km_curve.py      # P2: 生存曲線
│   ├── event_study.py   # P3: 介入前後 dynamic effect
│   ├── small_multiples.py # P4: facet grid
│   ├── ridge.py         # P5: 分布重ね
│   ├── box_strip_ci.py  # P6: box + strip + 95% CI
│   ├── sankey.py        # P7: 段階遷移 flow
│   ├── radial_network.py # P8: ego-network 局所図
│   ├── heatmap.py       # P9: 相関 / 共起行列
│   ├── parallel_coords.py # P10: 多軸 parallel coordinates
│   └── choropleth_jp.py # P11: 都道府県 choropleth (GeoJSON pending)
├── theme.py             # Plotly layout テンプレート (全レポート共通)
├── palettes.py          # Okabe-Ito 8色 + 460-hex アクセシビリティテーブル
├── typography.py        # フォント / サイズ規定
├── ci.py                # CI band 描画ヘルパー
├── null_overlay.py      # null model envelope 描画ヘルパー
├── shrinkage_badge.py   # 縮約済み値 badge
├── interactivity.py     # linked brushing (brief 内 primitive 横断)
└── export.py            # HTML / SVG / PDF 並走 export (kaleido)
```

**設計原則**: 全 primitive が `auto_ci=True` / `auto_null=True` / `shrinkage_badge=True` をデフォルトとする。CI band または null envelope の描画はレポート側コードではなく primitive が強制するため、`REPORT_PHILOSOPHY.md §3.1` の viz 漏れが構造的に発生しない。

### Glossary v3

[`docs/GLOSSARY_v3.md`](GLOSSARY_v3.md) — 全 45 レポートで使用する用語の canonical 定義。`forbidden_vocab` の 19 件の例外 (rationale + スコープ付き) を管理する。新語追加・例外追加は Glossary v3 への PR を通す。

---

**Last Updated**: 2026-05-06
**Version**: 1.3.0
**Schema**: v62+ (5層アーキテクチャ)
