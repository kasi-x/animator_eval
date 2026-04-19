# detailed_todo.md — 母データテーブル再構築 & レポート再編 実装指示書

作成日: 2026-04-19
対象読者: **本書を単独で読む実装担当モデル** (会話履歴なしで自走できることが要件)
関連文書: `CLAUDE.md`, `docs/REPORT_PHILOSOPHY.md`, `docs/REPORT_STRATEGY.md`, `docs/CALCULATION_COMPENDIUM.md`, `todo.md`

---

## 0. 本書の目的と実施にあたっての前提

### 0.1 目的

現状のアニメ業界評価システム (`animetor_eval`) の設計を、次の 4 つの不満を同時に解消する形で刷新する:

1. **anime.score 除外が方針依存**: 16 pathways はコード側で修正済みだが、`anime` テーブルに `score REAL` カラムが残っており、新規コードが誤って参照する余地がある (`src/database.py:178-191`, `src/etl/integrate.py:37`)
2. **レポート生成が monolithic**: 35+ レポートが各々で重指標を再計算。HTML render と指標計算が混在し、時間がかかる・キャッシュできない
3. **method-driven レポートが audience を想定していない**: 35 本のうち何本が実際に読まれているか検証不能。`docs/REPORT_STRATEGY.md` で 3 audience brief を提案済みだが、既存 35 本の統廃合は未着手
4. **v2 Philosophy gate が人力**: 「能力 framing 禁止」「Findings/Interpretation 分離」「CI/null-model/holdout 必須」はドキュメントだけで enforce されていない

### 0.2 達成する設計目標

- **silver 層から anime.score (および popularity, favourites) を完全除去**。カラムが存在しない = SELECT 不可
- **anime.score を表示したい場合は bronze から単一の audited helper 経由でのみ取得**。silver に display 専用カラムを逃がす設計は採らない
- **レポート直読用の gold 層 (meta_*)** を新設。各レポートは事前計算済みテーブル 1-2 個を読むだけで render 可能
- **3 audience brief** (政策提言 / 人材評価=現場効率化 / 新たな試み提案) に再編。35 本を半減
- **v2 gate の機械化**: 禁止語 lint、Section 構造 enforce、feature lineage の自動検証

### 0.3 前提知識 (必読)

- メダリオン (bronze/silver/gold) の 3 層モデル。本書では以下の命名で運用する:
  - **bronze**: ソース別生データ。anime.score, popularity, favourites, description, cover_*, banner, genres(JSON), tags(JSON), studios(JSON) など**全フィールドを含む**。`src_anilist_*`, `src_ann_*`, `src_allcinema_*`, `src_seesaawiki_*`, `src_keyframe_*` (既存)
  - **silver**: 正規化・統合・分析向けクレンジング済み。**anime.score / popularity / favourites を含まない**。構造情報 (year, episodes, format, duration, genres 構造データ等) のみ
  - **gold**: レポート直読用の事前集計層。既存 `feat_*`, `agg_*` を整理し、`meta_*` naming に統一する (本書で新設)

- **視聴者指標を表示したい場合の唯一の経路**: `src/utils/display_lookup.py` (本書で新設) に `get_display_score(conn, anime_id)` などのヘルパーを置き、レポートは**このヘルパー経由でのみ** bronze を参照する。`src/analysis/**` からは呼び出し禁止 (4.x の lint で enforce)

- 固定ルール (`CLAUDE.md`):
  - anime.score を scoring 公式・edge weight・optimization target に使わない (hard requirement)
  - 能力 framing 禁止 (legal requirement)
  - Entity resolution の false positive は名誉毀損リスク

- 既存アーキテクチャ:
  - 10-phase pipeline (`src/pipeline_phases/`)
  - Bronze → Silver ETL は `src/etl/integrate.py` に実装済み
  - `scripts/maintenance/seed_medallion.py` でシード投入 + ETL 検証可能
  - 35 reports: `scripts/report_generators/reports/`
  - v2 report 基盤: `scripts/report_generators/{section_builder,ci_utils,db_loaders,html_templates,stratified_loader}.py` + `reports/_base.py`
  - 26 pipeline JSON outputs: `result/json/`

### 0.4 非交渉条件 (hard constraints)

以下は本作業中に**絶対に違反してはならない**:

| # | 制約 | 違反の影響 |
|---|------|-----------|
| H1 | silver 層 (`anime` テーブル新版) に anime.score / popularity / favourites カラムを**作らない**。これらは bronze (`src_anilist_anime` 等) のみに存在し、display helper 経由でのみアクセスされる | anime.score 再汚染 |
| H2 | レポート Findings セクションで「能力」「優秀」「実力」「劣る」「ability」等の評価語を使わない | 法的リスク (defamation) |
| H3 | entity resolution のロジックは**変更しない**。本作業はデータ層・レポート層のみ | false positive 増 → 名誉毀損 |
| H4 | silver `credits` には `source` カラムを残す。5ソース混在を失うと信頼性検証ができなくなる | データ品質追跡不能 |
| H5 | 既存 1947 テストは green を維持。追加・更新は許可。削除は要確認 | regression 検知不能 |
| H6 | pre-commit hook の skip (--no-verify) 禁止 | 既存 gate を迂回してはならない |
| H7 | force push, `git reset --hard`, branch 削除は行わない | 作業ロスト |

---

## 1. 全体アーキテクチャ (TO-BE)

```
┌─────────────────────────────────────────────────────────────────┐
│ Scrapers (AniList / ANN / allcinema / SeesaaWiki / Keyframe)     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Bronze: src_*_anime, src_*_persons, src_*_credits                 │
│ (既存。無変更。anime.score / popularity / favourites 等を含む)     │
│                                                                    │
│   ↑ 表示用アクセスはここに到達する唯一の経路:                       │
│     src/utils/display_lookup.py の helper 経由                     │
│     (src/analysis/** からの呼び出し禁止)                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓ src/etl/integrate.py (拡張)
┌─────────────────────────────────────────────────────────────────┐
│ Silver (単一層。score / popularity / favourites / image 無し)     │
│  ─ anime              (元 anime_analysis をリネーム。score なし)   │
│  ─ persons                                                         │
│  ─ credits            (episode NULL = 全話通し; sentinel -1 廃止)  │
│  ─ studios, anime_studios (既存)                                   │
│  ─ anime_genres, anime_tags (新設: JSON 正規化)                    │
│  ─ anime_relations, characters, character_voice_actors (既存)      │
│  (廃止: legacy `anime` / `anime_display`)                          │
└─────────────────────────────────────────────────────────────────┘
                              ↓ feature engineering (既存 pipeline_phases)
┌─────────────────────────────────────────────────────────────────┐
│ Gold (meta_*): レポート直読用                                      │
│  ─ meta_person_parameters       (Person Parameter Card 用)       │
│  ─ meta_policy_attrition        (政策 Brief 離職分解 用)           │
│  ─ meta_policy_monopsony        (政策 Brief 労働市場 用)           │
│  ─ meta_policy_gender           (政策 Brief ジェンダー 用)          │
│  ─ meta_hr_studio_benchmark     (現場効率化 Brief 用)              │
│  ─ meta_hr_mentor_card          (現場効率化 Brief 用)              │
│  ─ meta_hr_attrition_risk       (現場効率化 Brief 用、要認証)       │
│  ─ meta_biz_whitespace          (新たな試み Brief ジャンル空白地)    │
│  ─ meta_biz_undervalued         (新たな試み Brief 過小露出群)       │
│  ─ meta_biz_trust_entry         (新たな試み Brief 信頼ネット参入)    │
│  (既存 feat_* は維持しつつ、meta_* は feat_* から派生)              │
└─────────────────────────────────────────────────────────────────┘
                              ↓ scripts/report_generators/reports/
┌─────────────────────────────────────────────────────────────────┐
│ Reports (3 audience + 共通基盤)                                    │
│  ─ Person Parameter Card (全 audience 共通基盤)                    │
│  ─ 政策提言 Brief (4-6 reports)                                   │
│  ─ 人材評価 (現場効率化) Brief (4-6 reports)                       │
│  ─ 新たな試み提案 Brief (4-6 reports)                             │
│  ─ 技術付録 (残存する既存レポート、読者は研究者・監査者のみ)           │
└─────────────────────────────────────────────────────────────────┘
```

### 1.1 命名規則 (厳守)

- **bronze**: `src_{source}_{entity}` (既存)
- **silver**: `anime`, `persons`, `credits`, `studios`, `anime_studios`, `anime_relations` (既存名を維持。**ただし anime テーブルから score / popularity / favourites カラムを削除する**)
- **display helper**: `src/utils/display_lookup.py`。bronze にアクセスする唯一の正規ルート。レポート層からのみ呼び出し可
- **gold**: `meta_{audience}_{topic}`。audience = {policy, hr, biz, common}。topic は snake_case
- **feat_\***: 既存のまま維持。gold の原料
- **agg_\***: 既存のまま維持

### 1.2 現状診断 (AS-IS) と物理実装方針

**現状は 3 つの anime テーブルが並立している** (schema v47 時点、`src/database.py` 読解):

| テーブル | カラムの例 | 実態 |
|---------|------------|-----|
| `anime` (legacy) | id, title_*, year, season, episodes, mal_id, anilist_id, **score** | 旧来コード・scraper・多くの分析が使う。score を持つ |
| `anime_analysis` (silver 相当) | id, title_*, year, season, quarter, episodes, format, duration, start/end_date, status, source, work_type, scale_class, 各 source_id | v2 architecture で導入された clean version。**score を持たない** |
| `anime_display` (silver.display 相当) | id, score, popularity, favourites, description, cover_*, banner, site_url, genres JSON, tags JSON, studios JSON, synonyms JSON | score + 画像 + description を保持。`anime_analysis.id` への FK |

この並立状態こそ問題の本体。**user 指示「silver にスコアを入れない」**を徹底するために、以下のように**物理再編**する (destructive OK):

**TO-BE**:

1. **`anime_display` テーブルを DROP**。score/description/cover_* は bronze にのみ残し、display_lookup helper で参照する
2. **legacy `anime` テーブルを DROP**。scraper/ETL/analysis を `anime_analysis` に切替えた上で削除
3. **`anime_analysis` を `anime` に rename**。名前は「どの層か」ではなく「何のデータか」を表すべき。silver が唯一の canonical table になるので suffix 不要
4. **`anime_genres` / `anime_tags` を新設** (正規化)。bronze の JSON から ETL 時に展開
5. **`studios` / `anime_studios` は既存を流用** (`src/database.py:254-271`)。無変更

結果として silver には `anime` (clean)、`persons`、`credits`、`studios`、`anime_studios`、`anime_genres`、`anime_tags`、`anime_relations`、`characters`、`character_voice_actors` のみが残る。

### 1.3 テーブル設計原則 (効率性 × 自己説明性)

**目標**: パイプラインが週次〜月次で走り、何度も再計算される。SQL だけ読んで他人が意味を理解できること。

#### 1.3.1 命名規則

| 接頭辞 | 層 | 例 |
|-------|-----|----|
| `src_{source}_` | bronze (raw per source) | `src_anilist_anime`, `src_ann_credits` |
| (prefix なし) | silver (integrated canonical) | `anime`, `persons`, `credits` |
| `{a}_{b}` | silver junction (2 エンティティの関連) | `anime_studios`, `anime_genres` |
| `feat_` | feature (analytical intermediate) | `feat_person_scores`, `feat_network` |
| `meta_{audience}_` | gold (report-ready) | `meta_policy_attrition`, `meta_hr_mentor_card` |
| `agg_` | pre-aggregation (集計キャッシュ) | `agg_year_studio_credits` |

**既存 `scores` テーブルは `person_scores` にリネーム**。`anime.score` と混同する名前は禁止。

#### 1.3.2 カラム設計

- **NOT NULL を徹底**: silver は ETL 後のクリーンデータなので、値が保証されるカラムは NOT NULL 必須。NULL の意味が「データ欠損」なのか「N/A」なのか分からなくなる曖昧さを排除
- **sentinel 値を避ける**: 現 `credits.episode DEFAULT -1` のような「-1 = 全話通し」は読みづらい。`episode INTEGER NULL` に変更し、**NULL = 全話通しクレジット**と明文化 (CHECK 制約で再確認)
- **CHECK 制約で値域を閉じる**:
  ```sql
  format   TEXT CHECK (format IN ('TV','MOVIE','OVA','ONA','SPECIAL','MUSIC')),
  season   TEXT CHECK (season IN ('WINTER','SPRING','SUMMER','FALL')),
  quarter  INTEGER CHECK (quarter BETWEEN 1 AND 4),
  is_main  INTEGER NOT NULL DEFAULT 0 CHECK (is_main IN (0,1))
  ```
- **カラムに 1 行コメント (SQL コメント形式)**: 非自明なものだけ
  ```sql
  CREATE TABLE credits (
      ...
      episode INTEGER,           -- NULL = 全話通しクレジット (作品レベル)
      source  TEXT NOT NULL,     -- 'anilist' / 'ann' / 'allcinema' / 'seesaawiki' / 'keyframe'
      ...
  );
  ```

#### 1.3.3 インデックス戦略 (ホットパス基準)

- `credits` は最ホット (pipeline が毎回 fullscan)。**複合インデックス**を追加:
  - `(anime_id, role)` — 作品 × 役割 (team / anime_stats / role_flow で必須)
  - `(person_id, source)` — entity resolution の source-aware lookup
- `anime_genres`: 逆索引 `(genre_name, anime_id)` — ジャンル → 作品一覧
- `anime_studios`: `(studio_id, is_main)` — スタジオ代表作の抽出
- `feat_*` は既に `iv_score`, `first_year` 等に index あり — 維持

**測定方針**: 全インデックス追加後 `pixi run pipeline` を走らせ、`EXPLAIN QUERY PLAN` で hot query が `SEARCH TABLE ... USING INDEX` になることを確認。SCAN なら index 未使用 → 再検討。

#### 1.3.4 外部キー + `PRAGMA foreign_keys = ON`

- SQLite は外部キーをデフォルト OFF。`src/database.py: get_connection()` で `conn.execute("PRAGMA foreign_keys = ON")` を必ず実行 (現状未実施のはず。要確認)
- 新設テーブルは FOREIGN KEY を必ず書く。ON DELETE CASCADE は anime 削除時に junction も消えるパターンで使う:
  ```sql
  FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
  ```
- これにより「孤児レコード」を物理的に不可能にする → データ品質ゲート

#### 1.3.5 自己説明のための `_description` メタカラム

- `meta_lineage` に `description TEXT NOT NULL` を追加。各 meta_* テーブルが「何を表すか」を 1-2 文で記述
- レポート生成時、Method Note の導入文として**この description をそのまま使う** → ドキュメントとコードの乖離を防ぐ

#### 1.3.6 genres / tags の正規化

- **JSON 文字列を silver に残さない**。`src/analysis/genre/*` が JSON を都度 parse するのは非効率 (pipeline 毎回で O(N) parse × 作品数)
- `anime_genres (anime_id, genre_name)`, `anime_tags (anime_id, tag_name, rank)` に分解すれば、SQL で `GROUP BY genre_name` がインデックス経由でできる
- bronze には JSON のまま残す (ソースの生データ保存責務)

#### 1.3.7 `studios` JSON カラムは廃止

- `anime.studios` JSON は `anime_studios(anime_id, studio_id, is_main)` の完全な冗長コピー。非効率かつ同期ずれの温床
- silver.anime から JSON 列を落とし、analysis は `anime_studios` JOIN に切替 (Task 1-5 の 4)

---

## 2. Phase 1: データ層の再構築

**目標**: silver 層から `anime_display` と legacy `anime` を物理削除し、`anime_analysis` を `anime` にリネームして**canonical 単一 silver** を確立する。bronze への表示用アクセスは単一 helper 経由に限定する。

### 2.1 Task 1-1: silver `anime` テーブルの canonical 化

**場所**: `src/database.py` の `init_db()` 内。

**方針**:

1. 既存 `CREATE TABLE anime_analysis` (`src/database.py:289`) を `anime` に改名
2. 既存 `CREATE TABLE anime` (legacy, score 付き) の定義を削除
3. 既存 `CREATE TABLE anime_display` の定義を削除
4. 新規 `anime_genres`, `anime_tags` を追加
5. インデックス名も `idx_anime_*` に統一

**新 DDL (canonical silver.anime)** — self-documenting で CHECK 制約付き:

```sql
CREATE TABLE IF NOT EXISTS anime (
    id            TEXT    PRIMARY KEY,                      -- 'anilist:N' / 'ann:N' / 'keyframe:slug' 等
    title_ja      TEXT    NOT NULL DEFAULT '',
    title_en      TEXT    NOT NULL DEFAULT '',
    year          INTEGER,                                   -- 放送/公開年 (NULL 可: 未確定作品用)
    season        TEXT    CHECK (season IN ('WINTER','SPRING','SUMMER','FALL')),
    quarter       INTEGER CHECK (quarter BETWEEN 1 AND 4),   -- season または start_date から導出
    episodes      INTEGER CHECK (episodes IS NULL OR episodes > 0),
    format        TEXT    CHECK (format IN ('TV','MOVIE','OVA','ONA','SPECIAL','MUSIC')),
    duration      INTEGER CHECK (duration IS NULL OR duration > 0),  -- 1話あたり分; production_scale の入力
    start_date    TEXT,                                      -- ISO 8601 'YYYY-MM-DD' (string で保持)
    end_date      TEXT,
    status        TEXT    CHECK (status IN ('FINISHED','RELEASING','NOT_YET_RELEASED','CANCELLED','HIATUS') OR status IS NULL),
    source        TEXT,                                      -- 原作タイプ (ORIGINAL/MANGA/LIGHT_NOVEL 等)
    work_type     TEXT    CHECK (work_type IN ('tv','tanpatsu') OR work_type IS NULL),
    scale_class   TEXT    CHECK (scale_class IN ('large','medium','small') OR scale_class IS NULL),
    mal_id        INTEGER,
    anilist_id    INTEGER,
    ann_id        INTEGER,
    allcinema_id  INTEGER,
    madb_id       TEXT,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(mal_id),
    UNIQUE(anilist_id)
);

-- 意図的に含めない (silver 層からの score 汚染防止):
--   score, popularity, popularity_rank, favourites, mean_score
--   description, cover_*, banner, site_url
--   genres/tags/studios/synonyms (JSON) → 正規化テーブルへ

CREATE INDEX IF NOT EXISTS idx_anime_year      ON anime(year);
CREATE INDEX IF NOT EXISTS idx_anime_format    ON anime(format);
CREATE INDEX IF NOT EXISTS idx_anime_anilist   ON anime(anilist_id);
CREATE INDEX IF NOT EXISTS idx_anime_year_fmt  ON anime(year, format);  -- 年×フォーマット集計 hotpath
```

**新規正規化テーブル** (JSON 分解先、FK + 逆索引):

```sql
CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id   TEXT NOT NULL,
    genre_name TEXT NOT NULL,
    PRIMARY KEY (anime_id, genre_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_anime_genres_genre ON anime_genres(genre_name, anime_id);
--   ↑ ジャンル → 作品 の逆索引。"Action 作品は?" が O(log N) で引ける

CREATE TABLE IF NOT EXISTS anime_tags (
    anime_id TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
    PRIMARY KEY (anime_id, tag_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_anime_tags_tag ON anime_tags(tag_name, rank DESC, anime_id);
```

**credits テーブルの見直し** (efficient + 自己説明):

```sql
CREATE TABLE IF NOT EXISTS credits (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id   TEXT    NOT NULL,
    anime_id    TEXT    NOT NULL,
    role        TEXT    NOT NULL,                 -- Role enum 24 種 (src/utils/role_groups.py)
    raw_role    TEXT,                             -- 元ソースの生の役職名
    episode     INTEGER CHECK (episode IS NULL OR episode > 0),
                                                  -- NULL = 全話通し (作品レベル)
                                                  -- 整数 = 特定話数 (話数指定クレジット)
    source      TEXT    NOT NULL,                 -- 'anilist'/'ann'/'allcinema'/'seesaawiki'/'keyframe'
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(person_id, anime_id, role, episode, source),   -- source を UNIQUE に追加 (同一 credit 同一 role を複数ソースから拾う場合がある)
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (anime_id)  REFERENCES anime(id)
);

CREATE INDEX IF NOT EXISTS idx_credits_person       ON credits(person_id);
CREATE INDEX IF NOT EXISTS idx_credits_anime        ON credits(anime_id);
CREATE INDEX IF NOT EXISTS idx_credits_role         ON credits(role);
CREATE INDEX IF NOT EXISTS idx_credits_anime_role   ON credits(anime_id, role);     -- team 分析 hotpath
CREATE INDEX IF NOT EXISTS idx_credits_person_src   ON credits(person_id, source);  -- entity resolution
```

**注意**: 現 `credits.episode` は `DEFAULT -1` を sentinel に使っている。migration で `episode = -1` を `NULL` に書換える必要がある (Task 1-3 参照)。

**`scores` → `person_scores` リネーム**:

```sql
ALTER TABLE scores RENAME TO person_scores;
-- anime.score と混同する命名を廃止
```

コード側も `FROM scores` / `UPDATE scores` を `person_scores` に一括置換する。

**禁止カラム** (silver の anime テーブルに追加してはならない):
- `score`, `popularity`, `popularity_rank`, `favourites`, `mean_score`
- `description`, `cover_*`, `banner`, `site_url`
- `genres`, `tags`, `studios`, `synonyms` (JSON 形式での保持)

上記は**すべて bronze (`src_anilist_anime` 等) のみに残す**。

### 2.2 Task 1-2: display helper モジュールの作成

**場所**: `src/utils/display_lookup.py` を新規作成

**目的**: bronze への表示用アクセスを単一関数群に集約し、`rg` で使用箇所を全列挙できるようにする。

**インターフェイス例**:

```python
"""
bronze から anime.score などの視聴者指標を「表示用」として取得するヘルパー。

このモジュールは以下のルールに従う:
  - src/analysis/** からの import を禁止 (v2 gate で enforce)
  - レポート (scripts/report_generators/**) からのみ呼び出し可
  - 返り値は「参考値」として表示する用途限定
  - スコアリング公式・edge weight・最適化ターゲット・分類境界には絶対に使わない

違反を検知する手段:
  rg 'from src.utils.display_lookup' src/analysis/  # 0 件であるべき
  rg 'get_display_score' src/analysis/              # 0 件であるべき
"""
from __future__ import annotations
import sqlite3
from typing import Optional

def get_display_score(conn: sqlite3.Connection, anime_id: str) -> Optional[float]:
    """AniList/MAL 視聴者評価を返す。表示専用。分析に使うな。

    anime_id は silver の anime.id ("anilist:123" 等)。
    """
    row = conn.execute(
        "SELECT anilist_id FROM anime WHERE id = ?", (anime_id,)
    ).fetchone()
    if not row or row[0] is None:
        return None
    src = conn.execute(
        "SELECT score FROM src_anilist_anime WHERE anilist_id = ?", (row[0],)
    ).fetchone()
    return src[0] if src else None

def get_display_description(conn, anime_id: str) -> Optional[str]:
    ...  # 同様に bronze から description を返す

def get_display_cover_url(conn, anime_id: str) -> Optional[str]:
    ...

def get_display_popularity_rank(conn, anime_id: str) -> Optional[int]:
    ...

def get_display_favourites(conn, anime_id: str) -> Optional[int]:
    ...

def get_display_genres(conn, anime_id: str) -> list[str]:
    """表示用 (JSON のまま)。分析用途なら anime_genres 正規化テーブルを使え。"""
    ...
```

**運用ルール**:
- 新規フィールドを bronze から取り出したくなったら、このモジュールにヘルパーを追加する
- 関数名は必ず `get_display_*` で統一 → lint で呼び出し箇所を機械検知可能
- 関数 docstring に「分析に使うな」を明記
- レポートコード内でこのヘルパーを呼んだ行は**必ず隣にコメント**で「表示用に参考値として取得」と記す (コードレビューの目印)

### 2.3 Task 1-3: スキーマ migration v47→v48 (canonical rename + 正規化)

**場所**: `src/database.py` の `_run_migrations()` 内。schema version を v47 → v48 に進める。

**方針** (destructive OK):
- `anime_analysis` → `anime` に rename (canonical silver を確立)
- legacy `anime` は DROP (必要データは既に `anime_analysis` に複製済みのはず。不足分は bronze から再充填)
- `anime_display` は DROP (silver から display を完全撤去)
- `scores` → `person_scores` に rename
- `credits.episode = -1` を `NULL` に書換
- `anime_genres` / `anime_tags` を新設し bronze から展開
- `PRAGMA foreign_keys = ON` を get_connection で ON に

**手順**:

```sql
BEGIN TRANSACTION;

-- (0) legacy `anime` に anime_analysis が持たない行があれば補填 (破壊的 rename の前段)
--     anime_analysis に存在しない id を旧 anime から持ってくる
INSERT OR IGNORE INTO anime_analysis
    (id, title_ja, title_en, year, season, episodes, mal_id, anilist_id, updated_at)
SELECT id, title_ja, title_en, year, season, episodes, mal_id, anilist_id, updated_at
FROM anime  -- legacy
WHERE id NOT IN (SELECT id FROM anime_analysis);

-- (1) legacy `anime` を DROP (silver から score 汚染源を消す)
DROP TABLE anime;

-- (2) anime_display を DROP (silver から display 責務を完全撤去)
DROP TABLE anime_display;

-- (3) 正規化テーブルを先に作成 (まだ無ければ)
CREATE TABLE IF NOT EXISTS anime_genres (
    anime_id   TEXT NOT NULL,
    genre_name TEXT NOT NULL,
    PRIMARY KEY (anime_id, genre_name),
    FOREIGN KEY (anime_id) REFERENCES anime_analysis(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_anime_genres_genre ON anime_genres(genre_name, anime_id);

CREATE TABLE IF NOT EXISTS anime_tags (
    anime_id TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
    PRIMARY KEY (anime_id, tag_name),
    FOREIGN KEY (anime_id) REFERENCES anime_analysis(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_anime_tags_tag ON anime_tags(tag_name, rank DESC, anime_id);

-- (4) bronze の JSON を正規化テーブルへ展開
INSERT OR IGNORE INTO anime_genres (anime_id, genre_name)
SELECT a.id, je.value
FROM anime_analysis a
JOIN src_anilist_anime s ON s.anilist_id = a.anilist_id,
     json_each(COALESCE(s.genres, '[]')) je
WHERE je.value IS NOT NULL;

INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank)
SELECT a.id,
       json_extract(je.value, '$.name'),
       json_extract(je.value, '$.rank')
FROM anime_analysis a
JOIN src_anilist_anime s ON s.anilist_id = a.anilist_id,
     json_each(COALESCE(s.tags, '[]')) je
WHERE json_extract(je.value, '$.name') IS NOT NULL;

-- (5) credits.episode の sentinel -1 を NULL に正規化
UPDATE credits SET episode = NULL WHERE episode = -1;

-- (6) scores → person_scores にリネーム
ALTER TABLE scores RENAME TO person_scores;

-- (7) 既存 anime_analysis を canonical 名 `anime` に rename
ALTER TABLE anime_analysis RENAME TO anime;
-- インデックスは自動追従するが、名前は idx_anime_analysis_* のまま残るので改名:
DROP INDEX IF EXISTS idx_anime_analysis_year;
DROP INDEX IF EXISTS idx_anime_analysis_format;
DROP INDEX IF EXISTS idx_anime_analysis_anilist;
CREATE INDEX IF NOT EXISTS idx_anime_year     ON anime(year);
CREATE INDEX IF NOT EXISTS idx_anime_format   ON anime(format);
CREATE INDEX IF NOT EXISTS idx_anime_anilist  ON anime(anilist_id);
CREATE INDEX IF NOT EXISTS idx_anime_year_fmt ON anime(year, format);

-- (8) credits ホットパス index を追加
CREATE INDEX IF NOT EXISTS idx_credits_anime_role  ON credits(anime_id, role);
CREATE INDEX IF NOT EXISTS idx_credits_person_src  ON credits(person_id, source);

-- (9) meta_lineage から source_display_allowed カラムを DROP
ALTER TABLE meta_lineage DROP COLUMN source_display_allowed;
-- 代わりに description カラムを追加 (自己説明)
ALTER TABLE meta_lineage ADD COLUMN description TEXT NOT NULL DEFAULT '';

UPDATE schema_meta SET version = '48';

COMMIT;
```

**追加: `get_connection()` で FK を ON**:

```python
def get_connection(path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")      # ← 追加
    return conn
```

**注意**:
- 全て 1 transaction 内で実行 (失敗時ロールバック)
- migration は idempotent に: 各 DDL で実在チェック (`PRAGMA table_info`、`sqlite_master`) を噛ませ、既実行でも落ちないこと
- bronze (`src_anilist_anime.score` など) は**無変更**。score の復元が必要なら bronze から取れる
- DROP TABLE `anime` / `anime_display` のリスク: FK 参照が壊れるテーブル (credits.anime_id, anime_studios.anime_id 等) は PRAGMA foreign_keys=OFF で実行するか、migration 完了後に FK を再張る
- SQLite 3.35+ で `DROP COLUMN` 使用可能 (pixi で入る sqlite は 3.46+ なので問題なし)

### 2.4 Task 1-4: ETL (`src/etl/integrate.py`) を修正

**場所**: `src/etl/integrate.py`

**修正内容** (destructive OK):

1. 現 ETL は `upsert_anime()` (legacy) と `upsert_anime_analysis()` / `upsert_anime_display()` の 3 系統を呼んでいる (`src/etl/integrate.py:69,15,44`)
2. `upsert_anime_display()` 呼び出しを**全削除**
3. `upsert_anime()` 呼び出しを**全削除**
4. `upsert_anime_analysis()` のみ残し、関数名を `upsert_anime()` にリネーム (migration で table が `anime` に rename されたので名前も合わせる)
5. ETL 終端で `anime_genres` / `anime_tags` を bronze から展開する一段を追加 (migration の (4) と同等を毎回の ETL で実行)

**具体的変更** (integrate_anilist 例):

```python
# 既存 (line 67-100 付近): Anime モデル + upsert_anime + upsert_anime_analysis + upsert_anime_display
# ↓ 以下に置き換える (destructive)

from src.models import AnimeAnalysis  # Anime シムは削除済み (Task 1-6)

anime = AnimeAnalysis(
    id=f"anilist:{row['anilist_id']}",
    title_ja=row["title_ja"] or "",
    title_en=row["title_en"] or "",
    year=row["year"],
    season=row["season"],
    episodes=row["episodes"],
    format=row["format"],
    status=row["status"],
    start_date=row["start_date"],
    end_date=row["end_date"],
    duration=row["duration"],
    source=row["source"],
    mal_id=row["mal_id"],
    anilist_id=row["anilist_id"],
    # score / description / cover_* / banner / site_url は AnimeAnalysis にそもそも無い
    # JSON (genres/tags/studios/synonyms) も持たない
)
upsert_anime(conn, anime)  # 内部で silver.anime に INSERT OR REPLACE

# genres / tags を正規化テーブルへ展開
for g in json.loads(row["genres"] or "[]"):
    conn.execute(
        "INSERT OR IGNORE INTO anime_genres (anime_id, genre_name) VALUES (?, ?)",
        (anime.id, g),
    )
for t in json.loads(row["tags"] or "[]"):
    tag = t if isinstance(t, str) else t.get("name")
    rank = t.get("rank") if isinstance(t, dict) else None
    if tag:
        conn.execute(
            "INSERT OR IGNORE INTO anime_tags (anime_id, tag_name, rank) VALUES (?, ?, ?)",
            (anime.id, tag, rank),
        )

# studios は既存 anime_studios テーブル経由 (既存ロジック流用)
```

**`upsert_anime()` の防衛ガード** (`src/database.py`):

```python
# 旧 upsert_anime (legacy) は削除。旧 upsert_anime_analysis() を以下に改名・強化する。
_FORBIDDEN_SILVER_COLUMNS = frozenset({
    "score", "popularity", "popularity_rank", "favourites", "mean_score",
    "description", "cover_large", "cover_medium", "cover_extra_large",
    "cover_large_path", "banner", "banner_path", "site_url",
    "genres", "tags", "studios", "synonyms",
})

def upsert_anime(conn: sqlite3.Connection, anime: AnimeAnalysis) -> None:
    """silver.anime テーブルへの upsert。

    AnimeAnalysis 型しか受け付けない (Pydantic で静的にも汚染カラムを排除)。
    ランタイム防衛線として dict 系のオーバーロードは削除する。
    """
    if not isinstance(anime, AnimeAnalysis):
        raise TypeError(
            f"upsert_anime requires AnimeAnalysis, got {type(anime).__name__}. "
            "Do not pass dicts with bronze columns."
        )
    row = anime.model_dump(exclude_none=True)
    leaked = _FORBIDDEN_SILVER_COLUMNS & set(row.keys())
    if leaked:
        raise ValueError(
            f"Forbidden columns in silver.anime upsert: {leaked}. "
            "These belong in bronze (src_*) only; accessed via display_lookup."
        )
    # INSERT OR REPLACE ... (allowed columns only)
```

- `upsert_anime_display()` は**関数ごと削除**
- すべての呼び出し箇所を grep で洗い出し、置換:
  ```bash
  rg -l 'upsert_anime_display|upsert_anime_analysis' src/ tests/ scripts/
  ```

### 2.5 Task 1-5: `src/analysis/**` の `anime.score` / JSON カラム参照を全除去

**スコープ**: `src/analysis/` 配下の全モジュール。anime.score に加え、JSON カラム (`anime.studios`, `anime.genres`, `anime.tags`) も同時に移行する (silver の物理カラム削除に伴い、`anime_studios` / `anime_genres` / `anime_tags` への JOIN に切替える)。

**手順**:

1. grep で以下を列挙:
   - `anime.score`, `a.score`, `a\.score`, `\.score`
   - `anime.popularity`, `anime.favourites`, `anime.description`, `anime.cover_*`, `anime.banner`, `anime.site_url`
   - `FROM anime ...` で SELECT 句に score/popularity 等を含む箇所
2. 各箇所の扱い:
   - **分析 (scoring, graph weight, optimization)** → 完全削除。もし指標計算に必要なら設計を見直し (anime.score 汚染の新規発見として `detailed_todo.md` に追記)
   - **display (HTML 出力, JSON export で情報表示)** → analysis 層から display 層へ責務移動。`src/analysis/` では score を扱わない。レポート生成側 (`scripts/report_generators/`) で `display_lookup.get_display_score()` を呼ぶ
   - **グラフノード属性** → 削除 (可視化で score を見たい場合はレポート側で付与)

3. 以下のファイルは**要注意** (現状 anime.score 等を参照):
   - `src/analysis/graph.py:88` — グラフノード属性の score=a.score を削除
   - `src/analysis/genre/affinity.py:71-76` — scores 配列を削除 (genre affinity 計算に anime.score は不要)
   - `src/analysis/studio/profile.py:66` — anime_scores 削除
   - `src/analysis/studio/timeseries.py:76` — 同上
   - `src/analysis/time_series.py:81-82` — year_scores 削除
   - `src/analysis/decade_analysis.py:64` — yd["scores"] 削除
   - `src/analysis/anime_stats.py:61,165,178` — "score" key を dict から外す
   - `src/analysis/growth.py:43,105-108` — career_scores 削除
   - `src/analysis/explain.py:86,344,445` — "score" 出力を削除 (レポート側で display_lookup)
   - `src/analysis/team_composition.py:67` — anime_score 削除
   - `src/analysis/anime_prediction.py:79` — "score" 出力を削除
   - `src/analysis/neo4j_direct.py:220,236` — Neo4j export の score 列を削除 (表示用 Neo4j が欲しければ別 graph を作る)
   - `src/analysis/neo4j_export.py:111` — 同上
   - `src/pipeline_phases/analysis_modules.py:381` — `anime_scores` dict 構築を削除
   - `src/pipeline_phases/entity_resolution.py:341` — entity マージで score 伝搬している箇所を削除
   - `src/pipeline_phases/export_and_viz.py:1098` — export 時の score 出力を削除 (display_lookup 経由で再追加したい場合はレポート生成側で)

4. **JSON カラム → 正規化テーブルへの切替** (anime.studios / genres / tags):
   - `anime.studios` 参照は以下を必ず含む (2026-04-19 時点の `rg 'anime\.studios|a\.studios'` より):
     - `src/analysis/scoring/akm.py:127,141,145,146,341,357,359` — studio 帰属先決定。`anime_studios` を JOIN し `is_main=1` を優先してから残りの studio を取る形に書き換え
     - `src/analysis/career_friction.py:196-197`, `src/analysis/talent_pipeline.py:85,90,95-96`, `src/analysis/explain.py:64-65`, `src/analysis/scoring/individual_contribution.py:90`, `src/analysis/ml_homonym_split.py:353` — いずれも `anime.studios` list アクセス。`studios_by_anime: dict[str, list[str]]` を `anime_studios` JOIN で 1 回構築して配るヘルパーを新設 (多重クエリ防止)
   - `anime.genres` / `anime.tags` 参照は `src/analysis/genre/*` が主。`anime_genres` / `anime_tags` への JOIN に切替
   - ヘルパー案: `src/analysis/studios_index.py` に `build_studio_index(conn) -> dict[anime_id, list[studio_id]]` を置く。Phase 1 (data_loading) で 1 回だけ実行し PipelineContext に載せる

**確認方法**:

```bash
# analysis 配下に anime.score 参照が無いことを確認
rg -n '\ba\.score\b|\banime\.score\b' src/analysis/ src/pipeline_phases/

# analysis 配下から display_lookup を import していないことを確認
rg 'from src\.utils\.display_lookup|get_display_' src/analysis/ src/pipeline_phases/

# silver の anime テーブルから score を SELECT していないことを確認
rg -n 'SELECT[^;]*\bscore\b[^;]*FROM anime\b' src/
```

### 2.6 Task 1-6: Pydantic モデル `Anime` の整理

**場所**: `src/models.py`

**現状**: `AnimeAnalysis` (score 無) と `AnimeDisplay` (score/画像/description 等) は既に存在する (`src/models.py:1311, 1349`)。問題は `Anime(AnimeAnalysis)` という**後方互換シム** (`src/models.py:1381`) が score/description/cover_* を追加で持っており、silver layer に score を運ぶ抜け穴になっている。

**修正方針** (destructive OK):

- `Anime` シムクラスを**削除**する (`src/models.py:1381-`)
- 全ての `from src.models import Anime` を `from src.models import AnimeAnalysis as Anime` か、型注釈を `AnimeAnalysis` に書き換え。短期的には alias を置き、その後 alias も削除
- `AnimeDisplay` は bronze 層相当 (raw に近い) なので、scraper が AnimeDisplay を生成して bronze (`src_anilist_anime` 等) に書き込む構成を維持
- ETL (`src/etl/integrate.py`) は bronze → `AnimeAnalysis` 変換のみを行う。score / 画像 / description には一切触れない (Task 1-4 の FORBIDDEN guard で静的にも blocked)

**具体的な変更点**:

1. `src/models.py`:
   - `class Anime(AnimeAnalysis):` ブロック全体を削除
   - 必要なら `Anime = AnimeAnalysis` を一時的に置く (import site での一括 refactor 後に削除)
2. scraper / pipeline / tests: `Anime` import を `AnimeAnalysis` に置換
3. 既存の `.score` アクセスは Task 1-5 で削除済みのため、`AnimeAnalysis` には存在しないので安全

**確認コマンド**:
```bash
# Anime シムが削除されていることを確認
rg -n '^class Anime\b' src/models.py  # 0 件

# 旧 Anime import が残っていないことを確認
rg -n 'from src\.models import.*\bAnime\b' src/ tests/ scripts/
# → AnimeAnalysis / AnimeDisplay / AnimeRelation / AnimeStudio 以外はヒット無しが理想
```

### 2.7 Task 1-7: import-time assertion で汚染を enforce

**場所**: `src/analysis/__init__.py` (なければ作成)

```python
"""
Runtime guard: src.analysis.* 配下は display_lookup や src_* (bronze) を参照してはならない。
"""
import sys

class _AnalysisImportGuard:
    """analysis パッケージ内のモジュールが display_lookup / bronze を import したら例外。"""
    _FORBIDDEN = ("src.utils.display_lookup",)

    def find_module(self, name, path=None):
        if name in self._FORBIDDEN:
            frame = sys._getframe(1).f_globals.get("__name__", "")
            if frame.startswith("src.analysis.") or frame.startswith("src.pipeline_phases."):
                raise ImportError(
                    f"{frame} must not import {name}. "
                    "display_lookup accesses bronze; analysis layer must stay on silver."
                )
        return None

# 本格実装では lint (4.x) で静的に捕らえる。ここは動的セーフティネット
```

実装優先度は低い (4.x の lint を優先)。時間余裕があれば入れる。

### 2.8 Phase 1 の完了判定

以下がすべて満たされたら Phase 1 完了:

- [ ] `PRAGMA table_info(anime);` で score / popularity / favourites / description / cover_* / banner カラムが**存在しない**
- [ ] `anime_genres`, `anime_tags` テーブルが作成され、データが入っている
- [ ] `src/utils/display_lookup.py` が存在し、レポート層から呼び出し可
- [ ] `src/etl/integrate.py` が silver に score 等を書かない
- [ ] `scripts/maintenance/seed_medallion.py` が成功
- [ ] `rg '\ba\.score\b|\banime\.score\b' src/analysis/ src/pipeline_phases/` が 0 件 (コメント除く)
- [ ] `rg 'from src\.utils\.display_lookup' src/analysis/ src/pipeline_phases/` が 0 件
- [ ] 既存 1947 テストが green
- [ ] `pixi run pipeline` が正常終了
- [ ] `pixi run lint` が clean

---

## 3. Phase 2: Gold 層 (meta_*) と lineage 書式

**目標**: レポート直読用の gold テーブル群を新設し、各テーブルに自己記述メタデータ (lineage) を持たせる。

### 3.1 Task 2-1: meta_\* テーブル共通仕様

**全 meta_\* テーブルは以下 2 パートからなる**:

**(a) データ本体**: レポートが直接 render するカラム群 (テーブルごとに固有)

**(b) lineage metadata**: 別テーブル `meta_lineage` に一括格納

```sql
CREATE TABLE IF NOT EXISTS meta_lineage (
    table_name TEXT PRIMARY KEY,           -- 'meta_policy_attrition' 等
    audience TEXT NOT NULL CHECK (audience IN ('policy','hr','biz','common','technical_appendix')),
    description TEXT NOT NULL,              -- 1-2 文: このテーブルは何を表すか (Method Note の導入文に使う)
    source_silver_tables TEXT NOT NULL,     -- JSON array: ["anime", "credits", "persons"]
    source_bronze_forbidden INTEGER NOT NULL DEFAULT 1 CHECK (source_bronze_forbidden IN (0,1)),
                                            -- 1 なら bronze (anime.score含む) の分析参照禁止。公開レポート層は常に 1 であること
    formula_version TEXT NOT NULL,          -- 'v2.3' 等 (semver 推奨)
    computed_at TIMESTAMP NOT NULL,
    ci_method TEXT,                         -- 'bootstrap_n1000' / 'analytical_se' / 'wilson' / null
    null_model TEXT,                        -- 'permutation_n500' / 'degree_preserving' / null
    holdout_method TEXT,                    -- 'temporal_split' / '5fold_cv' / null
    row_count INTEGER,
    notes TEXT                              -- 自由記述
);
```

**運用ルール**:

- 新規 meta_\* テーブルを作る**コードは必ず** meta_lineage に 1 行 upsert する
- `source_bronze_forbidden = 1` を変更する PR はレビュー必須 (anime.score 取り込みを意味する)
- `source_silver_tables` には**実際に読んだ silver テーブル名のみ**記す。display_lookup helper の呼び出しは analysis 層から禁止されているため (Task 1-7)、meta_\* 生成経路でも原則不要。ただしレポート render 時に表示スコアを添える場合はその旨を `notes` に記述
- レポートは meta_lineage を読んで Method Note を**自動生成**する (次節 3.5)

### 3.2 Task 2-2: Person Parameter Card 用 gold

**対象レポート**: `scripts/report_generators/reports/person_parameter_card.py` (既存、要確認)

**テーブル**: `meta_common_person_parameters`

```sql
CREATE TABLE IF NOT EXISTS meta_common_person_parameters (
    person_id TEXT PRIMARY KEY,
    scale_reach_pct REAL,              -- 規模到達力 (person_fe percentile)
    scale_reach_ci_low REAL,
    scale_reach_ci_high REAL,
    collab_width_pct REAL,             -- 協業幅 (versatility percentile)
    collab_width_ci_low REAL,
    collab_width_ci_high REAL,
    continuity_pct REAL,               -- 継続力 (1 - CV)
    continuity_ci_low REAL,
    continuity_ci_high REAL,
    mentor_contribution_pct REAL,      -- 育成貢献 (mentor residual percentile)
    mentor_contribution_ci_low REAL,
    mentor_contribution_ci_high REAL,
    centrality_pct REAL,               -- 中心性 (weighted PageRank)
    centrality_ci_low REAL,
    centrality_ci_high REAL,
    trust_accum_pct REAL,              -- 信頼蓄積
    trust_accum_ci_low REAL,
    trust_accum_ci_high REAL,
    role_evolution_pct REAL,           -- 役割進化
    role_evolution_ci_low REAL,
    role_evolution_ci_high REAL,
    genre_specialization_pct REAL,     -- ジャンル特化
    genre_specialization_ci_low REAL,
    genre_specialization_ci_high REAL,
    recent_activity_pct REAL,          -- 直近活発度
    recent_activity_ci_low REAL,
    recent_activity_ci_high REAL,
    compatibility_pct REAL,            -- 相性指標
    compatibility_ci_low REAL,
    compatibility_ci_high REAL,
    archetype TEXT,                    -- K-means cluster label
    archetype_confidence REAL,         -- silhouette-based
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**lineage meta 登録例** (カラム名を明示する形式で):

```sql
INSERT OR REPLACE INTO meta_lineage
    (table_name, audience, description, source_silver_tables, source_bronze_forbidden,
     formula_version, computed_at, ci_method, null_model, holdout_method, row_count, notes)
VALUES (
    'meta_common_person_parameters',
    'common',
    '各人物の 10 軸プロファイル (規模到達力・協業幅・継続力 等) と K=6 クラスタ由来のアーキタイプ。視聴者評価は一切使っていない。',
    '["anime", "credits", "persons", "feat_person_scores", "feat_career", "feat_genre_affinity"]',
    1,
    'v2.0',
    CURRENT_TIMESTAMP,
    'bootstrap_n1000',
    'degree_preserving_rewiring_n500',
    NULL,
    (SELECT COUNT(*) FROM meta_common_person_parameters),
    '10 axes per Person Parameter Card; archetypes by K-means K=6'
);
```

**既存資材**: `src/analysis/person_parameters.py` が既に存在。そこから JSON を出す代わりに meta_common_person_parameters を upsert する経路を追加。既存の JSON 出力は deprecate せず並存 (Phase 3 完了まで)

### 3.3 Task 2-3: 各 audience brief の meta_\* 設計

**設計指針**:
- 1 report = 1〜2 meta tables
- report 側は「SELECT * FROM meta_\*」で済むくらい事前集計する
- CI 列は原則 `*_ci_low`, `*_ci_high` のペアで持つ (再計算せず表示できる)
- 集計粒度はレポートのチャートに合わせる (年×スタジオ、etc.)

以下、各テーブルの**定義スケルトン**のみ示す (具体 DDL は 3.2 のパターンを踏襲)。

#### 政策提言 Brief

| meta テーブル | 集計粒度 | キー指標 | CI 要件 | null model |
|--------------|---------|---------|---------|-----------|
| `meta_policy_attrition` | cohort (debut_year) × treatment (tier/director/chaos/load) | ATE 点推定、HR | DML asymptotic SE × 1.96 | placebo |
| `meta_policy_monopsony` | year × studio | HHI, HHI\*, logit(stay) β | bootstrap | — |
| `meta_policy_gender` | transition_stage × cohort | 生存確率、log-rank χ² | analytical | — |
| `meta_policy_generation` | cohort × career_year_bin | S(k) 生存率、gen pyramid | bootstrap | — |

#### 人材評価 (現場効率化) Brief

| meta テーブル | 集計粒度 | キー指標 | CI 要件 | 認証要否 |
|--------------|---------|---------|---------|---------|
| `meta_hr_studio_benchmark` | studio × year | R5 定着率、VA, H_s, Attraction | Wilson + bootstrap | 公開可 |
| `meta_hr_mentor_card` | director_id | M̂_d (EB-shrunk) | bootstrap, null permutation | 公開可 |
| `meta_hr_attrition_risk` | person_id (new hires) | 予測リスク + SHAP top5 | calibration plot, C-index | **認証必須** |
| `meta_hr_succession` | veteran_id × candidate_id (aggregate) | successor score | — | aggregate 公開 |

#### 新たな試み提案 Brief

| meta テーブル | 集計粒度 | キー指標 | CI 要件 |
|--------------|---------|---------|---------|
| `meta_biz_whitespace` | genre × year | CAGR, penetration, W_g | bootstrap |
| `meta_biz_undervalued` | person_id (archetype-tagged) | U_p, archetype | — |
| `meta_biz_trust_entry` | gatekeeper_id (aggregate) | G_p, Reach_p | — |
| `meta_biz_team_template` | cluster × tier | template profile | silhouette |
| `meta_biz_independent_unit` | community_id | coverage, density, V_G | — |

### 3.4 Task 2-4: 既存 feat_\* と meta_\* の関係整理

**原則**:
- `feat_*` は **再利用される原料** (他 feat_* や meta_* の入力になる)
- `meta_*` は **レポート終端の出力** (他テーブルの入力にならない)

**feat_\* の扱い方針**:

| feat_ table | 扱い |
|-------------|------|
| feat_person_scores | 維持。multiple meta_\* の原料 |
| feat_network | 維持 |
| feat_career, feat_career_annual, feat_career_gaps | 維持 |
| feat_genre_affinity | 維持 |
| feat_contribution, feat_credit_contribution, feat_credit_activity | 維持 |
| feat_birank_annual | 維持 |
| feat_studio_affiliation | 維持 |
| feat_person_work_summary | 維持 |
| feat_work_context | 維持 |
| feat_person_role_progression | 維持 |
| feat_causal_estimates | **meta_policy_attrition に統合候補**。レポート直読されてないなら削除 |
| feat_cluster_membership | 維持 (複数レポートで使用) |
| feat_mentorships | **meta_hr_mentor_card に統合候補** |
| agg_milestones | 維持 |
| agg_director_circles | 維持 |

**判定手順**:
1. 各 feat_\* について `rg -l 'feat_xxx' scripts/report_generators/` で使用レポート数を数える
2. 1 レポートからしか読まれていない feat_\* は、対応する meta_\* に統合して feat は削除検討
3. 2 レポート以上から読まれている feat_\* は原料として維持

### 3.5 Task 2-5: Method Note 自動生成

**場所**: `scripts/report_generators/section_builder.py`

**追加機能**: `SectionBuilder.method_note_from_lineage(table_name)` メソッド

```python
def method_note_from_lineage(self, table_name: str, conn) -> str:
    """meta_lineage を読んで Method Note を HTML で自動生成。"""
    row = conn.execute(
        "SELECT * FROM meta_lineage WHERE table_name = ?", (table_name,)
    ).fetchone()
    if row is None:
        raise ValueError(f"No lineage registered for {table_name}")
    # HTML 構築:
    # - 使用 silver テーブル一覧
    # - source_bronze_forbidden の確認文言 (anime.score を使用していない旨)
    # - CI 手法、null model、holdout 手法
    # - formula_version, computed_at
    # - 免責事項 (JA + EN)
    return html
```

**効用**: v2 Philosophy の「method note 義務」を**レポートの書き手から取り除き**、データ層に記録された事実から自動生成される。手書き method note は禁止に。

---

## 4. Phase 3: レポート再編 (3 audience)

**目標**: 35 本のレポートを audience-driven に再編。半減を目標。

### 4.1 Task 3-1: 現 35 レポートの振り分け棚卸し

**出力**: `docs/REPORT_INVENTORY.md` を新規作成

**列**:
- report_file (path)
- 現在のクラス名 (V2_REPORT_CLASSES から)
- 新 audience 分類: {policy / hr / biz / common / technical_appendix / archived / deleted}
- 読む meta_\* テーブル (複数可)
- 統合先レポート (複数レポートを 1 本に統合する場合)
- 決定理由 (1 行)

**対象レポート** (`scripts/report_generators/reports/` 配下):

```
_base.py                       → 維持 (infra)
akm_diagnostics.py             → technical_appendix
anime_value_report.py          → archived (anime.score 依存の名残)
bias_detection.py              → common
biz_genre_whitespace.py        → biz (新設済み。meta_biz_whitespace へ接続)
biz_independent_unit.py        → biz
biz_team_template.py           → biz
biz_trust_entry.py             → biz
biz_undervalued_talent.py      → biz
bridge_analysis.py             → technical_appendix (bridges は biz_trust_entry に吸収)
career_dynamics.py             → policy or hr (統合先決定要)
career_friction_report.py      → policy (離職まわり)
career_transitions.py          → policy
cohort_animation.py            → technical_appendix (visualization heavy)
compatibility.py               → hr (meta_hr_team_chemistry に接続)
compensation_fairness.py       → policy
cooccurrence_groups.py         → technical_appendix
credit_statistics.py           → common (overview)
derived_params.py              → technical_appendix
dml_causal_inference.py        → technical_appendix (メソッド説明。本体は policy 吸収)
exit_analysis.py               → policy (policy_attrition に統合)
expected_ability.py            → hr
genre_analysis.py              → biz (whitespace に吸収 or 並存)
growth_scores.py               → hr or common
index_page.py                  → common (全体目次)
industry_analysis.py           → policy
industry_overview.py           → common (executive summary)
knowledge_network.py           → technical_appendix
longitudinal_analysis.py       → technical_appendix (41 charts 巨大)
madb_coverage.py               → technical_appendix (data statement)
mgmt_attrition_risk.py         → hr (meta_hr_attrition_risk へ)
mgmt_director_mentor.py        → hr (meta_hr_mentor_card へ)
mgmt_studio_benchmark.py       → hr (meta_hr_studio_benchmark へ)
mgmt_succession.py             → hr
mgmt_team_chemistry.py         → hr
ml_clustering.py               → technical_appendix
network_analysis.py            → technical_appendix
network_evolution.py           → technical_appendix
network_graph.py               → technical_appendix
person_parameter_card.py       → common (全 audience 共通基盤)
person_ranking.py              → ??? 保留。ranking は evaluative で危険。アーカイブ候補
policy_attrition.py            → policy (meta_policy_attrition へ)
policy_gender_bottleneck.py    → policy (meta_policy_gender へ)
policy_generational_health.py  → policy
policy_monopsony.py            → policy (meta_policy_monopsony へ)
score_layers_analysis.py       → technical_appendix
shap_explanation.py            → technical_appendix
structural_career.py           → common or policy
studio_impact.py               → hr (mgmt_studio_benchmark に統合)
studio_timeseries.py           → hr (mgmt_studio_benchmark に統合)
team_analysis.py               → hr (team chemistry に統合)
temporal_foresight.py          → technical_appendix
```

**振り分け後の目標件数**:
- policy: 4-6 本
- hr: 4-6 本
- biz: 4-6 本
- common: 2-3 本 (Person Parameter Card, Index, Industry Overview executive summary)
- technical_appendix: 10-15 本 (監査・研究者向け)
- archived: 3-5 本 (`result/reports/archived/` へ move)
- deleted: 無し (archive 優先)

**合計**: 35 → 20〜25 本 (archived を残数カウントから除外すれば 17-20 本)

### 4.2 Task 3-2: 3 audience brief の index page 設計

**現状**: `scripts/report_generators/reports/index_page.py` が 1 枚の index を生成

**変更**:
- `index_page.py` は top-level landing page になる
- 新規作成:
  - `policy_brief_index.py` (政策提言 Brief の目次 + executive summary)
  - `hr_brief_index.py` (人材評価 Brief の目次 + executive summary)
  - `biz_brief_index.py` (新たな試み Brief の目次 + executive summary)

**各 brief index の構造**:

```
<Brief タイトル>
  ├─ Executive Summary (2-4 page、findings only、CI 付き)
  ├─ 各レポートへのリンク (with 1-line TL;DR)
  ├─ Method Overview (各レポート共通の統計手法)
  ├─ Data Statement (source coverage, bias, limitations)
  └─ Disclaimers (JA + EN)
```

### 4.3 Task 3-3: 「人材評価」語彙の置換

**危険性**: 「人材評価」は能力評価 framing に滑りやすい。legal risk。

**対策**:
- HR Brief のタイトルは「**現場 workflow 分析**」または「**配置適合度プロファイル**」に。サブタイトルに「人材評価 (現場効率化)」
- 個別レポートは「評価」を使わない:
  - ❌ 「監督評価」→ ✅ 「監督育成貢献プロファイル」
  - ❌ 「スタッフ評価」→ ✅ 「配置適合度カード」
  - ❌ 「離職予測」→ ✅ 「離職リスクプロファイル」
- 4.x の v2 gate で禁止語 lint を必ず通す

### 4.4 Task 3-4: レポート本体の構造統一

**すべての再編後レポートは以下の構造を強制**:

```markdown
# <レポートタイトル>

## 概要 (1段落)
- 問い, 対象期間, サンプルサイズ

## Findings (事実のみ)
- 評価形容詞禁止
- 因果動詞禁止 (「〜が〜を引き起こす」禁止)
- 数値 + CI のみ
- チャート/テーブルで表示

## Method Note (自動生成; meta_lineage 由来)
- 使用 silver テーブル
- 計算式バージョン
- CI 手法, null model, holdout

## Interpretation (optional, 明示ラベル付き)
- 一人称 (私たちは...と解釈する)
- 代替解釈を最低 1 つ
- 限界の明示

## Data Statement
- ソース coverage
- 欠損処理
- 名前解決 confidence

## Disclaimers (JA + EN)
```

**実装**: `BaseReportGenerator` に `render_unified_structure()` メソッドを追加 (既存 SectionBuilder を内部で呼ぶ)

### 4.5 Task 3-5: 削除/アーカイブ/統合の実施

**手順**:

1. Task 3-1 の振り分けマトリクスをレビュー済みにする (ユーザ承認必須)
2. archived 指定のレポートを `scripts/report_generators/reports/archived/` に `git mv` で移動
3. V2_REPORT_CLASSES から除外 (一覧から削除)
4. 統合先に吸収するレポートは、対応する新レポートクラスが完成してから削除
5. 削除指定は現時点で**なし**。archive 経由を必ず踏む

### 4.6 Phase 3 完了判定

- [ ] `docs/REPORT_INVENTORY.md` が存在し、全 35 本の振り分けが決定済み
- [ ] 3 audience index ページが生成される
- [ ] 公開用レポート数が 17-20 本に収まる
- [ ] 各レポートが `meta_*` テーブルのみを直接 SELECT (feat_\* は経由するが meta_* を介すことが推奨)
- [ ] Method Note が全レポートで自動生成されている
- [ ] `pixi run python scripts/generate_reports_v2.py --list` が再編後の一覧を出す

---

## 5. Phase 4: v2 gate 自動化

**目標**: 法的・方法論的 gate を人力レビューから機械チェックに昇格。

### 5.1 Task 4-1: 禁止語 lint

**場所**: `scripts/lint_report_vocabulary.py` を新規作成

**検査対象**: `scripts/report_generators/reports/**/*.py` 内の文字列リテラル (HTML content として render されるもの)

**禁止語リスト** (`scripts/report_generators/forbidden_vocab.yaml` を新規作成):

```yaml
# 能力 framing に滑る語。Findings セクションで使用を禁止
ability_framing:
  ja:
    - 能力
    - 優秀
    - 実力
    - 劣る
    - 優れる
    - 劣っている
    - 才能
    - センス
    - 技能水準
  en:
    - ability
    - talent
    - skill level
    - inferior
    - superior
    - incompetent
    - competent

# 因果動詞。Findings セクション禁止 (Interpretation 内は許可)
causal_verbs:
  ja:
    - 引き起こす
    - 原因となる
    - 〜のせいで
    - もたらす
  en:
    - cause
    - lead to
    - result in
    - due to

# 評価形容詞。Findings セクション禁止
evaluative_adjectives:
  ja:
    - 素晴らしい
    - ひどい
    - 悪い
    - 良い (数値表現以外)
  en:
    - excellent
    - terrible
    - bad
    - good

# Findings vs Interpretation の境界語 (両セクションで文脈判定)
interpretation_markers:
  ja:
    - 解釈
    - 推測
    - 考えられる
  en:
    - interpret
    - speculate
    - we conclude
```

**lint ロジック**:
1. report クラスの `build()` メソッドを静的解析 (ast)
2. Findings セクションに相当する文字列 (SectionBuilder.add_finding への引数) 内で ability_framing, causal_verbs, evaluative_adjectives を検出したら error
3. Interpretation セクション内では ability_framing のみ error、causal_verbs は warning
4. pre-commit hook と CI で実行

**代替語提案辞書** (`scripts/report_generators/vocab_replacements.yaml`):

```yaml
能力: 構造スコア
優秀: 高スコア群 (数値表現を使う)
実力: ネットワーク位置
劣る: 下位パーセンタイル
ability: structural score
cause: associate with
```

lint が発見したら代替語を suggest する。

### 5.2 Task 4-2: Section 構造 enforce

**場所**: `scripts/report_generators/section_builder.py`

**追加機能**:

```python
class ReportSection:
    REQUIRED_SECTIONS = ["概要", "Findings", "Method Note", "Data Statement", "Disclaimers"]
    OPTIONAL_SECTIONS = ["Interpretation"]

    def __init__(self, ...):
        self._sections = {}

    def validate(self) -> None:
        missing = [s for s in self.REQUIRED_SECTIONS if s not in self._sections]
        if missing:
            raise ValueError(f"Missing required sections: {missing}")

        # Interpretation セクションがあれば alternative 解釈が最低 1 つ必要
        interp = self._sections.get("Interpretation")
        if interp and not interp.has_alternative:
            raise ValueError(
                "Interpretation section must contain at least one alternative interpretation"
            )

        # Method Note は自動生成由来かチェック (手書き禁止)
        method = self._sections.get("Method Note")
        if method and not method.auto_generated:
            raise ValueError("Method Note must be auto-generated from meta_lineage")
```

**効用**: build 時に例外 → PR merge 前に必ず検知。

### 5.3 Task 4-3: feature lineage による anime.score 自動チェック

**ロジック**:

1. 全 meta_\* テーブルの lineage から `source_silver_tables` を取得
2. `source_bronze_forbidden = 0` (= anime.score 取り込み許可) のテーブルを列挙
3. 該当テーブルは**公開レポートから参照禁止**。技術付録のみ許可
4. CI で差分チェック: PR で `source_bronze_forbidden = 0` が増えたら human review 必須

**実装**: `scripts/ci_check_lineage.py`

```python
def check_no_bronze_leak_in_public():
    conn = get_connection()
    leaky = conn.execute(
        "SELECT table_name FROM meta_lineage "
        "WHERE source_bronze_forbidden = 0 AND audience != 'technical_appendix'"
    ).fetchall()
    if leaky:
        raise SystemExit(
            f"Public reports must not pull bronze (anime.score) data. "
            f"Offenders: {leaky}"
        )
```

CI で `pixi run python scripts/ci_check_lineage.py` を毎回実行。

### 5.4 Task 4-4: pre-commit hook 統合

**場所**: `.pre-commit-config.yaml` (既存なら update、なければ新規)

```yaml
repos:
  - repo: local
    hooks:
      - id: report-vocabulary-lint
        name: Report vocabulary lint (forbid 能力/ability framing)
        entry: pixi run python scripts/lint_report_vocabulary.py
        language: system
        files: ^scripts/report_generators/reports/
        pass_filenames: true

      - id: ci-check-lineage
        name: Check no bronze leak in public reports
        entry: pixi run python scripts/ci_check_lineage.py
        language: system
        pass_filenames: false

      - id: section-structure-enforce
        name: Enforce v2 section structure
        entry: pixi run pytest tests/test_report_structure.py -x
        language: system
        pass_filenames: false
```

### 5.5 Phase 4 完了判定

- [ ] `scripts/lint_report_vocabulary.py` が存在し、禁止語を検出する
- [ ] `scripts/report_generators/forbidden_vocab.yaml` が編集可能な形で存在
- [ ] Section 構造 enforce が SectionBuilder.validate() で実装
- [ ] `scripts/ci_check_lineage.py` が lineage を検証
- [ ] pre-commit hook に 3 つ統合されている
- [ ] `pixi run lint && pixi run test && pre-commit run --all-files` が全て green

---

## 6. 実施順序と依存関係

### 6.1 推奨順序

```
Phase 1 (データ層) ─────────────────┐
   ├─ 1-1: silver anime DDL 再設計   │
   ├─ 1-2: display_lookup helper    ├─→ Phase 2 (gold 層)
   ├─ 1-3: migration v47→v48        │    ├─ 2-1: meta_lineage
   ├─ 1-4: ETL 修正 (FORBIDDEN guard)│    ├─ 2-2: meta_common_person_parameters
   ├─ 1-5: analysis 層切替           │    ├─ 2-3: 各 audience meta_*
   ├─ 1-6: Pydantic 分割             │    ├─ 2-4: feat_* 整理
   └─ 1-7: import guard             │    └─ 2-5: Method Note 自動生成
                                     │                 │
                                     │                 ▼
                                     │       Phase 3 (レポート再編)
                                     │           ├─ 3-1: INVENTORY.md
                                     │           ├─ 3-2: brief index
                                     │           ├─ 3-3: 語彙置換
                                     │           ├─ 3-4: 構造統一
                                     │           └─ 3-5: archive 移動
                                     │                         │
                                     │                         ▼
                                     └─→ Phase 4 (gate 自動化)
                                             ├─ 4-1: 禁止語 lint
                                             ├─ 4-2: section enforce
                                             ├─ 4-3: lineage check
                                             └─ 4-4: pre-commit
```

**並列化可能**:
- Phase 1 の 1-3 完了後、Phase 2 の 2-1 (meta_lineage DDL) は開始可能
- Phase 4 の 4-1 (禁止語 lint) は Phase 3 を待たず開始可能 (語彙辞書の完成)

### 6.2 commit 単位の粒度

- **1 commit = 1 Task 内の論理単位**。複数 Task を混ぜない
- 各 commit 前に `pixi run test && pixi run lint` green を確認
- commit message format (`CLAUDE.md` の既存慣習を踏襲):
  - `refactor(db): silver.anime から anime.score 等を物理除去 (Phase 1-1/1-3)`
  - `feat: display_lookup helper 追加 (Phase 1-2)`
  - `refactor: analysis 層を anime_studios JOIN に切替 (Phase 1-5)`
  - `docs: REPORT_INVENTORY.md 初版 (Phase 3-1)`
  - `feat: 禁止語 lint 実装 (Phase 4-1)`

### 6.3 checkpoint (ユーザレビュー必須)

以下のタイミングで **ユーザに確認を取ってから次に進む**:

1. **Phase 1-3 完了時** (migration v48 適用後、silver.anime から score 等が消え、`display_lookup.get_display_score` が bronze から値を返すことの確認)
2. **Phase 2-1 完了時** (meta_lineage スキーマレビュー)
3. **Phase 3-1 完了時** (REPORT_INVENTORY.md レビュー — 35 本の振り分けは user の戦略判断)
4. **Phase 4-1 完了時** (forbidden_vocab.yaml レビュー — legal risk 判断)

これらは**全ての Task 完了より重要**。checkpoint で反対意見が出たら立ち止まる。

---

## 7. 検証チェックリスト (最終確認)

### 7.1 機能テスト

- [ ] 新規 DB を `init_db()` から構築して silver の `anime` に score カラムが無く、`meta_lineage` テーブルが存在する
- [ ] `scripts/maintenance/seed_medallion.py` 実行で bronze → silver に投入 (silver には score が流れ込まない)
- [ ] `pixi run pipeline` が silver のみを参照して完走 (analysis 層から display_lookup / bronze への import は 0)
- [ ] `pixi run python scripts/generate_reports_v2.py` が新構造の 17-20 本を生成
- [ ] 各レポートの Method Note が meta_lineage から自動生成されている

### 7.2 汚染テスト

- [ ] `rg '\ba\.score\b|\banime\.score\b' src/analysis/ src/pipeline_phases/` → 0 件 (コメント除く)
- [ ] `rg 'from src\.utils\.display_lookup|get_display_' src/analysis/ src/pipeline_phases/` → 0 件
- [ ] `rg 'SELECT[^;]*\bscore\b[^;]*FROM anime\b' src/` → 0 件
- [ ] `PRAGMA table_info(anime);` → score / popularity / favourites / description / cover_* / banner / site_url / genres / tags / studios / synonyms カラム**無し**
- [ ] `PRAGMA table_info(src_anilist_anime);` → score カラム**有り** (bronze は無変更)
- [ ] `display_lookup.get_display_score(conn, anime_id)` が bronze から値を返す smoke test

### 7.3 gate 自動化テスト

- [ ] 禁止語を含むテストレポートを作って lint が検知
- [ ] Method Note を手書きで書いたテストレポートを作って enforce が検知
- [ ] Interpretation に alternative が無いテストレポートを作って enforce が検知
- [ ] `source_bronze_forbidden = 0` かつ audience != 'technical_appendix' を作って ci_check_lineage が検知

### 7.4 既存互換性

- [ ] 既存 1947 テスト全件 green
- [ ] pipeline の 26 JSON 出力が従来通り生成される (meta_\* は追加出力)
- [ ] API endpoint が regression なし (少なくとも `/api/v1/persons/{id}/profile` が動く)
- [ ] WebSocket pipeline monitor が動く

### 7.5 ドキュメント整合

- [ ] CLAUDE.md のアーキテクチャ説明を更新 (bronze/silver/gold の 3 層、score は bronze のみ & display_lookup helper 経由でのみアクセス可能である旨を明記)
- [ ] docs/ARCHITECTURE.md を更新
- [ ] docs/CALCULATION_COMPENDIUM.md が新テーブル名を使用
- [ ] docs/REPORT_STRATEGY.md を本書の結果で更新

---

## 8. 禁止事項・安全ガード (繰り返し)

1. **anime.score / popularity / favourites を silver の `anime` テーブルに入れない** (0.4 H1)
2. **能力 framing を Findings で使わない** (0.4 H2)
3. **entity resolution ロジックに触れない** (0.4 H3)
4. **既存テストを green に保つ** (0.4 H5)
5. **hook をスキップしない** (0.4 H6)
6. **destructive git 操作をしない** (0.4 H7)
7. **レポートを削除せず archive する** (4.5 参照)
8. **checkpoint で必ずユーザ確認を取る** (6.3 参照)
9. **commit 単位を小さく保ち、各 commit で test green を確認**
10. **本書の指示と矛盾する既存コード・メモリがあれば、本書を優先**

---

## 9. 不明点が出た場合の判断基準

判断に迷ったら、以下の優先順で決める:

1. **legal constraint** (CLAUDE.md "Legal Constraints", 本書 0.4 H1-H3) を絶対に破らない
2. **v2 Philosophy** (docs/REPORT_PHILOSOPHY.md) に従う
3. **本書の指示** に従う
4. **既存コードの慣習** (structlog, Pydantic v2, httpx async 等) に従う
5. それでも迷ったら、**より保守的な選択** (例: view より物理テーブル、削除より archive)
6. それでも決まらなければ、**ユーザに質問**

不明点を推測で解決するよりも、短い質問で確認する方が安全。

---

## 10. 作業完了時のデリバラブル

最終的に以下が成果物となる:

1. **コード**: Phase 1-4 の実装
2. **ドキュメント**:
   - `docs/REPORT_INVENTORY.md` (新規)
   - `docs/MOTHER_DATA_SCHEMA.md` (新規、Phase 1 の設計記録)
   - `docs/V2_GATE_AUTOMATION.md` (新規、Phase 4 の設計記録)
   - `docs/ARCHITECTURE.md` 更新
   - `CLAUDE.md` 更新
   - `docs/CALCULATION_COMPENDIUM.md` 更新
3. **設定ファイル**:
   - `scripts/report_generators/forbidden_vocab.yaml` (新規)
   - `scripts/report_generators/vocab_replacements.yaml` (新規)
   - `.pre-commit-config.yaml` 更新
4. **テスト**: 新規テスト (Phase 1 のスキーマ、Phase 4 の lint)
5. **ブランチ/PR**: 各 Phase ごとに PR を分ける。Phase を跨ぐ大 PR は禁止

---

以上。不明点があれば本書の該当 Task 番号 (例: Task 2-3) を添えて質問すること。
