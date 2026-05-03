# 5 層データアーキテクチャ提案 (v62 → v63)

**起票**: 2026-05-03 (21/01 AKM silent fail を受けて)
**Status**: 設計中、未実装

---

## 動機

現状の 3 層 (BRONZE / SILVER / GOLD) は責務混在で、特に SILVER に **「集約」と「正規化・代表値選抜」が混じっている** ことが 21/01 で露呈:

- `silver.duckdb anime` 表は `id LIKE 'anilist:a%'` / `mal:a%` / `bgm:s%` 等で **同 anime が source 別 row として並存**
- AKM/scoring も per-source 行に対して走り、joins (`anime_studios` 等) が想定通り機能せず silent fail
- entity_resolution は credits / persons には適用されるが anime には未適用

各層の責務を **分離・明示** する 5 層提案。

---

## 5 層設計

| 層 | 役割 | データ形式 | 例 |
|----|------|----------|------|
| **Raw** | scrape 直後 (HTML/JSON/XML) | filesystem | `data/anilist/cache/*.json` |
| **Source** | source 別 parsed parquet | parquet (hive partition) | `result/bronze/source=anilist/table=anime/` |
| **Conformed** | source 横断 schema 統一、source 並存 | duckdb | 現 `silver.duckdb` (`id='anilist:a123'` / `'mal:a123'` 別 row) |
| **Resolved** | entity_resolution 済 1 entity 1 row、欠損補填、代表値選抜 | duckdb | 新規 `resolved.duckdb` (`canonical_anime_id` 1 row) |
| **Mart** | scoring / 統計 / feature | duckdb | 現 `gold.duckdb` |

### 各層の責務 (詳細)

#### Raw (生データ層)
- scraper の生 cache (HTML / JSON)
- 不可変、scrape の証拠
- 既存運用継続、変更なし

#### Source (source 別構造化層)
- 現在の BRONZE parquet
- 各 source ごとに独立 schema
- 集約・統合は行わない (source の **完全性**を保つ)
- `anime.score` 等 display 系列も含む (source の生情報)

#### Conformed (横断統一層)
- 現在の SILVER duckdb
- 各 source の同種データを **schema 統一** (`anime` / `persons` / `credits` / `studios`)
- ID は `<source>:<source_id>` prefix で source 並存 (`anilist:a123`)
- entity_resolution はまだ行わない、source 並存
- score 系列は `display_*_<source>` prefix で隔離 (H1)

#### Resolved (新層、代表値選抜層)
- 1 canonical anime / person / studio = 1 row
- entity_resolution の最終結果として `canonical_id` 払い出し
- 各属性は source 間で代表値選抜:
  - `title_ja` = 5 source 中 majority vote
  - `year` = AniList > MAL > ANN の優先順位
  - `studios` = source 別 set の union or 重み付き
- 欠損補填: source A で空、B にある値を採用
- AKM / 全 scoring は **この層を読む**
- `meta_resolution_audit` で各 canonical_id がどの source row 由来かトレース

#### Mart (分析・統計層)
- 現在の GOLD duckdb
- AKM 結果 (theta_i / psi_j)
- 派生 feature (feat_*)
- スコア (person_scores, scores)
- レポート用集計 (agg_*)

---

## 命名マッピング (現 ↔ 新)

| 現状 | 新 | 移行 |
|------|------|------|
| `data/<source>/` | **Raw** | 名前そのまま、概念のみ追加 |
| `result/bronze/` | **Source** | 名前 alias 維持、ドキュメント上は Source |
| `result/silver.duckdb` | **Conformed** | 名前そのまま、責務「集約のみ」と再定義 |
| (新規) | **Resolved** | `result/resolved.duckdb` 新設 |
| `result/gold.duckdb` | **Mart** | 名前そのまま、責務「分析」と再定義 |

物理 file 名は当面 medallion (`silver.duckdb` / `gold.duckdb`) 維持、ドキュメント上の役割名のみ 5 層用語に切替。完全 rename は別 PR で。

---

## 移行ロードマップ

### Phase 0 (現在): 設計合意
- [x] 命名確定 (`Raw → Source → Conformed → Resolved → Mart`)
- [x] 本ドキュメント commit
- [x] CLAUDE.md / docs/ARCHITECTURE.md に概念追記 (実装前)

### Phase 1: 既存層の責務再定義
- [x] 1a: `silver_loaders/` を概念上 **Conformed loaders** と認識 (rename しない)
- ~~1b: AKM 含む scoring は **Resolved 層が無い前提で動作中** であることをドキュメント明示~~ — 不要 (改訂版 Phase 3 で直接 Resolved 切替を行うため、中間状態の記述は省略)
- [x] 1c: 22/01 (anime_studios coverage) は **Conformed 層の補強** タスクとして扱う

### Phase 2: Resolved 層 設計
- [ ] `src/etl/resolved/` 新規パッケージ
- [ ] `anime` の代表値選抜ロジック (priority order / majority vote / union)
- [ ] `persons` の代表値選抜ロジック (canonical_name_ja で名寄せ済を活用)
- [ ] `studios` の代表値選抜ロジック
- [ ] `credits` は per-source のまま、ただし person_id / anime_id は canonical
- [ ] schema: `result/resolved.duckdb` または `silver.duckdb` 内 `r_*` schema

### Phase 3: scoring 切替
- [ ] `silver_reader.load_anime_silver` → `resolved_reader.load_anime_resolved`
- [ ] AKM 入力を Resolved 層に切替
- [ ] 既存 SILVER 直読み箇所を全て検出 + 切替
- [ ] AKM 結果の質改善確認 (現 r²=0.60 → 改善期待)

### Phase 4: Mart の再生成
- [ ] Resolved 入力で gold pipeline 全 phase 再実行
- [ ] feat_* / person_scores / scores 全更新
- [ ] レポート再生成 (`scripts/generate_reports.py --all`)

### Phase 5: ドキュメント更新 ✅ 完了 (2026-05-02)
- [x] `docs/ARCHITECTURE.md` 全面改訂
- [x] `CLAUDE.md` の 3 層モデル記述を 5 層に
- [ ] 図の更新 (別 PR)

---

## 確定事項 (2026-05-03)

### 物理 file 設計 (Conformed + Mart 同居)
- **既存 `silver.duckdb` / `gold.duckdb` 削除** (clean slate)
- **`result/animetor.duckdb` 新規** — schema 分離で Conformed + Mart 同居
  - `conformed.*` schema: anime / persons / credits / studios 等 (現 silver)
  - `mart.*` schema: scores / feat_* / agg_* / score_history (現 gold)
- **`result/resolved.duckdb` 新規** — entity_resolved 1 row 層 (別 file で分離)
- 同居の利点: cross-schema join に ATTACH 不要、conformed → mart view 定義可、file 管理楽
- Resolved 別 file の利点: AKM/scoring 走行中に conformed ETL を並行できる経路確保

### Resolved 層 location
- **別 duckdb (`result/resolved.duckdb`)** で完全分離
- 理由: 21/01 で発覚した lock 競合の分離、責務明示、backup/rebuild 独立

### 代表値選抜 (E: 列別 ranking + majority vote tie-break)
- 各属性に **source 優先順位リスト** を定義 (例: `title_ja: [seesaawiki, anilist, mal, mediaarts]`)
- 上位 source の値が NULL なら次の source へ fallback
- 同優先順位で複数 source あれば **majority vote** (3+ 一致採用、tie 時は最上位 source)
- ranking テーブル: `src/etl/resolved/source_ranking.py` で宣言的に定義、後で調整可能

### 更新頻度 (A: バッチ完全再生成)
- Conformed 全更新後に Resolved を **全削除 → 全行再生成**
- entity_resolution の整合性優先 (人物名寄せ等の bug fix も即反映)
- インクリメンタル更新は将来検討 (Phase 6+)

---

## 改訂版 移行ロードマップ

### Phase 0 ✅ 完了 (2026-05-03)
- 命名確定 / オープンクエスチョン 4 件確定
- 本ドキュメント commit

### Phase 1: Conformed + Mart 統合 file への移行 (22/01 完了後着手)
- 新 file `result/animetor.duckdb` に schema `conformed` + `mart` 作成
- `src/etl/silver_loaders/` → `src/etl/conformed_loaders/` rename + INSERT 先を `conformed.*` に
- `src/analysis/io/silver_reader.py` → `conformed_reader.py` (target: `animetor.duckdb` の `conformed` schema)
- `src/analysis/io/gold_writer.py` → `mart_writer.py` (target: `animetor.duckdb` の `mart` schema)
- 旧 `silver.duckdb` / `gold.duckdb` 削除
- 全 import path 更新 (200+ 箇所予測)
- pixi.toml / Taskfile.yml の path 修正
- tests/ 全更新 + green 維持
- 既存 docs (ARCHITECTURE.md / CLAUDE.md) の SILVER/GOLD 言及を Conformed/Mart に書換

### Phase 2: Resolved 層 設計 + 実装
- `src/etl/resolved/` パッケージ
- `src/etl/resolved/source_ranking.py`: 列別 source ranking 宣言
- `src/etl/resolved/resolve_anime.py`: anime 代表値選抜 + 欠損補填
- `src/etl/resolved/resolve_persons.py`: persons (canonical_name_ja 活用)
- `src/etl/resolved/resolve_studios.py`: studios
- `src/etl/resolved/resolve_credits.py`: credits の person_id / anime_id を canonical 化
- `result/resolved.duckdb` 新規 schema
- entity_resolution 結果の audit (`meta_resolution_audit`) 充実

### Phase 3: scoring 切替
- `conformed_reader` の使用箇所を `resolved_reader` に置換 (AKM / scoring 全体)
- AKM 結果の質改善確認 (現 r²=0.60、entity-resolved 後の改善期待)

### Phase 4: Mart 再生成
- Resolved 入力で mart pipeline 全 phase 再実行
- `mart.feat_*` / `mart.person_scores` / `mart.scores` 全更新
- レポート再生成

### Phase 5: ドキュメント整備 ✅ 完了 (2026-05-02)
- [x] `docs/ARCHITECTURE.md` 全面改訂 (3 層 → 5 層)
- [x] `CLAUDE.md` の 3 層モデル記述差替
- [ ] 図の更新 (別 PR)

### Phase 6 (将来)
- 段階的更新 (インクリメンタル化)
- Resolved 層への直接 query API (`mart_reader` 経由不要のレポート)
- conformed → mart のリアルタイム view 定義 (集計の precompute 削減)

---

## 関連

- `docs/ARCHITECTURE.md`: 5 層モデル記述済 (Phase 5 ✅)
- `docs/ARCHITECTURE_CLEANUP.md`: 過去のクリーンアップ記録
- `CLAUDE.md`: 設計原則 + Hard Rules
- `result/audit/akm_refresh_summary.md`: 21/01 で発覚した silent fail (本提案の起源)
- TASK_CARDS/22_silver_coverage/01: Conformed 層の補強タスク (進行中)
