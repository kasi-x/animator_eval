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
- [ ] 本ドキュメント commit
- [ ] CLAUDE.md / docs/ARCHITECTURE.md に概念追記 (実装前)

### Phase 1: 既存層の責務再定義
- [ ] `silver_loaders/` を概念上 **Conformed loaders** と認識 (rename しない)
- [ ] AKM 含む scoring は **Resolved 層が無い前提で動作中** であることをドキュメント明示
- [ ] 22/01 (anime_studios coverage) は **Conformed 層の補強** タスクとして扱う

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

### Phase 5: ドキュメント更新
- [ ] `docs/ARCHITECTURE.md` 全面改訂
- [ ] `CLAUDE.md` の 3 層モデル記述を 5 層に
- [ ] 図の更新

---

## オープンクエスチョン

- **物理 file 名**: `silver.duckdb` / `gold.duckdb` のまま vs `conformed.duckdb` / `mart.duckdb` に rename
  - 推奨: 移行コスト避けて current 維持、ドキュメント用語のみ刷新
- **Resolved 層の location**: 新規 `resolved.duckdb` vs `silver.duckdb` 内 `r_*` schema
  - 推奨: 新規 `resolved.duckdb` (lock 競合分離、責務明示)
- **代表値選抜の優先順位**: source ranking の決定方法
  - 候補: AniList > MAL > ANN > MADB > Bangumi > seesaawiki > Keyframe (英語コミュニティ + 構造化度順)
  - or: data quality stats (完全度 / 検証経由) で動的決定
- **Resolved 層の更新頻度**: Conformed 全更新後にバッチ vs インクリメンタル
  - 推奨: バッチ (entity_resolution の整合性優先)

---

## 関連

- `docs/ARCHITECTURE.md`: 現 3 層モデル (実装中)
- `docs/ARCHITECTURE_CLEANUP.md`: 過去のクリーンアップ記録
- `CLAUDE.md`: 設計原則 + Hard Rules
- `result/audit/akm_refresh_summary.md`: 21/01 で発覚した silent fail (本提案の起源)
- TASK_CARDS/22_silver_coverage/01: Conformed 層の補強タスク (進行中)
