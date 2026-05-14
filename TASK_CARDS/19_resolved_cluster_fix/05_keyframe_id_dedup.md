# Task: keyframe BRONZE 内 ID dedup (SHA ↔ slug)

**ID**: `19_resolved_cluster_fix/05_keyframe_id_dedup`
**Priority**: 🟡 Medium
**Created**: 2026-05-15
**Estimated changes**: 調査次第 (推定 ~+100 / -10 lines + 1 maintenance script)
**Blocks**: keyframe orphan diff の精度、Phase 2b studio cross-source clustering の cluster size
**Blocked by**: なし (調査タスク)

---

## Goal

`silver.{anime,persons,studios}` 内 keyframe-source row で **同一実体を指す異なる ID**
(SHA hash / slug 等) が並存している可能性を検証し、必要なら dedup する。

`scripts/maintenance/keyframe_vs_others_diff.py` 実行結果 (2026-05-15):

- studio: kf:s prefix 2068 件 (うち natkey 一意 1624 件 = 重複 444 件)
- anime/persons: `keyframe:` prefix
- studios: `kf:` prefix
  → BRONZE writer の prefix inconsistency が観測済 (本タスクの一部か別タスクか要判断)

---

## Hard constraints

- **H1**: anime.score 系列は触らない
- **既存 DB rebuild 未実施**: コード修正 + テスト止まり。`result/{silver,resolved}.duckdb`
  の再構築はユーザが別途実行
- **削除前必ず audit**: row 削除する場合は事前に件数・代表例を CSV ダンプして確認
  ([feedback_destructive_ops.md](../../) 準拠)

---

## Pre-conditions

- [ ] keyframe scraper の ID 生成ロジック (`src/scrapers/keyframe/`) を読み、
      SHA hash と slug の **どちらが正規 ID か** を確定する
- [ ] BRONZE writer (`src/db/scraper/` or `src/etl/integrate.py`) で
      `kf:` vs `keyframe:` の prefix 使い分けの背景を確認

---

## Investigation Steps

### Step 1: 現状調査 (read-only) — ✅ 完了 (2026-05-15)

`scripts/maintenance/keyframe_id_audit.py` で実 silver DB を audit. 結果:

#### prefix 分布 (完全 inconsistency 確定)

| entity | `keyframe:` | `kf:` |
|--------|-------------|-------|
| anime | 2400 | 0 |
| persons | 35395 | 0 |
| studios | 0 | **2068** |

→ BRONZE writer の prefix 統一が **studios のみ別系統**. 修正は writer 側で可能.

#### ID 形式分布 (各 entity に 2-3 系統並存)

| entity | 系統 A (主) | 系統 B (副) | 系統 C |
|--------|-------------|-------------|--------|
| anime | `slug_kebab` 833 | `<48-hex>` 1567 (= 192bit hash) | — |
| persons | `slug_snake` 35304 | `p_<negative-epoch-ms>` 91 (URL 欠落救済) | — |
| studios | `s<numeric>` 995 | `n:<name>` 985 (URL 欠落救済) | `slug_kebab` 88 |

→ **救済 ID** (`p_-...` / `n:<name>`) は URL が無い entity を name/timestamp から
  合成. 同一実体に対し slug 版が後から増えると重複しうる.

#### 同 natural_key 並存 (重複候補)

| entity | dup row 数 | sample CSV |
|--------|-----------|------------|
| anime | 173 | `result/keyframe_id_audit_dup_anime.csv` |
| persons | 629 | `result/keyframe_id_audit_dup_person.csv` |
| studios | 63 | `result/keyframe_id_audit_dup_studio.csv` |

#### credits 参照量

`silver.credits` 6.79M rows のうち kf prefix 参照は person=430K, anime=430K
(全体の約 6.3%). dedup 影響は中規模.

### Step 1.5: Phase 2b 適用後 実 DB 数値 (2026-05-15)

prefix 統一 (v63 silver / v64 conformed / v65 mart) + resolve_studios Phase 2b
適用後の `result/resolved.duckdb`:

- conformed.studios 28,142 → canonical 21,039 (7,103 row merged)
- multi-source cluster: 3,687
- kf 含む cluster: 2,119 (旧 0)
- `kf:` prefix 全 DB で 0 件 (gold/animetor/resolved/silver 全 VARCHAR 列スキャン)

`<src>:n:<name>` 救済 ID の dedup 進捗:
- 救済 ID 含む cluster 計: 9,202
  - merged (multi-source): 3,548 (39%)
  - singleton 残: 5,654 (61%)
- `keyframe:n:*` singleton 残: 79

### Step 2: dedup 戦略決定 (調査結果反映後)

Step 1 audit で 3 系統の課題判明 → 各系統に対する戦略:

#### 2a. prefix 統一 (`keyframe:` vs `kf:`)
**確定**: BRONZE writer (`src/db/scraper/` 周辺) で studios のみ `kf:` 使用.
**戦略**: writer を `keyframe:` に統一 + studio prefix の参照点 (resolve_studios /
keyframe_vs_others_diff / `_cross_source_ids` の studio path) を同期.

#### 2b. 救済 ID (URL 欠落 entity)
- `keyframe:p_<negative-epoch-ms>` 91 件 (persons)
- `kf:n:<name>` 985 件 (studios)

**戦略**: 救済 ID の row に後から slug 版 row が増える流れを scraper 側で検知し、
救済 ID 行を上書き or 削除する upsert 化. または resolved 層で natkey 一意なら merge.

#### 2c. SHA hash ↔ slug の同一実体並存
- `keyframe:<48-hex>` 1567 件 (anime, 192bit hash)

**戦略**: hex hash は scrape 初期に URL→hash 化されていた古い世代 ID と推定.
slug 版が同 anime で生成されたら hash 版を deprecate / migrate.

#### 2d. 別実体だが name 衝突
**戦略**: dedup せず ER 側 `_definitely_different` guard 追加 ([02 と合わせて検討](02_persons_tmdb_homonym.md)).

### Step 3: 修正実装 (Step 2 の結果次第)

- 例: BRONZE writer 修正 → re-ingest スクリプト
- 例: silver-level merge migration script (`scripts/maintenance/v6X_keyframe_id_unify.py`)
- 例: ER 側 `_definitely_different` guard 追加

---

## Acceptance Criteria

- [ ] `scripts/maintenance/keyframe_id_audit.py` で現状を文書化済 (output CSV 同梱)
- [ ] dedup 戦略が `docs/` または本 task card に追記済
- [ ] (戦略実装時) `keyframe_vs_others_diff.py` の orphan studio 件数が減少
      (= cluster 化が進む) ことを確認
- [ ] 関連テスト追加 / pass

---

## Out of Scope

- Phase 2c+: keyframe vs 他 source の name 正規化越えクラスタリング
  (例: JP↔EN transliteration) — 別タスク
- keyframe scraper の re-scrape — 上流変化が無ければ不要

---

## Related

- [README.md](README.md) §クラスタ品質改善
- `scripts/maintenance/keyframe_vs_others_diff.py` — kf:/keyframe: 両 prefix
  対応済、orphan diff 出力で本タスクの impact 計測に使える
- `src/etl/resolved/resolve_studios.py` (Phase 2b 完了 2026-05-15):
  name 正規化ベース cross-source clustering 実装済。本タスクで kf ID dedup
  すれば cluster size 増加が期待される
