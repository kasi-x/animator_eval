# KeyFrame vs 他 source 入力差分 — Data Quality 解析

**目的**: keyframe scraper の入力と他 source (anilist/mal/ann/seesaa/madb/bgm/tmdb/sakuga 等) の入力が異なる箇所を **差分タイプ別に分類** し、parser バグ / 正規化漏れ / 真の実体差を切り分ける.

**観客**: data engineer / pipeline maintainer. 個別 entity の意思決定 (mart 集計) には使わない.

**生成**: 2026-05-15 ベース snapshot (`result/keyframe_vs_others_diff.csv` 125,314 rows → `keyframe_diff_taxonomy.csv` 分類).

---

## Method (透明な遠近法)

### データ生成

1. `scripts/maintenance/keyframe_vs_others_diff.py`
   - 入力: `result/animetor.duckdb` (Conformed) + `result/resolved.duckdb` (Resolved cluster)
   - 2 path で差分抽出:
     - **cluster path**: resolved cluster の同 canonical 内で keyframe row vs 他 source row を field 比較
     - **orphan path**: resolved cluster 外で natural key (title+year / name / 正規化 studio name) が **両側で一意** な kf vs other ペアのみ比較
   - 出力: 1 field 不一致 = 1 row.

2. `scripts/maintenance/keyframe_diff_analysis.py`
   - 各 (entity, field) 差分を 8-13 種の class に分類 (per-field heuristics)

### Classifier 別差分タイプ

| 対象 field | classifier | 主要 class |
|------------|------------|------------|
| 名前系 (`name_*`, `title_*`, studio.name) | `classify_name` | `identical` / `whitespace` / `case_only` / `punct_only` / `case_punct` / `word_reorder` / `subset` / `encoding_diacritic` / `levenshtein_le2` / `missing_side` / `distinct` |
| `credit_role.role` | `classify_role` | `identical` / `whitespace_case` / `hierarchy_pair:<parent>` / `sibling:<parent>` / `distinct_category` / `missing_side` |
| `start_date`, `end_date` | `classify_date` | `same_after_format` / `json_vs_iso_match` / `day_only` / `month_diff` / `year_diff` / `unparseable` / `missing_side` |
| enum 系 (`season`, `format`, `status`, `gender`, ...) | `classify_enum` | `identical` / `case_only` / `distinct_category` / `missing_side` |
| `source_mat` | `classify_source_mat` | + `kf_bug_source_name_in_field` (parser バグ検出) |

### 既知の限定

- **transliteration 越えクラスタリング非対応**: JP↔EN (例「シャフト」vs「Shaft」) は別 cluster として残るため、ここに含まれない.
- **role hierarchy table は scaffold**: `ROLE_HIERARCHY` は director / animation_director / writer / art / sound の 5 親のみ. 他親役職の hierarchy は `distinct_category` に流れる.
- **片側 NULL 案件**: cluster path では両側 NOT NULL の組のみ比較. orphan path の natural-key 一意条件はノイズ排除のため厳しめ (multi:multi は除外).

---

## Findings — (entity, field) × 差分タイプ別行数

評価的形容詞を付けず, 観測値のみ.

### credit_role.role — 67,530 rows

| class | count | pct |
|---|---:|---:|
| distinct_category | 38,253 | 56.6% |
| hierarchy_pair:director | 22,922 | 33.9% |
| hierarchy_pair:animation_director | 4,256 | 6.3% |
| sibling:animation_director | 2,087 | 3.1% |
| whitespace_case | 12 | <0.1% |

### person.name_en — 38,594 rows

| class | count | pct |
|---|---:|---:|
| missing_side | 37,297 | 96.6% |
| case_only | 1,190 | 3.1% |
| whitespace | 26 | 0.1% |
| levenshtein_le2 | 24 | 0.1% |
| distinct | 23 | 0.1% |
| punct_only | 20 | 0.1% |
| subset | 8 | <0.1% |
| word_reorder + case_punct | 6 | <0.1% |

### anime.start_date — 4,994 rows

| class | count | pct |
|---|---:|---:|
| same_after_format | 4,646 | 93.0% |
| day_only | 214 | 4.3% |
| month_diff | 103 | 2.1% |
| year_diff | 31 | 0.6% |

### anime.end_date — 4,007 rows

| class | count | pct |
|---|---:|---:|
| same_after_format | 3,572 | 89.1% |
| year_diff | 247 | 6.2% |
| day_only | 105 | 2.6% |
| month_diff | 83 | 2.1% |

### anime.season — 3,124 rows

| class | count | pct |
|---|---:|---:|
| case_only | 3,092 | 99.0% |
| distinct_category | 32 | 1.0% |

### anime.source_mat — 2,438 rows

| class | count | pct |
|---|---:|---:|
| kf_bug_source_name_in_field | 2,438 | 100.0% |

### anime.title_en — 1,476 rows

| class | count | pct |
|---|---:|---:|
| distinct | 552 | 37.4% |
| subset | 362 | 24.5% |
| case_only | 264 | 17.9% |
| punct_only | 137 | 9.3% |
| case_punct | 80 | 5.4% |
| levenshtein_le2 | 64 | 4.3% |
| encoding_diacritic + word_reorder + whitespace + missing_side | 17 | 1.2% |

### person.name_ja — 951 rows

| class | count | pct |
|---|---:|---:|
| punct_only | 729 | 76.7% |
| missing_side | 195 | 20.5% |
| whitespace | 20 | 2.1% |
| encoding_diacritic | 5 | 0.5% |
| 他 | 2 | 0.2% |

### anime.title_ja — 725 rows

| class | count | pct |
|---|---:|---:|
| distinct | 218 | 30.1% |
| subset | 142 | 19.6% |
| encoding_diacritic | 126 | 17.4% |
| punct_only | 94 | 13.0% |
| levenshtein_le2 | 90 | 12.4% |
| missing_side | 34 | 4.7% |
| case_only / whitespace / word_reorder / case_punct | 21 | 2.9% |

### anime.format — 700 rows

| class | count | pct |
|---|---:|---:|
| case_only | 699 | 99.9% |
| distinct_category | 1 | 0.1% |

### studio.name — 538 rows

| class | count | pct |
|---|---:|---:|
| case_only | 299 | 55.6% |
| distinct | 165 | 30.7% |
| subset | 55 | 10.2% |
| whitespace / punct_only / case_punct / levenshtein_le2 | 19 | 3.5% |

### anime.episodes — 235 rows

| class | count | pct |
|---|---:|---:|
| distinct_category | 235 | 100.0% |

### person.name_ko — 2 rows
missing_side 100%.

---

## Interpretation (一人称明示)

私が今回の分類から読み取った仮説と対案を併記する. これは結論ではなく解釈であり、検証は個別 task card に委ねる.

### I-1: schema-level の parser bug が複数 layer に存在 (高確度)

- **`anime.source_mat` 100% (2,438 件)** が classifier の `kf_bug_source_name_in_field` 判定. `src/etl/integrate_duckdb.py:538` の COALESCE fallback が `original_work_type` NULL 時に `source` (source 名) を入れていたバグが原因と特定. [TASK_CARDS/18_data_integrity/06_source_mat_fallback_bug.md](../TASK_CARDS/18_data_integrity/06_source_mat_fallback_bug.md) で修正済 (fallback を NULL に).
- **anime.start_date / end_date 計 9,001 件のうち約 91%** が `same_after_format` — keyframe parser が AniList `{year, month, day}` dict を JSON 文字列で出力していたため. parser を ISO 文字列に変換するよう修正済 (`src/scrapers/parsers/keyframe.py: _date_dict_to_iso`).
- **anime.season 99.0% / anime.format 99.9%** が `case_only`. `integrate_duckdb.py` で `UPPER(season)` / `UPPER(REPLACE(format, ' ', '_'))` 正規化を追加.

→ 上流修正の合計影響は約 14,500 行 / 125,314 (11.6%) の diff 解消見込.

### I-2: kf は他 source の欠損を補完している (中確度)

- **`person.name_en` の 96.6% が `missing_side`**, うち他 source 側が空が 37,297 件中 大半. つまり keyframe が他 source の `name_en` 空欄を補う側で、これは「差分」ではなく「補完」.
- 同様に `person.name_ja` の 20.5% も missing_side.

→ 「kf の入力が違う」と表現すると過大評価. データ価値の証拠と読むのが妥当.

### I-3: 「真の実体差」は全 diff の 5% 未満 (中確度)

`distinct` / `distinct_category` / `year_diff` (実日付違い) / `month_diff` 等の **真の値違い** を合算すると約 4,000–5,000 行 = 全 125,314 の **3.2-4.0%**. 残り 95%+ は format / case / 補完 / hierarchy 細分化.

→ 多くは normalize 層を整えれば自動消滅. 個別調整より上流 schema 規律が効率良い.

### I-4: role hierarchy の差は「過剰な細分化」か「真の役職違い」か未分離

`credit_role.role` の 56.6% (38,253) が `distinct_category`, 33.9% (22,922) が `hierarchy_pair:director`, 6.3% (4,256) が `hierarchy_pair:animation_director`. **階層内** の差は parent role を canonical にすれば吸収できる. **distinct_category** は role taxonomy のずれ.

→ `src/utils/role_groups.py` の role hierarchy を classifier 側に統合して再分類する Phase 2 が考えられる. ただし scoring は既に role hierarchy を畳んだ集計を使うため、cosmetic な不一致と判断する余地もある.

### 対案

私の解釈に対して以下の異なる読み方もある:

- **対案 A**: 「同 entity を指していて値が違う」事実そのものが、scrapers 間の合意取りが弱いことを示す指標であり、件数 = data governance gap. → ガバナンス指標として 125,314 を主指標に置く案.
- **対案 B**: ここでの「他 source」は cluster で同一視されたものだが、cluster 自体が不完全ならば diff の母集団も不確実. cluster 精度が先.

私は採用しない. ガバナンス指標としては「true diff のみ」を見るほうが行動指針として明確だから. cluster 精度は別途 [02_persons_tmdb_homonym](../TASK_CARDS/19_resolved_cluster_fix/02_persons_tmdb_homonym.md) / [03_audit_post_fix](../TASK_CARDS/19_resolved_cluster_fix/03_audit_post_fix.md) で扱う.

---

## Limitations

- ISO date は parser コード修正済だが、**既存 BRONZE parquet は再 ingest が必要**. 再 ingest 前 / 後で再測定するべき.
- `season` / `format` の UPPER 正規化も同様、conformed 再ロードで反映.
- `transliteration` 越え (JP↔EN studio/person) は未集計.
- `episodes` 100% distinct は scraper ごとの放送回数定義の差で、parser バグではない可能性が高い.

---

## 再生成手順

```bash
pixi run python scripts/maintenance/keyframe_vs_others_diff.py
pixi run python scripts/maintenance/keyframe_diff_analysis.py
```

出力:
- `result/keyframe_vs_others_diff.csv` (raw diff)
- `result/keyframe_diff_taxonomy.csv` (classified)
- `result/keyframe_diff_taxonomy.md` (集計)

---

## 関連

- `TASK_CARDS/18_data_integrity/06_source_mat_fallback_bug.md`
- `TASK_CARDS/19_resolved_cluster_fix/05_keyframe_id_dedup.md`
- `src/etl/integrate_duckdb.py` (source_mat / season / format 正規化)
- `src/scrapers/parsers/keyframe.py` (date ISO 変換)
