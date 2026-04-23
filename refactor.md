# refactor.md — Sonnet/Haiku 向け委託タスク

作成: 2026-04-23
スコープ: 直近 DONE (2026-04-20〜04-23) の範囲で見つけたリファクタ候補と、ついでに直したい「馬鹿 / 変 / 無益」コード。

---

## 方針 (全タスク共通)

1. **工程に名前をつける**。ネストの深い一枚関数は、ステップごとに named helper に分解する。関数名が仕様書になる状態を目指す。短くても OK (3-5 行の helper を厭わない)。
2. **dead code は即削除**。git log が履歴を持っているので「万が一」は考えない。対象外: `src/agents/` 等のエージェント領域。
3. **テスト**: まず `pixi run test-impact` で影響範囲を確認。該当モジュールだけに絞るときは `pixi run test-scoped tests/test_foo.py`。PR 直前のみ `pixi run test`。
4. **I/O 削減・ログ整合性は壊さない**。`structlog.get_logger()` の event 名 / フィールド名は既存を踏襲。
5. **behavior-preserving**。同じ入力で同じ出力になることを unit テストで保証できない変更は分割して段階的に。
6. **コメントは最小**。名前で伝える。WHY が非自明なときだけ 1 行コメント。
7. **バックワード互換のための残骸禁止**。削除したら `# removed` コメントや `_var` リネーム等を残さない。

---

## タスク一覧

### T1 — SQL / 色ヘルパーの共通化 (mechanical、影響小)

**対象**: `scripts/report_generators/` 配下のレポートモジュール群

1. `COALESCE(NULLIF(p.name_ja,''), NULLIF(p.name_zh,''), NULLIF(p.name_en,''), <fallback> AS name` が **46 箇所**で複製。
   - 新規 `scripts/report_generators/sql_fragments.py` に `person_display_name_sql(fallback_col: str, alias: str = "name") -> str` を作成。
   - 既存の COALESCE 断片を全て置き換える。
   - 影響ファイル (grep で洗い出し済み): `db_loaders.py`, `reports/bridge_analysis.py`, `reports/network_graph.py`, `reports/network_analysis.py`, `reports/score_layers_analysis.py`, `reports/cooccurrence_groups.py`, ほか `generate_all_reports.py` 内の一部。
2. `_hex_to_rgba()` (`reports/bridge_analysis.py:41`) と `_rgba()` (`reports/network_analysis.py:36`) は同一実装。`scripts/report_generators/color_utils.py` に `hex_to_rgba(hex_color, alpha)` として統合。
3. `_TIER_COLORS`, `_TRACK_COLORS`, `_CLUSTER_COLORS` のような色パレット定数もレポート間で重複している可能性。`color_utils.py` に `TIER_PALETTE`, `TRACK_PALETTE` として集約できるか要調査 (差分があるなら放置してよい)。

**検証**: 各レポートを `task report-<name>` で再生成し、生成 HTML サイズ・行数に差が無いこと。lint_vocab / smoke テストは通ること。

---

### T2 — VA core_scoring の工程化

**対象**: `src/analysis/va/pipeline/core_scoring.py`

`compute_va_core_scores_phase()` は 7 個の `with va_step(...)` ブロックが一枚板。ユーザー好み (工程に名前) に最合致。

分解 (提案):

- `_run_va_akm(context)` — akm_result → person_fe, sd_fe
- `_run_va_birank(context)` — グラフ有無分岐含む
- `_run_va_trust(context)`
- `_run_va_patronage(context)` — sd_birank 組み立て含む
- `_run_va_dormancy(context)` — `Credit` pseudo 変換込み (別 helper `_va_credits_to_pseudo_credits(va_credits)` にしても良い)
- `_run_va_awcc_placeholder(context)` — 現状 1 行。placeholder の事実を関数名で明示
- `_run_va_iv(context)` — `_build_sd_assignments` と `compute_va_sd_exposure` をここに閉じ込める

phase 本体:
```python
def compute_va_core_scores_phase(context):
    if skip_if_no_va_credits(context, "va_scoring_skipped"):
        return
    _run_va_akm(context)
    _run_va_birank(context)
    _run_va_trust(context)
    _run_va_patronage(context)
    _run_va_dormancy(context)
    _run_va_awcc_placeholder(context)
    _run_va_iv(context)
    logger.info("va_core_scoring_complete", ...)
```

**検証**: `pixi run test-scoped tests/test_va_pipeline_phases.py`。context の 7 フィールドが現行と同値であること。

---

### T3 — VA result_assembly の record builder 分解

**対象**: `src/analysis/va/pipeline/result_assembly.py`

- `_build_va_base_record(pid, context, pid_to_name) -> dict` — 10 フィールドの基本辞書
- `_enrich_with_diversity(record, diversity_metrics) -> None` (mutate) — character_diversity 7 フィールド
- `_sort_results_by_iv(results) -> list` — 1 行でも名前を付ける

`assemble_va_results()` 本体は 10 行程度に収まるはず。

---

### T4 — `api_reports.py` の error boundary & 不要コード削除

**対象**: `src/api_reports.py`

1. **try/except 定型の集約** (9 エンドポイント × 5 行 = 45 行の削減):
   ```python
   @contextmanager
   def _api_error_boundary(event_name: str, **log_ctx):
       try:
           yield
       except HTTPException:
           raise
       except Exception as e:
           logger.error(event_name, error=str(e), **log_ctx)
           raise HTTPException(status_code=500, detail=str(e))
   ```
   各エンドポイントの関数本体を `with _api_error_boundary("brief_fetch_error", brief_id=brief_id):` で囲む。
2. **`briefs_status()` のレスポンス組み立て抽出**: `_read_brief_status(brief_id) -> BriefResponse` を 1 関数に。
3. **馬鹿コード fix**:
   - L237-242 WebSocket の `await asyncio.sleep(0.5)` + コメント `# Run generation (simplified for demo)` — これは production の router に残すべきではない。実装するか削除するかを決定 (現状 "started"/"completed" を送るだけで中身無し。実装されるまで endpoint ごと削除 or `NotImplementedError` 相当を返す形に)。
   - L219 `_connected_clients: List[WebSocket]` — append/remove しかされず **一度も参照されない**。削除。
   - L15 `from typing import List` — 3.12 なので `list[WebSocket]` に。全 `List[...]` → `list[...]`。
4. `briefs_status()` 内の `for brief_id in ["policy", "hr", "business"]` のリテラルリストは `_BRIEF_IDS = ("policy", "hr", "business")` に格上げしてモジュール定数化。

**検証**: `pixi run test-scoped tests/test_api*.py`。404/500 挙動が変わらないこと。

---

### T5 — `name_utils.py` 工程分解 + 馬鹿コード修正

**対象**: `src/utils/name_utils.py`

1. **`infer_nationalities()` (50 行) 分解**:
   - `_from_script_direct(script) -> list[str] | None` — ko→KR / ja→JP / th→TH、他は None
   - `_resolve_zh_or_ja(hometown, *, use_llm) -> list[str]` — ja→cn→ko token → cache → LLM
   - `_resolve_arabic(hometown_lower) -> list[str]`
2. **`_llm_infer_nationality()` 分解**:
   - `_build_llm_prompt(hometown) -> str`
   - `_call_ollama_generate(prompt) -> str | None` — httpx.post → raw response text 抽出
   - `_parse_country_code(raw) -> str | None`
3. **馬鹿コード fix**:
   - L171 `raw = (resp.json().get("response") or resp.json().get("thinking") or "").strip()` — `resp.json()` を **3 回呼んでいる**。1 回に。
     ```python
     payload = resp.json()
     raw = (payload.get("response") or payload.get("thinking") or "").strip()
     ```
   - L175-176 `if code == "NU": code = None` — regex `\b([A-Z]{2})\b` が "NULL" から "NU" を拾う前提のハック。`_parse_country_code` 内で `raw.upper().startswith("NULL")` の早期 return に置き換える。
   - L91, L136, L178 の **裸の `except Exception:`** — 最低限 `(OSError, ValueError, json.JSONDecodeError)` 等に絞る。L178 は `httpx.HTTPError` + `ValueError` に絞って良い。
4. **`_save_hometown_cache()` の tmp-file atomic write** を `_atomic_write_json(path, data)` として独立関数化 (再利用可能性あり)。

**検証**: `pixi run test-scoped tests/test_name_utils.py tests/test_scraper_coverage.py`。`hometown_tokens.json` の cache 書き込み挙動も保全。

---

### T6 — `enrich_hometown_nationality.py` 分解 + 馬鹿コード修正

**対象**: `scripts/maintenance/enrich_hometown_nationality.py`

1. **`run()` (60 行) 分解**:
   - `_print_cache()` — `--show-cached` 分岐
   - `_ensure_llm_reachable_or_exit()` — `_check_llm()` + sys.exit
   - `_classify_candidate(row) -> Literal["script_covered","cache_hit","llm_needed"]`
   - `_process_row(conn, row, *, dry_run, script_only) -> str` — bucket 名を返す
   - `_print_summary(counts, dry_run)`

2. **馬鹿コード fix**:
   - L96 `updated = skipped = cached = failed = 0` — `failed` は **一度もインクリメントされない**。削除。
   - L126-131 のカウントロジック:
     ```python
     if code and not dry_run:
         _update_nationality(...)
         conn.commit()
         updated += 1
     elif code:
         updated += 1  # count as "would update"
     ```
     — `dry_run` 時と実更新時で `updated` の意味が混ざる。`would_update` と `updated` を分けるか、`dry_run` で label だけ変える形に整理。
   - L54-55 `_native_name(row)` は 2 行 wrapper で 1 箇所でしか使われず、`row["name_ja"] or ""` を返すだけ。name_ko / name_zh を見ないのは意図的 (native hint なので) だが、呼び出し元に inline で十分。または関数名を `_row_native_name_or_empty` に直して明示。
   - `conn.commit()` がループ内で毎回呼ばれる。候補が多い時に遅くなる可能性。batch (e.g. 50 件ごと) commit か、ループ後 1 回の commit で十分。

**検証**: `--dry-run --limit 10` で動作確認。既存テストは無い (maintenance スクリプト) ので、スモーク手動。

---

### T7 — `section_builder.py` の `_load_lineage_row` fix

**対象**: `scripts/report_generators/section_builder.py`

1. **変なコード fix** (L389-408):
   ```python
   row = conn.execute("SELECT * FROM meta_lineage WHERE table_name = ?", ...).fetchone()
   ...
   col_names = [d[0] for d in conn.execute("SELECT * FROM meta_lineage WHERE 0").description or []]
   if not col_names:
       col_names = [... 12 個のハードコード ...]
   ```
   - 問題 1: 列名を取るためだけに **2 回目のクエリ**を発行している。`cursor = conn.execute(...); row = cursor.fetchone(); col_names = [d[0] for d in cursor.description]` で済む。
   - 問題 2: `description or []` の `or []` はほぼ無意味 (description は list を返す。None を返すのは一部のドライバのみ)。
   - 問題 3: ハードコード fallback 列リストは実 schema と乖離するリスク (2026-04 現在で既に 12 列だが新カラムがあれば拾われない)。削除。
2. **`_parse_silver_tables` の `import json as _json`** (L413) — 関数内 import + alias が不要。ファイル先頭に `import json` を移す。alias は剥がす。
3. `method_note_from_lineage()` — 既に分解されている (good)。`build_data_statement()` と `build_disclaimer()` の巨大 HTML リテラルは `_DATA_STATEMENT_TEMPLATE` / `_DISCLAIMER_HTML` にモジュール定数として出し、`\uXXXX` エスケープを生の日本語に戻す (可読性)。

**検証**: `pixi run test-scoped tests/test_section_builder.py`。method note / data statement / disclaimer の HTML が同値であること (比較用 snapshot 推奨)。

---

### T8 — 巨大レポートモジュール内 `_build_*_section` の工程分解

**対象**: `scripts/report_generators/reports/bridge_analysis.py`, `network_analysis.py`, `network_graph.py`, `score_layers_analysis.py`

各 `_build_*_section` が 80〜200 行で、内部に以下の工程が一枚板で並ぶ:
- クエリ実行 (try/except 付き)
- 分布要約 (`distribution_summary`)
- findings HTML 組み立て (概要 → 性別層別 → Top-N)
- 図作成 (Violin / Scatter / Bar)
- `sb.validate_findings` の違反ハンドリング
- `ReportSection` 返却

工程を以下に分解 (モジュールごとに適宜):
- `_fetch_<section>_rows(conn) -> list[Row]` — try/except + 空リスト fallback
- `_findings_overview(summ) -> str`
- `_findings_gender_strata(rows) -> str`
- `_findings_top_n(rows, n) -> str`
- `_make_<chart>_figure(rows, summ) -> go.Figure`
- `_append_validation_warnings(findings, sb) -> str`

`_build_*_section` はこれらを呼び出して `ReportSection(...)` を返すだけの 15 行程度の関数にする。

**注意**: `score_layers_analysis.py` は 1491 行と巨大。いきなり全分解はリスク。**section 単位で 1 PR / commit** に分けて段階進行。

**検証**: 各 `task report-<name>` の生成 HTML diff が「空白のみ」になること。

---

## 馬鹿 / 無益コード単発 fix (T4-T7 に含まれる以外)

### F1 — `src/analysis/va/pipeline/core_scoring.py:82-83`
```python
# VA AWCC (placeholder — VA community bridging is less applicable)
context.va_awcc_scores = {pid: 0.0 for pid in context.va_person_ids}
```
placeholder であることを関数名で表現する (T2 の `_run_va_awcc_placeholder` に移す)。将来実装する予定があるなら `TODO.md` に項目追加、無いなら「VA AWCC は構造的に 0 固定」と docstring に明記。

### F2 — `src/utils/name_utils.py` の import 配置
L147-148 で `httpx` と `src.utils.config` を関数内 import している。`try: ... except ImportError: return None` で囲まれているが、`httpx` はプロジェクト必須依存なので ImportError はまず起きない。ファイル先頭に移して ImportError ハンドリングは削除。

---

## 優先順位 (委託推奨順)

| # | タスク | 規模 | 難度 | 依存 |
|---|--------|------|------|------|
| 1 | T1 SQL/色ヘルパー共通化 | 大 (46 箇所) | 低 (mechanical) | なし |
| 2 | T4 api_reports error boundary + dead code | 中 | 低 | なし |
| 3 | T2 VA core_scoring 分解 | 中 | 中 | なし |
| 4 | T5 name_utils 分解 + hack 修正 | 中 | 中 | なし |
| 5 | T7 section_builder `_load_lineage_row` fix | 小 | 低 | なし |
| 6 | T3 VA result_assembly 分解 | 小 | 低 | T2 後が望ましい |
| 7 | T6 enrich_hometown 分解 + fix | 中 | 低 | なし |
| 8 | T8 巨大レポートの工程分解 | 特大 | 中 | T1 完了後 |

T1〜T7 は独立なので並列委託可。T8 は最後に残し、section 単位で段階的に。

---

## 委託時の注意事項

1. **1 タスク = 1 commit = 1 PR** を基本。T8 のような巨大タスクは **section 単位** でさらに分割。
2. commit message は既存スタイル (`§X.Y <何を>: <1 行要約>` の形) を踏襲。DONE.md の entry と見出しを揃える。
3. `CLAUDE.md` の Hard Rules (anime.score 不使用 / ability 禁止 / disclaimer) はリファクタでも破らない。特に SQL 断片共通化 (T1) で anime.score を参照する SQL を混ぜないこと。
4. `feedback_function_decomposition.md` に従い、関数化で行数が多少増えても OK。名前で意図が分かることを優先。
5. Python 3.12 なので `typing.List` / `typing.Dict` / `typing.Optional` は全て `list` / `dict` / `X | None` に。リファクタ中に見つけたら直す。
6. `except Exception:` は見つけ次第絞り込む (具体的な例外型か、少なくとも log + re-raise)。
7. 不明点は **実装前に TODO.md に質問を書き出す**。勝手に「こうだろう」で進めない。
8. スキーマに触れる変更 (models_v2.py) は絶対に混ぜない。リファクタは SQL 読み取り構造の改善のみ。
