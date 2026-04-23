# TASK_CARDS — 実行指示書集

本ディレクトリは `TODO.md` を **実行可能な単位まで分解した指示書** の集合です。弱いモデルや初見のエンジニアが 1 タスクずつ独立に実行できるよう、以下を各カードで保証します:

- 前提条件 (何が完了している必要があるか)
- 触ってはいけないファイル・関数の明示
- シンボル名(関数・クラス)による指示(行番号は時間で劣化するため避ける)
- 手順の step-by-step 分解
- 期待 diff の特徴 (行数目安、新規関数の有無)
- Copy-paste で走る検証コマンド (`pytest` 必須)
- Stop-if 条件 (失敗時に進まない)
- ロールバック手順

---

## ディレクトリ構成

```
TASK_CARDS/
├── README.md                    # 本ファイル
├── _hard_constraints.md         # 全タスク共通の絶対遵守事項
├── _card_template.md            # カード記述テンプレート
├── 01_schema_fix/               # 🔴 Critical: 新 schema へ一発移行 (5 枚)
│   ├── README.md
│   ├── 00_target_schema.md          # 新 schema を DDL として書き下ろす
│   ├── 01_one_shot_copy.md          # 旧 DB → 新 DB の一発コピー
│   ├── 02_legacy_cleanup.md         # legacy migration 全削除
│   ├── 03_fresh_init_smoke.md       # smoke 1/2: fresh init
│   ├── 04_scraper_smoke.md          # smoke 2/2: scraper 書き込み
│   └── _naming_decisions/           # 旧 07-14 を新 schema 設計資料として保持
├── 02_phase4/                   # 🟠 Major: Phase 4 残務
│   ├── README.md
│   ├── 01_meta_lineage_population.md
│   ├── 02_pipeline_smoke_test.md
│   ├── 03_method_notes_validation.md
│   ├── 04_lineage_check_full_impl.md
│   └── 05_tech_appendix_vocab_audit.md
├── 03_consistency/              # 🟠 Major: コード一貫性
│   ├── README.md
│   ├── 01_scraper_unification.md
│   ├── 02_stop_anime_display_writes.md
│   ├── 03_entity_resolution_audit.md
│   ├── 04_episode_sentinel.md
│   └── 05_etl_init_exports.md
├── 04_duckdb/                   # 🟠 Major: DuckDB 全面移行
│   └── SENIOR_ONLY.md           # 弱いモデル禁止の警告と phase 概要
├── 05_hamilton/                 # 🟠 Major: Hamilton 導入
│   └── SENIOR_ONLY.md           # 同上
└── 06_tests/                    # 🟡 Minor: テストカバレッジ
    └── README.md                # 典型的なテスト追加、TODO.md 参照

注: 旧 07_schema_baseline/ は削除 (内容は 01_schema_fix/02+03+04 に統合)
```

---

## 実行順序

**必ず 01 → 02 → 03 の順で進めてください**。04 (DuckDB) と 05 (Hamilton) は 01-03 完了後、**人間レビュー必須**の判断を経て着手。

```
01_schema_fix/00 → 01 → 02 → 03 → 04  (厳密順、新 schema への一発移行)
                                ↓
        02_phase4 と 03_consistency は並行実行可
                                ↓
     (03 完了後、senior review) → 04_duckdb / 05_hamilton
                                ↓
              06_tests (随時)
                                    ↓
                              06_tests
```

**01 の内部順序は厳密**: schema 整合性は段階的に整えないと途中で DB が壊れる可能性があります。

---

## カードの読み方

各カードは以下の節を必ず含みます:

| 節 | 目的 |
|----|------|
| Goal | 完了状態を 1 文で |
| Hard constraints | 絶対違反禁止事項(毎カード再掲) |
| Pre-conditions | このタスクに入る前に必要な状態 |
| Files to modify | 変更対象ファイル |
| Files to NOT touch | 触ってはいけないファイル・関数 |
| Steps | 手順(シンボル名ベース、行番号は参考のみ) |
| Verification | 完了確認コマンド |
| Stop-if | 失敗時の中断条件 |
| Rollback | アボート時の復元手順 |
| Completion signal | 完了を宣言する条件 |

---

## 実行ルール (弱いモデル向け)

1. **1 カードずつ** 実行する。複数を同時に走らせない
2. 各 Step の **終わりに `git status` を必ず確認** し、想定外の変更がないかチェック
3. **Verification がパスしない限り完了宣言しない**
4. **Stop-if 条件に該当したら即中断** し、ロールバック手順に従う
5. **迷ったら中断してユーザに報告**。判断を先走らない
6. **Hard constraints を暗記**。全カードで再掲されるが、`_hard_constraints.md` を必ず先に読むこと
7. **行番号は参考値**。コードは日々変わるので、シンボル名(関数名・クラス名)で探すこと

---

## テスト戦略 (4-tier)

**フル suite は 2161 tests × 17 分**。毎 step 全件は非現実的。以下 4 層で使い分けます。

| Tier | コマンド | 所要 | 用途 |
|------|---------|------|------|
| **T1 影響のみ** | `pixi run test-impact` | 1-30 秒 | Step 中、編集直後の高速フィードバック。testmon が変更影響テストだけ選ぶ |
| **T2 前回失敗** | `pixi run test-quick` | 5-60 秒 | 失敗を直した直後、その失敗だけ再実行 (`--lf -x`) |
| **T3 scoped** | `pixi run test-scoped <paths_or_-k>` | 30 秒-3 分 | カード Verification 時、関連モジュールを並列実行 |
| **T4 full** | `pixi run test` | 3-8 分 (xdist 並列) | **commit 直前に 1 回だけ**。全 2161 件 |

### T1 (testmon) の挙動と注意

- `.testmondata` という cache ファイルをリポジトリ root に作る (git 無視推奨: `.gitignore` 追加)
- 初回実行は full suite 相当 (cache 構築)
- 2 回目以降、**変更ファイルを import しているテストだけ実行**
- **xdist と併用不可** (`-n auto` は使わず `-n0`)。テスト数が激減するので並列化不要
- **スキーマ変更・migration 追加時**は cache をリセット: `pixi run test-impact-reset`
  - (理由: DB スキーマが変わると testmon が依存を正しく追跡できない)

### T3 (scoped) の使い方例

```bash
# パス指定
pixi run test-scoped tests/test_db_schema.py tests/test_api.py

# -k パターン
pixi run test-scoped tests/ -k "scores or person_scores"

# 組み合わせ
pixi run test-scoped tests/test_entity_resolution*.py -k "merge"
```

### Tier の選び方 (各カード内で明示)

各タスクカードの **Verification セクション** に以下を明示:

- **T3 scope**: カード完了時に走らせる path / パターン
- **T4**: commit 直前に `pixi run test`

Step 内では T1 (testmon) を基本にし、失敗直後の修正では T2 を使う、と作業者が自由に選ぶ。

### 並列 (pytest-xdist)

`test` / `test-quick` / `test-scoped` は `-n auto` で CPU コア数並列実行。フル suite が 17 分 → 3-8 分に短縮。

---

## 共通検証コマンド (カード完了時の最小セット)

```bash
# 1. T2 (カード固有の scope、各カードで明示)
pixi run test-scoped [...カード指定のパス/パターン...]

# 2. lint
pixi run lint

# 3. invariant: anime.score が分析コードに漏れていない
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件

# 4. git 差分が想定範囲内
git diff --stat

# 5. commit 直前のみ: T3 full
pixi run test
```

**T2 + lint + invariant + git 差分** が揃わない限り次カードに進まない。
**T3** は同セッションで複数カード完了後、最後の commit 前にまとめて走らせればよい。
