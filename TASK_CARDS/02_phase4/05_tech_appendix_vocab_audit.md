# Task: Technical Appendix 語彙棚卸し + exception 登録

**ID**: `02_phase4/05_tech_appendix_vocab_audit`
**Priority**: 🟠 Major
**Estimated changes**: 約 +30 / -0 lines, 1-2 files
**Requires senior judgment**: **minor** (許容/非許容の判断)
**Blocks**: (なし)
**Blocked by**: (なし、独立して実行可)

---

## Goal

`scripts/report_generators/reports/technical_appendix/` 配下 (15 technical reports) の禁止語使用状況を棚卸し、**技術文書として許容される使用** (例: `statistical power`, `technical capability` などの文脈語) を exception として YAML に登録する。本質的な違反 (能力 framing) は本文修正を提案する。

---

## Hard constraints

- H2 能力 framing 禁止
- H5 既存テスト green 維持

**本タスク固有**:
- **exception 登録は必要最小限**。安易に許容すると H2 がザルになる
- 疑わしい場合は exception 登録せず、本文修正を提案する

---

## Pre-conditions

- [ ] `pixi run test` pass
- [ ] 現状の lint 結果を確認:
  ```bash
  pixi run python scripts/lint_report_vocabulary.py
  # 期待: OK, 0 violations (39 files checked)
  ```
  もし既に違反があれば本タスクの起点。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/report_generators/forbidden_vocab_exceptions.yaml` (新規 or 追記) | ファイル/パターン単位の exception 登録 |
| `scripts/lint_report_vocabulary.py` | exceptions.yaml を読み込み、該当パターンを skip する機構 (既存になければ実装) |

---

## Files to NOT touch

- レポート本体 (まずは棚卸しのみ、本文修正は判断が必要な別提案)
- 公開レポート (policy/hr/biz brief)

---

## Steps

### Step 0: 現状棚卸し

Technical appendix 配下を明示的に lint し、違反を列挙:

```bash
# 対象ディレクトリを確認
find scripts/report_generators/reports -type d -name 'technical*'
# 例: scripts/report_generators/reports/technical_appendix/

# lint を technical appendix のみに対して実行
pixi run python scripts/lint_report_vocabulary.py --path scripts/report_generators/reports/technical_appendix/ 2>&1 | tee /tmp/vocab_audit.txt
```

もし `--path` オプションが未実装なら、手動 grep:

```bash
rg -in '\b(ability|skill|talent|competence|capability)\b|能力|実力|優秀' \
   scripts/report_generators/reports/technical_appendix/ | tee /tmp/vocab_audit.txt
```

### Step 1: 違反を分類

`/tmp/vocab_audit.txt` の各行を以下の 3 カテゴリに分類:

| カテゴリ | 例 | 対処 |
|---------|-----|------|
| **A. 技術的文脈** | `statistical power`, `technical capability`, `model capability (to ...)`  | exception 登録 |
| **B. 引用・参照** | 論文タイトル、引用文内の単語 | exception 登録 |
| **C. 本質的違反** | 「この職種の能力を評価する」「優秀な作画家」 | **exception 登録せず**、本文修正提案 |

判断に迷ったら C と見なす (保守的に)。

### Step 2: exceptions.yaml 作成

`scripts/report_generators/forbidden_vocab_exceptions.yaml`:

```yaml
# 禁止語検査の exception 定義
#
# ポリシー:
# - 技術文書 (technical_appendix/) 内でのみ許容
# - 公開 brief (policy/hr/biz) では H2 を厳格適用、exception 認めない
# - 理由を必ず書く。"とりあえず通したい" 系の登録は禁止
#
exceptions:
  - path: "scripts/report_generators/reports/technical_appendix/network_centrality.md"
    pattern: "capability"
    reason: "technical context: 'graph capability for X' refers to algorithmic reachability, not human ability"
    approved_date: "2026-04-22"

  # 以下、Step 1 カテゴリ A/B で拾ったものを追加
```

### Step 3: lint スクリプトに exception サポートを追加 (未実装なら)

```bash
grep -n 'exceptions\|load_exceptions' scripts/lint_report_vocabulary.py
```

既存実装があれば skip。なければ以下を追加:

```python
import yaml
from pathlib import Path

_EXCEPTIONS_FILE = Path("scripts/report_generators/forbidden_vocab_exceptions.yaml")


def _load_exceptions() -> list[dict]:
    if not _EXCEPTIONS_FILE.exists():
        return []
    with _EXCEPTIONS_FILE.open() as f:
        data = yaml.safe_load(f) or {}
    return data.get("exceptions", [])


def _is_exempted(file_path: str, pattern: str, exceptions: list[dict]) -> bool:
    for ex in exceptions:
        if ex.get("path") == file_path and ex.get("pattern") == pattern:
            return True
    return False
```

lint ループ内で `_is_exempted(file, matched_word, exceptions)` を呼んで該当マッチを skip する。

### Step 4: lint 再実行

```bash
pixi run python scripts/lint_report_vocabulary.py
# 期待: OK, 0 violations (exception 済みを除く)
```

### Step 5: C カテゴリ (本質的違反) は別途報告

Step 1 で C カテゴリに分類されたものがあれば、そのファイル・行・該当語・修正案を **ユーザに報告**して指示を仰ぐ。本タスクでは本文修正しない。

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "vocabulary or lint_vocab"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. exceptions.yaml が valid YAML
pixi run python -c "import yaml; yaml.safe_load(open('scripts/report_generators/forbidden_vocab_exceptions.yaml'))"
# エラー出ないこと

# 2. lint pass
pixi run python scripts/lint_report_vocabulary.py
# 期待: OK, 0 violations

# 3. exception が本当にスキップされているか
# (exceptions.yaml から 1 件削除して実行 → fail することを確認 → 戻す)

# 4. 既存テスト
pixi run test-scoped tests/ -k "vocabulary or lint_vocab"

# 5. Lint
pixi run lint
```

---

## Stop-if conditions

- [ ] C カテゴリ (本質的違反) が 1 件以上検出された → **ユーザ報告して判断仰ぐ**
- [ ] exceptions.yaml の件数が 20 を超える → 過剰な exception、安易な通し方になっている疑い
- [ ] `pixi run test` fail

---

## Rollback

```bash
rm scripts/report_generators/forbidden_vocab_exceptions.yaml
git checkout scripts/lint_report_vocabulary.py
pixi run test-scoped tests/ -k "vocabulary or lint_vocab"
```

---

## Completion signal

- [ ] lint 0 violations (exception 適用後)
- [ ] exceptions.yaml に全件 `reason` が書かれている
- [ ] C カテゴリ残件は報告済み
- [ ] `git commit`:
  ```
  Audit technical appendix vocabulary and register exceptions

  Registers documented exceptions for technical-context usages
  (e.g., "statistical capability", "graph reachability") in
  forbidden_vocab_exceptions.yaml. Each exception has a written
  reason and approval date.

  Essential violations (H2) in N reports are reported separately
  for manual review.
  ```
