# 15 拡張目的レポート群 (PROJECT.md §6.14 起源)

**Objective**: PROJECT.md §6.14 で宣言した 8 種の拡張目的レポートを一級アウトプットとして実装し、各 brief への組込み + REPORT_INVENTORY 登録までを 1 カード = 1 レポートで完結させる。

---

## 前提

- **データ層は既存 SILVER + 拡張済 BRONZE** を使用
- 各レポートは `scripts/report_generators/reports/` 配下に新規ファイル
- `scripts/report_generators/reports/base.py` 継承
- 出力は HTML report、対応 brief への section 組込み
- 一部レポート (O4 / O7 / O8 / O5) は **前提データ整備が未完** — カード冒頭に依存記載、Pre-conditions で blocker 明示

---

## 共通実装規約 (全カード共通)

- `lint_vocab` 通過必須 (`ability/skill/talent/competence/capability` 禁則)
- method gate 表示必須 (CI / null model / holdout のいずれか最低 1 ヶ所)
- 全 SQL で `anime.score` SELECT 禁止 (lint で検証)
- disclaimer (JA + EN) を `build_disclaimer()` で挿入
- ReportSection の `validate_findings` 違反ハンドリング追加
- テスト: `tests/test_<report_name>.py` で smoke + lint_vocab + method gate
- Findings / Interpretation 分離 (`docs/REPORT_PHILOSOPHY.md`)

---

## Hard Rule リマインダ (全カード共通)

- **H1**: `anime.score` を scoring / formula / edge weight / 分類境界に使わない。display 用のみ可
- **H2**: 能力 framing 禁止語 (`ability/skill/talent/competence/capability` + 日本語「能力」「実力」「優秀」「劣る」「人材の質」)
- **H3**: entity_resolution ロジック不変
- **H5**: 既存 2161+ tests green 維持
- **H8**: 行番号信頼禁止

---

## カード構成 (実施推奨順)

| ID | 拡張目的 | スラグ | 依存 | Priority |
|----|---------|------|------|---------|
| [01_o3_ip_dependency](01_o3_ip_dependency.md) | O3 | IP 人的依存リスク | 既存 SILVER のみ | 🟠 |
| [02_o1_gender_ceiling](02_o1_gender_ceiling.md) | O1 | ジェンダー天井効果 | 既存 SILVER + gender カバレッジ確認 | 🟠 |
| [03_o2_mid_management](03_o2_mid_management.md) | O2 | 中堅枯渇 | 既存 SILVER のみ | 🟠 |
| [04_o4_foreign_talent](04_o4_foreign_talent.md) | O4 | 海外人材 | `name_zh/ko` + `country_of_origin` SILVER 充実 (Card 14) | 🟡 |
| [05_o6_cross_border](05_o6_cross_border.md) | O6 | 国際共同制作 | O4 の延長 | 🟡 |
| [06_o8_soft_power](06_o8_soft_power.md) | O8 | ソフトパワー指標 | 海外配信メタ取得経路 (Card 16) | 🟢 |
| [07_o7_historical](07_o7_historical.md) | O7 | 失われたクレジット復元 | schema (`confidence_tier`) + claim フロー | 🟢 |
| [08_o5_education](08_o5_education.md) | O5 | 教育機関キャリア追跡 | 出身校データ取得経路 (現状不在) | 🟢 |

横断タスク: [x_cross_cutting](x_cross_cutting.md)
- brief マッピング確定 (`docs/REPORT_INVENTORY.md`)
- 新 audience brief 追加検討 (教育機関 / 文化財 / クールジャパン)
- lint_vocab 拡張 (O7 で「失われた」「不在」等の表現再点検)
- method_note template 拡張 (O1-O8 で異なる手法バリエーション)

---

## 並列衝突回避

| 競合点 | 対処 |
|--------|------|
| `scripts/report_generators/reports/base.py` 編集 | **触らない**。各 Card は新規ファイルのみ |
| `docs/REPORT_INVENTORY.md` 編集 | 各 Card は **末尾追記**、中央部編集禁止 |
| brief 組込み (`scripts/report_generators/briefs/*`) | 各 Card は **追加** のみ、既存 section 順入替え禁止 |

---

## 完了判定

- 各 Card の Verification 全 pass
- `pixi run python scripts/lint_report_vocabulary.py` clean
- `rg 'anime\.score\b' scripts/report_generators/reports/<new_files>` 0 件
- 既存 2161+ tests green
- `docs/REPORT_INVENTORY.md` に新レポート登録

## 関連

- `TODO.md §14`: 旧記述。本カード群完了時に「→ TASK_CARDS/15」へ書き換え
- `docs/REPORT_PHILOSOPHY.md`: Findings / Interpretation 分離原則
- `docs/REPORT_INVENTORY.md`: 既存レポート + brief マッピング
- `PROJECT.md §6.14`: 拡張目的 8 種の元宣言
