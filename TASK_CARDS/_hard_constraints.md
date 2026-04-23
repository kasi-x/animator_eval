# _hard_constraints.md — 全タスク共通の絶対遵守事項

**全てのタスクカードはこれらを前提にしています**。違反するとプロジェクトが壊れます。

---

## H1. anime.score を scoring 公式に使わない (核心原則)

`CLAUDE.md` 記載の最重要 invariant。

**絶対禁止**:
- `anime.score` / `anime.popularity` / `anime.favourites` を scoring 公式、edge weight、optimization target、分類境界に使う
- SILVER 層の `anime` テーブルに `score` / `popularity` / `favourites` カラムを追加する
- `src/analysis/**` または `src/pipeline_phases/**` から `display_lookup.py` を import する

**許可**:
- BRONZE 層 (`src_anilist_anime` 等) に score を保持
- Report 層 (`scripts/report_generators/**`) から `display_lookup` 経由で表示目的のみ参照

**検証**:
```bash
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 必ず 0 件
rg 'display_lookup' src/analysis/ src/pipeline_phases/   # 必ず 0 件
```

---

## H2. 能力 framing 禁止 (法的要件)

`CLAUDE.md` 記載。defamation リスク対策。

**禁止語** (word boundary で検査、大文字小文字区別なし):
- `ability`, `skill`, `talent`, `competence`, `capability`
- 「能力」「実力」「優秀」「劣る」「人材の質」などの評価語

**検証**:
```bash
pixi run python scripts/lint_report_vocabulary.py   # 0 violations
```

**置換例**: 「能力評価」→「ネットワーク位置と協業密度の定量化」

---

## H3. Entity resolution ロジックは不変

`src/analysis/entity_resolution.py` の**ロジック**を変更しない。理由: false positive の増加は名誉毀損リスク。

**許可**:
- 監査記録(`meta_entity_resolution_audit` への INSERT 等)の追加
- ログ・コメントの追加

**禁止**:
- 類似度計算のアルゴリズム変更
- 閾値の変更
- マージ条件の追加/削除

---

## H4. `credits.evidence_source` カラムを消さない

5 ソース混在の信頼性検証に必須。`credits.source` → `evidence_source` rename (v54) 以降は `evidence_source` が唯一の出所カラム。

**許可**:
- `evidence_source` への値追加
- index 追加

**禁止**:
- `evidence_source` カラム削除
- 既存データを他テーブルに移してこのカラムを廃止する設計変更

---

## H5. 既存テスト 2161 件は green を維持

追加・更新は許可。**削除は要確認**。カードに明示的な削除指示がない限り削除しない。

**検証**:
```bash
pixi run test
# 2161 passed, 4 skipped を期待
```

---

## H6. pre-commit hook の skip 禁止

`--no-verify` は絶対に使わない。hook が失敗したら**中身を直す**。hook を迂回しない。

---

## H7. 破壊的 git 操作禁止

**禁止**:
- `git push --force` / `git push -f`
- `git reset --hard`
- `git checkout .` (全ファイルへのチェックアウト)
- ブランチ削除 (`git branch -D`)
- `git clean -f`

**許可** (必要時):
- 単一ファイルへの `git checkout path/to/file.py` (ロールバック用)
- 新しいコミット作成(amend でない)

---

## H8. 行番号を信じない

コードは編集ごとに行番号が変わる。以下のように扱う:

**禁止**: 「`src/database.py:8924` を編集」と書かれたら直接そこに飛ぶ
**必要**: 対象シンボル(関数名・クラス名)で `grep` し直してから編集

```bash
# NG
sed -i '8924s/foo/bar/' src/database.py

# OK
grep -n "def _migrate_v54_to_v55" src/database.py   # 現在位置を確認
# そのあと Edit ツール等でシンボル周辺の実テキストを編集
```

---

## H9. 「とりあえず動かして様子見」禁止

テストが赤のまま次に進まない。lint エラーを残さない。

**各 Step の終了条件**:
1. 自分が変更したファイルに対する test が pass
2. 全体 test suite が pass (最後の Step の終了時)
3. lint が clean
4. git diff が想定範囲内

いずれか失敗した場合は **Stop-if 条件に該当** として中断する。

---

## H10. 不明なら中断して報告

判断に迷ったら**推測で進めず**、現在の状態を報告して指示を仰ぐ。弱いモデルが「とりあえずやってみた」で壊す事故を避ける。
