# 05_hamilton — ⚠️ SENIOR ONLY

## 弱いモデルに実行させてはいけません

本 Section (Hamilton 導入 / `PipelineContext` 解消) は **設計判断** が中心のタスクです。弱いモデルに任せると、アーキテクチャが不整合な状態で固定化する危険があります。

### なぜ弱いモデル向きでないか

1. **Node 分解の粒度判断**: 1 関数 = 1 node にするか、機能群でまとめるか。現在の 20+ モジュールを DAG にするときの depth / width 選定
2. **既存 `PipelineContext` との橋渡し**: H-1 ではまだ context を残す設計だが、node 入出力の型定義で Python の type system を正しく使う必要
3. **Executor 選択**: `ThreadPoolExecutor` / `MultiProcessingExecutor` / 直列 — Rust 拡張との相互作用(GIL 解放・プロセス境界)で性能が大きく変わる
4. **Rust 拡張呼び出し規約**: `animetor_eval_core` の関数が thread-safe / process-safe か、node の executor 選定に影響
5. **部分再実行のキャッシュ戦略**: Hamilton の `CachingGraphAdapter` をどこまで使うか
6. **H-4 での `PipelineContext` 削除**: field 使用箇所の追跡と、各 node への引数分配が必要

---

## Phase 概要 (参考)

TODO.md §5 を参照してください。

- **H-1 (PoC)**: Phase 9 `analysis_modules` のみ Hamilton 化 (判断ポイント: ここで効果が出なければ中止)
- **H-2**: Phase 5-8 を Hamilton 化
- **H-3**: Phase 1-4 を Hamilton 化
- **H-4**: `PipelineContext` 完全削除、`src/pipeline.py` 書き換え
- **H-5**: 観測・運用機能 (tag, lifecycle hook, Hamilton UI)

---

## 中止判定 (重要)

H-1 終了時に以下のいずれかに該当したら **H-2 以降中止**:

- Hamilton overhead で Phase 9 並列実行が 20% 以上遅くなる
- 型ヒント + decorator の可読性が `PipelineContext` より悪い
- Rust 拡張との統合で顕著な複雑さが出る

この判断は **Senior の実測とレビューが必須**。弱いモデルには判断できない。

---

## 弱いモデルが関与してよい範囲

Senior が PoC の設計を済ませた後、以下は弱いモデルに振れる:

- `pixi add sf-hamilton` の依存追加
- Senior が作成した具体的な task card に従って、単一 node 変換の実装
- 既存テストが pass することの確認・ログ出力

---

## 着手前の判断事項 (Senior が決める)

1. ⏳ **H-1 スコープ境界**: Phase 9 の 20+ モジュールのうち、どれを先に変換するか (依存少ない順がおすすめ)
2. ⏳ **Node 入出力の型**: Pydantic dataclass を直接受けるか、DataFrame/ndarray 中心にするか
3. ⏳ **Executor 戦略**: 既存 `ThreadPoolExecutor` を Hamilton の同 executor で包むか、入れ替えるか
4. ⏳ **テスト fixture の再設計**: `PipelineContext` ベースのテストを node 単独テストにどう移行するか
5. ⏳ **DuckDB 移行との競合回避**: Phase D (SQLite 撤去) と H-4 (PipelineContext 削除) のどちらを先にするか

---

## 次にやるべきこと

1. `01_schema_fix/` 全完了を確認
2. `02_phase4/` / `03_consistency/` を完了させる
3. **Senior が** H-1 の具体スコープと node 分解案を決める
4. **Senior が** 本 Section の task card を分割・作成する
5. H-1 終了時に中止判定を行う

---

## 参考: TODO.md での元定義

```
### 5.1 Phase H-1: PoC (analysis_modules だけ)
- [ ] pixi add sf-hamilton
- [ ] analysis_modules.py を Hamilton module に変換
- [ ] 並列性能確認
- [ ] 既存テスト pass

### 5.2 H-2: Phase 5-8
### 5.3 H-3: Phase 1-4
### 5.4 H-4: PipelineContext 削除
### 5.5 H-5: 観測・運用機能
```

詳細は `TODO.md §5` を参照。
