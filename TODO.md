# TODO — Animetor Eval

## 懸念事項・法的リスク
- [ ] 名寄せの精度が不十分な場合、信用毀損リスクあり — 名寄せは保守的に（false positive を極力排除）
- [ ] MAL のスクレイピングは利用規約に注意 — Jikan API (非公式REST) を使うが、レート制限 (3req/s) を厳守
- [ ] メディア芸術データベースの SPARQL エンドポイント安定性が不明
- [ ] スコアを「能力」として解釈されないよう、出力時に必ず「ネットワーク密度・位置指標」と明記
- [ ] AniList API はレート制限 (90req/min) あり — 指数バックオフ実装済み

## フェーズ別進捗

### Phase 1: データモデル・基盤 (MVP) ✅
- [x] データクラス定義 (Anime, Person, Credit) — Pydantic v2
- [x] SQLite スキーマ設計 — WAL mode, foreign keys
- [x] config.py の拡充 — パス定数, PageRank パラメータ, 役職重み (24種)
- [x] structlog 導入 — 全モジュール移行済み

### Phase 2: データ収集 ✅
- [x] Jikan API (MAL) — アニメスタッフクレジット取得 (httpx 非同期)
- [x] AniList GraphQL API — 補完データ (httpx 非同期, ページネーション対応)
- [x] メディア芸術DB SPARQL — httpx + typer
- [x] JVMG (Wikidata SPARQL) — httpx + typer

### Phase 3: 名寄せ (Entity Resolution) ✅
- [x] 基本文字列正規化（NFKC, 全角半角, スペース, 敬称除去）
- [x] 完全一致クラスタリング
- [x] クロスソースマッチ (MAL ↔ AniList)
- [x] ローマ字比較による照合改善
- [ ] 漢字読み照合（要外部辞書）
- [ ] 類似度ベースクラスタリング（慎重に — false positive 回避が最優先）

### Phase 4: グラフ構築 ✅
- [x] NetworkX 二部グラフ構築 (person ↔ anime)
- [x] コラボレーショングラフ (person ↔ person)
- [x] 監督→アニメーター有向グラフ
- [x] 役職ベースのエッジ重み (24種)
- [x] 中心性指標計算 (degree, betweenness, closeness, eigenvector)
- [x] 大規模グラフ最適化 (近似betweenness, closeness省略)
- [x] グラフサマリー (密度, 平均次数, クラスタリング係数, 連結成分数)

### Phase 5: スコアリング ✅
- [x] 重み付き PageRank (Authority) — d=0.85, person ノード限定
- [x] 継続起用スコア (Trust) — 時間減衰 (半減期3年), 監督著名度ボーナス
- [x] OpenSkill (Skill) — PlackettLuce, 年度バッチ処理
- [x] 3軸統合スコア (composite) — 設定可能な重み (default: A:0.4, T:0.35, S:0.25)
- [x] 離脱検出 (detect_engagement_decay) — パイプライン統合済み
- [x] スコア正規化 (0-100, min-max per axis)

### Phase 6: 出力・可視化 ✅
- [x] JSON エクスポート (scores.json, report.json, circles.json, anime_stats.json, summary.json, transitions.json)
- [x] テキストレポート (report.txt, キャリアサマリー付き)
- [x] CSV エクスポート (scores.csv, UTF-8 BOM, パーセンタイル・キャリア列付き)
- [x] HTML レポート (report.html, インライン SVG チャート付き)
- [x] SQLite 保存 (scores テーブル, score_history テーブル)
- [x] CLI (typer + Rich) — stats, ranking, profile, search, compare, similar, export, validate, timeline
- [x] 可視化 (matplotlib) — スコア分布, レーダーチャート, ネットワーク図, キャリアタイムライン
- [x] 合成データ生成 (synthetic.py) — テスト・デモ用

### Phase 7: 品質・堅牢性 ✅
- [x] pytest テスト 320件 — 全パス (<5秒)
- [x] ruff lint — クリーン
- [x] データバリデーションモジュール (validation.py, credit quality check)
- [x] パイプラインに離脱検出・キャリア分析・監督サークル・アニメ統計・遷移分析・信頼度統合
- [x] DB 統計・ヘルスチェック (get_db_stats)
- [x] データソース鮮度追跡 (data_sources テーブル)
- [x] スコア安定性検出 (stability.py)
- [x] スコア説明モジュール (explain.py)
- [x] CI/CD (GitHub Actions)
- [x] --dry-run パイプラインモード
- [x] CLI ランキングフィルタ (--role, --sort, --year-from, --year-to)
- [x] DB スキーマバージョニング・マイグレーション
- [x] パイプライン実行履歴追跡 (pipeline_runs テーブル)
- [x] スコア信頼度 (confidence.py) — クレジット数・ソース多様性・活動年数ベース

### Phase 8: 高度な分析 ✅
- [x] FastAPI サーバー (api.py) — 12 エンドポイント（persons, search, profile, similar, history, ranking, anime, transitions, stats, health, summary）
- [x] スコアブレークダウン (explain.py の pipeline 統合)
- [x] 役職遷移分析 (transitions.py) — 遷移確率・平均年数・最頻出パス
- [x] コラボレーションクラスター検出 (clusters.py) — Louvain コミュニティ検出
- [x] Neo4j 互換 CSV エクスポート (neo4j_export.py)
- [x] スコア履歴追跡 (score_history テーブル)
- [x] 人物類似検索 (similarity.py) — コサイン類似度
- [x] 信頼区間付きスコア (confidence.py)
- [x] キャリアタイムライン可視化 (visualize.py)

### Phase 9: さらなる洗練
- [ ] エッジ減衰の時間パラメータ最適化（実データでの検証）
- [ ] より高精度な名寄せ (AI-assisted)
- [ ] Neo4j 直接接続（大規模運用向け）
- [x] Plotly インタラクティブ可視化 — ✅ 完了 (score distribution, radar, scatter, timeline, network)
- [ ] 外部 ID 連携 (AniDB, ANN)
- [ ] WebSocket リアルタイム更新
- [ ] 国際化 (i18n)
