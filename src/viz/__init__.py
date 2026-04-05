"""viz — バックエンド非依存の可視化フレームワーク.

5層アーキテクチャ:
  Layer 1: DataProvider     — データ取得・変換
  Layer 2: ChartSpec        — バックエンド非依存チャート仕様
  Layer 3: ExplanationMeta  — 構造化説明メタデータ
  Layer 4: Renderer         — Plotly / CairoMakie
  Layer 5: ReportAssembler  — チャート+説明 → 完成レポート
"""
