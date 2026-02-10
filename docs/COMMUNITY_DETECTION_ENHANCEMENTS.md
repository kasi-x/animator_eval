# Community Detection Enhancements — 派閥分析の拡張機能

## 概要

コミュニティ検出（派閥分析）モジュールに、**師弟関係の推定**と**時系列での能力評価**機能を追加しました。

これにより、コミュニティ内の人間関係の構造と、メンバーの時間的な成長パターンを定量的に分析できるようになりました。

---

## 追加機能

### 1. 師弟関係検出（Mentorship Detection）

コミュニティ内のメンター-メンティー関係を自動推定します。

**検出基準**:
- 役職階層: 上位役職（監督・作画監督）と下位役職（原画・動画）のペア
- 共演回数: 最低2作品以上の共同参加
- 信頼度: 共演作品数、役職ギャップ、活動期間から算出（0-100）

**実装**:
```python
def detect_mentorships_in_community(
    community_members: list[str],
    credits: list[Credit],
    anime_map: dict[str, Anime],
    min_shared_works: int = 2,
) -> list[tuple[str, str, float]]:
    """コミュニティ内の師弟関係を検出.

    Returns:
        [(mentor_id, mentee_id, confidence), ...]
    """
```

**使用例**:
```python
from src.analysis.community_detection import detect_mentorships_in_community

mentorships = detect_mentorships_in_community(
    ["director1", "animator1", "animator2"],
    all_credits,
    anime_map,
    min_shared_works=3
)

for mentor, mentee, confidence in mentorships:
    print(f"{mentor} → {mentee} (信頼度: {confidence:.1f})")
```

---

### 2. 時系列能力評価（Temporal Ability Metrics）

コミュニティ形成期におけるメンバーの能力を3つの視点で評価します。

#### 2.1 当時の能力（Ability at Formation Time）

コミュニティが最も活発だった時期における実際のスコア。

**特徴**:
- その時点で計測された客観的な評価値
- 現在の composite スコアを使用（理想的には当時のスコアを再計算）

#### 2.2 当時推定の潜在能力（Prospective Potential）

形成期時点で推定される潜在能力。**未来のデータを使わない前向き推定**。

**計算要素**:
- 成長率: 初期クレジット数と最近クレジット数の比較
- キャリア初期ボーナス: 新人ほど高い潜在能力を持つと仮定
- 上昇トレンド: クレジット数が増加傾向なら +20点、減少なら -10点

**実装**:
```python
def compute_prospective_potential(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    evaluation_year: int,
    current_score: float,
) -> float:
    """当時の潜在能力を推定（未来データを使わない前向き推定）.

    Returns:
        推定潜在能力（0-100）
    """
```

**例**:
- 2020年時点のスコア: 55
- クレジット数が増加傾向: +15ボーナス
- キャリア3年目: +7ボーナス
- **潜在能力**: 77

#### 2.3 事後推定の潜在能力（Retrospective Potential）

未来のデータを含めて後付けで推定する潜在能力。**事後分析用**。

**計算要素**:
- 未来のピークスコア: その人が最終的に到達した最高スコア
- 当時との差分: ピークスコアと評価時スコアのギャップ
- キャリアステージ補正: 初期ほど潜在能力が高い（ピークまでの余地が大きい）

**実装**:
```python
def compute_retrospective_potential(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    evaluation_year: int,
    current_score: float,
    future_peak_score: float,
) -> float:
    """事後推定の潜在能力（未来データを使った後付け推定）.

    Returns:
        事後推定潜在能力（0-100）
    """
```

**例**:
- 2020年時点のスコア: 55
- 2025年のピークスコア: 85
- キャリア3年目（初期）: 潜在能力係数 0.9
- **事後潜在能力**: 55 + (85 - 55) × 0.9 = 82

---

### 3. コミュニティ形成期の特定

コミュニティが最も活発だった期間（3年間のウィンドウ）を自動検出します。

**実装**:
```python
def get_community_formation_period(
    community_members: list[str],
    credits: list[Credit],
    anime_map: dict[str, Anime],
) -> tuple[int, int] | None:
    """コミュニティの形成期（最も活発だった期間）を特定.

    Returns:
        (start_year, end_year) または None
    """
```

**アルゴリズム**:
1. メンバー全員のクレジット年を集計
2. 3年間のスライディングウィンドウでクレジット密度を計算
3. 最も密度が高いウィンドウを形成期として返す

---

## データ構造の変更

### Community Dataclass

新しいフィールドを追加:

```python
@dataclass
class Community:
    # 既存フィールド
    community_id: int
    members: list[str]
    size: int
    density: float
    # ...

    # 新規フィールド
    mentorship_pairs: list[tuple[str, str, float]]  # 師弟関係
    avg_ability_at_formation: float                 # 形成期平均能力
    avg_prospective_potential: float                # 前向き潜在能力
    avg_retrospective_potential: float              # 後付き潜在能力
    ability_range: tuple[float, float]              # 能力範囲 (min, max)
```

### JSON Export Format

エクスポートデータに新セクション追加:

```json
{
  "communities": [
    {
      "community_id": 0,
      "size": 5,
      "density": 0.8,
      "members": [...],
      "mentorships": [
        {
          "mentor_id": "director1",
          "mentor_name": "監督A",
          "mentee_id": "animator1",
          "mentee_name": "アニメーター1",
          "confidence": 95.0
        }
      ],
      "ability_metrics": {
        "avg_ability_at_formation": 75.5,
        "avg_prospective_potential": 82.3,
        "avg_retrospective_potential": 78.9,
        "ability_range": {
          "min": 55.0,
          "max": 95.0
        }
      }
    }
  ]
}
```

---

## 使用方法

### 基本的な使い方

```python
from src.analysis.community_detection import (
    detect_communities,
    compute_community_features,
)
from src.analysis.graph import create_person_collaboration_network

# 1. コラボレーショングラフ構築
collab_graph = create_person_collaboration_network(credits, anime_map)

# 2. コミュニティ検出
communities = detect_communities(
    collab_graph,
    min_community_size=5,
    resolution=1.0
)

# 3. 特徴量計算（師弟関係・能力評価を含む）
features = compute_community_features(
    communities,
    credits,
    anime_map,
    person_scores  # composite スコアを含む辞書
)

# 4. 結果表示
for comm_id, comm in communities.items():
    print(f"\nコミュニティ {comm_id}:")
    print(f"  メンバー数: {comm.size}")
    print(f"  師弟関係: {len(comm.mentorship_pairs)}組")
    print(f"  形成期平均能力: {comm.avg_ability_at_formation:.1f}")
    print(f"  潜在能力（前向き）: {comm.avg_prospective_potential:.1f}")
    print(f"  潜在能力（後付き）: {comm.avg_retrospective_potential:.1f}")
```

### スタンドアロン実行

```bash
pixi run python -m src.analysis.community_detection
```

出力例:
```
検出されたコミュニティ数: 3
ブリッジ人物数: 12

コミュニティ 0:
  サイズ: 15人
  密度: 0.667
  中心メンバー:
    - 宮崎駿 (次数: 120)
    - 鈴木敏夫 (次数: 98)
    - 久石譲 (次数: 85)
  師弟関係: 8組
    - 宮崎駿 → 庵野秀明 (信頼度: 95.0)
    - 宮崎駿 → 近藤勝也 (信頼度: 92.0)
    - 近藤勝也 → 田中敦子 (信頼度: 88.0)
  形成期の平均能力: 82.5
  当時推定の潜在能力: 85.3
  事後推定の潜在能力: 84.1
  能力範囲: 65.0 - 95.0
```

---

## 技術的詳細

### 依存関係

- `src.analysis.mentorship`: 師弟関係推定の既存モジュールを統合
- `src.analysis.career`: CAREER_STAGE による役職階層定義
- NetworkX: Louvain法（greedy_modularity_communities）

### パフォーマンス

- メンターシップ検出: O(M × N) — M: メンバー数, N: 平均クレジット数
- 潜在能力計算: O(N) — 各メンバーのクレジット数
- コミュニティ全体: O(C × M × N) — C: コミュニティ数

**推奨**:
- 大規模データセット（>10000人）では `min_community_size=10` 以上を推奨
- メンターシップの `min_shared_works=3` で誤検出を削減

---

## テスト

テストスクリプト: `/tmp/test_community_enhancements.py`

```bash
pixi run python /tmp/test_community_enhancements.py
```

**テスト項目**:
- ✅ Mentorship Detection (5組検出)
- ✅ Prospective Potential (成長率計算)
- ✅ Retrospective Potential (事後推定)
- ✅ Formation Period (2020-2022)
- ✅ Full Community Detection (能力メトリクス統合)

---

## ユースケース

### 1. スタジオ採用分析

「このコミュニティの潜在能力が高い新人は誰か？」

```python
for comm in communities.values():
    for member_id in comm.members:
        prospective = compute_prospective_potential(...)
        if prospective > 80 and current_score < 60:
            print(f"{member_id}: 高潜在能力の新人候補")
```

### 2. メンター配置最適化

「どの監督がメンター役として最も影響力があるか？」

```python
mentor_counts = defaultdict(int)
for comm in communities.values():
    for mentor, mentee, conf in comm.mentorship_pairs:
        mentor_counts[mentor] += 1

top_mentors = sorted(mentor_counts.items(), key=lambda x: -x[1])
```

### 3. コミュニティ健全性評価

「潜在能力と現実のギャップが大きいコミュニティは？」

```python
for comm in communities.values():
    gap = comm.avg_retrospective_potential - comm.avg_ability_at_formation
    if gap > 20:
        print(f"コミュニティ {comm.community_id}: 未達成ポテンシャル大")
```

---

## 今後の拡張案

1. **動的メンターシップ追跡**: 師弟関係の時系列変化を追跡
2. **潜在能力の信頼区間**: 推定精度を信頼区間で表現
3. **役職遷移パターン**: メンティーの役職昇進パターンを分析
4. **外部ブリッジスコア**: コミュニティ間の橋渡し役の重要度評価

---

## 参考資料

- `src/analysis/mentorship.py`: 師弟関係推定の詳細
- `src/analysis/temporal_influence.py`: 時系列プロファイル分析
- `docs/NEO4J_MIGRATION.md`: グラフDB統合ガイド

---

**実装日**: 2026-02-10
**バージョン**: v1.0
**著者**: Claude Opus 4.6 + Human Collaboration
