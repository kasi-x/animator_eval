# 06_tests — テストカバレッジ

**優先度**: 🟡 Minor
**Requires senior judgment**: partial (テスト対象選定は senior、テスト実装は弱いモデル可)

---

## 背景

`TODO.md §6` に記載されているテストカバレッジ不足:

- **T01**: `pipeline_phases/` の 13/15 ファイルが未テスト
- **T02**: `patronage_dormancy.py` 直接テスト未整備
- **T03**: VA パイプライン 7 モジュール全て未テスト
- **T04**: `generate_all_reports.py` 分割後ヘルパーの単体テスト

---

## 弱いモデルに任せられる部分

既存テスト (`tests/test_*.py`) のパターンを踏襲する形なら、弱いモデルでもテスト追加は可能。

ただし、**以下の前提条件を満たす必要あり**:

1. Senior が「何を testing するか」を指定 (関数 input/output 定義、edge case 列挙)
2. Senior が fixture / synthetic data の用意方法を示す
3. 弱いモデルは「指定された関数を指定されたケースで呼んで assert するテスト」を書く

---

## 推奨順序

| 順序 | 対象 | 理由 |
|------|------|------|
| 1 | `post_processing.py` (T01 の一部) | percentile / CI 計算は副作用少なく、単体テストしやすい |
| 2 | `patronage_dormancy.py` (T02) | 指数減衰 / 猶予期間のロジックを純粋関数として検証できる |
| 3 | `core_scoring.py` (T01 の一部) | AKM/IV/BiRank は合成データで動作確認が可能 |
| 4 | VA modules (T03) | スコープが大きいので最後 |
| 5 | Report generator helpers (T04) | 既存実装で動作確認しやすい |

---

## カード化について

本 Section の詳細カードは **需要に応じて senior が作成**します。

弱いモデルに振る場合は、以下の形式で個別 card を用意すること:

```markdown
# Task: test_core_scoring_akm_basic

**対象関数**: `src/pipeline_phases/core_scoring.py:run_akm()`
**Input 仕様**: [具体的な DataFrame スキーマ]
**期待 output**: [theta_i の shape / 範囲 / 特性]
**Edge case**: [movers 0 人のケース、n=1 のケース...]
**Fixture**: [使用可能な fixture: tests/conftest.py:synthetic_small]
```

---

## 共通パターン

既存テストを読む:

```bash
# シナプスを得る
ls tests/test_pipeline_phases/
cat tests/test_pipeline_phases/test_data_loading.py  # 既存テストの書き方
cat tests/conftest.py                                 # 利用可能な fixture
```

新規テスト追加時の典型:

```python
import pytest
from src.pipeline_phases.core_scoring import run_akm
from tests.conftest import synthetic_small  # 例

def test_akm_produces_person_fixed_effects(synthetic_small):
    result = run_akm(synthetic_small.credits, synthetic_small.anime)
    assert result.theta_i.shape[0] == synthetic_small.n_persons
    assert result.theta_i.dtype == np.float64
    # 値域の sanity check
    assert result.theta_i.min() > -10
    assert result.theta_i.max() < 10
```

---

## 検証

```bash
# 追加テストが pass
pixi run test -- tests/test_pipeline_phases/test_core_scoring.py -v

# 全体に副作用がない
pixi run test
# 期待: 既存 2161 + 新規 = 合計増加
```

---

## 完了シグナル

各テスト追加ごとに:
- 新規テストが pass
- 既存テストに影響なし
- `pixi run lint` pass
- `git commit` (1 テストファイル = 1 コミットが目安)
