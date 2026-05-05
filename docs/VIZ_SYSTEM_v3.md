# Visualization System v3

本文書は Animetor Eval が公開する全レポートの可視化規範を定める。
`REPORT_PHILOSOPHY.md` v2.1 (Findings/Interpretation 分離・方法論最低要件) と
`REPORT_DESIGN_v3.md` (5 タプル method-driven 構造) の可視化対応物である。

v3 は次の三本柱で成る。

1. **theme 一元化** — palette / typography / annotation / CI band / null
   envelope の単一 source。各 report の `fig.update_layout` 重複を撲滅する。
2. **primitive ベース** — 高頻出パターンを 8 個の chart primitive に集約。
   primitive は CI / null / shrinkage を **デフォルトで描画**する。
3. **export 並走** — interactive HTML、static SVG、印刷用 PDF を同一 spec から
   生成する。

---

## 1. なぜ primitive ベースか

現状の `scripts/report_generators/reports/*.py` では各 report が plotly を
直接呼び、CI band / null envelope / annotation / palette を再発明している。
帰結は (i) 描画品質の report 間揺れ、(ii) `REPORT_PHILOSOPHY.md` §3.1
(CI 必須) の viz 反映漏れ、(iii) palette 不統一による colorblind 不適合である。

v3 では `src/viz/primitives/` に 8 個の chart primitive を置き、レポートは
`Spec` データクラスを渡すだけで描画が完結する形に再構築する。primitive は
内部で次を強制する。

- CI band がデータに存在すれば **必ず** 描画する (`auto_ci=True`)。
- null model overlay がデータに存在すれば **必ず** 描画する (`auto_null=True`)。
- shrinkage 適用済の値は **必ず** badge を付ける (`shrinkage_badge=True`)。

これにより `REPORT_PHILOSOPHY.md` §3 の「viz 漏れ」が構造的に発生しなくなる。

---

## 2. primitive カタログ (8 種)

| ID | primitive | 用途 | CI 表現 | null 表現 |
|----|-----------|------|---------|-----------|
| `P1` | `CIScatter` | 点推定 + 区間 (forest plot 含む) | error bar / box | reference line + null band |
| `P2` | `KMCurve` | 生存曲線 (cohort/strata 重ね) | Greenwood band | permutation envelope |
| `P3` | `EventStudyPanel` | 介入前後 dynamic effect | bootstrap band | placebo lines (gray) |
| `P4` | `SmallMultiples` | facet grid (cohort × role 等) | per-facet band | shared null reference |
| `P5` | `RidgePlot` | 分布の重ね (cohort × theta_i 等) | KDE quantile band | null KDE shading |
| `P6` | `BoxStripCI` | 分布要約 + raw 点 + CI | hinge + whisker + 95% CI mark | null median line |
| `P7` | `SankeyFlow` | 段階遷移 (career stage 推移) | edge width 縮約 | edge baseline width |
| `P8` | `RadialNetwork` | ego-network 局所図 | edge weight + node size CI | null density shading |

各 primitive の詳細仕様は §6 で個別記述する。

---

## 3. theme 一元化

### 3.1 構成

```
src/viz/
├── __init__.py
├── theme.py              # 中央テーマ (dark / light / print)
├── palettes.py           # CB-safe palette + 固定 mapping
├── typography.py         # font stack / size scale
├── annotations.py        # 共通 annotation (data boundary, n=, p=)
├── ci.py                 # CI band / whisker 描画関数
├── null_overlay.py       # null model envelope 描画関数
├── shrinkage_badge.py    # 縮約適用 badge
├── primitives/
│   ├── __init__.py
│   ├── ci_scatter.py     # P1
│   ├── km_curve.py       # P2
│   ├── event_study.py    # P3
│   ├── small_multiples.py # P4
│   ├── ridge.py          # P5
│   ├── box_strip_ci.py   # P6
│   ├── sankey.py         # P7
│   └── radial_network.py # P8
├── interactivity.py      # linked brushing / cross-filter
└── export.py             # HTML / SVG / PDF 並走
```

### 3.2 theme module

`Theme` は次を保持する。

```python
@dataclass(frozen=True)
class Theme:
    name: Literal["dark", "light", "print"]
    palette: Palette
    typography: Typography
    grid: GridStyle
    annotations: AnnotationStyle
    ci_band: CIBandStyle
    null_overlay: NullOverlayStyle
```

呼び出し側は `apply_theme(fig, theme="dark")` を 1 行呼ぶ。各 report の
`fig.update_layout` 直書きは禁止 (CI で grep 検出)。

### 3.3 dark / light / print

| theme | 背景 | 用途 |
|-------|------|------|
| `dark` | glass morphism (現行 plotly_dark + CSS) | web brief 既定 |
| `light` | 高コントラスト白背景 | embed / 印刷向け web |
| `print` | モノクロ + パターン fill | PDF / SVG export |

`print` theme は **モノクロでも識別可能** であることを保証する。線種・
パターン fill・marker 形状の組合せで色に依存しない。

---

## 4. palette (CB-safe)

### 4.1 base palette

Okabe-Ito 8 色 + neutral 2 色を base とする。Okabe-Ito は 8 色で全ての
2 色対が CB simulation 上で識別可能。

```python
OKABE_ITO = [
    "#000000",  # black
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#009E73",  # bluish green
    "#F0E442",  # yellow
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
]
```

連続値は viridis (CB-safe perceptually uniform) を使用する。発散値は
RdBu_r (中央値ありの場合のみ)。

### 4.2 固定 mapping

カテゴリ変数を report 横断で同じ色にマップする。読者が複数 report を
並べた時に色の意味が一貫する。

| カテゴリ | mapping |
|----------|---------|
| career stage | 初級=`#56B4E9` / 中級=`#009E73` / 上級=`#CC79A7` |
| role group (24→7 集約) | animator/director/designer/production/writing/technical/other |
| cohort decade | 1970s..2020s に viridis 6 等分 |
| gender | F=`#D55E00` / M=`#0072B2` / unknown=`#7a7a7a` |
| significance | sig=`#000000` / non-sig=`#a0a0a0` |
| null vs observed | observed=`#000000` / null band=`#a0a0a040` |

`palettes.py` から直接 import する。hex 直書きは禁止 (CI で grep)。

### 4.3 dark theme での明度補正

dark 背景で Okabe-Ito の `#000000` は使えないため、dark theme では
`text` color を `#e0e0e0` に置換し、palette 全体に明度補正を掛ける
(`palettes.adjust_for_dark()`)。

---

## 5. CI / null / shrinkage の標準描画

### 5.1 CI band (`ci.py`)

```python
def add_ci_band(
    fig: go.Figure,
    x: Sequence,
    lo: Sequence,
    hi: Sequence,
    *,
    color: str,
    opacity: float = 0.18,
    name: str | None = None,
    legendgroup: str | None = None,
) -> None:
    """連続 x に対する CI band を toself fill で描画。"""
```

```python
def add_ci_whisker(
    fig: go.Figure,
    x: Sequence,
    y: Sequence,
    lo: Sequence,
    hi: Sequence,
    *,
    color: str,
    direction: Literal["x", "y"] = "x",
) -> None:
    """離散点に対する error bar 描画。"""
```

### 5.2 null overlay (`null_overlay.py`)

```python
def add_null_envelope(
    fig: go.Figure,
    x: Sequence,
    null_lo: Sequence,
    null_hi: Sequence,
    *,
    name: str = "null 95%",
    color: str = "#a0a0a0",
    opacity: float = 0.10,
    pattern: Literal["solid", "diagonal"] = "diagonal",
) -> None:
    """null model の P2.5-P97.5 envelope を band として描画。
    print theme では diagonal pattern fill。"""
```

```python
def add_null_reference_line(
    fig: go.Figure,
    value: float,
    *,
    label: str = "null median",
    direction: Literal["h", "v"] = "h",
) -> None:
    """null model の中央値線を描画 (HHI=0.001, HR=1.0 等)。"""
```

### 5.3 shrinkage badge (`shrinkage_badge.py`)

縮約適用済の値を提示する場合、chart 右上に badge を付与する。

```python
def add_shrinkage_badge(
    fig: go.Figure,
    *,
    method: str,            # e.g. "James-Stein", "Empirical Bayes (Beta)"
    n_threshold: int,       # e.g. 30
) -> None:
    """『縮約適用: James-Stein (n<30 で適用)』を annotation で描画。"""
```

---

## 6. primitive 仕様

### 6.1 P1 `CIScatter` (forest plot を含む)

```python
@dataclass
class CIScatterSpec:
    points: list[CIPoint]            # 各点 (label, x, ci_lo, ci_hi, p?)
    x_label: str
    y_label: str = ""
    log_x: bool = False
    reference: float | None = None   # 例 HR=1
    null_band: tuple[float, float] | None = None
    shrinkage: ShrinkageSpec | None = None
    sort_by: Literal["x", "label", "p"] = "x"
    color_by: str | None = None      # palette mapping key
    significance_threshold: float = 0.05
```

描画規則:

- 点 marker は `square` 固定 (forest plot 慣習)。
- error bar 太さ 2px、color は palette mapping、有意 (p<thr) は塗り潰し /
  非有意は中抜き。
- `reference` 線は dashed gray、annotation で値を表示。
- `null_band` があれば背景 band として描画。
- `sort_by` で行順ソート、`autorange="reversed"` で上から並ぶ。
- `print` theme: marker shape を p 値で変化 (square/circle/diamond)。

### 6.2 P2 `KMCurve`

```python
@dataclass
class KMCurveSpec:
    strata: list[KMStratum]   # 各 (label, t, S(t), ci_lo, ci_hi, n_at_risk[])
    x_label: str = "経過時間 t"
    y_label: str = "S(t)"
    null_envelope: NullSeries | None = None
    risk_table: bool = True   # at-risk 数を下に表示
    median_marker: bool = True
```

描画規則:

- strata ごとに color 割当 (palette mapping または固定指定)。
- step plot (`line_shape="hv"`) で KM の階段を表現。
- Greenwood CI band を半透明 fill で重ね。
- `median_marker=True` で中央生存時間に縦線を引く。
- `risk_table=True` で plot 下部に at-risk 数 table を併置 (subplot)。
- `null_envelope` で permutation null の P2.5-P97.5 を背景 band。

### 6.3 P3 `EventStudyPanel`

介入前後の dynamic effect 推定を可視化する。`-k..+k` 期 lead/lag の係数
を CI 付きで描画。

```python
@dataclass
class EventStudySpec:
    leads_lags: list[int]      # e.g. [-5, -4, ..., -1, 0, 1, ..., 5]
    estimates: list[float]
    ci_lo: list[float]
    ci_hi: list[float]
    placebo_runs: list[list[float]] | None = None  # 各 placebo run
    treatment_label: str = "t = 0"
    pre_period_normalization: int = -1  # この期で 0 化
```

描画規則:

- t=0 (介入時) に縦線 + label。
- pre-period (`pre_period_normalization`) で点が必ず 0 (normalization 確認)。
- 線形補間 + 各点に CI whisker。
- `placebo_runs` があれば薄い灰色線で重ね (`opacity=0.2`)。

### 6.4 P4 `SmallMultiples`

facet grid (`cohort × role` 等)。各 facet は P1/P2/P5/P6 の任意 primitive
を内包可能。

```python
@dataclass
class SmallMultiplesSpec:
    facets: list[FacetCell]   # (row_label, col_label, sub_spec)
    sub_primitive: Literal["CIScatter", "KMCurve", "RidgePlot", "BoxStripCI"]
    shared_x: bool = True
    shared_y: bool = True
    shared_null: NullSeries | None = None
    n_cols: int = 4
```

描画規則:

- `make_subplots(rows, cols, shared_xaxes, shared_yaxes)` を使用。
- shared null は全 facet 共通で背景 band。
- facet タイトルは `row=cohort=2010s, col=role=animator` 形式。
- 8 facet 超過は scrollable container で wrap。

### 6.5 P5 `RidgePlot`

分布の重ね描画 (joyplot)。cohort 別 theta_i 分布等。

```python
@dataclass
class RidgePlotSpec:
    distributions: list[RidgeRow]   # (label, samples, color?)
    x_label: str
    overlap: float = 0.6            # ridge 重なり度
    quantile_band: tuple[float, float] = (0.25, 0.75)
    null_distribution: list[float] | None = None
```

描画規則:

- 各 row は KDE で密度推定、`fill="tozeroy"` で塗り潰し。
- `quantile_band` で IQR を濃色強調。
- `null_distribution` があれば全 row に共通の灰色 KDE を背景重ね。

### 6.6 P6 `BoxStripCI`

box + raw 点 strip + 95% CI mark。分布要約と raw を同時提示。

```python
@dataclass
class BoxStripCISpec:
    groups: list[BoxGroup]   # (label, samples, ci_lo, ci_hi, n)
    x_label: str = ""
    y_label: str = ""
    show_strip: bool = True   # raw 点を strip 表示
    strip_max_n: int = 200    # subsampling 上限
    null_median: float | None = None
```

描画規則:

- box は IQR + median、whisker は 1.5 IQR。
- strip jitter 幅 0.3、`marker.size=4`、点が 200 超なら subsample。
- 95% CI は box の右側に短い縦 marker (`▼` shape) で重ね。
- n を box 下に annotation 表示。
- `null_median` で水平 dashed 線。

### 6.7 P7 `SankeyFlow`

段階遷移 (career stage 推移、studio 移籍経路)。

```python
@dataclass
class SankeyFlowSpec:
    nodes: list[SankeyNode]  # (id, label, layer, color?)
    links: list[SankeyLink]  # (source_id, target_id, value, color?)
    layer_labels: list[str]  # 各 layer (e.g. ["t-5", "t", "t+5"])
    null_baseline: dict[tuple[str, str], float] | None = None
    min_link_value: int = 5
```

描画規則:

- `go.Sankey` を使用、palette mapping を node category に適用。
- `null_baseline` を edge tooltip に併記 (`obs=42, null_med=8`)。
- `min_link_value` 未満の link は集約 (`Other` node に流す) して可読性確保。

### 6.8 P8 `RadialNetwork`

ego-network 局所図 (個人の協業者を放射状に配置)。

```python
@dataclass
class RadialNetworkSpec:
    ego_label: str
    neighbors: list[Neighbor]   # (label, edge_weight, ci_lo?, ci_hi?, color?)
    sort_by: Literal["weight", "label"] = "weight"
    max_neighbors: int = 30
    null_density: float | None = None
```

描画規則:

- 円周上に neighbor を `weight` 降順で配置 (時計回り)。
- edge 太さは weight、半透明度は CI 幅と反比例。
- ego ノードは中央、ラベル太字。
- `null_density` があれば背景に同心円で null 密度を shading。

---

## 7. interactivity

### 7.1 linked brushing

複数 primitive を 1 page に並べる brief で、brushing で全 chart に highlight
を伝播する。

```python
def link_brushing(figures: list[go.Figure], key: str) -> str:
    """各 fig の data に共通 customdata を載せ、JS で highlight 伝播。"""
```

実装は plotly の `customdata` + `Plotly.restyle` を使った軽量 JS で
完結させる (外部依存なし)。

### 7.2 cross-filter

brief 上部に共通フィルタ UI (cohort decade / role group / studio tier) を
置き、フィルタ変更で全 chart の trace visibility を切り替える。

```python
def cross_filter_panel(
    facets: list[CrossFilterFacet],
    target_div_ids: list[str],
) -> str:
    """フィルタ UI HTML + JS。各 chart の trace を visible/hidden 切替。"""
```

### 7.3 hover-link

person/anime label の hover で他 chart の同一 entity をハイライト。
`person_id` を customdata に必ず載せる。

---

## 8. export 並走

### 8.1 並走対象

| format | 用途 | 解像度 / 制約 |
|--------|------|---------------|
| HTML (interactive) | brief web 既定 | plotly.js (CDN), responsive |
| SVG | 論文 / プレス資料 | vector, theme=light |
| PDF | 印刷 brief | theme=print, モノクロ可 |
| PNG | サムネイル | 2x retina |

### 8.2 API

```python
def render(spec: PrimitiveSpec, *, theme: str = "dark") -> Figure
def embed(fig: Figure, div_id: str, *, height: int = 480) -> str
def export(fig: Figure, path: Path, *, format: Literal["svg", "pdf", "png"])
```

### 8.3 PDF 生成

`kaleido` (plotly の static export 依存) を使用。`print` theme で再描画
してから export する (dark theme をそのまま PDF 化しない)。

---

## 9. 既存コードからの移行

### 9.1 互換 layer

既存の `scripts/report_generators/html_templates.plotly_div_safe()` を
維持し、内部で `viz.embed()` を呼ぶ thin wrapper に格下げする。
旧 API を呼んでいる report は段階移行する。

```python
# html_templates.py (移行後)
def plotly_div_safe(fig, div_id, height=500):
    # 既存 callers との互換維持
    from src.viz import embed
    return embed(fig, div_id, height=height)
```

### 9.2 `fig.update_layout` 検出

CI step で `git grep -n "fig.update_layout" scripts/report_generators/`
を実行し、新規追加を blocking 化する。既存箇所は移行 PR で潰す。

### 9.3 hex 直書き検出

`#[0-9a-fA-F]{6}` を `scripts/report_generators/reports/` で検出し、
新規追加を blocking 化する。`palettes.OKABE_ITO[i]` 等の参照を強制。

---

## 10. 受け入れ基準 (Phase 0)

1. `src/viz/{theme,palettes,ci,null_overlay,shrinkage_badge}.py` 実装。
2. `src/viz/primitives/ci_scatter.py` (P1) 実装。
3. `policy_attrition` の Cox forest plot が `CIScatter` 経由で描画される。
4. 既存 plotly_div_safe API が thin wrapper で互換。
5. Okabe-Ito palette がテストで shape (8 色, CB simulation pass) を検証。
6. 既存テストが壊れない。

---

## 11. 用語

- **primitive**: 高頻出 chart パターンの parametric テンプレート。
- **CI band**: 95% 信頼区間の塗り潰し帯。Greenwood / bootstrap / delta 由来。
- **null envelope**: 帰無モデルの P2.5-P97.5 範囲 band。
- **CB-safe**: colorblind safe (deuteranopia, protanopia, tritanopia
  シミュレーションで識別可能)。

---

## 改訂履歴

- **v3.0 (2026-05-05)**: 初版。`REPORT_PHILOSOPHY.md` v2.1 §3 (CI / null /
  holdout / shrinkage / sensitivity) を viz primitive で構造的に強制する
  仕様。Phase 0 (theme + palette + CIScatter + 1 report 実証) を本 PR
  スコープと定める。
