"""ML Clustering report — v2 compliant.

Computes PCA + K-Means clustering on scores.json feature vectors
and produces rich visualizations:
  1. PCA 2D Scatter (Scattergl with name search)
  2. PCA Loadings Heatmap
  3. Cluster Profile Heatmap (Z-score)
  4. Silhouette Analysis
  5. PCA Explained Variance
  6. Cluster Summary Table
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..helpers import (
    EXPLORER_URL,
    FEATURE_NAMES,
    extract_features,
    get_feat_person_scores,
)
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class MLClusteringReport(BaseReportGenerator):
    name = "ml_clustering"
    title = "MLクラスタリング分析"
    subtitle = "PCA次元圧縮 × K-Meansクラスタリング"
    filename = "ml_clustering.html"

    def generate(self) -> Path | None:
        scores = get_feat_person_scores()
        if not scores or not isinstance(scores, list) or len(scores) < 10:
            return None

        from sklearn.cluster import KMeans
        from sklearn.decomposition import PCA
        from sklearn.metrics import silhouette_samples, silhouette_score
        from sklearn.preprocessing import StandardScaler

        person_ids, names, features, roles = extract_features(scores)
        n_persons = len(person_ids)
        n_clusters = min(8, max(3, n_persons // 5))

        features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(features)

        n_components = min(features.shape[1], features.shape[0], 10)
        pca = PCA(n_components=n_components, random_state=42)
        X_pca = pca.fit_transform(X_scaled)
        X_2d = X_pca[:, :2]

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(X_scaled)

        if n_persons > n_clusters:
            sample_size = min(n_persons, 5000)
            if n_persons > sample_size:
                rng = np.random.default_rng(42)
                idx = rng.choice(n_persons, size=sample_size, replace=False)
                sil_avg = silhouette_score(X_scaled[idx], cluster_labels[idx])
                sil_samples = np.zeros(n_persons)
                sil_samples[idx] = silhouette_samples(X_scaled[idx], cluster_labels[idx])
            else:
                sil_avg = silhouette_score(X_scaled, cluster_labels)
                sil_samples = silhouette_samples(X_scaled, cluster_labels)
        else:
            sil_avg = 0.0
            sil_samples = np.zeros(n_persons)

        centers_orig = scaler.inverse_transform(kmeans.cluster_centers_)
        cluster_names = self._name_clusters_distinctive(centers_orig, FEATURE_NAMES)

        sb = SectionBuilder()
        sections: list[str] = []

        # Section 1: Stats summary
        sections.append(sb.build_section(
            self._build_stats_section(sb, n_persons, n_clusters, sil_avg)
        ))

        # Section 2: PCA 2D scatter with name search
        sections.append(sb.build_section(
            self._build_pca_scatter_section(
                sb, X_2d, cluster_labels, cluster_names, n_clusters,
                person_ids, names, features, pca, n_persons,
            )
        ))

        # Section 3: PCA Loadings Heatmap
        sections.append(sb.build_section(
            self._build_loadings_section(sb, pca, n_components)
        ))

        # Section 4: Cluster Profile Heatmap
        sections.append(sb.build_section(
            self._build_profile_heatmap_section(
                sb, centers_orig, cluster_names, n_clusters
            )
        ))

        # Section 5: Silhouette Analysis
        sections.append(sb.build_section(
            self._build_silhouette_section(
                sb, sil_samples, cluster_labels, cluster_names, n_clusters, sil_avg
            )
        ))

        # Section 6: PCA Explained Variance
        sections.append(sb.build_section(
            self._build_variance_section(sb, pca)
        ))

        # Section 7: Cluster Summary Table
        sections.append(sb.build_section(
            self._build_summary_table_section(
                sb, centers_orig, cluster_labels, cluster_names, n_clusters,
            )
        ))

        return self.write_report("\n".join(sections))

    # ── Cluster naming ──────────────────────────────────────────

    @staticmethod
    def _name_clusters_distinctive(
        centers: np.ndarray, feature_names: list[str],
    ) -> dict[int, str]:
        """Name clusters by their most distinctive z-score features."""
        n_clusters = len(centers)
        means = centers.mean(axis=0)
        stds = centers.std(axis=0) + 1e-10
        z = (centers - means) / stds

        used: set[str] = set()
        names: dict[int, str] = {}
        for c in range(n_clusters):
            row = z[c]
            order = np.argsort(-np.abs(row))
            primary = ""
            for idx in order:
                feat = feature_names[idx]
                direction = "高" if row[idx] > 0 else "低"
                candidate = f"{direction}{feat}"
                if candidate not in used:
                    primary = candidate
                    used.add(candidate)
                    break
            if not primary:
                primary = f"クラスタ {c}"
            names[c] = primary
        return names

    # ── Section 1: Stats ────────────────────────────────────────

    def _build_stats_section(
        self, sb: SectionBuilder,
        n_persons: int, n_clusters: int, sil_avg: float,
    ) -> ReportSection:
        findings = (
            f"<p>対象人物: {n_persons:,}, クラスタ数: {n_clusters}, "
            f"特徴量: {len(FEATURE_NAMES)}, "
            f"シルエットスコア: {sil_avg:.3f}</p>"
            "<p>手法の注釈: 特徴量にIV Scoreとその構成要素（BiRank, Patronage, Person FE）を"
            "同時に含むため多重共線性がある。クラスタがIV方向に引きずられる可能性あり。</p>"
        )
        return ReportSection(
            title="クラスタリング概要",
            findings_html=findings,
            method_note=(
                f"K-Means (K={n_clusters}, n_init=10, random_state=42) を scores.json の "
                f"{len(FEATURE_NAMES)}次元の標準化特徴量ベクトルに適用。"
                "シルエットスコアは -1 (分離不良) から 1 (良好に分離) の範囲を取る。"
            ),
            section_id="stats",
        )

    # ── Section 2: PCA 2D Scatter ───────────────────────────────

    def _build_pca_scatter_section(
        self, sb: SectionBuilder,
        X_2d: np.ndarray, labels: np.ndarray, cluster_names: dict[int, str],
        n_clusters: int, person_ids: list[str], names: list[str],
        features: np.ndarray, pca, n_persons: int,
    ) -> ReportSection:
        ev = pca.explained_variance_ratio_

        hover_text = [
            f"{names[i]}<br>クラスタ: {cluster_names[int(labels[i])]}"
            f"<br>IV Score: {features[i, 3]:.1f}"
            for i in range(n_persons)
        ]
        max_2d = min(8000, n_persons)
        rng = np.random.RandomState(42)
        sample_idx = (
            rng.choice(n_persons, max_2d, replace=False) if n_persons > max_2d
            else np.arange(n_persons)
        )

        fig = go.Figure()
        for c in range(n_clusters):
            c_idx = sample_idx[labels[sample_idx] == c]
            fig.add_trace(go.Scattergl(
                x=X_2d[c_idx, 0].tolist(), y=X_2d[c_idx, 1].tolist(),
                mode="markers",
                marker=dict(size=4, opacity=0.6),
                name=cluster_names[c],
                text=[hover_text[i] for i in c_idx],
                hovertemplate="%{text}<extra></extra>",
            ))
        fig.update_layout(
            title=(
                f"PCA 2D クラスタ散布図 "
                f"(分散説明率: {ev[0]:.1%} + {ev[1]:.1%})"
            ),
            xaxis_title=f"PC1 ({ev[0]:.1%})",
            yaxis_title=f"PC2 ({ev[1]:.1%})",
        )

        # Name-search JS data (person positions encoded as JSON for JS highlight)
        persons_pos_json = json.dumps(
            [{"pid": person_ids[i], "name": names[i],
              "x": round(float(X_2d[i, 0]), 4), "y": round(float(X_2d[i, 1]), 4),
              "cl": int(labels[i])}
             for i in range(n_persons)],
            ensure_ascii=False, separators=(",", ":"),
        )

        search_html = (
            '<div style="display:flex;gap:0.5rem;margin-bottom:0.8rem;align-items:center;">'
            '<input id="pca2d-search" type="text" placeholder="人名を入力 → 点がハイライト..."'
            ' style="flex:1;padding:0.5rem 0.8rem;background:#1a1a3e;color:#fff;'
            'border:1px solid #a0d2db;border-radius:6px;font-size:0.9rem;">'
            '<button id="pca2d-clear" style="padding:0.5rem 0.8rem;background:#333;color:#fff;'
            'border:1px solid #666;border-radius:6px;cursor:pointer;">クリア</button>'
            '</div>'
            '<div id="pca2d-result" style="font-size:0.8rem;color:#a0d2db;'
            'margin-bottom:0.5rem;min-height:1.2em;"></div>'
        )

        # Safe DOM-based search JS (no innerHTML — uses textContent and createElement)
        search_js = f"""<script>
(function(){{
  var PERSONS={persons_pos_json};
  var blinkTimer=null,blinkCount=0,N_TRACES={n_clusters};
  var EXPLORER="{EXPLORER_URL}";
  function stopBlink(){{
    if(blinkTimer){{clearInterval(blinkTimer);blinkTimer=null;}}
    try{{Plotly.deleteTraces('pca2d',[N_TRACES]);}}catch(e){{}}
  }}
  function highlightPersons(m){{
    stopBlink();if(!m.length)return;
    Plotly.addTraces('pca2d',{{
      type:'scattergl',mode:'markers',
      x:m.map(function(p){{return p.x;}}),
      y:m.map(function(p){{return p.y;}}),
      text:m.map(function(p){{return p.name+' / C'+(p.cl+1);}}),
      hovertemplate:'%{{text}}<extra>検索</extra>',
      marker:{{size:14,color:'#F8EC6A',symbol:'star',line:{{width:2,color:'#fff'}}}},
      name:'検索結果',showlegend:true
    }});
    blinkCount=0;
    blinkTimer=setInterval(function(){{
      blinkCount++;
      try{{Plotly.restyle('pca2d',{{'marker.size':blinkCount%2===0?14:10}},[N_TRACES]);}}catch(e){{}}
    }},600);
  }}
  function renderResults(m){{
    var res=document.getElementById('pca2d-result');
    while(res.firstChild)res.removeChild(res.firstChild);
    if(!m.length){{res.textContent='該当なし';return;}}
    res.appendChild(document.createTextNode(m.length+'件ヒット: '));
    m.slice(0,5).forEach(function(p,i){{
      if(i>0)res.appendChild(document.createTextNode(', '));
      var a=document.createElement('a');
      a.href=EXPLORER+'/#person/'+encodeURIComponent(p.pid);
      a.target='_blank';a.style.color='#7CC8F2';
      a.textContent=p.name;res.appendChild(a);
    }});
    if(m.length>5)res.appendChild(document.createTextNode(' ...'));
  }}
  document.getElementById('pca2d-search').addEventListener('input',function(){{
    var q=this.value.trim().toLowerCase();
    if(!q){{stopBlink();document.getElementById('pca2d-result').textContent='';return;}}
    var m=PERSONS.filter(function(p){{return p.name.toLowerCase().indexOf(q)>=0;}});
    renderResults(m);highlightPersons(m);
  }});
  document.getElementById('pca2d-clear').addEventListener('click',function(){{
    document.getElementById('pca2d-search').value='';
    document.getElementById('pca2d-result').textContent='';
    stopBlink();
  }});
}})();
</script>"""

        findings = (
            f"<p>{len(FEATURE_NAMES)}次元の特徴量をPCAで2次元に圧縮し、"
            f"K-Meansクラスタ別に色分け表示 (n={n_persons:,}, "
            f"subsample={min(max_2d, n_persons):,})。"
            "近い点は類似した特性を持つ人物。</p>"
        )

        return ReportSection(
            title="PCA 2D クラスタ散布図",
            findings_html=findings,
            visualization_html=(
                search_html
                + plotly_div_safe(fig, "pca2d", height=600)
                + search_js
            ),
            method_note=(
                f"{len(FEATURE_NAMES)}次元の標準化特徴量に対するPCA。"
                f"PC1は分散の{ev[0]:.1%}、PC2は{ev[1]:.1%}を説明。"
                "WebGL性能のためScatterglを使用。"
                "検索は人物名の部分一致でハイライトする。"
            ),
            section_id="pca_scatter",
        )

    # ── Section 3: PCA Loadings ─────────────────────────────────

    def _build_loadings_section(
        self, sb: SectionBuilder, pca, n_components: int,
    ) -> ReportSection:
        n_show = min(5, n_components)
        loadings = pca.components_[:n_show]
        ev = pca.explained_variance_ratio_

        fig = go.Figure(data=go.Heatmap(
            z=loadings.tolist(),
            x=FEATURE_NAMES,
            y=[f"PC{i+1} ({ev[i]:.1%})" for i in range(n_show)],
            colorscale="RdBu_r",
            zmid=0,
            text=[
                [f"{loadings[r, c]:.2f}" for c in range(len(FEATURE_NAMES))]
                for r in range(n_show)
            ],
            texttemplate="%{text}",
            hovertemplate=(
                "PC: %{y}<br>特徴量: %{x}<br>負荷量: %{z:.3f}<extra></extra>"
            ),
        ))
        fig.update_layout(title="PCA 主成分負荷量 (上位5PC)", height=350)

        findings = (
            "<p>各主成分（PC）がどの特徴量を反映しているかを示す。"
            "絶対値が大きいほどそのPCへの貢献が大きい。"
            "赤=正の寄与、青=負の寄与。</p>"
        )

        return ReportSection(
            title="PCA 主成分負荷量",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "pca_loadings", height=350),
            method_note=(
                "sklearn PCA による主成分負荷量。"
                "元の特徴量が主成分にどうマッピングされるかを示す。"
            ),
            section_id="pca_loadings",
        )

    # ── Section 4: Profile Heatmap ──────────────────────────────

    def _build_profile_heatmap_section(
        self, sb: SectionBuilder,
        centers_orig: np.ndarray, cluster_names: dict[int, str],
        n_clusters: int,
    ) -> ReportSection:
        z_scores = (
            (centers_orig - centers_orig.mean(axis=0))
            / (centers_orig.std(axis=0) + 1e-10)
        )

        fig = go.Figure(data=go.Heatmap(
            z=z_scores.tolist(),
            x=FEATURE_NAMES,
            y=[cluster_names[c] for c in range(n_clusters)],
            colorscale="RdBu_r",
            zmid=0,
            text=[
                [f"{z_scores[r, c]:.2f}" for c in range(len(FEATURE_NAMES))]
                for r in range(n_clusters)
            ],
            texttemplate="%{text}",
            hovertemplate=(
                "クラスタ: %{y}<br>特徴量: %{x}<br>"
                "Z-score: %{z:.2f}<extra></extra>"
            ),
        ))
        fig.update_layout(
            title="クラスタプロファイル ヒートマップ (Z-score)",
            height=max(300, 60 * n_clusters + 100),
        )

        findings = (
            "<p>各クラスタの特徴量平均をZ-scoreで可視化。"
            "赤は全クラスタ平均より高い特徴、青は低い特徴。"
            "クラスタ間の特性差を一覧できる。</p>"
        )

        return ReportSection(
            title="クラスタプロファイル ヒートマップ",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "profile_heatmap", height=max(300, 60 * n_clusters + 100)),
            method_note=(
                "Z-score = (クラスタ中心 - 平均) / 標準偏差 を全クラスタ中心に対して算出。"
                "Z-score算出前に、StandardScaler から元スケールへ逆変換している。"
            ),
            section_id="profile_heatmap",
        )

    # ── Section 5: Silhouette Analysis ──────────────────────────

    def _build_silhouette_section(
        self, sb: SectionBuilder,
        sil_samples: np.ndarray, labels: np.ndarray,
        cluster_names: dict[int, str], n_clusters: int, sil_avg: float,
    ) -> ReportSection:
        max_bars = 5000
        fig = go.Figure()
        y_lower = 0

        for c in range(n_clusters):
            mask = labels == c
            c_sil = np.sort(sil_samples[mask])
            if len(c_sil) > max_bars // n_clusters:
                step = max(1, len(c_sil) // (max_bars // n_clusters))
                c_sil = c_sil[::step]
            y_upper = y_lower + len(c_sil)
            fig.add_trace(go.Bar(
                x=c_sil.tolist(),
                y=list(range(y_lower, y_upper)),
                orientation="h",
                name=cluster_names[c],
                marker=dict(line=dict(width=0)),
                hovertemplate=(
                    f"クラスタ: {cluster_names[c]}<br>"
                    "シルエット係数: %{x:.3f}<extra></extra>"
                ),
            ))
            y_lower = y_upper

        fig.add_vline(
            x=sil_avg, line_dash="dash", line_color="white",
            annotation_text=f"平均: {sil_avg:.3f}",
        )
        fig.update_layout(
            title=f"シルエット分析 (平均スコア: {sil_avg:.3f})",
            xaxis_title="シルエット係数",
            yaxis=dict(showticklabels=False),
            barmode="stack",
            showlegend=True,
            height=500,
        )

        findings = (
            f"<p>各データポイントのシルエット係数 (平均: {sil_avg:.3f})。"
            "1に近いほどクラスタ分離が良好。"
            "負の値はクラスタ割当が不適切な可能性を示す。</p>"
        )

        return ReportSection(
            title="シルエット分析",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "silhouette", height=500),
            method_note=(
                "sklearn によるサンプルごとのシルエット係数。"
                f"描画性能のため最大{max_bars}本にサブサンプル。"
            ),
            section_id="silhouette",
        )

    # ── Section 6: Explained Variance ───────────────────────────

    def _build_variance_section(
        self, sb: SectionBuilder, pca,
    ) -> ReportSection:
        ev = pca.explained_variance_ratio_
        cumulative = [sum(ev[:i + 1]) for i in range(len(ev))]
        pc_labels = [f"PC{i+1}" for i in range(len(ev))]

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=pc_labels, y=ev.tolist(), name="寄与率",
                marker_color="#E09BC2",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=pc_labels, y=cumulative, name="累積寄与率",
                line=dict(color="#3BC494", width=3), mode="lines+markers",
            ),
            secondary_y=True,
        )
        fig.update_layout(title="PCA 分散説明率", height=400)
        fig.update_yaxes(title_text="寄与率", secondary_y=False)
        fig.update_yaxes(title_text="累積寄与率", secondary_y=True)

        findings = (
            f"<p>各主成分の分散説明率と累積寄与率。"
            f"PC1–PC3の累積: {cumulative[min(2, len(cumulative)-1)]:.1%}。"
            "少数のPCで大部分の分散を説明できるかを確認。</p>"
        )

        return ReportSection(
            title="PCA 分散説明率",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "explained_var", height=400),
            method_note=(
                "sklearn PCA の分散説明率。"
                "累積線は上位N主成分で説明される総分散を示す。"
            ),
            section_id="explained_variance",
        )

    # ── Section 7: Cluster Summary Table ────────────────────────

    def _build_summary_table_section(
        self, sb: SectionBuilder,
        centers_orig: np.ndarray, labels: np.ndarray,
        cluster_names: dict[int, str], n_clusters: int,
    ) -> ReportSection:
        table_rows: list[str] = []
        for c in range(n_clusters):
            mask = labels == c
            size = int(mask.sum())
            profile = {
                FEATURE_NAMES[fi]: float(centers_orig[c, fi])
                for fi in range(len(FEATURE_NAMES))
            }
            top_feats = sorted(profile.items(), key=lambda x: -x[1])[:3]
            feat_str = ", ".join(f"{f[0]}={f[1]:.1f}" for f in top_feats)
            iv = profile.get("iv_score", 0)
            tc = profile.get("total_credits", 0)
            table_rows.append(
                f"<tr><td>{cluster_names[c]}</td>"
                f"<td>{size:,}</td>"
                f"<td>{feat_str}</td>"
                f"<td>{iv:.3f}</td>"
                f"<td>{tc:.0f}</td></tr>"
            )

        table_html = (
            '<table style="width:100%;border-collapse:collapse;">'
            "<thead><tr>"
            "<th>クラスタ</th><th>人数</th><th>代表特徴</th>"
            "<th>平均IV Score</th><th>平均クレジット数</th>"
            "</tr></thead><tbody>"
            + "\n".join(table_rows)
            + "</tbody></table>"
        )

        findings = (
            "<p>各クラスタの人数と代表的な特徴量（上位3）の"
            "クラスタ中心値（inverse-transformed）。</p>"
        )

        return ReportSection(
            title="クラスタサマリー",
            findings_html=findings + table_html,
            method_note=(
                "プロファイル値は KMeans クラスタ中心を逆変換したもの。"
                "代表特徴は中心座標の絶対値による上位項目。"
            ),
            section_id="cluster_summary",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='ml_clustering',
    audience='technical_appendix',
    claim=(
        'PCA 上位 3 主成分 (累積寄与 ≥ 60%) で K-Means (K=5) を実行し、'
        'silhouette ≥ 0.30 の cluster 構造が得られる'
    ),
    identifying_assumption=(
        'PCA は線形次元圧縮 — 非線形多様体 (manifold) 構造は捕捉外。'
        'K-Means は球形 cluster を仮定。silhouette は cluster 同士の'
        '分離度を測るが、内部均質性は別指標 (Calinski-Harabasz) で確認。'
    ),
    null_model=['N1', 'N6'],
    sources=['feat_person_scores'],
    meta_table='meta_ml_clustering',
    estimator='PCA(n=3) + K-Means(K=5) + silhouette',
    ci_estimator='bootstrap', n_resamples=500,
    extra_limitations=[
        'PCA 線形仮定で非凸構造を捕捉できない',
        'K-Means の label switching で実行間で cluster ID 不変ではない',
        'silhouette < 0.30 で cluster 解釈は弱い (実行間で安定しない)',
    ],
)
