"""Derived Parameters report — v2 compliant.

Transparency report for all derived metric parameters:
- Section 1: IV lambda weights documentation
- Section 2: Work scale tier classification thresholds
- Section 3: Role weight mapping
- Section 4: Pipeline parameter registry
"""

from __future__ import annotations

from pathlib import Path


from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator


class DerivedParamsReport(BaseReportGenerator):
    name = "derived_params"
    title = "導出パラメータ透明性レポート"
    subtitle = "IVウェイト・Tier分類閾値・役職ウェイト・パラメータ登録簿"
    filename = "derived_params.html"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        sections: list[str] = []
        sections.append(sb.build_section(self._build_iv_weights_section(sb)))
        sections.append(sb.build_section(self._build_tier_thresholds_section(sb)))
        sections.append(sb.build_section(self._build_role_weights_section(sb)))
        sections.append(sb.build_section(self._build_param_registry_section(sb)))
        return self.write_report("\n".join(sections))

    def _build_iv_weights_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT component, weight, justification
                FROM feat_iv_weights
                WHERE weight IS NOT NULL
                ORDER BY weight DESC
            """).fetchall()
        except Exception:
            rows = []

        # Static documentation regardless of DB availability
        iv_formula = (
            "IV_i = (λ₁ × theta_i + λ₂ × birank_i + λ₃ × studio_exp_i "
            "+ λ₄ × awcc_i + λ₅ × patronage_i) × D_i"
        )
        findings = (
            f"<p>統合価値（IV）算出式: <code>{iv_formula}</code></p>"
            "<p>D_i = 休眠乗数（加重和の後に適用）。</p>"
        )

        if rows:
            findings += "<p>feat_iv_weightsから取得したlambdaウェイト:</p><ul>"
            for r in rows:
                findings += (
                    f"<li><strong>{r['component']}</strong>: λ={r['weight']:.3f} "
                    f"— {r['justification'] or '根拠未記録'}</li>"
                )
            findings += "</ul>"
        else:
            findings += (
                "<p>feat_iv_weightsテーブルなし。"
                "デフォルトのウェイトは固定事前値（視聴者評価に対するCV最適化なし）。"
                "現在のlambda値はCALCULATION_COMPENDIUM.mdを参照。</p>"
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="IVウェイト（lambda）",
            findings_html=findings,
            method_note=(
                "Lambdaウェイトは固定事前ウェイト（anime.scoreに対する交差検証なし）。"
                "anime.scoreに対するCV最適化は禁止（CLAUDE.md）。"
                "ウェイトの根拠: theta_i（スタジオ効果を除いた個人貢献）、"
                "birank_i（二部ネットワーク中心性）、studio_exp_i（機会文脈）、"
                "awcc_i（協業深度）、patronage_i（メンター支援）。"
            ),
            section_id="iv_weights",
        )

    def _build_tier_thresholds_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT tier, staff_min, staff_max, episode_min, episode_max,
                       format_types, description
                FROM feat_tier_thresholds
                ORDER BY tier
            """).fetchall()
        except Exception:
            rows = []

        findings = "<p>スケールTier分類閾値（1=マイクロ, 5=メジャー）。"
        findings += "Tier割り当てにはフォーマット+話数+尺のみを使用（視聴者評価不使用）:</p>"

        if rows:
            findings += "<ul>"
            for r in rows:
                findings += (
                    f"<li><strong>Tier {r['tier']}</strong>: "
                    f"スタッフ={r['staff_min']}〜{r['staff_max'] or '∞'}、"
                    f"話数={r['episode_min']}〜{r['episode_max'] or '∞'}、"
                    f"フォーマット={r['format_types'] or '任意'}</li>"
                )
            findings += "</ul>"
        else:
            findings += (
                "<p>feat_tier_thresholdsテーブルなし。デフォルト閾値: "
                "Tier 1 (マイクロ): staff ≤ 20 or episodes ≤ 1。"
                "Tier 2 (小): staff 21–50 or episodes 2–12 TV。"
                "Tier 3 (中): staff 51–100 or episodes 13–26 TV。"
                "Tier 4 (大): staff 101–200 or episodes 27–52 TV。"
                "Tier 5 (メジャー): staff > 200 or 映画/ONA with大規模クルー。"
                "詳細はCALCULATION_COMPENDIUM.md参照。</p>"
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="スケールTier分類閾値",
            findings_html=findings,
            method_note=(
                "Tier分類: (format, episodes, staff_count, duration)に基づく決定論的ルール。"
                "anime.score（視聴者評価）は使用しない。"
                "エッジケース（例: 長話数のOVA）はformatを優先。"
            ),
            section_id="tier_thresholds",
        )

    def _build_role_weights_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT role_category, weight, justification
                FROM feat_role_weights
                ORDER BY weight DESC
            """).fetchall()
        except Exception:
            rows = []

        findings = "<p>エッジ重み計算に使用される役職ウェイトマッピング:</p>"
        findings += "<p>エッジ重み = role_weight × episode_coverage × duration_mult（視聴者評価不使用）。</p>"

        if rows:
            findings += "<ul>"
            for r in rows:
                findings += (
                    f"<li><strong>{r['role_category']}</strong>: "
                    f"weight={r['weight']:.3f} "
                    f"— {r['justification'] or ''}</li>"
                )
            findings += "</ul>"
        else:
            findings += (
                "<p>feat_role_weightsテーブルなし。"
                "デフォルトの役職ウェイト（src/utils/role_groups.py由来）: "
                "director=1.0, series_director=0.9, episode_director=0.7, "
                "animation_director=0.65, key_animator=0.5, in-between=0.2。"
                "完全な表はCALCULATION_COMPENDIUM.md参照。</p>"
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="役職ウェイトマッピング",
            findings_html=findings,
            method_note=(
                "役職ウェイトの使用箇所: エッジ重み計算（graph_construction.py）、"
                "AWCC（feat_network）、信頼スコアリング（trust.py）。"
                "ウェイトは固定事前値であり、データ駆動最適化ではない。"
                "単一の真実: src/utils/role_groups.py。"
            ),
            section_id="role_weights",
        )

    def _build_param_registry_section(self, sb: SectionBuilder) -> ReportSection:
        try:
            rows = self.conn.execute("""
                SELECT param_name, param_value, param_type, phase, description
                FROM feat_param_registry
                ORDER BY phase, param_name
            """).fetchall()
        except Exception:
            rows = []

        findings = "<p>パイプラインパラメータ登録簿（全設定可能パラメータ）:</p>"

        if rows:
            table_rows = "".join(
                f"<tr>"
                f"<td>{r['phase']}</td>"
                f"<td>{r['param_name']}</td>"
                f"<td>{r['param_value']}</td>"
                f"<td>{r['param_type'] or ''}</td>"
                f"<td>{r['description'] or ''}</td>"
                f"</tr>"
                for r in rows
            )
            table_html = (
                '<div style="overflow-x:auto;"><table>'
                "<thead><tr><th>フェーズ</th><th>パラメータ</th><th>値</th>"
                "<th>型</th><th>説明</th></tr></thead>"
                f"<tbody>{table_rows}</tbody></table></div>"
            )
            findings += table_html
        else:
            findings += (
                "<p>feat_param_registryテーブルなし。"
                "主要パラメータ: AKM_MIN_CREDITS=3, AKM_CONNECTED_SET=True, "
                "LOUVAIN_RESOLUTION=1.0, BETWEENNESS_K=200, DORMANCY_HALFLIFE=3。"
                "環境変数によるオーバーライドはsrc/utils/config.py参照。</p>"
            )

        violations = sb.validate_findings(findings)
        if violations:
            findings += f'<p style="color:#e05080;font-size:0.8rem;">[v2: {"; ".join(violations)}]</p>'

        return ReportSection(
            title="パラメータ登録簿",
            findings_html=findings,
            method_note=(
                "feat_param_registryは実行時にパイプラインフェーズが populate する。"
                "パラメータは環境変数（src/utils/config.py）でオーバーライド可能。"
                "このテーブルにより再現性が担保される: 同一パラメータでの再実行は"
                "同一結果を生成する。"
            ),
            section_id="param_registry",
        )
