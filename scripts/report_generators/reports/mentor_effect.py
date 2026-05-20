"""Mentor effect event-study + matched DiD report (HR brief)。

`infer_mentorships()` で推定済 mentor-mentee pair の構造的位置変化を
event-study で観察し、非 mentor control 群との matched DiD で confounding を
部分除去する。

H1: anime.score 非依存。
H2: 主観的評価 frame NG → 「協業経験あり/なし person の構造的位置の差」のみ。

データ前提: pipeline で feat_mentor_pairs / feat_mentor_event_study が
生成されていればレンダー、無ければ skeleton。
"""

from __future__ import annotations

from pathlib import Path

import structlog

from ..section_builder import ReportSection
from ._base import BaseReportGenerator

log = structlog.get_logger(__name__)


class MentorEffectReport(BaseReportGenerator):
    """Mentor との初協業前後の mentee の構造的位置変化 (HR brief)。"""

    name = "mentor_effect"
    title = "Mentor effect (event-study + matched DiD)"
    subtitle = (
        "mentor との初協業 event-year を 0 とする pre/post window 比較 + "
        "非 mentor control 群との matched DiD で confounding 部分除去"
    )
    doc_type = "main"
    filename = "mentor_effect.html"

    def generate(self) -> Path | None:
        rows = self._load_event_study_rows()
        did = self._load_matched_did_row()

        if not rows and not did:
            body = self.builder.build_section(
                ReportSection(
                    title="Mentor effect (設計枠組み)",
                    section_id="mentor_setup",
                    findings_html=(
                        "<p>mentor pair event-study / matched DiD 結果は本データ範囲では"
                        "未投入 (feat_mentor_event_study / feat_mentor_did_matched 不在)。"
                        "<code>src/analysis/career/mentor_effect.py</code> の "
                        "<code>compute_pair_event_study</code> + "
                        "<code>estimate_matched_did</code> を pipeline で起動する設計。"
                        "</p>"
                    ),
                    method_note=(
                        "pair event-study: pre_window = (-3, -1), post_window = (1, 5)。"
                        "matched DiD: 非 mentor control 群を candidate pool として treated と"
                        "同 event_year で δ 計算、bootstrap CI 500 回。"
                        "confounding 完全除去はできない (selection on observables)。"
                    ),
                    interpretation_html=(
                        "<p>本稿の解釈: 本セクションは mentor との協業経験前後の "
                        "mentee の構造的位置変化を測る枠組み。"
                        "「経験豊富な mentor が構造的に有利な mentee を選ぶ」逆因果は "
                        "matched DiD で部分除去するが、観測不可能な confounder は残ると考えられる。"
                        "</p>"
                    ),
                )
            )
            return self.write_report(body)

        # ── event-study findings ──
        event_table = "".join(
            f"<tr><td>{r[0]}</td><td>{r[1]:+.3f}</td>"
            f"<td>{r[2]:,}</td><td>{r[3]:,}</td></tr>"
            for r in rows[:30]
        )
        event_html = (
            "<p>Pair event-study (上位 30 件 by |delta| 降順):</p>"
            "<table><thead><tr><th>pair_id</th><th>Δθ_mentee</th>"
            "<th>n_pre</th><th>n_post</th></tr></thead>"
            f"<tbody>{event_table}</tbody></table>"
        )

        sections = [
            self.builder.build_section(
                ReportSection(
                    title="Pair event-study",
                    section_id="mentor_event_study",
                    findings_html=event_html,
                    method_note=(
                        "Pre_window = event_year + (-3, -1), post_window = (1, 5)。"
                        "両 window で >= 1 観測必要。"
                    ),
                    interpretation_html=(
                        "<p>本稿の解釈: 正の Δθ_mentee は mentor 協業後の構造的位置向上を示すが、"
                        "selection bias で過大評価される可能性があると考えられる。matched DiD 結果と"
                        "併読する必要がある。</p>"
                    ),
                )
            )
        ]
        if did:
            did_html = (
                f"<p>matched DiD estimate: <strong>{did[0]:+.4f}</strong> "
                f"(CI [{did[1]:+.4f}, {did[2]:+.4f}])</p>"
                f"<p>n_treated = {did[3]:,}, n_control = {did[4]:,}</p>"
            )
            sections.append(
                self.builder.build_section(
                    ReportSection(
                        title="Matched DiD",
                        section_id="mentor_matched_did",
                        findings_html=did_html,
                        method_note=(
                            "Bootstrap CI = 500 回。control = 非 mentor person、"
                            "同 event_year 仮想 anchor で δ 計算。"
                        ),
                        interpretation_html=(
                            "<p>本稿の解釈: CI が 0 を跨がない場合、観測可能 confounder を超えた"
                            "mentor 効果の signal と考えられる。完全な因果同定にはさらなる identification "
                            "strategy (IV 等) が必要である。</p>"
                        ),
                    )
                )
            )

        body = "\n".join(sections)
        return self.write_report(body)

    def _load_event_study_rows(self) -> list:
        try:
            return self.conn.execute("""
                SELECT pair_id, delta, n_pre, n_post
                FROM feat_mentor_event_study
                WHERE delta IS NOT NULL
                ORDER BY ABS(delta) DESC
                LIMIT 100
            """).fetchall()
        except Exception:
            return []

    def _load_matched_did_row(self) -> tuple | None:
        try:
            r = self.conn.execute("""
                SELECT did_estimate, ci_low, ci_high, n_treated, n_control
                FROM feat_mentor_did_matched
                LIMIT 1
            """).fetchone()
            return r
        except Exception:
            return None


# ---------------------------------------------------------------------------
# v3 SPEC
# ---------------------------------------------------------------------------
from .._spec import SensitivityAxis, make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name="mentor_effect",
    audience="hr",
    sensitivity_grid=[
        SensitivityAxis(name="pre_window", values=["(-3,-1)", "(-5,-1)"]),
        SensitivityAxis(name="post_window", values=["(1,5)", "(1,10)"]),
        SensitivityAxis(name="control_matching", values=["cohort", "cohort+role"]),
    ],
    claim=(
        "mentor との初協業 event-year を 0 とする pre/post 比較 (Δθ_mentee) と "
        "非 mentor control 群との matched DiD で、mentor 協業経験の構造的位置への "
        "効果を測定する。CI が 0 を跨がない場合に hr ガイダンスに採用する。"
    ),
    identifying_assumption=(
        "selection on observables: cohort と role で matching、観測可能 confounder は"
        "control 群選定で吸収。observed_pair 推定の精度は `mentorship.py` の "
        "min_shared_works / min_stage_gap 設定に依存。"
    ),
    null_model=["bootstrap CI 500 回 (両 arm 独立置換)"],
    sources=["credits", "persons", "feat_person_scores", "feat_mentor_pairs"],
    meta_table="meta_mentor_effect",
    estimator="event-study mean delta + matched DiD",
    ci_estimator="bootstrap", n_resamples=500,
    extra_limitations=[
        "selection bias 完全除去不可 (matched DiD は部分除去のみ)",
        "candidate_controls は entity resolution 済 person のみ",
        "theta_i panel が年次で揃ってない年は window 内観測数不均衡",
        "mentor relationship 推定そのものに精度限界",
    ],
    alternative_interpretations=(
        "正の Δθ は mentor 効果ではなく maturation effect (mentee 自身の年齢経験成長) を反映している可能性。matched control の事前 trajectory 一致確認が必要。",
        "infer_mentorships() の mentor 推定が選抜バイアス (協業数多 = 構造的 hub) を導入し、観測 effect が hub への被選抜効果を測ってる可能性。",
        "post window theta 上昇は anime production scale 増大 (作品大型化) の trend を反映、mentor relationship とは独立変動の可能性。time FE 拡張で確認要。",
    ),
)
