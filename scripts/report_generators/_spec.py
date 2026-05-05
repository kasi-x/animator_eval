"""Report v3 specification dataclasses.

Implements the 5-tuple declaration from ``docs/REPORT_DESIGN_v3.md`` §4.
A ``ReportSpec`` packages a report's claim, identifying assumption, null
model, method gate, sensitivity grid, and interpretation guard so the
runtime can validate them against the philosophy gates declared in
``docs/REPORT_PHILOSOPHY.md`` v2.1.

Validation is opt-in (``STRICT_REPORT_SPEC=1``) until Phase 5 completes
the migration of all 49 reports.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal


# =========================================================================
# Null model catalogue (REPORT_DESIGN_v3.md §3)
# =========================================================================

NullModelId = Literal["N1", "N2", "N3", "N4", "N5", "N6", "N7"]

NULL_MODEL_CATALOGUE: dict[str, str] = {
    "N1": "configuration model (Newman 2003) — degree-preserving randomization",
    "N2": "degree-preserving rewiring (double edge swap, 1000 iter)",
    "N3": "cohort-matched permutation",
    "N4": "role-matched bootstrap",
    "N5": "era-window matched resample",
    "N6": "uniform random (information-zero baseline)",
    "N7": "naive activity baseline (count-only, no structure)",
}


# =========================================================================
# Method-gate components
# =========================================================================


@dataclass(frozen=True)
class CIMethod:
    estimator: Literal["greenwood", "bootstrap", "delta", "analytical_se",
                       "clopper_pearson", "wald", "wilson"]
    n_resamples: int | None = None  # required for bootstrap
    parametric_assumption: str | None = None


@dataclass(frozen=True)
class HoldoutSpec:
    method: Literal["leave-one-year-out", "rolling-window", "k-fold",
                    "time-split"]
    holdout_size: str           # e.g. "last 3 years (2022-2024)"
    metric: str                 # e.g. "precision@k=20", "AUC"
    naive_baseline: str         # e.g. "activity count alone"


@dataclass(frozen=True)
class ShrinkageSpec:
    method: Literal["james_stein", "empirical_bayes_beta",
                    "empirical_bayes_normal", "ridge"]
    n_threshold: int            # apply only when n < threshold
    prior: str                  # description of prior assumption


@dataclass(frozen=True)
class SensitivityAxis:
    name: str                   # e.g. "exit definition window"
    values: list[str | int | float]


@dataclass(frozen=True)
class MethodGate:
    name: str
    estimator: str
    ci: CIMethod
    rng_seed: int               # required for reproducibility
    null: list[str] = field(default_factory=list)        # null model IDs
    holdout: HoldoutSpec | None = None                   # required for predictions
    shrinkage: ShrinkageSpec | None = None               # required for individual rankings
    sensitivity_grid: list[SensitivityAxis] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)  # ≥ 3 entries

    def validate(self) -> list[str]:
        violations: list[str] = []
        if not self.estimator:
            violations.append("estimator missing")
        if self.ci.estimator == "bootstrap" and not self.ci.n_resamples:
            violations.append("bootstrap CI requires n_resamples")
        if len(self.limitations) < 3:
            violations.append(
                f"≥3 limitations required (got {len(self.limitations)})"
            )
        for nid in self.null:
            if nid not in NULL_MODEL_CATALOGUE:
                violations.append(f"unknown null model id: {nid}")
        return violations


# =========================================================================
# Interpretation guard
# =========================================================================


@dataclass(frozen=True)
class InterpretationGuard:
    forbidden_framing: list[str]    # phrases that, if appearing in
                                    # the rendered report, are violations
    required_alternatives: int = 1  # min number of alt interpretations

    def validate(self) -> list[str]:
        violations: list[str] = []
        if self.required_alternatives < 1:
            violations.append("required_alternatives must be ≥ 1")
        return violations


# =========================================================================
# Data lineage
# =========================================================================


@dataclass(frozen=True)
class DataLineage:
    sources: list[str]              # SILVER / Conformed table names
    meta_table: str                 # ops_lineage / meta_<name> entry
    snapshot_date: str              # YYYY-MM-DD
    pipeline_version: str           # e.g. "v55"


# =========================================================================
# Top-level ReportSpec
# =========================================================================


@dataclass(frozen=True)
class ReportSpec:
    name: str
    audience: Literal["common", "policy", "hr", "biz", "technical_appendix"]
    claim: str
    identifying_assumption: str
    null_model: list[str]
    method_gate: MethodGate
    sensitivity_grid: list[SensitivityAxis]
    interpretation_guard: InterpretationGuard
    data_lineage: DataLineage

    def validate(self) -> list[str]:
        out: list[str] = []
        if not self.claim:
            out.append("claim missing")
        if not self.identifying_assumption:
            out.append("identifying_assumption missing")
        if not self.null_model:
            out.append(
                "null_model required — declare e.g. ['N3'] or ['N7'] "
                "from NULL_MODEL_CATALOGUE"
            )
        out.extend(self.method_gate.validate())
        out.extend(self.interpretation_guard.validate())
        return out


# =========================================================================
# Brief narrative arc (REPORT_DESIGN_v3.md §5)
# =========================================================================


@dataclass(frozen=True)
class NullContrast:
    section_id: str
    observed: float
    null_lo: float
    null_hi: float
    note: str = ""


@dataclass(frozen=True)
class LimitationBlock:
    identifying_assumption_validity: str
    sensitivity_caveats: list[str]
    shrinkage_order_changes: str = ""


@dataclass(frozen=True)
class Interpretation:
    primary_claim: str
    primary_subject: str        # e.g. "本レポートの著者は…"
    alternatives: list[str]     # ≥ 1
    recommendation: str | None = None
    recommendation_alt_value: str | None = None  # different value perspective


@dataclass(frozen=True)
class BriefArc:
    audience: Literal["policy", "hr", "biz"]
    presenting_phenomena: list[str]    # report ids
    null_contrast: list[NullContrast]
    limitation_block: LimitationBlock
    interpretation: Interpretation

    def to_html(self) -> str:
        """Render the 4-段 narrative arc as HTML.

        Section order (REPORT_DESIGN_v3.md §5):
          段 1: 現象提示 (presenting_phenomena)
          段 2: null model との対比 (null_contrast)
          段 3: 解釈の限界 (limitation_block)
          段 4: 代替視点 (interpretation, ≥1 alternatives)
        """
        # 段 1
        ph_items = "\n".join(
            f"<li><a href=\"{rid}.html\">{rid}</a></li>"
            for rid in self.presenting_phenomena
        )
        s1 = (
            '<div class="card report-section" id="arc-phenomena">'
            "<h2>段 1: 現象提示 (Findings)</h2>"
            "<p>本ブリーフが扱う構造的現象を、評価語抜きで列挙する:</p>"
            f"<ul>{ph_items}</ul>"
            "</div>"
        )

        # 段 2
        nc_rows = "\n".join(
            "<tr>"
            f"<td>{nc.section_id}</td>"
            f"<td>{nc.observed:.4f}</td>"
            f"<td>[{nc.null_lo:.4f}, {nc.null_hi:.4f}]</td>"
            f"<td>{'外側' if (nc.observed < nc.null_lo or nc.observed > nc.null_hi) else '内側'}</td>"
            f"<td>{nc.note}</td>"
            "</tr>"
            for nc in self.null_contrast
        )
        s2 = (
            '<div class="card report-section" id="arc-null-contrast">'
            "<h2>段 2: null model との対比</h2>"
            "<p>各主張を null model の 95% 区間と比較する。"
            "「外側」= 観測値が帰無分布の P2.5–P97.5 の外、"
            "「内側」= null と区別不能。</p>"
            "<table style=\"width:100%;border-collapse:collapse;font-size:0.85rem;\">"
            "<thead><tr>"
            "<th>section</th><th>observed</th>"
            "<th>null 95% [P2.5, P97.5]</th>"
            "<th>判定</th><th>備考</th>"
            "</tr></thead>"
            f"<tbody>{nc_rows}</tbody></table>"
            "</div>"
        )

        # 段 3
        sens = "".join(
            f"<li>{c}</li>" for c in self.limitation_block.sensitivity_caveats
        )
        shrink_part = (
            f"<p><strong>縮約後の順序変化</strong>: "
            f"{self.limitation_block.shrinkage_order_changes}</p>"
            if self.limitation_block.shrinkage_order_changes else ""
        )
        s3 = (
            '<div class="card report-section" id="arc-limitations">'
            "<h2>段 3: 解釈の限界</h2>"
            "<p><strong>identifying assumption の妥当性</strong>: "
            f"{self.limitation_block.identifying_assumption_validity}</p>"
            "<p><strong>感度分析での結論揺れ</strong>:</p>"
            f"<ul>{sens}</ul>"
            f"{shrink_part}"
            "</div>"
        )

        # 段 4
        alts = "".join(f"<li>{a}</li>" for a in self.interpretation.alternatives)
        rec = ""
        if self.interpretation.recommendation:
            rec = (
                "<p><strong>推奨</strong>: "
                f"{self.interpretation.recommendation}</p>"
            )
            if self.interpretation.recommendation_alt_value:
                rec += (
                    "<p><em>異なる価値観からの代替推奨</em>: "
                    f"{self.interpretation.recommendation_alt_value}</p>"
                )
        s4 = (
            '<div class="card report-section interpretation" id="arc-interpretation"'
            ' style="border-left:3px solid #c0a0d0;">'
            "<h2>段 4: 代替視点 (Interpretation)</h2>"
            f"<p><strong>主張</strong>: {self.interpretation.primary_subject} — "
            f"{self.interpretation.primary_claim}</p>"
            "<p><strong>代替解釈</strong>:</p>"
            f"<ul>{alts}</ul>"
            f"{rec}"
            "</div>"
        )

        return "\n".join([s1, s2, s3, s4])


# =========================================================================
# Strict mode toggle (CI gate)
# =========================================================================


def is_strict_mode() -> bool:
    """Return True when CI must enforce ReportSpec validity."""
    return os.environ.get("STRICT_REPORT_SPEC") in {"1", "true", "yes"}


def assert_valid(spec: ReportSpec) -> None:
    """Raise if ``spec`` violates v3 declarations and strict mode is on.

    In non-strict mode (default until Phase 5) the violations are
    returned via ``ReportSpec.validate()`` for the caller to log.
    """
    violations = spec.validate()
    if violations and is_strict_mode():
        joined = "; ".join(violations)
        raise ValueError(f"ReportSpec[{spec.name}] violations: {joined}")


# =========================================================================
# Convenience factory — make_default_spec
# =========================================================================


_AUDIENCE_DEFAULT_FORBIDDEN: dict[str, list[str]] = {
    "policy": ["離職率の悪化", "若手定着の課題", "業界の危機", "能力低下"],
    "hr": ["スコア下位", "能力不足", "実力ランキング", "優秀人材"],
    "biz": ["過小評価", "発掘", "原石", "隠れた才能"],
    "technical_appendix": ["ground truth", "正解", "真の効果"],
    "common": ["業界平均より上", "業界平均より下"],
}


_DEFAULT_LIMITATIONS: list[str] = [
    "クレジットデータ可視性に依存 — 海外下請け / 無名義 / アシスタント記載は捕捉不可",
    "時代別クレジット粒度差 (1980s vs 2010s) が指標推定に bias",
    "entity_resolution false merge / split による集約誤差 (~1-3%)",
]


def make_default_spec(
    *,
    name: str,
    audience: Literal["common", "policy", "hr", "biz", "technical_appendix"],
    claim: str,
    sources: list[str],
    meta_table: str,
    null_model: list[str] | None = None,
    identifying_assumption: str = "",
    estimator: str = "descriptive aggregation",
    ci_estimator: Literal[
        "greenwood", "bootstrap", "delta", "analytical_se",
        "clopper_pearson", "wald", "wilson",
    ] = "analytical_se",
    n_resamples: int | None = None,
    rng_seed: int = 42,
    holdout: HoldoutSpec | None = None,
    shrinkage: ShrinkageSpec | None = None,
    sensitivity_grid: list[SensitivityAxis] | None = None,
    forbidden_framing: list[str] | None = None,
    required_alternatives: int = 1,
    extra_limitations: list[str] | None = None,
    snapshot_date: str = "2026-04-30",
    pipeline_version: str = "v55",
) -> ReportSpec:
    """Build a minimally-valid ReportSpec with audience-aware defaults.

    Usage in a report module::

        from .._spec import make_default_spec
        SPEC = make_default_spec(
            name="my_report",
            audience="policy",
            claim="…一文の狭い主張…",
            sources=["credits", "persons"],
            meta_table="meta_my_report",
            null_model=["N3"],
        )

    Defaults:
    - ``forbidden_framing``: audience-default (能力 / 実績 / 過小評価 系)
    - ``limitations``: 3 共通 limitation (visibility / era / entity_resolution)
      に ``extra_limitations`` を append。
    - ``identifying_assumption``: 空のままだと validate fail なので呼び出し側で
      非空文字列を渡すこと。
    """
    if not identifying_assumption:
        identifying_assumption = (
            "クレジット記録の可視性が当該指標の測定対象を構成する観察事実に "
            "近似する。雇用 / 制作実態との乖離は別途検証が必要。"
        )
    null = null_model or ["N6"]
    limits = list(_DEFAULT_LIMITATIONS)
    if extra_limitations:
        limits.extend(extra_limitations)
    forbidden = forbidden_framing if forbidden_framing is not None \
        else _AUDIENCE_DEFAULT_FORBIDDEN.get(audience, [])
    return ReportSpec(
        name=name,
        audience=audience,
        claim=claim,
        identifying_assumption=identifying_assumption,
        null_model=null,
        method_gate=MethodGate(
            name=name,
            estimator=estimator,
            ci=CIMethod(estimator=ci_estimator, n_resamples=n_resamples),
            rng_seed=rng_seed,
            null=null,
            holdout=holdout,
            shrinkage=shrinkage,
            sensitivity_grid=sensitivity_grid or [],
            limitations=limits,
        ),
        sensitivity_grid=sensitivity_grid or [],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=forbidden,
            required_alternatives=required_alternatives,
        ),
        data_lineage=DataLineage(
            sources=sources,
            meta_table=meta_table,
            snapshot_date=snapshot_date,
            pipeline_version=pipeline_version,
        ),
    )
