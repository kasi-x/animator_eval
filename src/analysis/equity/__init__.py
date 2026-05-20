"""Equity analysis: 機会格差を構造分離する手法を集約。

Oaxaca-Blinder decomposition / Cotton extension などを提供。
gender / cohort / studio tier の subgroup 比較を causal 解釈と分離して扱う。
"""

from src.analysis.equity.oaxaca_decomp import (
    OaxacaResult,
    OaxacaSubgroupReport,
    decompose_oaxaca_blinder,
    decompose_subgroup,
    fit_group_ols,
)

__all__ = [
    "OaxacaResult",
    "OaxacaSubgroupReport",
    "decompose_oaxaca_blinder",
    "decompose_subgroup",
    "fit_group_ols",
]
