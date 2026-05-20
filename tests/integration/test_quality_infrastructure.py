"""Integration tests for quality infrastructure (Session 2 ラウンド 4).

Cross-module integration:
- viz_quality × cross_reference × repro_footer
- quality_scorecard × ci_check_spec_coverage × ci_check_method_gate
- multiple_testing × Oaxaca subgroup outputs
- DDL ↔ schema integrity
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


class TestQualityScorecard:
    def test_all_reports_scored(self):
        from scripts.report_generators.quality_scorecard import score_all_reports
        scores = score_all_reports()
        assert len(scores) >= 50
        # All scores in [0, 100]
        for s in scores:
            assert 0 <= s.total <= 100

    def test_no_zero_score(self):
        """No report should score 0 (minimum baseline)."""
        from scripts.report_generators.quality_scorecard import score_all_reports
        scores = score_all_reports()
        for s in scores:
            assert s.total >= 50, f"{s.report_name} scored {s.total}"

    def test_mean_above_baseline(self):
        from scripts.report_generators.quality_scorecard import score_all_reports
        scores = score_all_reports()
        mean = sum(s.total for s in scores) / len(scores)
        assert mean >= 75, f"mean score {mean:.1f} < 75"


class TestSpecCoverage:
    def test_all_modules_have_spec(self):
        from scripts.report_generators.ci_check_spec_coverage import audit_spec_coverage
        total, with_spec, problems = audit_spec_coverage()
        assert total > 0
        assert with_spec == total
        assert problems == [] or all("Invalid SPEC" in p for p in problems) is False


class TestMethodGateAudit:
    def test_all_reports_pass_method_gate(self):
        from scripts.report_generators.ci_check_method_gate import audit_all
        issues = audit_all()
        assert issues == {}, f"method gate issues: {issues}"


class TestCrossReference:
    def test_all_reports_have_cross_ref(self):
        from scripts.report_generators.cross_reference import (
            find_reports_without_cross_refs,
        )
        from scripts.report_generators.reports import V2_REPORT_CLASSES
        names = [c.name for c in V2_REPORT_CLASSES if getattr(c, "name", None)]
        missing = find_reports_without_cross_refs(names)
        assert missing == []


class TestVizQualityAccessibility:
    def test_okabe_ito_palette_meets_aa(self):
        from scripts.report_generators.viz_quality import (
            COLOR_BG_DARK,
            PALETTE_OKABE_ITO,
            palette_wcag_audit,
        )
        audit = palette_wcag_audit(PALETTE_OKABE_ITO, COLOR_BG_DARK)
        passes = sum(1 for _, _, p in audit if p)
        assert passes >= 6, f"only {passes}/8 Okabe-Ito colors pass AA"


class TestReproducibilityFooter:
    def test_compute_spec_hash_deterministic(self):
        """同じ SPEC は同じ hash を生成する."""
        from scripts.report_generators._spec import make_default_spec
        from scripts.report_generators.reproducibility_footer import compute_spec_hash

        spec1 = make_default_spec(
            name="test", audience="policy",
            claim="claim text", sources=["a"], meta_table="m",
        )
        spec2 = make_default_spec(
            name="test", audience="policy",
            claim="claim text", sources=["a"], meta_table="m",
        )
        assert compute_spec_hash(spec1) == compute_spec_hash(spec2)

    def test_spec_hash_changes_with_claim(self):
        from scripts.report_generators._spec import make_default_spec
        from scripts.report_generators.reproducibility_footer import compute_spec_hash

        spec1 = make_default_spec(
            name="test", audience="policy",
            claim="claim A", sources=["a"], meta_table="m",
        )
        spec2 = make_default_spec(
            name="test", audience="policy",
            claim="claim B", sources=["a"], meta_table="m",
        )
        assert compute_spec_hash(spec1) != compute_spec_hash(spec2)


class TestMultipleTestingIntegration:
    def test_bonferroni_holm_bh_consistency(self):
        """3 補正法とも family-wise rejection 一致性が保たれる."""
        from src.analysis.quality.multiple_testing import adjust
        pvals = [0.001, 0.01, 0.03, 0.04, 0.05]
        bonf = adjust("bonferroni", pvals, alpha=0.05)
        holm_r = adjust("holm", pvals, alpha=0.05)
        bh = adjust("bh", pvals, alpha=0.05)
        # BH >= Holm >= Bonferroni in power
        assert bh.n_rejected_adjusted >= holm_r.n_rejected_adjusted
        assert holm_r.n_rejected_adjusted >= bonf.n_rejected_adjusted


class TestDDLIntegrity:
    def test_new_mart_tables_syntactically_valid(self, tmp_path):
        """全 _DDL statement が DuckDB に対し parse + create 可能."""
        from src.analysis.io.mart_writer import _DDL
        db = tmp_path / "test_ddl.duckdb"
        c = duckdb.connect(str(db))
        try:
            for stmt in _DDL.split(";"):
                s = stmt.strip()
                if s:
                    c.execute(s)
        finally:
            c.close()

    def test_session2_tables_present(self, tmp_path):
        """Session 2 後半で追加された 9 テーブルが DDL から生成される."""
        from src.analysis.io.mart_writer import _DDL
        db = tmp_path / "test_session2.duckdb"
        c = duckdb.connect(str(db))
        try:
            for stmt in _DDL.split(";"):
                s = stmt.strip()
                if s:
                    c.execute(s)
            rows = c.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'feat_%' ORDER BY table_name"
            ).fetchall()
            tables = {r[0] for r in rows}
            required = {
                "feat_did_hte", "feat_mentor_pairs", "feat_mentor_event_study",
                "feat_mentor_did_matched", "feat_credit_anomaly_flags",
                "feat_did_robustness", "feat_network_resilience",
                "feat_cohort_inequality", "feat_oaxaca_decomposition",
            }
            missing = required - tables
            assert not missing, f"missing tables: {missing}"
        finally:
            c.close()


class TestLineageRegister:
    def test_collects_lineage_rows(self):
        from scripts.report_generators.lineage_register import _collect_lineage_rows
        rows = _collect_lineage_rows()
        assert len(rows) >= 50
        for r in rows:
            assert r["table_name"]
            assert r["audience"]
            # sources is JSON-serialized
            sources = json.loads(r["source_silver_tables"])
            assert isinstance(sources, list)


class TestKeyFindingsLoader:
    def test_load_default_path_absent_returns_empty(self, tmp_path):
        from scripts.report_generators.briefs._keyfindings_loader import load_keyfindings
        # path absent → []
        non_existent = tmp_path / "no.json"
        assert load_keyfindings("policy", path=non_existent) == []

    def test_write_load_roundtrip(self, tmp_path):
        from scripts.report_generators.briefs._keyfindings_loader import (
            load_keyfindings,
            write_keyfindings,
        )
        payload = {
            "policy": [
                dict(metric_label="x", value=0.5, unit="d",
                     ci_low=0.3, ci_high=0.7, method_gate="bs"),
            ],
            "hr": [], "business": [],
        }
        p = tmp_path / "kf.json"
        write_keyfindings(payload, path=p)
        loaded = load_keyfindings("policy", path=p)
        assert len(loaded) == 1
        assert loaded[0].metric_label == "x"
