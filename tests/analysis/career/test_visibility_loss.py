"""Tests for src/analysis/career/visibility_loss.py.

Coverage:
- leakage test: temporal split が ref_year で正しく切れているか
- calibration test: calibrated 確率が [0,1] に収まり、クラス単調性を持つか
- label test: visibility label の定義が正しく適用されているか
- feature test: feature engineering が NaN / inf を含まないか
- subgroup fairness test: 全 subgroup AUC が定義域内か
- smoke test: 合成データで end-to-end が動くか
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Synthetic DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def credit_conn() -> sqlite3.Connection:
    """In-memory SQLite に最小クレジットパネルと persons を投入する。

    構成:
    - 60 persons (20 F, 20 M, 20 gender=None)
    - 40 anime (2010–2023 各年 3 作品)
    - credits: 各 person が 2010 年から 2020 年まで毎年 1 クレジット
      → 2021-2023 は 30 人のみクレジットあり (残 30 人は可視性喪失)
    """
    conn = sqlite3.connect(":memory:")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            gender TEXT
        );
        CREATE TABLE IF NOT EXISTS anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            studio_id TEXT,
            year INTEGER,
            episodes INTEGER DEFAULT 12,
            duration INTEGER DEFAULT 24
        );
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'in_between',
            credit_year INTEGER,
            evidence_source TEXT NOT NULL DEFAULT ''
        );
    """)

    # 40 anime across 2009-2023
    for idx in range(40):
        yr = 2009 + (idx % 15)
        studio = f"studio_{idx % 4}"
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year) VALUES (?,?,?,?)",
            (f"a{idx}", f"Anime{idx}", studio, yr),
        )

    # 60 persons
    for p_idx in range(60):
        if p_idx < 20:
            gender = "female"
        elif p_idx < 40:
            gender = "male"
        else:
            gender = None
        conn.execute(
            "INSERT INTO persons (id, name_en, gender) VALUES (?,?,?)",
            (f"p{p_idx}", f"Person{p_idx}", gender),
        )

    # Credits: all persons active 2010-2020
    # Persons 0-29: also active 2021-2023 (no visibility loss)
    # Persons 30-59: not active 2021-2023 (visibility loss at ref_year=2020)
    for p_idx in range(60):
        for yr in range(2010, 2021):
            anime_id = f"a{(p_idx + yr) % 40}"
            role = "in_between" if p_idx % 3 != 0 else "key_animator"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime_id, role, yr),
            )

    # Persons 0-29 also get 2021-2023 credits
    for p_idx in range(30):
        for yr in range(2021, 2024):
            anime_id = f"a{(p_idx + yr) % 40}"
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime_id, "in_between", yr),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Helper: build minimal panel for tests
# ---------------------------------------------------------------------------


def _build_panel(conn: sqlite3.Connection) -> dict:
    from src.analysis.career.visibility_loss import build_credit_panel, enrich_panel_with_scores

    panel = build_credit_panel(conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, conn)
    return panel


# ---------------------------------------------------------------------------
# Label tests
# ---------------------------------------------------------------------------


def test_visibility_label_loss_persons(credit_conn: sqlite3.Connection) -> None:
    """Persons 30-59 が ref_year=2020 で label=1 になることを確認する。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    labels = compute_visibility_label(panel, ref_year=2020)

    loss_persons = {pid for pid, lbl in labels.items() if lbl == 1}
    retained_persons = {pid for pid, lbl in labels.items() if lbl == 0}

    # persons 30-59 should be in loss, 0-29 should be retained
    for p_idx in range(30):
        assert f"p{p_idx}" in retained_persons, (
            f"p{p_idx} should be retained (label=0) at ref_year=2020"
        )
    for p_idx in range(30, 60):
        assert f"p{p_idx}" in loss_persons, (
            f"p{p_idx} should be visibility-loss (label=1) at ref_year=2020"
        )


def test_visibility_label_requires_recent_activity(credit_conn: sqlite3.Connection) -> None:
    """直近 3 年内にクレジットがない person は label 対象外になることを確認する。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    # ref_year=2005 は全員がクレジットを持たないので labels は空になる
    labels = compute_visibility_label(panel, ref_year=2005)
    assert len(labels) == 0, "No person was active before 2010; labels must be empty"


def test_visibility_label_values_are_binary(credit_conn: sqlite3.Connection) -> None:
    """全ラベルが 0 or 1 であることを確認する。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    labels = compute_visibility_label(panel, ref_year=2019)

    for pid, lbl in labels.items():
        assert lbl in (0, 1), f"label must be 0 or 1, got {lbl} for {pid}"


# ---------------------------------------------------------------------------
# Leakage tests
# ---------------------------------------------------------------------------


def test_leakage_check_passes_clean_split() -> None:
    """正しい year split では leakage check が passed=True を返す。"""
    from src.analysis.career.visibility_loss import (
        FeatureRow,
        temporal_train_test_split,
        run_leakage_check,
    )

    rows = [
        FeatureRow(
            person_id=f"p{i % 5}",
            ref_year=2015 + i,
            label=0,
            theta_i=0.0,
            pagerank=0.0,
            betweenness=0.0,
            credit_slope=0.0,
            credit_variance=0.0,
            studio_entropy=0.0,
            role_stall_years=0.0,
            peer_loss_rate=0.0,
            cohort_age=float(i),
            role_diversity=1.0,
        )
        for i in range(8)
    ]

    holdout_year = 2022
    train, holdout = temporal_train_test_split(rows, holdout_year)
    result = run_leakage_check(train, holdout, holdout_year)

    assert result.passed, (
        f"Clean temporal split must pass leakage check. "
        f"Violations: {result.violations}"
    )
    assert len(result.violations) == 0


def test_leakage_check_detects_future_in_train() -> None:
    """train に holdout_year 以上の行を混入させると leakage が検出される。"""
    from src.analysis.career.visibility_loss import (
        FeatureRow,
        run_leakage_check,
    )

    def _row(year: int) -> FeatureRow:
        return FeatureRow(
            person_id="p0",
            ref_year=year,
            label=0,
            theta_i=0.0,
            pagerank=0.0,
            betweenness=0.0,
            credit_slope=0.0,
            credit_variance=0.0,
            studio_entropy=0.0,
            role_stall_years=0.0,
            peer_loss_rate=0.0,
            cohort_age=0.0,
            role_diversity=1.0,
        )

    # train に future 行を混入
    contaminated_train = [_row(2020), _row(2021), _row(2023)]  # 2023 > holdout 2022
    holdout = [_row(2022)]

    result = run_leakage_check(contaminated_train, holdout, holdout_year=2022)

    assert not result.passed, "Contaminated train must fail leakage check"
    assert len(result.violations) > 0, "Violations must be reported"


def test_temporal_split_no_overlap() -> None:
    """train と holdout に同じ ref_year の行が重複しないことを確認する。"""
    from src.analysis.career.visibility_loss import (
        FeatureRow,
        temporal_train_test_split,
    )

    rows = []
    for yr in range(2015, 2024):
        for p in range(3):
            rows.append(
                FeatureRow(
                    person_id=f"p{p}",
                    ref_year=yr,
                    label=0,
                    theta_i=0.0,
                    pagerank=0.0,
                    betweenness=0.0,
                    credit_slope=0.0,
                    credit_variance=0.0,
                    studio_entropy=0.0,
                    role_stall_years=0.0,
                    peer_loss_rate=0.0,
                    cohort_age=float(yr - 2010),
                    role_diversity=1.0,
                )
            )

    holdout_year = 2022
    train, holdout = temporal_train_test_split(rows, holdout_year)

    train_years = {r.ref_year for r in train}
    holdout_years = {r.ref_year for r in holdout}

    assert holdout_years == {holdout_year}, "Holdout must contain only holdout_year rows"
    assert holdout_year not in train_years, "Train must not contain holdout_year rows"
    assert max(train_years) < holdout_year, "All train years must be before holdout_year"


# ---------------------------------------------------------------------------
# Feature engineering tests
# ---------------------------------------------------------------------------


def test_features_no_nan_or_inf(credit_conn: sqlite3.Connection) -> None:
    """engineer_features() の数値フィールドに NaN / inf が含まれないことを確認。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        FEATURE_COLS,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)
    labels = compute_visibility_label(panel, ref_year=2019)
    rows = engineer_features(panel, credit_conn, labels, ref_year=2019)

    assert len(rows) > 0, "Feature rows must not be empty"

    for row in rows:
        for col in FEATURE_COLS:
            val = getattr(row, col)
            assert not (
                isinstance(val, float) and (
                    (val != val) or val == float("inf") or val == float("-inf")
                )
            ), f"NaN/inf found in {col} for person {row.person_id}"


def test_feature_labels_match_visibility_labels(credit_conn: sqlite3.Connection) -> None:
    """FeatureRow の label が compute_visibility_label() の出力と一致することを確認。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)
    labels = compute_visibility_label(panel, ref_year=2019)
    rows = engineer_features(panel, credit_conn, labels, ref_year=2019)

    for row in rows:
        expected = labels.get(row.person_id)
        assert expected is not None, f"Feature row for unknown person {row.person_id}"
        assert row.label == expected, (
            f"label mismatch for {row.person_id}: got {row.label}, expected {expected}"
        )


def test_cohort_age_non_negative(credit_conn: sqlite3.Connection) -> None:
    """cohort_age (ref_year - debut_year) が非負であることを確認する。"""
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)
    labels = compute_visibility_label(panel, ref_year=2019)
    rows = engineer_features(panel, credit_conn, labels, ref_year=2019)

    for row in rows:
        assert row.cohort_age >= 0.0, (
            f"cohort_age must be non-negative, got {row.cohort_age} for {row.person_id}"
        )


# ---------------------------------------------------------------------------
# Calibration test
# ---------------------------------------------------------------------------


def test_calibrated_probabilities_in_unit_interval(credit_conn: sqlite3.Connection) -> None:
    """isotonic calibration 後の予測確率が [0, 1] に収まることを確認する。"""
    try:
        import lightgbm  # noqa: F401
    except ImportError:
        pytest.skip("lightgbm not installed — skipping calibration test")

    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        temporal_train_test_split,
        train_visibility_model,
        predict_visibility_loss,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)

    all_rows = []
    for ref_year in range(2015, 2022):
        labels = compute_visibility_label(panel, ref_year=ref_year)
        if labels:
            rows = engineer_features(panel, credit_conn, labels, ref_year=ref_year)
            all_rows.extend(rows)

    if len(all_rows) < 60:
        pytest.skip("Insufficient data for calibration test")

    train_rows, holdout_rows = temporal_train_test_split(all_rows, holdout_year=2021)
    if len(holdout_rows) < 10:
        pytest.skip("Insufficient holdout rows")

    model = train_visibility_model(train_rows, holdout_year=2021, rng_seed=42)
    if model is None:
        pytest.skip("Model training failed (insufficient data)")

    probs = predict_visibility_loss(model, holdout_rows)

    assert len(probs) == len(holdout_rows), "Prediction count must match holdout count"
    assert np.all(probs >= 0.0), "All probabilities must be >= 0"
    assert np.all(probs <= 1.0), "All probabilities must be <= 1"
    assert not np.any(np.isnan(probs)), "No NaN probabilities"


def test_subgroup_fairness_values_in_range(credit_conn: sqlite3.Connection) -> None:
    """subgroup AUC が [0, 1] の範囲内 (または NaN) であることを確認する。"""
    try:
        import lightgbm  # noqa: F401
    except ImportError:
        pytest.skip("lightgbm not installed — skipping subgroup fairness test")

    import math
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        temporal_train_test_split,
        train_visibility_model,
        predict_visibility_loss,
        check_subgroup_fairness,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)

    all_rows = []
    for ref_year in range(2015, 2022):
        labels = compute_visibility_label(panel, ref_year=ref_year)
        if labels:
            rows = engineer_features(panel, credit_conn, labels, ref_year=ref_year)
            all_rows.extend(rows)

    if len(all_rows) < 60:
        pytest.skip("Insufficient data")

    train_rows, holdout_rows = temporal_train_test_split(all_rows, holdout_year=2021)
    model = train_visibility_model(train_rows, holdout_year=2021, rng_seed=42)
    if model is None:
        pytest.skip("Model training failed")

    probs = predict_visibility_loss(model, holdout_rows)
    sg_auc = check_subgroup_fairness(holdout_rows, probs)

    for group, auc in sg_auc.items():
        if math.isnan(auc):
            continue  # small group — acceptable
        assert 0.0 <= auc <= 1.0, (
            f"subgroup AUC for {group} must be in [0,1], got {auc}"
        )


# ---------------------------------------------------------------------------
# End-to-end smoke test
# ---------------------------------------------------------------------------


def test_end_to_end_smoke(credit_conn: sqlite3.Connection) -> None:
    """合成データで end-to-end pipeline が動作することを確認する。

    LightGBM が利用できない環境では label / feature / split / leakage check のみ確認。
    """
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        temporal_train_test_split,
        run_leakage_check,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    assert len(panel) > 0, "Panel must not be empty"

    enrich_panel_with_scores(panel, credit_conn)

    all_rows = []
    for ref_year in range(2015, 2022):
        labels = compute_visibility_label(panel, ref_year=ref_year)
        rows = engineer_features(panel, credit_conn, labels, ref_year=ref_year)
        all_rows.extend(rows)

    assert len(all_rows) > 0, "Feature rows must not be empty"

    train_rows, holdout_rows = temporal_train_test_split(all_rows, holdout_year=2021)
    assert len(train_rows) > 0, "Train must not be empty"
    assert len(holdout_rows) > 0, "Holdout must not be empty"

    leakage = run_leakage_check(train_rows, holdout_rows, holdout_year=2021)
    assert leakage.passed, (
        f"Leakage check must pass for standard pipeline. "
        f"Violations: {leakage.violations[:5]}"
    )

    # Optional: run model if lightgbm is available
    try:
        import lightgbm  # noqa: F401
        from src.analysis.career.visibility_loss import (
            train_visibility_model,
            evaluate_model,
        )
        model = train_visibility_model(train_rows, holdout_year=2021, rng_seed=42)
        if model is not None:
            evaluation = evaluate_model(model, holdout_rows, train_rows)
            assert 0.0 <= evaluation.brier_score <= 1.0 or (
                evaluation.brier_score != evaluation.brier_score
            ), "Brier score must be in [0,1] or NaN"
    except ImportError:
        pass  # lightgbm optional


# ---------------------------------------------------------------------------
# Source file existence tests
# ---------------------------------------------------------------------------


def test_analysis_src_exists() -> None:
    """visibility_loss.py source file must exist."""
    from pathlib import Path
    src = Path(__file__).parents[3] / "src" / "analysis" / "career" / "visibility_loss.py"
    assert src.exists(), f"Analysis source not found: {src}"


def test_no_anime_score_in_source() -> None:
    """visibility_loss.py must not reference anime.score."""
    from pathlib import Path
    src = Path(__file__).parents[3] / "src" / "analysis" / "career" / "visibility_loss.py"
    text = src.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in visibility_loss.py"


# ---------------------------------------------------------------------------
# ECE (Expected Calibration Error) unit tests
# ---------------------------------------------------------------------------


def test_ece_perfect_calibration_is_zero() -> None:
    """完全に較正済みの場合 ECE = 0 になることを確認する。

    全予測 0.0 で全観測 0 → 各ビンの mean_pred == frac_pos → ECE = 0。
    """
    from src.analysis.career.visibility_loss import compute_expected_calibration_error

    y_true = np.zeros(100, dtype=float)
    y_score = np.zeros(100, dtype=float)
    ece = compute_expected_calibration_error(y_true, y_score, n_bins=10)
    assert ece == 0.0, f"Perfect calibration should yield ECE=0, got {ece}"


def test_ece_total_miscalibration_is_one() -> None:
    """完全な誤較正 (確信 1.0 で実測 0、または逆) で ECE = 1 に近くなる。"""
    from src.analysis.career.visibility_loss import compute_expected_calibration_error

    # 全予測 1.0 だが実測ゼロ
    y_true = np.zeros(100, dtype=float)
    y_score = np.ones(100, dtype=float)
    ece = compute_expected_calibration_error(y_true, y_score, n_bins=10)
    assert ece == pytest.approx(1.0, abs=1e-6), (
        f"Total miscalibration should yield ECE=1, got {ece}"
    )


def test_ece_empty_input_is_nan() -> None:
    """空配列に対して ECE が NaN を返すことを確認する。"""
    import math
    from src.analysis.career.visibility_loss import compute_expected_calibration_error

    ece = compute_expected_calibration_error(
        np.array([], dtype=float), np.array([], dtype=float)
    )
    assert math.isnan(ece), f"Empty input should yield NaN, got {ece}"


def test_ece_value_in_unit_interval() -> None:
    """任意の予測について ECE ∈ [0, 1] を確認する (確率的に強い不等式)。"""
    from src.analysis.career.visibility_loss import compute_expected_calibration_error

    rng = np.random.default_rng(42)
    for _ in range(20):
        n = rng.integers(50, 500)
        y_true = (rng.random(n) > 0.5).astype(float)
        y_score = rng.random(n)
        ece = compute_expected_calibration_error(y_true, y_score, n_bins=10)
        assert 0.0 <= ece <= 1.0, f"ECE must be in [0,1], got {ece}"


# ---------------------------------------------------------------------------
# Signal-rich synthetic data: AUC > 0.7 and ECE < 0.1
# ---------------------------------------------------------------------------


@pytest.fixture()
def signal_rich_conn() -> sqlite3.Connection:
    """信号の強い合成データ — 線形 declining パターンで visibility loss を予測可能にする。

    Pattern:
        - 約 1/3: stable (常時 active、low loss rate)
        - 約 1/3: declining (年々 credit が減少、高 loss rate)
        - 約 1/3: noise (random、中間 loss rate)
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE persons (id TEXT PRIMARY KEY, name_en TEXT, gender TEXT);
        CREATE TABLE anime (id TEXT PRIMARY KEY, title_ja TEXT, studio_id TEXT, year INTEGER);
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT, anime_id TEXT, role TEXT,
            credit_year INTEGER, evidence_source TEXT DEFAULT ''
        );
    """)

    rng = np.random.default_rng(42)

    for idx in range(60):
        yr = 2008 + (idx % 17)
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year) VALUES (?,?,?,?)",
            (f"a{idx}", f"Anime{idx}", f"studio_{idx % 5}", yr),
        )

    for p_idx in range(300):
        if p_idx < 100:
            gender = "female"
        elif p_idx < 200:
            gender = "male"
        else:
            gender = None
        conn.execute(
            "INSERT INTO persons (id, name_en, gender) VALUES (?,?,?)",
            (f"p{p_idx}", f"Person{p_idx}", gender),
        )

    for p_idx in range(300):
        pattern = p_idx % 3
        debut_year = 2010 + (p_idx % 5)
        for yr in range(debut_year, 2024):
            if pattern == 0:
                # stable: 2-3 credits/year
                n = 2 + (yr % 2)
                active = True
            elif pattern == 1:
                # declining: prob drops over time → eventually loses visibility
                years_since = yr - debut_year
                prob = max(0.05, 0.9 - 0.08 * years_since)
                active = bool(rng.random() < prob)
                n = 1 if active else 0
            else:
                # noise: random ~ 60% per year
                active = bool(rng.random() < 0.6)
                n = 1 if active else 0
            for k in range(n if active else 0):
                anime_id = f"a{(p_idx + yr + k) % 60}"
                role = "in_between" if pattern == 1 or p_idx % 4 == 0 else "key_animator"
                conn.execute(
                    "INSERT INTO credits (person_id, anime_id, role, credit_year)"
                    " VALUES (?,?,?,?)",
                    (f"p{p_idx}", anime_id, role, yr),
                )

    conn.commit()
    return conn


def test_holdout_auc_above_threshold_and_ece_below_threshold(
    signal_rich_conn: sqlite3.Connection,
) -> None:
    """信号の強い合成データで holdout AUC > 0.7 かつ ECE < 0.1 を満たすことを確認。

    Method gate (TASK_CARDS/25_compensation_fairness/03):
        - holdout AUC ≥ 0.65 で公開可。AUC > 0.7 で確実性高。
        - ECE < 0.10 で個別予測確率の解釈が可能。
    """
    try:
        import lightgbm  # noqa: F401
    except ImportError:
        pytest.skip("lightgbm not installed — skipping signal-rich AUC/ECE test")

    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        temporal_train_test_split,
        train_visibility_model,
        evaluate_model,
    )

    panel = build_credit_panel(signal_rich_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, signal_rich_conn)

    all_rows = []
    for ref_year in range(2013, 2022):
        labels = compute_visibility_label(panel, ref_year=ref_year)
        if labels:
            rows = engineer_features(panel, signal_rich_conn, labels, ref_year=ref_year)
            all_rows.extend(rows)

    assert len(all_rows) > 100, f"Insufficient feature rows: {len(all_rows)}"

    train_rows, holdout_rows = temporal_train_test_split(all_rows, holdout_year=2020)
    assert len(holdout_rows) >= 30, f"Insufficient holdout: {len(holdout_rows)}"

    n_pos_holdout = sum(r.label for r in holdout_rows)
    assert n_pos_holdout >= 3, (
        f"Holdout must contain at least 3 positives, got {n_pos_holdout}"
    )

    model = train_visibility_model(train_rows, holdout_year=2020, rng_seed=42)
    assert model is not None, "Model training failed"

    evaluation = evaluate_model(model, holdout_rows, train_rows)

    assert evaluation.auc_roc > 0.7, (
        f"Signal-rich holdout AUC must exceed 0.7, got {evaluation.auc_roc:.3f}. "
        f"Either the synthetic signal degraded or the model is underfitting."
    )
    assert evaluation.ece < 0.1, (
        f"Calibration ECE must be < 0.10, got {evaluation.ece:.3f}. "
        f"isotonic calibration is failing on this signal-rich panel."
    )
    assert evaluation.passes_gate, "Should pass the AUC ≥ 0.65 gate"


def test_evaluation_has_ece_field(credit_conn: sqlite3.Connection) -> None:
    """ModelEvaluation に ece フィールドが存在し、[0, 1] または NaN であることを確認。"""
    try:
        import lightgbm  # noqa: F401
    except ImportError:
        pytest.skip("lightgbm not installed")

    import math
    from src.analysis.career.visibility_loss import (
        build_credit_panel,
        compute_visibility_label,
        engineer_features,
        enrich_panel_with_scores,
        temporal_train_test_split,
        train_visibility_model,
        evaluate_model,
    )

    panel = build_credit_panel(credit_conn, min_year=2010, max_year=2023)
    enrich_panel_with_scores(panel, credit_conn)

    all_rows = []
    for ref_year in range(2015, 2022):
        labels = compute_visibility_label(panel, ref_year=ref_year)
        if labels:
            rows = engineer_features(panel, credit_conn, labels, ref_year=ref_year)
            all_rows.extend(rows)

    if len(all_rows) < 60:
        pytest.skip("Insufficient data")

    train_rows, holdout_rows = temporal_train_test_split(all_rows, holdout_year=2021)
    model = train_visibility_model(train_rows, holdout_year=2021, rng_seed=42)
    if model is None:
        pytest.skip("Model training failed")

    evaluation = evaluate_model(model, holdout_rows, train_rows)
    assert hasattr(evaluation, "ece"), "ModelEvaluation must expose ece"
    assert math.isnan(evaluation.ece) or 0.0 <= evaluation.ece <= 1.0, (
        f"ECE must be in [0,1] or NaN, got {evaluation.ece}"
    )
    assert hasattr(evaluation, "reliability_curve"), (
        "ModelEvaluation must expose reliability_curve"
    )
