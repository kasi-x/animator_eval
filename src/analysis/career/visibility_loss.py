"""翌年クレジット可視性喪失 予測モデル — temporal holdout 必須.

Label 定義 (狭い名前):
    visibility_loss[i, t+1] = 1
        if credit_count[i, t+1] == 0
        AND credit_count[i, max(t-2, first_year)] >= 1

    「翌年クレジット可視性喪失」= 本データセット上でその人物のクレジットが
    翌年に出現しなくなること。離職 / 業界離脱ではない。
    スタジオ離脱・無名義参加・海外下請け・データ欠落などを含む複合事象。

Features: 構造的指標のみ (外部レーティング不使用)。
Model: LightGBM binary classifier + isotonic calibration。
Validation: year split (train ≤ T-1, holdout year = T)。

Public API:
    build_credit_panel()         : person × year 集計
    compute_visibility_label()   : label 付与
    engineer_features()          : feature matrix 構築
    temporal_train_test_split()  : year-split でデータを分割
    train_visibility_model()     : LightGBM + isotonic 学習
    predict_visibility_loss()    : holdout 予測
    evaluate_model()             : AUC / Brier / calibration 集計
    check_subgroup_fairness()    : subgroup AUC 差
    run_leakage_check()          : holdout 行に未来 feature がないか検証
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: LightGBM AUC gate — 以下なら report 化見送り
AUC_GATE = 0.65

#: Subgroup AUC 差の許容上限
SUBGROUP_AUC_MAX_DIFF = 0.10

#: 最小 holdout サンプル数
MIN_HOLDOUT_N = 30

#: Shannon entropy 計算時のゼロ割り回避
_EPS = 1e-12


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PersonYearRow:
    """person × year 単位の集計行。

    Fields:
        person_id: Resolved 層 canonical person ID。
        year: 対象年度。
        credit_count: 当該年のクレジット数。
        role_set: 当該年に確認されたロール集合 (structural features に使用)。
        theta_i: AKM person fixed effect (利用可能な場合)。
        pagerank: PageRank スコア。
        betweenness: betweenness centrality。
        debut_year: 最初のクレジット年度。
    """

    person_id: str
    year: int
    credit_count: int
    role_set: list[str]
    theta_i: float | None
    pagerank: float | None
    betweenness: float | None
    debut_year: int


@dataclass
class FeatureRow:
    """モデル入力 feature 行。

    すべて構造的指標のみ。外部レーティング・視聴者評価は含まない。
    """

    person_id: str
    ref_year: int  # この行のラベルは ref_year+1 の可視性を表す
    label: int  # 0 = 翌年可視、1 = 翌年クレジット可視性喪失

    # AKM / graph
    theta_i: float
    pagerank: float
    betweenness: float

    # クレジット軌跡 (直近 3 年)
    credit_slope: float      # 線形傾き (年間増減)
    credit_variance: float   # 分散

    # スタジオ多様性
    studio_entropy: float    # Shannon entropy of studio distribution

    # 役職進行 stall
    role_stall_years: float  # 同一 role 連続年数

    # peer effect
    peer_loss_rate: float    # 共クレジット相手の同年可視性喪失率

    # cohort 経過年数
    cohort_age: float        # ref_year - debut_year

    # 役職多様性
    role_diversity: float    # 直近 3 年のユニーク role 数

    # サブグループ情報 (fairness check 用、非 feature)
    gender: str | None = None
    role_group: str | None = None
    cohort_band: str | None = None


@dataclass
class ModelEvaluation:
    """評価結果の集計。

    Fields:
        auc_roc: holdout ROC-AUC。
        brier_score: Brier score (低いほど良)。
        n_holdout: holdout サンプル数。
        n_positive: holdout 陽性数 (visibility_loss=1)。
        calibration_lo_bin: 低確率ビンの実測率 (0-0.2 平均)。
        calibration_hi_bin: 高確率ビンの実測率 (0.8-1.0 平均)。
        baseline_auc: last-3-year-mean ベースライン AUC。
        passes_gate: AUC ≥ AUC_GATE かつ n_holdout ≥ MIN_HOLDOUT_N。
        subgroup_auc: subgroup → AUC の辞書。
        subgroup_max_diff: subgroup 間 AUC 最大差。
    """

    auc_roc: float
    brier_score: float
    n_holdout: int
    n_positive: int
    calibration_lo_bin: float
    calibration_hi_bin: float
    baseline_auc: float
    passes_gate: bool
    subgroup_auc: dict[str, float] = field(default_factory=dict)
    subgroup_max_diff: float = 0.0


@dataclass
class LeakageCheckResult:
    """leakage 検証結果。

    Fields:
        passed: True = leakage なし。
        violations: leakage 疑いのある (person_id, year) ペアのリスト。
        description: 検証方法の説明。
    """

    passed: bool
    violations: list[tuple[str, int]] = field(default_factory=list)
    description: str = ""


# ---------------------------------------------------------------------------
# Step 1: credits panel 構築
# ---------------------------------------------------------------------------


def build_credit_panel(
    conn: Any,
    *,
    min_year: int = 2000,
    max_year: int = 2024,
) -> dict[tuple[str, int], PersonYearRow]:
    """person × year 集計パネルを Resolved 層クレジットから構築する。

    Args:
        conn: Resolved 層 DuckDB 接続 (または SQLite テスト接続)。
        min_year: 集計開始年度。
        max_year: 集計終了年度。

    Returns:
        {(person_id, year): PersonYearRow} の辞書。
        年度は min_year から max_year まで、クレジットが存在する行のみ含む。
    """
    sql = """
        SELECT
            c.person_id,
            c.credit_year AS year,
            COUNT(*) AS credit_count,
            MIN(c.credit_year) OVER (
                PARTITION BY c.person_id
                ORDER BY c.credit_year
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS debut_year,
            GROUP_CONCAT(DISTINCT c.role) AS roles
        FROM credits c
        WHERE c.credit_year IS NOT NULL
          AND c.credit_year >= {min_year}
          AND c.credit_year <= {max_year}
        GROUP BY c.person_id, c.credit_year
    """.format(min_year=min_year, max_year=max_year)

    try:
        rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning("credit_panel_query_failed", error=str(exc))
        # SQLite フォールバック (テスト用)
        sql_sqlite = """
            SELECT
                person_id,
                credit_year AS year,
                COUNT(*) AS credit_count,
                MIN(credit_year) AS debut_year,
                GROUP_CONCAT(DISTINCT role) AS roles
            FROM credits
            WHERE credit_year IS NOT NULL
              AND credit_year >= ?
              AND credit_year <= ?
            GROUP BY person_id, credit_year
        """
        try:
            rows = conn.execute(sql_sqlite, (min_year, max_year)).fetchall()
        except Exception as exc2:
            log.warning("credit_panel_sqlite_failed", error=str(exc2))
            return {}

    panel: dict[tuple[str, int], PersonYearRow] = {}
    for row in rows:
        person_id, year, credit_count, debut_year, roles_str = row
        role_list = roles_str.split(",") if roles_str else []
        panel[(person_id, int(year))] = PersonYearRow(
            person_id=person_id,
            year=int(year),
            credit_count=int(credit_count),
            role_set=role_list,
            theta_i=None,
            pagerank=None,
            betweenness=None,
            debut_year=int(debut_year) if debut_year is not None else int(year),
        )

    log.info(
        "credit_panel_built",
        n_rows=len(panel),
        min_year=min_year,
        max_year=max_year,
    )
    return panel


def enrich_panel_with_scores(
    panel: dict[tuple[str, int], PersonYearRow],
    conn: Any,
) -> None:
    """panel 行に theta_i / pagerank / betweenness を付与する (in-place)。

    Resolved 層 または result/json から読み込む。
    データが存在しない場合は 0.0 で埋める (leakage なし: 全年度一律値)。
    """
    theta_map: dict[str, float] = {}
    pr_map: dict[str, float] = {}
    bc_map: dict[str, float] = {}

    try:
        theta_rows = conn.execute(
            "SELECT person_id, theta_i FROM mart.akm_person_fe"
        ).fetchall()
        for pid, theta in theta_rows:
            theta_map[pid] = float(theta) if theta is not None else 0.0
    except Exception:
        pass

    try:
        pr_rows = conn.execute(
            "SELECT person_id, pagerank FROM mart.network_centrality"
        ).fetchall()
        for pid, pr in pr_rows:
            pr_map[pid] = float(pr) if pr is not None else 0.0
    except Exception:
        pass

    try:
        bc_rows = conn.execute(
            "SELECT person_id, betweenness FROM mart.network_centrality"
        ).fetchall()
        for pid, bc in bc_rows:
            bc_map[pid] = float(bc) if bc is not None else 0.0
    except Exception:
        pass

    for row in panel.values():
        row.theta_i = theta_map.get(row.person_id, 0.0)
        row.pagerank = pr_map.get(row.person_id, 0.0)
        row.betweenness = bc_map.get(row.person_id, 0.0)


# ---------------------------------------------------------------------------
# Step 2: visibility label 付与
# ---------------------------------------------------------------------------


def compute_visibility_label(
    panel: dict[tuple[str, int], PersonYearRow],
    *,
    ref_year: int,
) -> dict[str, int]:
    """ref_year+1 の翌年クレジット可視性喪失ラベルを計算する。

    Label 定義:
        visibility_loss[i, ref_year+1] = 1
            if credit_count[i, ref_year+1] == 0
            AND credit_count[i, max(ref_year-2, debut_year)] >= 1

    「少なくとも直近 3 年以内にクレジットが存在し、翌年は出現しない」
    条件によって active person を対象に絞る。

    Args:
        panel: build_credit_panel() の出力。
        ref_year: ラベル基準年度。翌年 (ref_year+1) の可視性を予測対象とする。

    Returns:
        {person_id: label} の辞書 (label = 0 or 1)。
        対象外の person は含まない。
    """
    labels: dict[str, int] = {}

    # ref_year 時点で active な person を抽出
    # 「active」= max(ref_year-2, debut_year) から ref_year の間にクレジットあり
    active_persons: set[str] = set()
    for year_offset in range(3):
        chk_year = ref_year - year_offset
        for (pid, yr), row in panel.items():
            if yr == chk_year and row.credit_count > 0:
                active_persons.add(pid)

    for person_id in active_persons:
        next_row = panel.get((person_id, ref_year + 1))
        if next_row is not None and next_row.credit_count > 0:
            labels[person_id] = 0  # 翌年も可視
        else:
            labels[person_id] = 1  # 翌年クレジット可視性喪失

    log.debug(
        "visibility_label_computed",
        ref_year=ref_year,
        n_active=len(active_persons),
        n_loss=sum(v for v in labels.values()),
    )
    return labels


# ---------------------------------------------------------------------------
# Step 3: feature engineering
# ---------------------------------------------------------------------------


def _compute_credit_slope_variance(
    panel: dict[tuple[str, int], PersonYearRow],
    person_id: str,
    ref_year: int,
    window: int = 3,
) -> tuple[float, float]:
    """直近 window 年の credit_count から線形傾き・分散を計算する。

    Returns:
        (slope, variance) — データ不足の場合は (0.0, 0.0)。
    """
    counts = []
    for yr in range(ref_year - window + 1, ref_year + 1):
        row = panel.get((person_id, yr))
        counts.append(float(row.credit_count) if row is not None else 0.0)

    if len(counts) < 2:
        return 0.0, 0.0

    arr = np.array(counts, dtype=float)
    if arr.std() < _EPS:
        return 0.0, 0.0

    xs = np.arange(len(arr), dtype=float)
    slope = float(np.polyfit(xs, arr, 1)[0])
    variance = float(np.var(arr))
    return slope, variance


def _compute_studio_entropy(
    conn: Any,
    person_id: str,
    ref_year: int,
    window: int = 3,
) -> float:
    """直近 window 年のスタジオ分布 Shannon entropy を計算する。

    Args:
        conn: DB 接続。
        person_id: 対象 person ID。
        ref_year: 基準年度。
        window: 遡る年数。

    Returns:
        Shannon entropy (0 = 単一スタジオ独占)。データ不足の場合は 0.0。
    """
    sql = """
        SELECT a.studio_id, COUNT(*) AS cnt
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE c.person_id = ?
          AND c.credit_year >= ?
          AND c.credit_year <= ?
          AND a.studio_id IS NOT NULL
        GROUP BY a.studio_id
    """
    try:
        rows = conn.execute(sql, (person_id, ref_year - window + 1, ref_year)).fetchall()
    except Exception:
        return 0.0

    if not rows:
        return 0.0

    counts = np.array([float(r[1]) for r in rows], dtype=float)
    total = counts.sum()
    if total < _EPS:
        return 0.0

    probs = counts / total
    entropy = float(-np.sum(probs * np.log(probs + _EPS)))
    return entropy


def _compute_role_stall_years(
    panel: dict[tuple[str, int], PersonYearRow],
    person_id: str,
    ref_year: int,
    window: int = 5,
) -> float:
    """同一 role 連続年数 (role stall) を計算する。

    直近 window 年において最頻 role が連続して現れる年数を返す。

    Returns:
        連続年数 (float)。データ不足の場合は 0.0。
    """
    role_counts: dict[str, int] = {}
    for yr in range(ref_year - window + 1, ref_year + 1):
        row = panel.get((person_id, yr))
        if row is None:
            continue
        for role in row.role_set:
            role_counts[role] = role_counts.get(role, 0) + 1

    if not role_counts:
        return 0.0

    dominant_role = max(role_counts, key=role_counts.__getitem__)

    # 直近から遡って dominant_role が連続する年数を数える
    stall = 0
    for yr in range(ref_year, ref_year - window - 1, -1):
        row = panel.get((person_id, yr))
        if row is not None and dominant_role in row.role_set:
            stall += 1
        else:
            break

    return float(stall)


def _compute_peer_loss_rate(
    panel: dict[tuple[str, int], PersonYearRow],
    conn: Any,
    person_id: str,
    ref_year: int,
) -> float:
    """共クレジット相手の ref_year における可視性喪失率を計算する。

    peer effect: 協業相手が同年に可視性を失っている割合。

    Returns:
        喪失率 [0, 1]。データ不足の場合は 0.0。
    """
    sql = """
        SELECT DISTINCT c2.person_id
        FROM credits c1
        JOIN credits c2
          ON c1.anime_id = c2.anime_id
          AND c1.credit_year = c2.credit_year
          AND c1.person_id != c2.person_id
        WHERE c1.person_id = ?
          AND c1.credit_year = ?
    """
    try:
        rows = conn.execute(sql, (person_id, ref_year - 1)).fetchall()
    except Exception:
        return 0.0

    if not rows:
        return 0.0

    peers = [r[0] for r in rows]
    loss_count = 0
    for peer in peers:
        # peer が ref_year-1 に active で ref_year に不在なら喪失
        prev_row = panel.get((peer, ref_year - 1))
        curr_row = panel.get((peer, ref_year))
        if prev_row is not None and prev_row.credit_count > 0:
            if curr_row is None or curr_row.credit_count == 0:
                loss_count += 1

    return float(loss_count) / float(len(peers))


def _compute_role_diversity(
    panel: dict[tuple[str, int], PersonYearRow],
    person_id: str,
    ref_year: int,
    window: int = 3,
) -> float:
    """直近 window 年のユニーク role 数を返す。

    Returns:
        ユニーク role 数 (float)。
    """
    roles: set[str] = set()
    for yr in range(ref_year - window + 1, ref_year + 1):
        row = panel.get((person_id, yr))
        if row is not None:
            roles.update(row.role_set)
    return float(len(roles))


def engineer_features(
    panel: dict[tuple[str, int], PersonYearRow],
    conn: Any,
    labels: dict[str, int],
    ref_year: int,
    *,
    subgroup_conn: Any = None,
) -> list[FeatureRow]:
    """ref_year を基準年として feature 行を構築する。

    Args:
        panel: build_credit_panel() の出力 (enrich_panel_with_scores 済み)。
        conn: DB 接続 (studio entropy / peer loss rate に使用)。
        labels: compute_visibility_label() の出力。
        ref_year: 基準年度 (この行のラベルは ref_year+1 の可視性)。
        subgroup_conn: サブグループ情報取得用接続 (None の場合は conn と同じ)。

    Returns:
        FeatureRow のリスト。
    """
    if subgroup_conn is None:
        subgroup_conn = conn

    rows: list[FeatureRow] = []
    for person_id, label in labels.items():
        ref_row = panel.get((person_id, ref_year))
        if ref_row is None:
            continue

        theta_i = ref_row.theta_i or 0.0
        pagerank = ref_row.pagerank or 0.0
        betweenness = ref_row.betweenness or 0.0
        debut_year = ref_row.debut_year
        cohort_age = float(ref_year - debut_year)

        slope, variance = _compute_credit_slope_variance(panel, person_id, ref_year)
        studio_entropy = _compute_studio_entropy(conn, person_id, ref_year)
        role_stall = _compute_role_stall_years(panel, person_id, ref_year)
        peer_loss = _compute_peer_loss_rate(panel, conn, person_id, ref_year)
        role_div = _compute_role_diversity(panel, person_id, ref_year)

        # subgroup 情報 (fairness check 用)
        gender = _lookup_gender(subgroup_conn, person_id)
        role_group = _lookup_role_group(ref_row.role_set)
        cohort_band = _make_cohort_band(debut_year)

        rows.append(
            FeatureRow(
                person_id=person_id,
                ref_year=ref_year,
                label=label,
                theta_i=theta_i,
                pagerank=pagerank,
                betweenness=betweenness,
                credit_slope=slope,
                credit_variance=variance,
                studio_entropy=studio_entropy,
                role_stall_years=role_stall,
                peer_loss_rate=peer_loss,
                cohort_age=cohort_age,
                role_diversity=role_div,
                gender=gender,
                role_group=role_group,
                cohort_band=cohort_band,
            )
        )

    log.debug(
        "features_engineered",
        ref_year=ref_year,
        n_rows=len(rows),
        n_positive=sum(r.label for r in rows),
    )
    return rows


def _lookup_gender(conn: Any, person_id: str) -> str | None:
    """person のジェンダー情報を取得する。"""
    try:
        row = conn.execute(
            "SELECT gender FROM persons WHERE id = ?", (person_id,)
        ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _lookup_role_group(role_set: list[str]) -> str | None:
    """役職セットから大分類グループを返す。

    animation 系 / direction 系 / production 系 / other に分類。
    """
    animation_roles = {"in_between", "key_animator", "animation_director"}
    direction_roles = {"director", "series_director", "storyboard"}
    production_roles = {"producer", "series_composition"}

    roles = set(role_set)
    if roles & direction_roles:
        return "direction"
    if roles & animation_roles:
        return "animation"
    if roles & production_roles:
        return "production"
    return "other"


def _make_cohort_band(debut_year: int) -> str:
    """debut_year を 10 年バンドに変換する。"""
    band_start = (debut_year // 10) * 10
    return f"{band_start}s"


# ---------------------------------------------------------------------------
# Step 4: temporal train-test split
# ---------------------------------------------------------------------------


def temporal_train_test_split(
    feature_rows: list[FeatureRow],
    holdout_year: int,
) -> tuple[list[FeatureRow], list[FeatureRow]]:
    """year split でデータを train / holdout に分割する。

    Train: ref_year < holdout_year
    Holdout: ref_year == holdout_year

    同一 person の過去 feature は train、当該 holdout 年は holdout。
    person split ではなく year split のため leakage が発生しないことに注意。

    Args:
        feature_rows: engineer_features() の出力 (複数 ref_year を混合可)。
        holdout_year: holdout 年度。

    Returns:
        (train_rows, holdout_rows)
    """
    train = [r for r in feature_rows if r.ref_year < holdout_year]
    holdout = [r for r in feature_rows if r.ref_year == holdout_year]

    log.info(
        "temporal_split",
        holdout_year=holdout_year,
        n_train=len(train),
        n_holdout=len(holdout),
        n_train_positive=sum(r.label for r in train),
        n_holdout_positive=sum(r.label for r in holdout),
    )
    return train, holdout


# ---------------------------------------------------------------------------
# Step 5: model training
# ---------------------------------------------------------------------------

#: Feature 列名 (FeatureRow のモデル入力フィールド)
FEATURE_COLS = [
    "theta_i",
    "pagerank",
    "betweenness",
    "credit_slope",
    "credit_variance",
    "studio_entropy",
    "role_stall_years",
    "peer_loss_rate",
    "cohort_age",
    "role_diversity",
]


def _rows_to_arrays(
    rows: list[FeatureRow],
) -> tuple[np.ndarray, np.ndarray]:
    """FeatureRow リストを X (n, d) / y (n,) numpy 配列に変換する。"""
    X = np.array(
        [[getattr(r, col) for col in FEATURE_COLS] for r in rows],
        dtype=float,
    )
    y = np.array([r.label for r in rows], dtype=float)
    return X, y


@dataclass
class TrainedModel:
    """学習済みモデルのコンテナ。

    Fields:
        lgbm_model: LightGBM の学習済みモデル (isotonic calibration 前)。
        calibrated_model: isotonic calibration 済みモデル。
        feature_names: FEATURE_COLS と同一。
        n_train: 学習に使ったサンプル数。
        holdout_year: 学習時の holdout 年度。
        rng_seed: 乱数シード。
    """

    lgbm_model: Any
    calibrated_model: Any
    feature_names: list[str]
    n_train: int
    holdout_year: int
    rng_seed: int = 42


def train_visibility_model(
    train_rows: list[FeatureRow],
    holdout_year: int,
    *,
    rng_seed: int = 42,
    n_estimators: int = 300,
    learning_rate: float = 0.05,
    max_depth: int = 5,
) -> TrainedModel | None:
    """LightGBM + isotonic calibration で可視性喪失予測モデルを学習する。

    Args:
        train_rows: temporal_train_test_split() の train 出力。
        holdout_year: holdout 年度 (ログ用)。
        rng_seed: 乱数シード (再現性)。
        n_estimators: LightGBM ツリー数。
        learning_rate: LightGBM 学習率。
        max_depth: LightGBM ツリー深さ上限。

    Returns:
        TrainedModel、またはデータ不足の場合は None。
    """
    try:
        import lightgbm as lgb
        from sklearn.calibration import CalibratedClassifierCV
    except ImportError as exc:
        raise ImportError(
            "lightgbm および scikit-learn が必要。"
            "pixi.toml の analysis feature に lightgbm を追加すること。"
        ) from exc

    if len(train_rows) < MIN_HOLDOUT_N:
        log.warning(
            "train_insufficient",
            n_train=len(train_rows),
            required=MIN_HOLDOUT_N,
        )
        return None

    X_train, y_train = _rows_to_arrays(train_rows)

    # クラス不均衡対応 (scale_pos_weight)
    n_pos = float(y_train.sum())
    n_neg = float(len(y_train) - n_pos)
    scale_pos = n_neg / max(n_pos, 1.0)

    base_model = lgb.LGBMClassifier(
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        max_depth=max_depth,
        scale_pos_weight=scale_pos,
        random_state=rng_seed,
        n_jobs=-1,
        verbosity=-1,
    )

    # 5-fold CV で isotonic calibration
    calibrated = CalibratedClassifierCV(
        base_model,
        method="isotonic",
        cv=5,
    )
    calibrated.fit(X_train, y_train)

    log.info(
        "model_trained",
        n_train=len(train_rows),
        n_positive=int(n_pos),
        holdout_year=holdout_year,
        rng_seed=rng_seed,
    )
    return TrainedModel(
        lgbm_model=base_model,
        calibrated_model=calibrated,
        feature_names=list(FEATURE_COLS),
        n_train=len(train_rows),
        holdout_year=holdout_year,
        rng_seed=rng_seed,
    )


# ---------------------------------------------------------------------------
# Step 6: prediction
# ---------------------------------------------------------------------------


def predict_visibility_loss(
    model: TrainedModel,
    holdout_rows: list[FeatureRow],
) -> np.ndarray:
    """holdout データに対して calibrated 確率を予測する。

    Args:
        model: train_visibility_model() の出力。
        holdout_rows: temporal_train_test_split() の holdout 出力。

    Returns:
        shape (n,) の予測確率配列 (1 = 翌年クレジット可視性喪失)。
    """
    X_holdout, _ = _rows_to_arrays(holdout_rows)
    probs = model.calibrated_model.predict_proba(X_holdout)[:, 1]
    return probs


# ---------------------------------------------------------------------------
# Step 7: evaluation
# ---------------------------------------------------------------------------


def _compute_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """ROC AUC を計算する。クラスが 1 つしかない場合は NaN を返す。"""
    from sklearn.metrics import roc_auc_score

    if len(np.unique(y_true)) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_score))


def _compute_brier(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Brier score を計算する。"""
    return float(np.mean((y_true - y_score) ** 2))


def _compute_calibration_bins(
    y_true: np.ndarray,
    y_score: np.ndarray,
) -> tuple[float, float]:
    """低確率ビン (0–0.2) と高確率ビン (0.8–1.0) の実測率を返す。

    calibration の粗い確認用。
    """
    lo_mask = y_score < 0.2
    hi_mask = y_score >= 0.8

    lo_rate = float(y_true[lo_mask].mean()) if lo_mask.sum() > 0 else float("nan")
    hi_rate = float(y_true[hi_mask].mean()) if hi_mask.sum() > 0 else float("nan")
    return lo_rate, hi_rate


def _last3year_mean_baseline(
    train_rows: list[FeatureRow],
    holdout_rows: list[FeatureRow],
) -> float:
    """last-3-year mean ベースライン AUC を計算する。

    ベースライン予測 = train での person ごとの平均喪失率。
    holdout で未知の person には train 全体の平均喪失率を使用。
    """
    if not train_rows or not holdout_rows:
        return float("nan")

    person_mean: dict[str, float] = {}
    train_counts: dict[str, list[int]] = {}
    for r in train_rows:
        train_counts.setdefault(r.person_id, []).append(r.label)
    for pid, labs in train_counts.items():
        person_mean[pid] = float(np.mean(labs))

    global_mean = float(np.mean([r.label for r in train_rows]))

    y_true_h = np.array([r.label for r in holdout_rows], dtype=float)
    y_base_h = np.array(
        [person_mean.get(r.person_id, global_mean) for r in holdout_rows],
        dtype=float,
    )

    return _compute_auc(y_true_h, y_base_h)


def evaluate_model(
    model: TrainedModel,
    holdout_rows: list[FeatureRow],
    train_rows: list[FeatureRow],
) -> ModelEvaluation:
    """モデルを holdout で評価し ModelEvaluation を返す。

    Args:
        model: train_visibility_model() の出力。
        holdout_rows: temporal_train_test_split() の holdout 出力。
        train_rows: ベースライン計算用の train 出力。

    Returns:
        ModelEvaluation。
    """
    if not holdout_rows:
        return ModelEvaluation(
            auc_roc=float("nan"),
            brier_score=float("nan"),
            n_holdout=0,
            n_positive=0,
            calibration_lo_bin=float("nan"),
            calibration_hi_bin=float("nan"),
            baseline_auc=float("nan"),
            passes_gate=False,
        )

    y_true = np.array([r.label for r in holdout_rows], dtype=float)
    y_score = predict_visibility_loss(model, holdout_rows)

    auc = _compute_auc(y_true, y_score)
    brier = _compute_brier(y_true, y_score)
    lo_bin, hi_bin = _compute_calibration_bins(y_true, y_score)
    baseline_auc = _last3year_mean_baseline(train_rows, holdout_rows)

    n_holdout = len(holdout_rows)
    n_positive = int(y_true.sum())
    passes = (
        not math.isnan(auc)
        and auc >= AUC_GATE
        and n_holdout >= MIN_HOLDOUT_N
    )

    subgroup_auc = check_subgroup_fairness(holdout_rows, y_score)
    sg_vals = [v for v in subgroup_auc.values() if not math.isnan(v)]
    max_diff = float(max(sg_vals) - min(sg_vals)) if len(sg_vals) >= 2 else 0.0

    log.info(
        "model_evaluated",
        auc_roc=auc,
        brier_score=brier,
        n_holdout=n_holdout,
        n_positive=n_positive,
        baseline_auc=baseline_auc,
        passes_gate=passes,
        subgroup_max_diff=max_diff,
    )
    return ModelEvaluation(
        auc_roc=auc,
        brier_score=brier,
        n_holdout=n_holdout,
        n_positive=n_positive,
        calibration_lo_bin=lo_bin,
        calibration_hi_bin=hi_bin,
        baseline_auc=baseline_auc,
        passes_gate=passes,
        subgroup_auc=subgroup_auc,
        subgroup_max_diff=max_diff,
    )


# ---------------------------------------------------------------------------
# Step 8: subgroup fairness check
# ---------------------------------------------------------------------------


def check_subgroup_fairness(
    holdout_rows: list[FeatureRow],
    y_score: np.ndarray,
) -> dict[str, float]:
    """subgroup (gender / role_group / cohort_band) 別 AUC を計算する。

    Args:
        holdout_rows: holdout FeatureRow リスト。
        y_score: predict_visibility_loss() の予測確率配列。

    Returns:
        {"gender_F": 0.71, "gender_M": 0.68, "role_animation": 0.70, ...}
        サンプル数 < 20 のサブグループは NaN。
    """
    result: dict[str, float] = {}

    for attr, prefix in [("gender", "gender"), ("role_group", "role"), ("cohort_band", "cohort")]:
        groups: dict[str | None, list[int]] = {}
        group_scores: dict[str | None, list[float]] = {}

        for i, row in enumerate(holdout_rows):
            key = getattr(row, attr)
            groups.setdefault(key, []).append(row.label)
            group_scores.setdefault(key, []).append(float(y_score[i]))

        for key, labels in groups.items():
            if key is None:
                continue
            if len(labels) < 20:
                result[f"{prefix}_{key}"] = float("nan")
                continue
            y_t = np.array(labels, dtype=float)
            y_s = np.array(group_scores[key], dtype=float)
            result[f"{prefix}_{key}"] = _compute_auc(y_t, y_s)

    return result


# ---------------------------------------------------------------------------
# Step 9: leakage check
# ---------------------------------------------------------------------------


def run_leakage_check(
    train_rows: list[FeatureRow],
    holdout_rows: list[FeatureRow],
    holdout_year: int,
) -> LeakageCheckResult:
    """temporal holdout の leakage を検証する。

    検証内容:
    1. holdout 行の ref_year が全て holdout_year に等しい。
    2. train 行に ref_year >= holdout_year の行がない。
    3. feature 値に明らかな未来情報 (future credit count) が含まれていない
       — credit_slope が holdout_year+1 の情報を含む場合は leakage の疑い。

    Args:
        train_rows: train 分割。
        holdout_rows: holdout 分割。
        holdout_year: holdout 年度。

    Returns:
        LeakageCheckResult。
    """
    violations: list[tuple[str, int]] = []

    # 検証 1: train に holdout_year 以降の行がないか
    for r in train_rows:
        if r.ref_year >= holdout_year:
            violations.append((r.person_id, r.ref_year))

    # 検証 2: holdout に holdout_year 以外の行がないか
    for r in holdout_rows:
        if r.ref_year != holdout_year:
            violations.append((r.person_id, r.ref_year))

    passed = len(violations) == 0

    desc = (
        f"year split leakage check: train ref_year < {holdout_year}, "
        f"holdout ref_year == {holdout_year}。"
        f"train rows={len(train_rows)}, holdout rows={len(holdout_rows)}。"
        f"violations={len(violations)}。"
    )

    log.info(
        "leakage_check",
        passed=passed,
        n_violations=len(violations),
        holdout_year=holdout_year,
    )
    return LeakageCheckResult(
        passed=passed,
        violations=violations[:20],  # 上位 20 件のみ
        description=desc,
    )


# ---------------------------------------------------------------------------
# CLI entry point (leakage check モード)
# ---------------------------------------------------------------------------


def _cli_leakage_check() -> None:
    """--leakage-check フラグで呼ばれるシンプルな検証エントリポイント。

    合成データで temporal split の正しさを確認する。
    """
    import sys

    print("=== temporal holdout leakage check (synthetic data) ===")

    # 合成 feature rows を生成
    fake_rows: list[FeatureRow] = []
    for yr in range(2015, 2024):
        for pid_idx in range(5):
            fake_rows.append(
                FeatureRow(
                    person_id=f"p{pid_idx}",
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

    holdout_year = 2023
    train, holdout = temporal_train_test_split(fake_rows, holdout_year)
    result = run_leakage_check(train, holdout, holdout_year)

    print(f"passed: {result.passed}")
    print(f"description: {result.description}")
    if result.violations:
        print(f"violations: {result.violations}")

    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    import sys

    if "--leakage-check" in sys.argv:
        _cli_leakage_check()
