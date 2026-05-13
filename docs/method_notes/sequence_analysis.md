# Method Note: Sequence Analysis for Career Trajectory Typology

**Report**: `career_typology`
**Module**: `src/analysis/career/trajectory_typology.py`
**Version**: v1.0 (2026-05-13)

---

## 1. Motivation

Career trajectory research (Abbott 1995; Blair-Loy 1999; Biemann et al. 2011) uses
sequence analysis to identify recurring patterns in longitudinal data without
imposing a parametric model.  For animation-industry credits, the challenge is that:

- Individual career lengths vary (3 to 40+ years)
- Role transitions are non-linear (regression to earlier roles is observed)
- Data gaps are structural (a person may not receive credits in every year)

Optimal Matching (OM) distance provides a global alignment-based dissimilarity
measure that handles variable length and non-linear transitions.

---

## 2. Sequence Construction

**Unit**: person × year → primary role  
**Primary role** = the CAREER_STAGE-highest role held in that credit year.  
**Non-production roles** (stage 0: voice actor, music, original creator, etc.) are
excluded; years with only non-production credits are omitted from the sequence.

**Gap handling**: Years without observed production credits are excluded rather than
interpolated.  This is conservative: it avoids injecting fictitious role
assignments, at the cost of treating a 5-year gap as a sequence boundary.

**Minimum length**: Persons with fewer than 3 observed production years are excluded.
This threshold balances coverage (short sequences add noise to OM distance) with
inclusion (many animators have brief observed careers in this dataset).

---

## 3. Optimal Matching Distance

The OM distance between two sequences $a = (a_1, \ldots, a_m)$ and
$b = (b_1, \ldots, b_n)$ is defined as the minimum cost to transform $a$ into $b$
using elementary operations:

| Operation | Cost |
|-----------|------|
| Substitution of $a_i$ with $b_j$ | $\|s(a_i) - s(b_j)\|$ where $s(\cdot)$ = CAREER_STAGE |
| Insertion (add an element) | $c_\text{indel} = 1.0$ |
| Deletion (remove an element) | $c_\text{indel} = 1.0$ |

**Substitution cost**: proportional to the structural distance between stages.
Substituting stage 1 (in_between) with stage 5 (animation director) costs 4,
whereas substituting stage 1 with stage 2 (second key) costs 1.

**Indel cost** ($c_\text{indel} = 1.0$): A constant gap penalty.  When
$c_\text{indel}$ is low relative to substitution costs, OM favours matching
elements via indels rather than substitutions.  At $c_\text{indel} = 1.0$, a
one-stage substitution is as costly as one indel, which is a conservative choice
that slightly over-penalises timing differences.

**Implementation**: Needleman-Wunsch dynamic programming.  Time $O(mn)$ per pair.
For large datasets, the $O(n^2)$ pairwise computation may be slow; sampling or
approximate methods are recommended for $n > 5000$.

**Assumption**: CAREER_STAGE integer values are treated as an interval scale.  The
actual skill-development distance between consecutive stages is not necessarily
equal; this is a modelling assumption.  Sensitivity analyses with alternative
cost matrices (e.g. uniform substitution cost = 1 regardless of stage distance)
are recommended for publication.

---

## 4. Hierarchical Clustering (Ward)

Ward's minimum variance linkage is applied to the condensed OM distance matrix
(via `scipy.spatial.distance.squareform`).  Ward linkage minimises the total
within-cluster variance after merging, and tends to produce compact, similarly
sized clusters.

**Why Ward**: Ward is known to work well with OM distance matrices in social
sequence analysis (Studer & Ritschard 2016).  Alternative linkages (average,
complete) are recommended as sensitivity checks.

**k selection**: The number of clusters $k \in [3, 7]$ is chosen by the highest
mean silhouette coefficient on the precomputed OM distance matrix.

---

## 5. Silhouette Coefficient

The mean silhouette coefficient (Rousseeuw 1987) is:

$$\bar{s} = \frac{1}{n} \sum_{i=1}^{n} \frac{b_i - a_i}{\max(a_i, b_i)}$$

where $a_i$ = mean distance from $i$ to members of its cluster,
and $b_i$ = mean distance from $i$ to the nearest other cluster.

$\bar{s} \in [-1, 1]$.  Values near 1 indicate well-separated clusters;
values near 0 indicate overlapping clusters.

**Stop-if gate**: If $\bar{s} < 0.2$ for all evaluated $k$, the typology
extraction is aborted and the report declares "typology structure absent."
This is reported as a null finding per `docs/REPORT_PHILOSOPHY.md §3.5`.

**Threshold rationale**: 0.2 is a conservative minimum interpretable separation.
Kaufman & Rousseeuw (1990) suggest $\bar{s} > 0.5$ for strong structure and
$\bar{s} \in [0.2, 0.5]$ for reasonable structure.  The 0.2 threshold allows
reporting of weak-but-present typologies while preventing noise from being
reported as structure.

---

## 6. Markov Transition Matrix

Within each cluster, a first-order Markov transition probability matrix is
computed from consecutive-year (year $t$ → year $t+1$) role transitions observed
in member sequences.

**Limitation**: This assumes the Markov property — the transition at year $t$ depends
only on the stage at year $t$, not on history.  For career trajectories, this is
known to be violated (path dependence exists).  The matrix is presented as a
descriptive summary of within-cluster flow patterns, not as a generative model.

**Gap handling**: Only consecutive-year pairs are counted; years with no credit
(gap years) are excluded from the transition count.

---

## 7. Cluster Labels

Cluster labels are derived mechanistically from the medoid's stage sequence:

| Pattern detected | Label |
|-----------------|-------|
| Monotone ascending | `progressive-ascent` |
| Monotone descending | `descending-role-shift` |
| ≥70% same-stage transitions | `early-specialist-stable` |
| Net rise, late acceleration | `delayed-advancement` |
| Peak in middle | `mid-career-peak-role` |
| Net descent with oscillation | `broad-to-focused` |
| Mixed ascending + descending | `multi-role-alternation` |
| Single observed year | `single-year-observed` |
| No dominant pattern | `unclassified` |

**Important**: Labels are structural descriptors.  They do not imply evaluation of
career "success," trajectory "quality," or individual "progression rate."
See `CLAUDE.md` Hard Rule H2.

---

## 8. Known Limitations

1. **Coverage bias**: TV-series credits are overrepresented.  Specialists in
   theatrical or OVA work may have shorter sequences, clustering into
   `single-year-observed` or being excluded by the minimum length filter.

2. **Gap sequences**: Persons with intermittent career activity (sabbatical,
   illness, overseas work) will have fragmented sequences.  OM distance treats
   these as editing costs, which may over-distance them from continuous-career persons.

3. **Stage interval assumption**: CAREER_STAGE values (1–6) are treated as equally
   spaced.  The transition from stage 5 (animation director) to stage 6 (director)
   may be structurally larger than a one-unit difference suggests.

4. **Entity resolution errors**: False merges (two persons aggregated as one) create
   anomalous mixed sequences.  The estimated false-merge rate (~1–3%) introduces
   noise into the OM distance computation.

5. **Computational cost**: $O(n^2)$ pairwise OM computation is expensive for large $n$.
   The current implementation is pure Python + NumPy.  For production use with
   $n > 2000$, consider vectorised C extensions or approximate distance methods.

---

## 9. References

- Abbott, A. & Forrest, J. (1986). Optimal matching methods for historical sequences.
  *Journal of Interdisciplinary History*, 16(3), 471–494.
- Kaufman, L. & Rousseeuw, P.J. (1990). *Finding Groups in Data*.
  John Wiley & Sons.
- Rousseeuw, P.J. (1987). Silhouettes: A graphical aid to the interpretation and
  validation of cluster analysis. *Journal of Computational and Applied Mathematics*, 20, 53–65.
- Studer, M. & Ritschard, G. (2016). What matters in differences between life
  trajectories: A comparative review of sequence dissimilarity measures.
  *Journal of the Royal Statistical Society A*, 179(2), 481–511.
