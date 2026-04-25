"""bangumi GraphQL query strings and endpoint constants.

These are pinned query documents sent to the bangumi GraphQL endpoint.
All field selections are kept conservative (only fields needed by the
BRONZE row builders in the orchestrator scripts) to keep response sizes
manageable.

Hard constraint H1: SubjectRating.score / rank / total are fetched and
stored in BRONZE as display metadata ONLY.  They must NEVER flow into the
scoring path.  This module has no opinion on what callers do with those
fields, but the constraint is documented here so future editors are aware.

Endpoint: https://api.bgm.tv/v0/graphql (POST, Content-Type: application/json)
Server: bangumi/server-private (Fastify + mercurius), Altair UI at /v0/altair/
Confirmed working: 2026-04-25
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BANGUMI_GRAPHQL_URL = "https://api.bgm.tv/v0/graphql"

# Reuse the same UA as the v0 REST client for consistent host-level rate limiting.
DEFAULT_USER_AGENT = "animetor_eval/0.1 (https://github.com/kashi-x)"

# ---------------------------------------------------------------------------
# Position code → label mapping (SubjectRelatedPerson.position)
# ---------------------------------------------------------------------------
# GraphQL returns integer codes; REST v0 returned strings like "导演", "脚本".
# This table maps the known integer codes to their Chinese/Japanese labels.
# Source: bangumi REST /v0/subjects/{id}/persons responses cross-referenced with
# GraphQL introspection (2026-04-25).  Codes are 1-indexed; unmapped codes
# should be stored as-is (str(code)) — do not drop unknown values.
#
# Note: a single code may correspond to multiple role labels in different
# subject contexts (e.g. code 4 = "分镜" AND "演出" on different subjects).
# This mapping stores the most common label for each code as reference only.
ANIME_POSITION_LABELS: dict[int, str] = {
    # Source: bangumi/server pkg/vars/staffs.go.json (type=2 anime)
    1: "原作",
    2: "导演",
    3: "脚本",
    4: "分镜",
    5: "演出",
    6: "音乐",
    7: "人物原案",
    8: "人物设定",
    9: "构图",
    10: "系列构成",
    11: "美术监督",
    13: "色彩设计",
    14: "总作画监督",
    15: "作画监督",
    16: "机械设定",
    17: "摄影监督",
    18: "监修",
    19: "道具设计",
    20: "原画",
    21: "第二原画",
    22: "动画检查",
    25: "背景美术",
    26: "色彩指定",
    28: "剪辑",
    29: "原案",
    35: "企画",
    36: "企划制作人",
    37: "制作管理",
    41: "系列监督",
    42: "製作",
    44: "音响监督",
    45: "音响",
    46: "音效",
    51: "补间动画",
    52: "执行制片人",
    54: "制片人",
    56: "制作进行",
    58: "总制片人",
    69: "CG 导演",
    72: "副导演",
    73: "OP・ED 分镜",
    74: "总导演",
    77: "动作作画监督",
    87: "动画制片人",
    88: "特效作画监督",
    89: "主演出",
    90: "作画监督助理",
    110: "总作画监督助理",
}

# ---------------------------------------------------------------------------
# Staff position code → Japanese label mapping
# ---------------------------------------------------------------------------
# Source: bangumi/server pkg/vars/staffs.go.json jp field (type=2 anime), 2026-04-25.
# Multiple JP aliases per code → first / most common term kept.
# Codes with empty jp field fall back to standard industry terminology.

ANIME_POSITION_LABELS_JP: dict[int, str] = {
    1: "原作",
    2: "監督",
    3: "シナリオ",
    4: "絵コンテ",
    5: "演出",
    6: "音楽",
    7: "キャラクター原案",
    8: "キャラクターデザイン",
    9: "レイアウト",
    10: "シリーズ構成",
    11: "美術監督",
    13: "色彩設計",
    14: "チーフ作画監督",
    15: "作画監督",
    16: "メカニックデザイン",
    17: "撮影監督",
    18: "監修",
    19: "プロップデザイン",
    20: "原画",
    21: "第二原画",
    22: "動画チェック",
    25: "背景",
    26: "色彩指定",
    28: "編集",
    29: "原案",
    35: "企画",
    36: "企画プロデューサー",
    37: "制作マネージャー",
    41: "シリーズ監督",
    42: "製作",
    44: "音響監督",
    45: "音響",
    46: "音響効果",
    51: "動画",
    52: "製作総指揮",
    54: "プロデューサー",
    56: "制作進行",
    58: "チーフプロデューサー",
    69: "CG監督",
    72: "助監督",
    73: "OP・ED絵コンテ",
    74: "総監督",
    77: "アクション作画監督",
    87: "アニメーションプロデューサー",
    88: "エフェクト作画監督",
    89: "チーフ演出",
    90: "作画監督補佐",
    110: "総作画監督補佐",
}

# ---------------------------------------------------------------------------
# Platform code → label mapping (Subject.platform)
# ---------------------------------------------------------------------------
# Source: bangumi/server pkg/vars/platform.go.json (type=2 anime), 2026-04-25.

ANIME_PLATFORM_LABELS: dict[int, str] = {
    0: "其他",
    1: "TV",
    2: "OVA",
    3: "剧场版",
    5: "WEB",
    2006: "动态漫画",
}

# ---------------------------------------------------------------------------
# Subject relation code → label mapping (SubjectRelation.relation)
# ---------------------------------------------------------------------------
# Source: bangumi/server pkg/vars/relations.go.json (type=2 anime), 2026-04-25.
# Used when subject-to-subject relations are scraped (SUBJECT_FULL_QUERY.relations).

ANIME_RELATION_LABELS: dict[int, str] = {
    1: "改编",
    2: "前传",
    3: "续集",
    4: "总集篇",
    5: "全集",
    6: "番外篇",
    7: "角色出演",
    8: "相同世界观",
    9: "不同世界观",
    10: "不同演绎",
    11: "衍生",
    12: "主线故事",
    14: "联动",
    99: "其他",
}

# ---------------------------------------------------------------------------
# Health-check query (minimal introspection)
# ---------------------------------------------------------------------------

INTROSPECTION_QUERY = """{
  __schema {
    queryType {
      name
    }
  }
}"""

# ---------------------------------------------------------------------------
# Shared fragments
# ---------------------------------------------------------------------------

_IMAGES_FIELDS = """
  small
  grid
  large
  medium
"""

_SUBJECT_FIELDS = """
  id
  name
  nameCN: name_cn
  summary
  infobox {
    key
    values {
      k
      v
    }
  }
  images {
    large
    medium
    small
    grid
  }
  airtime {
    date
    month
    weekday
    year
  }
  rating {
    score
    rank
    total
  }
  tags {
    name
    count
  }
"""

# Fields for SlimPerson (nested under SubjectRelatedPerson.person)
_SLIM_PERSON_FIELDS = """
  id
  name
  type
  career
  images {
    large
    medium
    small
    grid
  }
"""

# Fields for SlimCharacter (nested under SubjectRelatedCharacter.character)
_SLIM_CHARACTER_FIELDS = """
  id
  name
  images {
    large
    medium
    small
    grid
  }
"""

# ---------------------------------------------------------------------------
# Subject full query (single subject)
# ---------------------------------------------------------------------------
# Fetches subject + up to 50 persons + up to 50 characters + up to 50 relations.
# Callers should use this for per-subject verification; production backfills
# should prefer SUBJECT_BATCH_QUERY for throughput.


def SUBJECT_FULL_QUERY(subject_id: int) -> str:  # noqa: N802
    """Return a GraphQL document for a single subject with full nested data.

    Fetches persons(limit:50), characters(limit:50), relations(limit:50).
    For subjects with more nested entities, callers must paginate separately.

    Args:
        subject_id: bangumi subject integer ID.

    Returns:
        GraphQL query string (ready to send as the ``query`` field of the request body).
    """
    return f"""{{
  subject(id: {subject_id}) {{
    {_SUBJECT_FIELDS}
    persons(limit: 50, offset: 0) {{
      person {{
        {_SLIM_PERSON_FIELDS}
      }}
      position
    }}
    characters(limit: 50, offset: 0) {{
      character {{
        {_SLIM_CHARACTER_FIELDS}
      }}
      type
      order
    }}
    relations(limit: 50, offset: 0) {{
      subject {{
        id
        name
        type
      }}
      relation
      order
    }}
  }}
}}"""


# ---------------------------------------------------------------------------
# Batched subject query (N aliased queries in one POST)
# ---------------------------------------------------------------------------
# bangumi GraphQL supports allowBatchedQueries via aliased root fields.
# We alias each subject as s{id} to allow unambiguous mapping in the response.
# Batch size cap: 50 subjects per POST to keep responses < ~2MB.


def SUBJECT_BATCH_QUERY(subject_ids: list[int]) -> str:  # noqa: N802
    """Return a GraphQL document containing N aliased subject queries.

    Each subject is aliased as ``s{id}`` (e.g. ``s328: subject(id: 328) {...}``).
    The response ``data`` object will have keys ``"s328"``, ``"s329"``, etc.
    Callers must parse the aliases back to integer IDs — see
    ``BangumiGraphQLClient.fetch_subjects_batched()``.

    Batch size should not exceed 50 (recommended: 25) to keep response sizes
    manageable.  For very large subject sets, callers should chunk and call
    this function multiple times.

    Args:
        subject_ids: list of bangumi subject IDs (non-empty, len <= 50 recommended).

    Returns:
        GraphQL query string with N aliased root fields.

    Raises:
        ValueError: if subject_ids is empty.
    """
    if not subject_ids:
        raise ValueError("subject_ids must be non-empty")

    aliases = []
    for sid in subject_ids:
        aliases.append(f"""  s{sid}: subject(id: {sid}) {{
    {_SUBJECT_FIELDS}
    persons(limit: 50, offset: 0) {{
      person {{
        {_SLIM_PERSON_FIELDS}
      }}
      position
    }}
  }}""")

    body = "\n".join(aliases)
    return f"{{\n{body}\n}}"
