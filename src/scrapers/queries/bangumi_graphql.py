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
    characters(limit: 50, offset: 0) {{
      character {{
        {_SLIM_CHARACTER_FIELDS}
      }}
      type
      order
    }}
  }}""")

    body = "\n".join(aliases)
    return f"{{\n{body}\n}}"


# ---------------------------------------------------------------------------
# Person query (single person)
# ---------------------------------------------------------------------------


def PERSON_QUERY(person_id: int) -> str:  # noqa: N802
    """Return a GraphQL document for fetching a single person's full detail.

    Fields mirror the /v0/persons/{id} REST response shape used by the
    existing BRONZE row builder (``_build_person_row`` in bangumi_main.py).

    Args:
        person_id: bangumi person integer ID.

    Returns:
        GraphQL query string.
    """
    return f"""{{
  person(id: {person_id}) {{
    id
    name
    type
    career
    summary
    infobox {{
      key
      values {{
        k
        v
      }}
    }}
    images {{
      large
      medium
      small
      grid
    }}
    locked: lock
  }}
}}"""


# ---------------------------------------------------------------------------
# Character query (single character)
# ---------------------------------------------------------------------------


def CHARACTER_QUERY(character_id: int) -> str:  # noqa: N802
    """Return a GraphQL document for fetching a single character's full detail.

    Fields mirror the /v0/characters/{id} REST response shape used by the
    existing BRONZE row builder (``_build_character_row`` in bangumi_main.py).

    Note: ``last_modified`` is absent from character responses in the v0 REST
    API and may also be absent from the GraphQL schema.  The adapter normalises
    this gracefully.

    Args:
        character_id: bangumi character integer ID.

    Returns:
        GraphQL query string.
    """
    return f"""{{
  character(id: {character_id}) {{
    id
    name
    summary
    infobox {{
      key
      values {{
        k
        v
      }}
    }}
    images {{
      large
      medium
      small
      grid
    }}
    locked: lock
  }}
}}"""
