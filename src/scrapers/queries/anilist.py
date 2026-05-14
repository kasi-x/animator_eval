"""AniList GraphQL query strings."""

ANIME_STAFF_QUERY = """
query ($id: Int, $staffPage: Int, $staffPerPage: Int, $charPage: Int, $charPerPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    idMal
    title { romaji english native }
    seasonYear
    season
    episodes
    averageScore
    meanScore
    coverImage { large extraLarge medium }
    bannerImage
    description
    format
    status
    startDate { year month day }
    endDate { year month day }
    duration
    source
    countryOfOrigin
    isLicensed
    isAdult
    hashtag
    siteUrl
    genres
    synonyms
    tags { id name rank description category isAdult }
    popularity
    favourites
    trailer { id site thumbnail }
    studios { edges { isMain node { id name isAnimationStudio favourites siteUrl } } }
    relations { edges { relationType node { id title { romaji } format } } }
    externalLinks { url site type }
    rankings { rank type format year season allTime context }
    airingSchedule { nodes { airingAt episode } }
    staff(page: $staffPage, perPage: $staffPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native alternative }
          image { large medium }
          dateOfBirth { year month day }
          dateOfDeath { year month day }
          age
          gender
          languageV2
          primaryOccupations
          yearsActive
          homeTown
          bloodType
          description
          favourites
          siteUrl
        }
      }
    }
    characters(page: $charPage, perPage: $charPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native alternative }
          image { large medium }
          description
          gender
          dateOfBirth { year month day }
          age
          bloodType
          favourites
          siteUrl
        }
        voiceActors {
          id
          name { full native alternative }
          image { large medium }
          dateOfBirth { year month day }
          dateOfDeath { year month day }
          age
          gender
          languageV2
          primaryOccupations
          yearsActive
          homeTown
          bloodType
          description
          favourites
          siteUrl
        }
      }
    }
  }
}
"""

ANIME_STAFF_MINIMAL_QUERY = """
query ($id: Int, $staffPage: Int, $staffPerPage: Int, $charPage: Int, $charPerPage: Int) {
  Media(id: $id, type: ANIME) {
    id
    staff(page: $staffPage, perPage: $staffPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node { id }
      }
    }
    characters(page: $charPage, perPage: $charPerPage) {
      pageInfo { hasNextPage }
      edges {
        role
        node {
          id
          name { full native alternative }
          image { large medium }
          description
          gender
          dateOfBirth { year month day }
          age
          bloodType
          favourites
          siteUrl
        }
        voiceActors(language: JAPANESE) { id }
      }
    }
  }
}
"""

# Master taxonomies. Both return the full collection in a single request — no
# pagination needed. Run once per scrape session as a bootstrap step.
GENRE_COLLECTION_QUERY = """
query {
  GenreCollection
}
"""

MEDIA_TAG_COLLECTION_QUERY = """
query {
  MediaTagCollection {
    id
    name
    description
    category
    isAdult
    isGeneralSpoiler
    isMediaSpoiler
  }
}
"""

# MediaTrend: per-Media historical popularity / trending / averageScore over time.
# Paginated. AniList returns one row per (mediaId, date) point; older anime have
# years of history. Display-only (H1: trending/popularity/averageScore must not
# enter scoring). Used for temporal popularity / soft-power analysis.
MEDIA_TREND_QUERY = """
query ($mediaId: Int, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    mediaTrends(mediaId: $mediaId, sort: DATE_DESC) {
      mediaId
      date
      trending
      averageScore
      popularity
      inProgress
      releasing
      episode
    }
  }
}
"""

# Page.airingSchedules: chronological global airing schedule. Paginated.
# Complements per-anime airingSchedule (already captured in ANIME_STAFF_QUERY)
# by providing a global timeline view (e.g. "what aired in 2018 Q3").
GLOBAL_AIRING_SCHEDULE_QUERY = """
query ($page: Int, $perPage: Int, $airingAt_greater: Int, $airingAt_lesser: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    airingSchedules(
      airingAt_greater: $airingAt_greater,
      airingAt_lesser:  $airingAt_lesser,
      sort: TIME
    ) {
      id
      mediaId
      airingAt
      timeUntilAiring
      episode
    }
  }
}
"""

PERSON_DETAILS_QUERY = """
query ($id: Int) {
  Staff(id: $id) {
    id
    name { full native alternative }
    image { large medium }
    dateOfBirth { year month day }
    dateOfDeath { year month day }
    age
    gender
    languageV2
    primaryOccupations
    yearsActive
    homeTown
    bloodType
    description
    favourites
    siteUrl
  }
}
"""

TOP_ANIME_QUERY = """
query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage total }
    media(type: ANIME, sort: $sort) {
      id
      idMal
      title { romaji english native }
      seasonYear
      season
      episodes
      averageScore
      meanScore
      coverImage { large extraLarge medium }
      bannerImage
      description
      format
      status
      startDate { year month day }
      endDate { year month day }
      duration
      source
      countryOfOrigin
      isLicensed
      isAdult
      hashtag
      siteUrl
      genres
      synonyms
      tags { id name rank description category isAdult }
      popularity
      favourites
      trailer { id site thumbnail }
      studios { edges { isMain node { id name isAnimationStudio favourites siteUrl } } }
      relations { edges { relationType node { id title { romaji } format } } }
      externalLinks { url site type }
      rankings { rank type format year season allTime context }
      airingSchedule { nodes { airingAt episode } }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Rebuild batch queries: bypass the 5000-result top-anime cap by fetching
# specific id sets via Page.media(id_in:). GraphQL aliases pull multiple
# staff/character pages in one round-trip, so 1 request covers up to 30
# anime + their first ~100 staff + ~75 characters.
# ---------------------------------------------------------------------------

_STAFF_NODE_FIELDS = """
  edges {
    role
    node {
      id name { full native alternative } image { large medium }
      dateOfBirth { year month day } dateOfDeath { year month day }
      age gender languageV2 primaryOccupations yearsActive homeTown
      bloodType description favourites siteUrl
    }
  }
"""

_CHAR_NODE_FIELDS = """
  edges {
    role
    node {
      id name { full native alternative } image { large medium }
      description gender dateOfBirth { year month day }
      age bloodType favourites siteUrl
    }
    voiceActors {
      id name { full native alternative } image { large medium }
      dateOfBirth { year month day } dateOfDeath { year month day }
      age gender languageV2 primaryOccupations yearsActive homeTown
      bloodType description favourites siteUrl
    }
  }
"""


def _staff_alias(page: int) -> str:
    return (
        f"staff_p{page}: staff(page: {page}, perPage: 25) "
        f"{{ pageInfo {{ hasNextPage }} {_STAFF_NODE_FIELDS} }}"
    )


def _char_alias(page: int) -> str:
    return (
        f"characters_p{page}: characters(page: {page}, perPage: 25) "
        f"{{ pageInfo {{ hasNextPage }} {_CHAR_NODE_FIELDS} }}"
    )


def build_rebuild_batch_query(staff_pages: int = 4, char_pages: int = 3) -> str:
    """Multi-anime batch query (id_in) with staff/character page aliases.

    Returns a query taking $ids: [Int]. Page.perPage caps batch size at 50.
    """
    staff_aliases = "\n      ".join(_staff_alias(p) for p in range(1, staff_pages + 1))
    char_aliases = "\n      ".join(_char_alias(p) for p in range(1, char_pages + 1))
    return f"""
query ($ids: [Int]) {{
  Page(perPage: 50) {{
    pageInfo {{ total }}
    media(id_in: $ids, type: ANIME) {{
      id idMal
      title {{ romaji english native }}
      seasonYear season episodes averageScore meanScore
      coverImage {{ large extraLarge medium }} bannerImage description
      format status startDate {{ year month day }} endDate {{ year month day }}
      duration source countryOfOrigin isLicensed isAdult hashtag siteUrl
      genres synonyms tags {{ id name rank description category isAdult }}
      popularity favourites trailer {{ id site thumbnail }}
      studios {{ edges {{ isMain node {{ id name isAnimationStudio favourites siteUrl }} }} }}
      relations {{ edges {{ relationType node {{ id title {{ romaji }} format }} }} }}
      externalLinks {{ url site type }}
      rankings {{ rank type format year season allTime context }}
      airingSchedule {{ nodes {{ airingAt episode }} }}
      {staff_aliases}
      {char_aliases}
    }}
  }}
}}
"""


def build_anime_deep_pages_query(
    staff_start: int,
    staff_count: int,
    char_start: int,
    char_count: int,
) -> str:
    """Single-anime deep-page query (used for second-pass fetches when
    1-pass hasNextPage indicates more pages remain).

    Returns query taking $id: Int.
    """
    parts = []
    for i in range(staff_count):
        parts.append(_staff_alias(staff_start + i))
    for i in range(char_count):
        parts.append(_char_alias(char_start + i))
    body = "\n      ".join(parts)
    return f"""
query ($id: Int) {{
  Media(id: $id, type: ANIME) {{
    id
    {body}
  }}
}}
"""
