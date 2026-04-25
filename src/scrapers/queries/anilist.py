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
    tags { name rank }
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
          age
          gender
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
        voiceActors(language: JAPANESE) {
          id
          name { full native alternative }
          image { large medium }
          dateOfBirth { year month day }
          age
          gender
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

PERSON_DETAILS_QUERY = """
query ($id: Int) {
  Staff(id: $id) {
    id
    name { full native alternative }
    image { large medium }
    dateOfBirth { year month day }
    age
    gender
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
      tags { name rank }
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
