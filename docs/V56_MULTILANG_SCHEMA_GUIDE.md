# v56 多言語スキーマ ガイド

## 概要

**v56** では SILVER layer に多言語対応カラムを追加。

| テーブル | カラム | 型 | 説明 |
|---------|--------|-----|------|
| `persons` | `name_ja` | TEXT | 日本語名（主） |
| `persons` | `name_en` | TEXT | 英語名（外国人用） |
| `persons` | `name_ko` | TEXT | 韓国語名（韓国籍者） |
| `persons` | `name_zh` | TEXT | 中国語名（中国籍者） |
| `persons` | `nationality` | TEXT[] (JSON) | 国籍コード配列 |
| `persons` | `years_active` | TEXT[] (JSON) | 活動期間 |
| `anime` | `country_of_origin` | TEXT | 制作国 |
| `studios` | `country_of_origin` | TEXT | スタジオ本社国 |

---

## Sample Queries

### 1. 多言語名を持つ人物を検索

```sql
SELECT
    id, name_ja, name_ko, name_zh,
    nationality
FROM persons
WHERE (name_ko IS NOT NULL AND name_ko != '')
   OR (name_zh IS NOT NULL AND name_zh != '')
ORDER BY name_ja;
```

**用途**: 韓国・中国の制作スタッフを検索する際に使用。

### 2. Nationality JSON 配列から特定国籍を抽出

```sql
-- SQLite: JSON1 拡張
SELECT
    id, name_ja, nationality,
    json_extract(nationality, '$[*]') AS nationalities
FROM persons
WHERE json_array_length(nationality) > 0
LIMIT 20;
```

**結果例**:
```
id: 'anilist:12345'
name_ja: '田中太郎'
nationality: '["JP"]'
nationalities: '["JP"]'

id: 'anilist:67890'
name_ja: '李明'
nationality: '["CN", "JP"]'
nationalities: '["CN", "JP"]'
```

### 3. DuckDB での複数国籍者を検索

```sql
-- DuckDB: JSON array unpacking
SELECT
    p.id,
    p.name_ja,
    json_array_length(p.nationality) AS num_nationalities,
    p.nationality
FROM persons p
WHERE json_array_length(p.nationality) > 1
ORDER BY p.name_ja;
```

### 4. 国別クレジット数（多言語対応）

```sql
-- 国籍別にクレジット集計
SELECT
    json_extract_scalar(p.nationality, '$[0]') AS primary_nationality,
    COUNT(*) AS credit_count,
    COUNT(DISTINCT p.id) AS person_count
FROM credits c
JOIN persons p ON c.person_id = p.id
WHERE json_array_length(p.nationality) > 0
GROUP BY json_extract_scalar(p.nationality, '$[0]')
ORDER BY credit_count DESC;
```

### 5. Years Active を活用したギャップ検出

```sql
-- 活動実績からギャップ期間を推定
SELECT
    p.id,
    p.name_ja,
    MIN(c.credit_year) AS first_credit_year,
    MAX(c.credit_year) AS latest_credit_year,
    MAX(c.credit_year) - MIN(c.credit_year) + 1 AS career_span,
    p.years_active AS recorded_years
FROM persons p
JOIN credits c ON p.id = c.person_id
WHERE c.credit_year IS NOT NULL
GROUP BY p.id, p.name_ja, p.years_active
HAVING MAX(c.credit_year) - MIN(c.credit_year) > 10
ORDER BY career_span DESC;
```

### 6. Country of Origin を使った制作国別分析

```sql
-- アニメの制作国別にスタッフを分類
SELECT
    a.country_of_origin,
    r.code AS role,
    COUNT(*) AS count,
    COUNT(DISTINCT c.person_id) AS person_count
FROM credits c
JOIN anime a ON c.anime_id = a.id
JOIN roles r ON c.role = r.code
WHERE a.country_of_origin IS NOT NULL
GROUP BY a.country_of_origin, r.code
ORDER BY a.country_of_origin, count DESC;
```

### 7. Studio Country of Origin を活用した国別スタジオ分析

```sql
-- スタジオの本社国と制作作品数
SELECT
    s.country_of_origin,
    s.name,
    COUNT(DISTINCT ast.anime_id) AS anime_produced
FROM studios s
LEFT JOIN anime_studios ast ON s.id = ast.studio_id
WHERE s.country_of_origin IS NOT NULL
GROUP BY s.country_of_origin, s.name
ORDER BY anime_produced DESC;
```

---

## Data Quality Notes

### Known Issues (v56)

1. **Name Mismatches**: 일부 CJK 이름이 잘못 입력된 경우 있음
   - 예: 한국 스タ프 → `name_ja` 에 한국어 입력 (잘못됨)
   - 해결: 재 scrape 후 `name_ja` 정정 필요

2. **Nationality Array**: 복수 국적자는 JSON array로 저장
   - 형식: `["JP", "KR"]` (동음자 정렬)
   - 예외: 확인되지 않은 경우 빈 배열 `[]`

3. **Years Active**: 아직 parsing 미완료
   - 계획: v57 에서 credit data 기반 자동 추정

---

## Migration Notes

v55 → v56 마이그레이션:
```sql
-- 신규 컬럼이 기존 데이터에 자동으로 추가됨 (ALTER TABLE)
-- 기본값: name_ko='', name_zh='', nationality='[]', years_active='[]'
```

---

## v57 Roadmap

- [ ] `anime.country_of_origin` 다수결로 `studios.country_of_origin` 채우기
- [ ] `title.native` 분기별로 `title_zh`/`title_ko` 저장
- [ ] `years_active` 자동 계산 및 저장

