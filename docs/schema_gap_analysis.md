# Schema Gap Analysis

## Current Schema Coverage vs Application Requirements

### 1. Book Qualifiers (`Map<String, Object> qualifiers` in Book.java)

**What it is**: Special qualifiers like "New York Times Bestseller" or other search-specific attributes stored as a flexible key-value map.

**Current Schema Coverage**:
- ✅ **PARTIALLY COVERED** by `book_collections` + `book_collections_join`
  - NYT Bestseller → `collection_type='BESTSELLER_LIST'`, `source='NYT'`
  - Awards/Accolades → Could use `collection_type='AWARD'`
  - Special tags → Could use `collection_type='TAG'`
- ✅ **PARTIALLY COVERED** by `book_collections_join.raw_item_json` (JSONB)
  - Can store arbitrary qualifier data as JSON

**Gap**:
- ⚠️ Qualifiers that don't fit the "collection" paradigm need a dedicated table
- ⚠️ No direct mapping for arbitrary key-value pairs that aren't collections

**Recommendation**:
```sql
-- Option 1: Add to existing schema
create table if not exists book_qualifiers (
  id text primary key, -- NanoID
  book_id uuid not null references books(id) on delete cascade,
  qualifier_key text not null, -- 'nyt_bestseller', 'award_winner', etc.
  qualifier_value jsonb not null, -- Flexible JSON data
  source text, -- Where this qualifier came from
  valid_from date,
  valid_until date,
  created_at timestamptz not null default now(),
  unique(book_id, qualifier_key)
);
create index on book_qualifiers(book_id);
create index on book_qualifiers(qualifier_key);
```

**Alternative**: Store in `book_external_ids` or `book_raw_data` as JSONB, but less queryable.

---

### 2. Book Recommendation Cache (`List<String> cachedRecommendationIds` in Book.java)

**What it is**: List of book IDs that are recommended based on this book, cached to avoid repeated API calls.

**Current Schema Coverage**:
- ❌ **NOT COVERED** - No table for recommendation relationships

**Gap**:
- No way to persist recommendation relationships between books
- No caching of recommendation metadata (score, reason, date cached)

**Recommendation**:
```sql
-- Option 1: Dedicated recommendation cache table
create table if not exists book_recommendations (
  id text primary key, -- NanoID
  source_book_id uuid not null references books(id) on delete cascade,
  recommended_book_id uuid not null references books(id) on delete cascade,
  recommendation_source text, -- 'GOOGLE_SIMILAR', 'AUTHOR_MATCH', 'CATEGORY_MATCH'
  relevance_score float, -- Recommendation strength
  reason text, -- Why recommended
  cached_at timestamptz not null default now(),
  expires_at timestamptz, -- When to refresh
  created_at timestamptz not null default now(),
  unique(source_book_id, recommended_book_id, recommendation_source)
);
create index on book_recommendations(source_book_id);
create index on book_recommendations(recommended_book_id);
create index on book_recommendations(cached_at);
```

**Alternative**: Could use `book_collections` with `collection_type='RECOMMENDATION_SET'` but it's a stretch.

---

### 3. Cover Image Metadata (width, height, isHighResolution in Book.java)

**What it is**:
- `coverImageWidth`: Width in pixels
- `coverImageHeight`: Height in pixels
- `isCoverHighResolution`: Boolean flag for quality
- `preferredCoverSource`: Which source to use
- `fallbackCoverSource`: Backup source

**Current Schema Coverage**:
- ✅ **PARTIALLY COVERED** by `book_image_links`
  - Has `url` and `image_type` (small, thumbnail, large, etc.)
  - Has `source` field for provider
- ❌ **NOT COVERED**: No width/height/quality metadata

**Gap**:
- No pixel dimensions stored
- No quality/resolution indicators
- No preferred/fallback source logic

**Recommendation**:
```sql
-- Option 1: Extend book_image_links table
ALTER TABLE book_image_links
ADD COLUMN IF NOT EXISTS width integer,
ADD COLUMN IF NOT EXISTS height integer,
ADD COLUMN IF NOT EXISTS is_high_resolution boolean,
ADD COLUMN IF NOT EXISTS file_size_bytes integer,
ADD COLUMN IF NOT EXISTS format text; -- 'JPEG', 'PNG', 'WEBP'

-- Option 2: Separate metadata table (cleaner)
create table if not exists book_cover_metadata (
  id text primary key, -- NanoID
  book_id uuid not null references books(id) on delete cascade,
  s3_image_path text, -- Reference to our stored version
  preferred_source text, -- 'S3', 'GOOGLE_BOOKS', 'OPEN_LIBRARY'
  fallback_source text,
  width integer,
  height integer,
  is_high_resolution boolean,
  file_size_bytes integer,
  format text,
  color_profile text, -- 'RGB', 'CMYK'
  dominant_colors text[], -- For UI theming
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(book_id)
);
create index on book_cover_metadata(book_id);
```

---

## Summary Table

| Domain Model Field | Schema Coverage | Location | Gap |
|-------------------|-----------------|----------|-----|
| `qualifiers` | ⚠️ Partial | `book_collections` for some types | Need flexible key-value storage |
| `cachedRecommendationIds` | ❌ Missing | None | Need recommendation cache table |
| `coverImageWidth` | ❌ Missing | None | Need to extend `book_image_links` |
| `coverImageHeight` | ❌ Missing | None | Need to extend `book_image_links` |
| `isCoverHighResolution` | ❌ Missing | None | Need quality metadata |
| `preferredCoverSource` | ❌ Missing | None | Need source preferences |
| `fallbackCoverSource` | ❌ Missing | None | Need fallback logic |

## Recommended Actions

### Priority 1: Add Missing Tables
```sql
-- 1. Book Recommendations Cache (REQUIRED)
create table if not exists book_recommendations (
  id text primary key,
  source_book_id uuid not null references books(id) on delete cascade,
  recommended_book_id uuid not null references books(id) on delete cascade,
  recommendation_source text,
  relevance_score float,
  cached_at timestamptz not null default now(),
  expires_at timestamptz,
  unique(source_book_id, recommended_book_id, recommendation_source)
);

-- 2. Book Qualifiers (RECOMMENDED)
create table if not exists book_qualifiers (
  id text primary key,
  book_id uuid not null references books(id) on delete cascade,
  qualifier_key text not null,
  qualifier_value jsonb not null,
  source text,
  created_at timestamptz not null default now(),
  unique(book_id, qualifier_key)
);
```

### Priority 2: Extend Existing Tables
```sql
-- Extend book_image_links with metadata
ALTER TABLE book_image_links
ADD COLUMN IF NOT EXISTS width integer,
ADD COLUMN IF NOT EXISTS height integer,
ADD COLUMN IF NOT EXISTS is_high_resolution boolean,
ADD COLUMN IF NOT EXISTS file_size_bytes integer;
```

### Alternative: Use JSONB in Existing Tables

If you want to avoid new tables, you could:

1. **Store qualifiers in `book_raw_data`** or add a `metadata jsonb` column to `books`
2. **Store recommendations in `book_collections`** with special collection types
3. **Store cover metadata as JSONB in `book_image_links`**

But this makes querying harder and loses referential integrity.

## Decision Matrix

| Approach | Pros | Cons |
|----------|------|------|
| Add new tables | Clean, queryable, maintains integrity | More tables to manage |
| Extend existing tables | Fewer tables, logical grouping | May overload table purpose |
| Use JSONB columns | Flexible, no schema changes | Hard to query, no constraints |

## Recommended Approach

1. ✅ **CREATE** `book_recommendations` table (critical for cache)
2. ✅ **CREATE** `book_qualifiers` table (for flexible metadata)
3. ✅ **EXTEND** `book_image_links` with width/height/quality columns
4. ✅ **USE** `books.s3_image_path` as the preferred source indicator