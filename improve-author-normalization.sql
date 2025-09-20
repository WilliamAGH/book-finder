-- ============================================================================
-- IMPROVED AUTHOR NAME NORMALIZATION
-- Handles: accents, punctuation, suffixes, corporate authors, special cases
-- ============================================================================

-- Drop old function if exists
DROP FUNCTION IF EXISTS normalize_author_name(text);

-- Create comprehensive author name normalization function
CREATE OR REPLACE FUNCTION normalize_author_name(author_name text)
RETURNS text AS $$
DECLARE
  normalized text;
BEGIN
  -- Return NULL for NULL input
  IF author_name IS NULL OR trim(author_name) = '' THEN
    RETURN NULL;
  END IF;

  normalized := author_name;

  -- Step 1: Convert to lowercase
  normalized := lower(normalized);

  -- Step 2: Remove accents and diacritics
  normalized := translate(normalized,
    'àáäâãåèéëêìíïîòóöôõùúüûñçğışÀÁÄÂÃÅÈÉËÊÌÍÏÎÒÓÖÔÕÙÚÜÛÑÇĞİŞ',
    'aaaaaaeeeeiiiiooooouuuuncgisAAAAAEEEEIIIIOOOOOUUUUNCGIS');

  -- Step 3: Handle common suffixes and titles (move to end for consistency)
  -- Remove periods from common abbreviations first
  normalized := regexp_replace(normalized, '\s+jr\.?(?:\s|$)', ' jr', 'gi');
  normalized := regexp_replace(normalized, '\s+sr\.?(?:\s|$)', ' sr', 'gi');
  normalized := regexp_replace(normalized, '\s+ph\.?d\.?(?:\s|$)', ' phd', 'gi');
  normalized := regexp_replace(normalized, '\s+m\.?d\.?(?:\s|$)', ' md', 'gi');
  normalized := regexp_replace(normalized, '\s+esq\.?(?:\s|$)', ' esq', 'gi');

  -- Handle Roman numerals (II, III, IV, etc.)
  normalized := regexp_replace(normalized, '\s+([ivxlcdm]+)\.?(?:\s|$)', ' \1', 'gi');

  -- Step 4: Remove possessives
  normalized := regexp_replace(normalized, '''s(?:\s|$)', '', 'g');
  normalized := regexp_replace(normalized, '['']s(?:\s|$)', '', 'g');

  -- Step 5: Handle corporate authors and special cases
  -- Remove common corporate suffixes
  normalized := regexp_replace(normalized, '\s+inc\.?(?:\s|$)', ' inc', 'gi');
  normalized := regexp_replace(normalized, '\s+corp\.?(?:\s|$)', ' corp', 'gi');
  normalized := regexp_replace(normalized, '\s+ltd\.?(?:\s|$)', ' ltd', 'gi');
  normalized := regexp_replace(normalized, '\s+llc\.?(?:\s|$)', ' llc', 'gi');
  normalized := regexp_replace(normalized, '\s+co\.?(?:\s|$)', ' co', 'gi');

  -- Remove editorial annotations
  normalized := regexp_replace(normalized, '\[from old catalog\]', '', 'gi');
  normalized := regexp_replace(normalized, '\(editor\)', '', 'gi');
  normalized := regexp_replace(normalized, '\(author\)', '', 'gi');
  normalized := regexp_replace(normalized, '\(translator\)', '', 'gi');
  normalized := regexp_replace(normalized, '\(illustrator\)', '', 'gi');
  normalized := regexp_replace(normalized, '\(ed\.\)', '', 'gi');
  normalized := regexp_replace(normalized, '\(eds\.\)', '', 'gi');

  -- Step 6: Normalize punctuation
  -- Replace multiple spaces, hyphens, underscores with single space
  normalized := regexp_replace(normalized, '[-_]+', ' ', 'g');
  -- Remove all other punctuation except spaces
  normalized := regexp_replace(normalized, '[^a-z0-9\s]', ' ', 'g');
  -- Collapse multiple spaces
  normalized := regexp_replace(normalized, '\s+', ' ', 'g');

  -- Step 7: Handle specific patterns
  -- "Last, First" → "first last"
  IF normalized ~ '^[a-z]+,\s+[a-z]+' THEN
    normalized := regexp_replace(normalized, '^([a-z]+),\s+([a-z]+)', '\2 \1', 'g');
  END IF;

  -- Step 8: Final cleanup
  normalized := trim(normalized);

  RETURN normalized;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Test the function with various cases
SELECT
  author_name,
  normalize_author_name(author_name) as normalized
FROM (VALUES
  ('Stephen King'),
  ('Stephen King Jr.'),
  ('STEPHEN KING'),
  ('King, Stephen'),
  ('José García Márquez'),
  ('J.K. Rowling'),
  ('J. K. Rowling'),
  ('O''Reilly Media, Inc.'),
  ('United States. Congress. House. [from old catalog]'),
  ('William Shakespeare III'),
  ('Dr. Martin Luther King Jr.'),
  ('Mary Smith-Jones'),
  ('François Müller'),
  ('Time Inc.'),
  (NULL),
  (''),
  ('   John    Doe   ')
) AS t(author_name);

-- ============================================================================
-- Update existing authors with improved normalization
-- ============================================================================

-- First, let's see what changes would be made
WITH author_changes AS (
  SELECT
    id,
    name,
    normalized_name as old_normalized,
    normalize_author_name(name) as new_normalized
  FROM authors
  WHERE normalized_name != normalize_author_name(name)
    OR (normalized_name IS NULL AND normalize_author_name(name) IS NOT NULL)
)
SELECT
  name,
  old_normalized,
  new_normalized,
  CASE
    WHEN old_normalized = new_normalized THEN 'No change'
    WHEN old_normalized IS NULL THEN 'Was NULL'
    ELSE 'Changed'
  END as status
FROM author_changes
LIMIT 20;

-- Apply the improved normalization to all authors
UPDATE authors
SET normalized_name = normalize_author_name(name),
    updated_at = NOW()
WHERE normalized_name != normalize_author_name(name)
   OR (normalized_name IS NULL AND normalize_author_name(name) IS NOT NULL);

-- ============================================================================
-- Find potential duplicates after normalization
-- ============================================================================

WITH normalized_groups AS (
  SELECT
    normalize_author_name(name) as normalized,
    array_agg(DISTINCT name ORDER BY name) as variations,
    array_agg(DISTINCT id ORDER BY id) as author_ids,
    COUNT(DISTINCT id) as count
  FROM authors
  WHERE name IS NOT NULL
  GROUP BY normalize_author_name(name)
  HAVING COUNT(DISTINCT id) > 1
)
SELECT
  normalized,
  count,
  variations
FROM normalized_groups
ORDER BY count DESC
LIMIT 20;

-- ============================================================================
-- Create function to merge duplicate authors
-- ============================================================================

CREATE OR REPLACE FUNCTION merge_duplicate_authors()
RETURNS TABLE (
  groups_found integer,
  authors_merged integer
) AS $$
DECLARE
  rec RECORD;
  primary_author_id text;
  merge_count integer := 0;
  group_count integer := 0;
BEGIN
  -- Find groups of authors with same normalized name
  FOR rec IN
    SELECT
      normalize_author_name(name) as normalized,
      array_agg(id ORDER BY created_at, id) as author_ids
    FROM authors
    WHERE name IS NOT NULL
    GROUP BY normalize_author_name(name)
    HAVING COUNT(*) > 1
  LOOP
    group_count := group_count + 1;
    primary_author_id := rec.author_ids[1];

    -- Update all book references to point to primary author
    UPDATE book_authors_join
    SET author_id = primary_author_id
    WHERE author_id = ANY(rec.author_ids[2:])
      AND NOT EXISTS (
        SELECT 1 FROM book_authors_join baj2
        WHERE baj2.book_id = book_authors_join.book_id
          AND baj2.author_id = primary_author_id
      );

    -- Merge author external IDs
    UPDATE author_external_ids
    SET author_id = primary_author_id
    WHERE author_id = ANY(rec.author_ids[2:])
      AND NOT EXISTS (
        SELECT 1 FROM author_external_ids aei2
        WHERE aei2.source = author_external_ids.source
          AND aei2.external_id = author_external_ids.external_id
          AND aei2.author_id = primary_author_id
      );

    -- Delete duplicate book-author entries
    DELETE FROM book_authors_join
    WHERE author_id = ANY(rec.author_ids[2:]);

    -- Delete duplicate authors (keep primary)
    DELETE FROM authors
    WHERE id = ANY(rec.author_ids[2:]);

    merge_count := merge_count + array_length(rec.author_ids, 1) - 1;
  END LOOP;

  RETURN QUERY SELECT group_count, merge_count;
END;
$$ LANGUAGE plpgsql;

-- Comment on functions
COMMENT ON FUNCTION normalize_author_name IS 'Comprehensive author name normalization: handles accents, punctuation, suffixes, corporate names';
COMMENT ON FUNCTION merge_duplicate_authors IS 'Merges authors with identical normalized names, preserving relationships';