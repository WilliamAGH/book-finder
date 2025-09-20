-- Fix clustering functions to handle partial unique indexes properly

-- Drop existing functions
DROP FUNCTION IF EXISTS cluster_books_by_isbn();
DROP FUNCTION IF EXISTS cluster_books_by_google_canonical();

-- Recreate ISBN clustering function with proper conflict handling
CREATE OR REPLACE FUNCTION cluster_books_by_isbn()
RETURNS TABLE (clusters_created integer, books_clustered integer) AS $$
DECLARE
  rec RECORD;
  cluster_uuid uuid;
  clusters_count integer := 0;
  books_count integer := 0;
  book_uuid uuid;
  existing_cluster uuid;
BEGIN
  FOR rec IN
    SELECT
      extract_isbn_work_prefix(isbn13) as prefix,
      array_agg(id ORDER BY published_date DESC NULLS LAST) as book_ids,
      min(title) as canonical_title,
      count(*) as book_count
    FROM books
    WHERE isbn13 IS NOT NULL
    GROUP BY extract_isbn_work_prefix(isbn13)
    HAVING count(*) > 1 AND extract_isbn_work_prefix(isbn13) IS NOT NULL
  LOOP
    -- Check if cluster exists
    SELECT id INTO existing_cluster
    FROM work_clusters
    WHERE isbn_prefix = rec.prefix;

    IF existing_cluster IS NULL THEN
      -- Create new cluster
      INSERT INTO work_clusters (isbn_prefix, canonical_title, confidence_score, cluster_method, member_count)
      VALUES (rec.prefix, rec.canonical_title, 0.9, 'ISBN_PREFIX', rec.book_count)
      RETURNING id INTO cluster_uuid;

      clusters_count := clusters_count + 1;
    ELSE
      -- Update existing cluster
      UPDATE work_clusters
      SET canonical_title = rec.canonical_title,
          member_count = rec.book_count,
          updated_at = NOW()
      WHERE id = existing_cluster
      RETURNING id INTO cluster_uuid;
    END IF;

    -- Clear existing members for this cluster
    DELETE FROM work_cluster_members WHERE cluster_id = cluster_uuid;

    -- Add members (first one is primary)
    FOR i IN 1..array_length(rec.book_ids, 1) LOOP
      book_uuid := rec.book_ids[i];

      INSERT INTO work_cluster_members (cluster_id, book_id, is_primary, confidence, join_reason)
      VALUES (cluster_uuid, book_uuid, (i = 1), 0.9, 'ISBN_PREFIX')
      ON CONFLICT (cluster_id, book_id) DO UPDATE SET
        is_primary = EXCLUDED.is_primary,
        confidence = EXCLUDED.confidence,
        join_reason = EXCLUDED.join_reason;

      books_count := books_count + 1;
    END LOOP;
  END LOOP;

  RETURN QUERY SELECT clusters_count, books_count;
END;
$$ LANGUAGE plpgsql;

-- Recreate Google clustering function with proper conflict handling
CREATE OR REPLACE FUNCTION cluster_books_by_google_canonical()
RETURNS TABLE (clusters_created integer, books_clustered integer) AS $$
DECLARE
  rec RECORD;
  cluster_uuid uuid;
  clusters_count integer := 0;
  books_count integer := 0;
  google_id text;
  existing_cluster uuid;
BEGIN
  FOR rec IN
    SELECT
      canonical_volume_link,
      array_agg(DISTINCT bei.book_id) as book_ids,
      min(b.title) as canonical_title,
      count(DISTINCT bei.book_id) as book_count
    FROM book_external_ids bei
    JOIN books b ON b.id = bei.book_id
    WHERE canonical_volume_link IS NOT NULL
      AND source = 'GOOGLE_BOOKS'
    GROUP BY canonical_volume_link
    HAVING count(DISTINCT bei.book_id) > 1
  LOOP
    -- Extract Google Books ID from canonical link
    google_id := regexp_replace(rec.canonical_volume_link, '.*[?&]id=([^&]+).*', '\1');

    IF google_id IS NOT NULL AND google_id != rec.canonical_volume_link THEN
      -- Check if cluster exists
      SELECT id INTO existing_cluster
      FROM work_clusters
      WHERE google_canonical_id = google_id;

      IF existing_cluster IS NULL THEN
        -- Create new cluster
        INSERT INTO work_clusters (google_canonical_id, canonical_title, confidence_score, cluster_method, member_count)
        VALUES (google_id, rec.canonical_title, 0.85, 'GOOGLE_CANONICAL', rec.book_count)
        RETURNING id INTO cluster_uuid;

        clusters_count := clusters_count + 1;
      ELSE
        -- Update existing cluster
        UPDATE work_clusters
        SET canonical_title = COALESCE(work_clusters.canonical_title, rec.canonical_title),
            member_count = rec.book_count,
            updated_at = NOW()
        WHERE id = existing_cluster
        RETURNING id INTO cluster_uuid;
      END IF;

      -- Clear existing members for this cluster
      DELETE FROM work_cluster_members WHERE cluster_id = cluster_uuid;

      -- Add members
      FOR i IN 1..array_length(rec.book_ids, 1) LOOP
        INSERT INTO work_cluster_members (cluster_id, book_id, is_primary, confidence, join_reason)
        VALUES (cluster_uuid, rec.book_ids[i], (i = 1), 0.85, 'GOOGLE_CANONICAL')
        ON CONFLICT (cluster_id, book_id) DO UPDATE SET
          is_primary = EXCLUDED.is_primary,
          confidence = EXCLUDED.confidence,
          join_reason = EXCLUDED.join_reason;

        books_count := books_count + 1;
      END LOOP;
    END IF;
  END LOOP;

  RETURN QUERY SELECT clusters_count, books_count;
END;
$$ LANGUAGE plpgsql;