-- ============================================================================
-- TEST WORK CLUSTERING SYSTEM WITH SYNTHETIC DATA
-- ============================================================================

-- Clean up any previous test data
DELETE FROM work_cluster_members WHERE book_id IN (
  SELECT id FROM books WHERE title LIKE 'TEST BOOK:%'
);
DELETE FROM work_clusters WHERE canonical_title LIKE 'TEST BOOK:%';
DELETE FROM book_external_ids WHERE book_id IN (
  SELECT id FROM books WHERE title LIKE 'TEST BOOK:%'
);
DELETE FROM books WHERE title LIKE 'TEST BOOK:%';

-- ============================================================================
-- Test 1: ISBN Prefix Clustering
-- Create multiple editions of "Harry Potter" with same ISBN prefix
-- ============================================================================

-- Insert test books with same ISBN prefix (first 11 digits)
INSERT INTO books (id, title, subtitle, isbn13, isbn10, publisher, published_date, slug, language)
VALUES
  -- Same work, different editions (ISBN prefix: 97805451037)
  ('00199660-0001-7000-8000-000000000001'::uuid, 'TEST BOOK: Harry Potter and the Philosophers Stone', 'Edition 1', '9780545103770', '0545103770', 'Scholastic', '2010-06-01', 'test-hp-1', 'en'),
  ('00199660-0002-7000-8000-000000000002'::uuid, 'TEST BOOK: Harry Potter and the Philosophers Stone', 'Edition 2', '9780545103771', '0545103771', 'Scholastic', '2011-06-01', 'test-hp-2', 'en'),
  ('00199660-0003-7000-8000-000000000003'::uuid, 'TEST BOOK: Harry Potter and the Philosophers Stone', 'Edition 3', '9780545103772', '0545103772', 'Scholastic', '2012-06-01', 'test-hp-3', 'en'),

  -- Another work with same prefix
  ('00199660-0004-7000-8000-000000000004'::uuid, 'TEST BOOK: Harry Potter and the Chamber of Secrets', 'Edition 1', '9780545103773', '0545103773', 'Scholastic', '2010-07-01', 'test-hp-cos-1', 'en'),
  ('00199660-0005-7000-8000-000000000005'::uuid, 'TEST BOOK: Harry Potter and the Chamber of Secrets', 'Edition 2', '9780545103774', '0545103774', 'Scholastic', '2011-07-01', 'test-hp-cos-2', 'en'),

  -- Different publisher/prefix (ISBN prefix: 97814088556)
  ('00199660-0006-7000-8000-000000000006'::uuid, 'TEST BOOK: Harry Potter and the Philosophers Stone', 'UK Edition', '9781408855560', '1408855569', 'Bloomsbury', '2014-09-01', 'test-hp-uk-1', 'en'),
  ('00199660-0007-7000-8000-000000000007'::uuid, 'TEST BOOK: Harry Potter and the Philosophers Stone', 'UK Special', '9781408855561', '1408855561', 'Bloomsbury', '2015-09-01', 'test-hp-uk-2', 'en');

-- ============================================================================
-- Test 2: Google Canonical Link Clustering
-- ============================================================================

-- Add external IDs with canonical links for clustering
INSERT INTO book_external_ids (
  id, book_id, source, external_id, canonical_volume_link
) VALUES
  -- These share the same canonical Google Books ID
  ('test-ext-001', '00199660-0001-7000-8000-000000000001'::uuid, 'GOOGLE_BOOKS', 'test_gb_001', 'https://books.google.com/books?id=HP_TEST_001&hl=en'),
  ('test-ext-002', '00199660-0002-7000-8000-000000000002'::uuid, 'GOOGLE_BOOKS', 'test_gb_002', 'https://books.google.com/books?id=HP_TEST_001&hl=en'),
  ('test-ext-003', '00199660-0003-7000-8000-000000000003'::uuid, 'GOOGLE_BOOKS', 'test_gb_003', 'https://books.google.com/books?id=HP_TEST_001&hl=en'),

  -- Different canonical ID for Chamber of Secrets
  ('test-ext-004', '00199660-0004-7000-8000-000000000004'::uuid, 'GOOGLE_BOOKS', 'test_gb_004', 'https://books.google.com/books?id=HP_COS_001&hl=en'),
  ('test-ext-005', '00199660-0005-7000-8000-000000000005'::uuid, 'GOOGLE_BOOKS', 'test_gb_005', 'https://books.google.com/books?id=HP_COS_001&hl=en');

-- ============================================================================
-- Test 3: Mixed Work IDs
-- ============================================================================

-- Add some work identifiers to test comprehensive clustering
INSERT INTO book_external_ids (
  id, book_id, source, external_id, openlibrary_work_id, goodreads_work_id
) VALUES
  ('test-ext-006', '00199660-0006-7000-8000-000000000006'::uuid, 'OPENLIBRARY', 'OL_TEST_001', '/works/OL82548W', '1234567'),
  ('test-ext-007', '00199660-0007-7000-8000-000000000007'::uuid, 'OPENLIBRARY', 'OL_TEST_002', '/works/OL82548W', '1234567');

-- ============================================================================
-- Run clustering functions
-- ============================================================================

-- Show books before clustering
SELECT 'BEFORE CLUSTERING:' as phase;
SELECT id, title, subtitle, isbn13, publisher
FROM books
WHERE title LIKE 'TEST BOOK:%'
ORDER BY title, published_date;

-- Run ISBN-based clustering
SELECT 'ISBN CLUSTERING RESULTS:' as operation;
SELECT * FROM cluster_books_by_isbn();

-- Run Google canonical clustering
SELECT 'GOOGLE CLUSTERING RESULTS:' as operation;
SELECT * FROM cluster_books_by_google_canonical();

-- ============================================================================
-- Verify results
-- ============================================================================

-- Check clustering statistics
SELECT 'CLUSTERING STATISTICS:' as report;
SELECT * FROM get_clustering_stats();

-- Show created clusters
SELECT 'CREATED CLUSTERS:' as report;
SELECT
  wc.id,
  wc.isbn_prefix,
  wc.google_canonical_id,
  wc.canonical_title,
  wc.cluster_method,
  wc.member_count,
  wc.confidence_score
FROM work_clusters wc
WHERE wc.canonical_title LIKE 'TEST BOOK:%'
ORDER BY wc.canonical_title;

-- Show cluster memberships
SELECT 'CLUSTER MEMBERSHIPS:' as report;
SELECT
  wc.canonical_title as work_title,
  b.title as book_title,
  b.subtitle as edition,
  b.isbn13,
  wcm.is_primary,
  wcm.confidence,
  wcm.join_reason
FROM work_cluster_members wcm
JOIN work_clusters wc ON wc.id = wcm.cluster_id
JOIN books b ON b.id = wcm.book_id
WHERE b.title LIKE 'TEST BOOK:%'
ORDER BY wc.canonical_title, wcm.is_primary DESC, b.published_date;

-- Test the get_book_editions function
SELECT 'EDITIONS FOR FIRST TEST BOOK:' as report;
SELECT * FROM get_book_editions('00199660-0001-7000-8000-000000000001'::uuid);

-- Check the view
SELECT 'BOOK WORK EDITIONS VIEW:' as report;
SELECT
  b1.title as book_title,
  b1.isbn13,
  work_title,
  related_title,
  related_isbn13,
  cluster_method
FROM book_work_editions b1
JOIN books b ON b.id = b1.book_id
WHERE b.title LIKE 'TEST BOOK:%'
LIMIT 10;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 'CLUSTERING SUMMARY:' as report;
WITH summary AS (
  SELECT
    COUNT(DISTINCT wc.id) as total_clusters,
    COUNT(DISTINCT wcm.book_id) as total_clustered_books,
    COUNT(DISTINCT CASE WHEN wc.cluster_method = 'ISBN_PREFIX' THEN wc.id END) as isbn_clusters,
    COUNT(DISTINCT CASE WHEN wc.cluster_method = 'GOOGLE_CANONICAL' THEN wc.id END) as google_clusters,
    STRING_AGG(DISTINCT wc.canonical_title, ', ' ORDER BY wc.canonical_title) as clustered_works
  FROM work_clusters wc
  JOIN work_cluster_members wcm ON wcm.cluster_id = wc.id
  JOIN books b ON b.id = wcm.book_id
  WHERE b.title LIKE 'TEST BOOK:%'
)
SELECT * FROM summary;

-- ============================================================================
-- Cleanup (commented out - run manually if needed)
-- ============================================================================

-- DELETE FROM work_cluster_members WHERE book_id IN (
--   SELECT id FROM books WHERE title LIKE 'TEST BOOK:%'
-- );
-- DELETE FROM work_clusters WHERE canonical_title LIKE 'TEST BOOK:%';
-- DELETE FROM book_external_ids WHERE book_id IN (
--   SELECT id FROM books WHERE title LIKE 'TEST BOOK:%'
-- );
-- DELETE FROM books WHERE title LIKE 'TEST BOOK:%';