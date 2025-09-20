-- ============================================================================
-- REALISTIC CLUSTERING TEST WITH KNOWN ISBNs
-- Testing with actual ISBN patterns from real books to verify clustering logic
-- ============================================================================

-- Clean up any previous test data
DELETE FROM work_cluster_members WHERE book_id IN (
  SELECT id FROM books WHERE title LIKE 'CLUSTER_TEST:%'
);
DELETE FROM work_clusters WHERE canonical_title LIKE 'CLUSTER_TEST:%';
DELETE FROM book_external_ids WHERE book_id IN (
  SELECT id FROM books WHERE title LIKE 'CLUSTER_TEST:%'
);
DELETE FROM books WHERE title LIKE 'CLUSTER_TEST:%';

-- ============================================================================
-- Test Case 1: Stephen King - Different Books (Should NOT cluster by ISBN)
-- ============================================================================

INSERT INTO books (id, title, isbn13, isbn10, publisher, published_date, slug)
VALUES
  -- The Shining - Different editions, same work
  ('00199670-0001-7000-8000-000000000001'::uuid,
   'CLUSTER_TEST: The Shining',
   '9780307743657', -- Anchor Books 2012 edition
   '0307743659',
   'Anchor Books',
   '2012-06-01',
   'test-shining-1'),

  ('00199670-0002-7000-8000-000000000002'::uuid,
   'CLUSTER_TEST: The Shining',
   '9780307743664', -- Same publisher, different format (trade paperback)
   '0307743667',
   'Anchor Books',
   '2013-01-01',
   'test-shining-2'),

  ('00199670-0003-7000-8000-000000000003'::uuid,
   'CLUSTER_TEST: The Shining',
   '9781444720723', -- Hodder UK edition (different publisher)
   '1444720724',
   'Hodder & Stoughton',
   '2011-11-01',
   'test-shining-3'),

  -- Doctor Sleep - DIFFERENT book by same author
  ('00199670-0004-7000-8000-000000000004'::uuid,
   'CLUSTER_TEST: Doctor Sleep',
   '9781476727653', -- Scribner hardcover
   '1476727651',
   'Scribner',
   '2013-09-24',
   'test-doctor-sleep-1'),

  ('00199670-0005-7000-8000-000000000005'::uuid,
   'CLUSTER_TEST: Doctor Sleep',
   '9781476727660', -- Scribner paperback
   '147672766X',
   'Scribner',
   '2014-04-01',
   'test-doctor-sleep-2');

-- ============================================================================
-- Test Case 2: J.K. Rowling - Harry Potter (Same work, different editions)
-- ============================================================================

INSERT INTO books (id, title, isbn13, isbn10, publisher, published_date, slug)
VALUES
  -- Harry Potter and the Philosopher's Stone - US editions (Scholastic)
  ('00199670-0006-7000-8000-000000000006'::uuid,
   'CLUSTER_TEST: Harry Potter and the Sorcerers Stone',
   '9780439708180', -- Scholastic paperback
   '0439708184',
   'Scholastic',
   '1999-09-01',
   'test-hp1-us-1'),

  ('00199670-0007-7000-8000-000000000007'::uuid,
   'CLUSTER_TEST: Harry Potter and the Sorcerers Stone',
   '9780439708197', -- Scholastic hardcover
   '0439708192',
   'Scholastic',
   '1998-09-01',
   'test-hp1-us-2'),

  -- UK editions (Bloomsbury) - same work, different title
  ('00199670-0008-7000-8000-000000000008'::uuid,
   'CLUSTER_TEST: Harry Potter and the Philosophers Stone',
   '9780747532699', -- Bloomsbury UK paperback
   '0747532699',
   'Bloomsbury',
   '1997-06-26',
   'test-hp1-uk-1'),

  ('00199670-0009-7000-8000-000000000009'::uuid,
   'CLUSTER_TEST: Harry Potter and the Philosophers Stone',
   '9780747532743', -- Bloomsbury UK hardcover
   '0747532745',
   'Bloomsbury',
   '1997-06-26',
   'test-hp1-uk-2'),

  -- Harry Potter and the Chamber of Secrets - DIFFERENT book in series
  ('00199670-0010-7000-8000-000000000010'::uuid,
   'CLUSTER_TEST: Harry Potter and the Chamber of Secrets',
   '9780439064866', -- Scholastic
   '0439064864',
   'Scholastic',
   '1999-06-01',
   'test-hp2-us-1'),

  ('00199670-0011-7000-8000-000000000011'::uuid,
   'CLUSTER_TEST: Harry Potter and the Chamber of Secrets',
   '9780747538493', -- Bloomsbury
   '0747538492',
   'Bloomsbury',
   '1998-07-02',
   'test-hp2-uk-1');

-- ============================================================================
-- Test Case 3: Format variations (Hardcover vs Paperback of SAME edition)
-- ============================================================================

INSERT INTO books (id, title, isbn13, isbn10, publisher, published_date, slug)
VALUES
  -- The Hunger Games - Same publisher, same year, different formats
  ('00199670-0012-7000-8000-000000000012'::uuid,
   'CLUSTER_TEST: The Hunger Games',
   '9780439023481', -- Scholastic hardcover
   '0439023483',
   'Scholastic Press',
   '2008-09-14',
   'test-hunger-hc'),

  ('00199670-0013-7000-8000-000000000013'::uuid,
   'CLUSTER_TEST: The Hunger Games',
   '9780439023528', -- Scholastic paperback (same ISBN prefix!)
   '0439023521',
   'Scholastic Press',
   '2010-07-03',
   'test-hunger-pb'),

  -- Different publisher edition
  ('00199670-0014-7000-8000-000000000014'::uuid,
   'CLUSTER_TEST: The Hunger Games',
   '9781407132082', -- Scholastic UK
   '1407132083',
   'Scholastic UK',
   '2011-01-01',
   'test-hunger-uk');

-- ============================================================================
-- Add Google canonical links to test that clustering method
-- ============================================================================

INSERT INTO book_external_ids (id, book_id, source, external_id, canonical_volume_link)
VALUES
  -- The Shining editions all point to same Google Books work
  ('test-ext-101', '00199670-0001-7000-8000-000000000001'::uuid, 'GOOGLE_BOOKS', 'gb_shining_1',
   'https://books.google.com/books?id=8VnJLu3AvvQC&hl=en'),
  ('test-ext-102', '00199670-0002-7000-8000-000000000002'::uuid, 'GOOGLE_BOOKS', 'gb_shining_2',
   'https://books.google.com/books?id=8VnJLu3AvvQC&hl=en'),
  ('test-ext-103', '00199670-0003-7000-8000-000000000003'::uuid, 'GOOGLE_BOOKS', 'gb_shining_3',
   'https://books.google.com/books?id=8VnJLu3AvvQC&hl=en'),

  -- Doctor Sleep has different canonical ID
  ('test-ext-104', '00199670-0004-7000-8000-000000000004'::uuid, 'GOOGLE_BOOKS', 'gb_doctor_1',
   'https://books.google.com/books?id=0CEVJnkpYgEC&hl=en'),
  ('test-ext-105', '00199670-0005-7000-8000-000000000005'::uuid, 'GOOGLE_BOOKS', 'gb_doctor_2',
   'https://books.google.com/books?id=0CEVJnkpYgEC&hl=en'),

  -- HP1 US editions
  ('test-ext-106', '00199670-0006-7000-8000-000000000006'::uuid, 'GOOGLE_BOOKS', 'gb_hp1_us_1',
   'https://books.google.com/books?id=wrOQLV6xB-wC&hl=en'),
  ('test-ext-107', '00199670-0007-7000-8000-000000000007'::uuid, 'GOOGLE_BOOKS', 'gb_hp1_us_2',
   'https://books.google.com/books?id=wrOQLV6xB-wC&hl=en');

-- ============================================================================
-- ANALYSIS: Check ISBN prefixes before clustering
-- ============================================================================

SELECT 'ISBN PREFIX ANALYSIS:' as report;
SELECT
  title,
  isbn13,
  extract_isbn_work_prefix(isbn13) as isbn_prefix,
  publisher
FROM books
WHERE title LIKE 'CLUSTER_TEST:%'
ORDER BY extract_isbn_work_prefix(isbn13), title;

-- ============================================================================
-- Run clustering
-- ============================================================================

SELECT 'RUNNING ISBN CLUSTERING:' as operation;
SELECT * FROM cluster_books_by_isbn();

SELECT 'RUNNING GOOGLE CLUSTERING:' as operation;
SELECT * FROM cluster_books_by_google_canonical();

-- ============================================================================
-- VERIFICATION 1: Check for false positives
-- Different books should NOT be clustered together
-- ============================================================================

SELECT 'FALSE POSITIVE CHECK:' as verification;
WITH book_clusters AS (
  SELECT
    b.title,
    b.isbn13,
    wc.canonical_title,
    wc.cluster_method,
    wcm.cluster_id
  FROM books b
  JOIN work_cluster_members wcm ON wcm.book_id = b.id
  JOIN work_clusters wc ON wc.id = wcm.cluster_id
  WHERE b.title LIKE 'CLUSTER_TEST:%'
)
SELECT
  bc1.title as book1,
  bc2.title as book2,
  bc1.cluster_method,
  CASE
    WHEN bc1.title != bc2.title
     AND SPLIT_PART(bc1.title, ':', 2) != SPLIT_PART(bc2.title, ':', 2)
    THEN '❌ FALSE POSITIVE - Different books clustered!'
    ELSE '✅ OK - Same work'
  END as status
FROM book_clusters bc1
JOIN book_clusters bc2 ON bc1.cluster_id = bc2.cluster_id
  AND bc1.title < bc2.title
ORDER BY status DESC, bc1.title;

-- ============================================================================
-- VERIFICATION 2: Check for false negatives
-- Same books with different ISBNs should be clustered
-- ============================================================================

SELECT 'FALSE NEGATIVE CHECK:' as verification;
WITH same_books AS (
  SELECT
    b1.id as book1_id,
    b1.title as book1_title,
    b1.isbn13 as book1_isbn,
    b2.id as book2_id,
    b2.title as book2_title,
    b2.isbn13 as book2_isbn,
    -- Extract the actual book title for comparison
    TRIM(SPLIT_PART(b1.title, ':', 2)) as clean_title1,
    TRIM(SPLIT_PART(b2.title, ':', 2)) as clean_title2
  FROM books b1
  CROSS JOIN books b2
  WHERE b1.title LIKE 'CLUSTER_TEST:%'
    AND b2.title LIKE 'CLUSTER_TEST:%'
    AND b1.id < b2.id
    AND (
      -- Same exact title
      TRIM(SPLIT_PART(b1.title, ':', 2)) = TRIM(SPLIT_PART(b2.title, ':', 2))
      -- Or US/UK variants of Harry Potter 1
      OR (TRIM(SPLIT_PART(b1.title, ':', 2)) IN ('Harry Potter and the Sorcerers Stone', 'Harry Potter and the Philosophers Stone')
          AND TRIM(SPLIT_PART(b2.title, ':', 2)) IN ('Harry Potter and the Sorcerers Stone', 'Harry Potter and the Philosophers Stone'))
    )
),
clustering_check AS (
  SELECT
    sb.*,
    wcm1.cluster_id as cluster1,
    wcm2.cluster_id as cluster2,
    CASE
      WHEN wcm1.cluster_id = wcm2.cluster_id THEN '✅ Correctly clustered'
      WHEN wcm1.cluster_id IS NULL OR wcm2.cluster_id IS NULL THEN '⚠️  Not clustered'
      ELSE '❌ FALSE NEGATIVE - Same book in different clusters!'
    END as status
  FROM same_books sb
  LEFT JOIN work_cluster_members wcm1 ON wcm1.book_id = sb.book1_id
  LEFT JOIN work_cluster_members wcm2 ON wcm2.book_id = sb.book2_id
)
SELECT
  book1_title,
  book1_isbn,
  book2_title,
  book2_isbn,
  status
FROM clustering_check
ORDER BY status DESC, book1_title;

-- ============================================================================
-- VERIFICATION 3: Summary Statistics
-- ============================================================================

SELECT 'CLUSTERING SUMMARY:' as report;
SELECT
  COUNT(DISTINCT wc.id) as total_clusters,
  COUNT(DISTINCT wcm.book_id) as books_clustered,
  COUNT(DISTINCT CASE WHEN wc.cluster_method = 'ISBN_PREFIX' THEN wc.id END) as isbn_clusters,
  COUNT(DISTINCT CASE WHEN wc.cluster_method = 'GOOGLE_CANONICAL' THEN wc.id END) as google_clusters
FROM work_clusters wc
JOIN work_cluster_members wcm ON wcm.cluster_id = wc.id
JOIN books b ON b.id = wcm.book_id
WHERE b.title LIKE 'CLUSTER_TEST:%';

-- ============================================================================
-- DETAILED CLUSTER VIEW
-- ============================================================================

SELECT 'DETAILED CLUSTERS:' as report;
SELECT
  wc.cluster_method,
  wc.isbn_prefix,
  wc.google_canonical_id,
  wc.canonical_title,
  STRING_AGG(DISTINCT b.title || ' (' || b.isbn13 || ')', E'\n  ' ORDER BY b.title) as members
FROM work_clusters wc
JOIN work_cluster_members wcm ON wcm.cluster_id = wc.id
JOIN books b ON b.id = wcm.book_id
WHERE b.title LIKE 'CLUSTER_TEST:%'
GROUP BY wc.id, wc.cluster_method, wc.isbn_prefix, wc.google_canonical_id, wc.canonical_title
ORDER BY wc.cluster_method, wc.canonical_title;

-- ============================================================================
-- CLEANUP (commented out - run manually if needed)
-- ============================================================================

-- DELETE FROM work_cluster_members WHERE book_id IN (SELECT id FROM books WHERE title LIKE 'CLUSTER_TEST:%');
-- DELETE FROM work_clusters WHERE canonical_title LIKE 'CLUSTER_TEST:%';
-- DELETE FROM book_external_ids WHERE book_id IN (SELECT id FROM books WHERE title LIKE 'CLUSTER_TEST:%');
-- DELETE FROM books WHERE title LIKE 'CLUSTER_TEST:%';