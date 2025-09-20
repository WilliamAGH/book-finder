-- Example: Find all editions of a work
-- This shows how different ISBNs/editions are tracked

-- Method 1: Using the get_book_editions function
-- Given a book ID, find all its editions
SELECT * FROM get_book_editions('00199660-0001-7000-8000-000000000001'::uuid);

-- Method 2: Direct query to see all editions in a cluster
SELECT
    wc.canonical_title as work_title,
    wc.cluster_method,
    wc.isbn_prefix,
    b.title as edition_title,
    b.isbn13,
    b.isbn10,
    b.publisher,
    b.published_date,
    b.slug,
    wcm.is_primary,
    wcm.confidence
FROM work_clusters wc
JOIN work_cluster_members wcm ON wcm.cluster_id = wc.id
JOIN books b ON b.id = wcm.book_id
WHERE wc.canonical_title = 'The Shining'  -- Or any book title
ORDER BY wcm.is_primary DESC, b.published_date DESC;

-- Method 3: Find books with multiple editions
SELECT
    wc.canonical_title,
    wc.member_count as edition_count,
    STRING_AGG(
        b.publisher || ' (' || b.isbn13 || ')',
        ', '
        ORDER BY b.published_date
    ) as editions
FROM work_clusters wc
JOIN work_cluster_members wcm ON wcm.cluster_id = wc.id
JOIN books b ON b.id = wcm.book_id
WHERE wc.member_count > 1
GROUP BY wc.id, wc.canonical_title, wc.member_count
ORDER BY wc.member_count DESC;