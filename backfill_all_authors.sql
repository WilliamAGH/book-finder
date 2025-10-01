-- Backfill authors for ALL books from raw JSON data (Google Books, NYT, etc)
-- Run this once to fix all existing books with missing authors
--
-- Usage: source .env && psql "$SPRING_DATASOURCE_URL" -f backfill_all_authors.sql

DO $$
DECLARE
    book_rec RECORD;
    author_names TEXT[];
    author_name TEXT;
    v_author_id TEXT;
    normalized_name TEXT;
    position_idx INTEGER;
    books_processed INTEGER := 0;
    authors_added INTEGER := 0;
    source_type TEXT;
BEGIN
    -- Loop through all books missing authors
    FOR book_rec IN 
        SELECT DISTINCT b.id, brd.raw_json_response, brd.source
        FROM books b
        LEFT JOIN book_authors_join baj ON b.id = baj.book_id
        LEFT JOIN book_raw_data brd ON b.id = brd.book_id
        WHERE baj.book_id IS NULL 
          AND brd.raw_json_response IS NOT NULL
          AND brd.source IN ('GOOGLE_BOOKS', 'AGGREGATED')
    LOOP
        books_processed := books_processed + 1;
        source_type := book_rec.source;
        author_names := NULL;
        
        -- Extract authors based on source
        IF source_type = 'GOOGLE_BOOKS' THEN
            -- Google Books: volumeInfo.authors is an array
            BEGIN
                SELECT ARRAY(
                    SELECT jsonb_array_elements_text(
                        book_rec.raw_json_response::jsonb -> 'volumeInfo' -> 'authors'
                    )
                ) INTO author_names;
            EXCEPTION WHEN OTHERS THEN
                author_names := NULL;
            END;
            
        ELSIF source_type = 'AGGREGATED' THEN
            -- NYT/Aggregated: author and contributor fields
            DECLARE
                raw_author TEXT;
                raw_contributor TEXT;
            BEGIN
                raw_author := book_rec.raw_json_response::jsonb->>'author';
                raw_contributor := book_rec.raw_json_response::jsonb->>'contributor';
                
                IF raw_author IS NOT NULL AND raw_author != '' THEN
                    author_names := ARRAY[raw_author];
                ELSIF raw_contributor IS NOT NULL AND raw_contributor != '' THEN
                    -- Extract from contributor (e.g., "by Dan Brown")
                    author_names := ARRAY[TRIM(REGEXP_REPLACE(raw_contributor, '^by\s+', '', 'i'))];
                END IF;
            END;
        END IF;
        
        -- Skip if no authors found
        IF author_names IS NULL OR array_length(author_names, 1) IS NULL THEN
            CONTINUE;
        END IF;
        
        -- Insert each author and link to book
        position_idx := 0;
        FOREACH author_name IN ARRAY author_names
        LOOP
            IF author_name IS NULL OR TRIM(author_name) = '' THEN
                CONTINUE;
            END IF;
            
            author_name := TRIM(author_name);
            normalized_name := LOWER(REGEXP_REPLACE(author_name, '[^a-zA-Z0-9\s]', '', 'g'));
            
            -- Insert or get author
            BEGIN
                INSERT INTO authors (id, name, normalized_name, created_at, updated_at)
                VALUES (
                    SUBSTRING(MD5(RANDOM()::TEXT) FROM 1 FOR 10),
                    author_name,
                    normalized_name,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
                RETURNING id INTO v_author_id;
                
                -- If no ID returned, fetch existing
                IF v_author_id IS NULL THEN
                    SELECT id INTO v_author_id FROM authors WHERE name = author_name LIMIT 1;
                END IF;
                
                -- Link book to author
                INSERT INTO book_authors_join (id, book_id, author_id, position, created_at)
                VALUES (
                    SUBSTRING(MD5(RANDOM()::TEXT || book_rec.id::TEXT || position_idx::TEXT) FROM 1 FOR 12),
                    book_rec.id,
                    v_author_id,
                    position_idx,
                    NOW()
                )
                ON CONFLICT (book_id, author_id) DO NOTHING;
                
                authors_added := authors_added + 1;
                position_idx := position_idx + 1;
                
            EXCEPTION WHEN OTHERS THEN
                -- Skip this author if there's an error
                RAISE WARNING 'Error processing author % for book %: %', author_name, book_rec.id, SQLERRM;
                CONTINUE;
            END;
        END LOOP;
        
        -- Progress logging every 100 books
        IF books_processed % 100 = 0 THEN
            RAISE NOTICE 'Processed % books, added % authors', books_processed, authors_added;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'COMPLETE: Processed % books, added % author links', books_processed, authors_added;
END $$;
