-- Backfill descriptions from raw JSON data
-- Run once to populate missing book descriptions
--
-- Usage: source .env && psql "$SPRING_DATASOURCE_URL" -f backfill_descriptions.sql

DO $$
DECLARE
    updated_count INTEGER := 0;
BEGIN
    -- Update books.description from raw JSON where it's missing
    UPDATE books b
    SET description = CASE
        -- NYT/Aggregated data: use 'description' field
        WHEN brd.source = 'AGGREGATED' THEN 
            COALESCE(
                NULLIF(brd.raw_json_response::jsonb->>'description', ''),
                NULLIF(brd.raw_json_response::jsonb->>'summary', '')
            )
        -- Google Books data: use volumeInfo.description
        WHEN brd.source = 'GOOGLE_BOOKS' THEN 
            NULLIF(brd.raw_json_response::jsonb->'volumeInfo'->>'description', '')
        ELSE NULL
    END,
    updated_at = NOW()
    FROM book_raw_data brd
    WHERE b.id = brd.book_id
      AND (b.description IS NULL OR b.description = '')
      AND (
          (brd.source = 'AGGREGATED' AND (brd.raw_json_response::jsonb->>'description' IS NOT NULL OR brd.raw_json_response::jsonb->>'summary' IS NOT NULL))
          OR
          (brd.source = 'GOOGLE_BOOKS' AND brd.raw_json_response::jsonb->'volumeInfo'->>'description' IS NOT NULL)
      );
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RAISE NOTICE 'Updated % book descriptions from raw JSON', updated_count;
END $$;
