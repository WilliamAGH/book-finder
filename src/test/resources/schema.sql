-- Test Schema for H2 In-Memory Database
-- This schema is executed when the 'test' profile is active (using H2).
-- It defines the table structure for CachedBook and related entities, ensuring
-- compatibility with H2, especially for types not natively supported by H2 (e.g., pgvector's vector type).

DROP TABLE IF EXISTS cached_book_categories CASCADE;
DROP TABLE IF EXISTS cached_books CASCADE;

CREATE TABLE cached_books (
    id VARCHAR(255) NOT NULL,
    google_books_id VARCHAR(255) NOT NULL,
    title VARCHAR(1000) NOT NULL,
    -- Storing authors as JSON. H2's JSON type is generally compatible with text-based JSON.
    authors JSON, -- H2 compatible JSON
    description TEXT,
    cover_image_url VARCHAR(255),
    isbn10 VARCHAR(20),
    isbn13 VARCHAR(20),
    published_date TIMESTAMP,
    average_rating NUMERIC(38,2),
    ratings_count INTEGER,
    page_count INTEGER,
    language VARCHAR(10),
    publisher VARCHAR(255),
    info_link VARCHAR(255),
    preview_link VARCHAR(255),
    purchase_link VARCHAR(255),
    -- Storing rawData as JSON.
    raw_data JSON, -- H2 compatible JSON
    -- The 'embedding' field (PgVector in Java) is converted to a String by PgVectorConverter.
    -- For H2, this String representation is stored in a TEXT column.
    -- This avoids H2 needing to understand PostgreSQL's 'vector' type during tests.
    embedding TEXT, -- Using TEXT for H2 compatibility, assuming PgVectorConverter outputs String
    created_at TIMESTAMP NOT NULL,
    last_accessed TIMESTAMP NOT NULL,
    access_count INTEGER NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE cached_book_categories (
    book_id VARCHAR(255) NOT NULL,
    category TEXT,
    FOREIGN KEY (book_id) REFERENCES cached_books(id) ON DELETE CASCADE
); 