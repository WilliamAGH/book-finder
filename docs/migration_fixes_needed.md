# Migration Script Fixes Needed

## Critical Issues with Current Migration (`migrate-s3-to-db.js`)

### 1. Schema Mismatch - WRONG TABLES!
The migration tries to insert these fields into `books` table:
- ❌ `average_rating` → Should go to `book_external_ids`
- ❌ `ratings_count` → Should go to `book_external_ids`
- ❌ `info_link` → Should go to `book_external_ids`
- ❌ `preview_link` → Should go to `book_external_ids`
- ❌ `purchase_link` → Should go to `book_external_ids`
- ❌ `list_price` → Should go to `book_external_ids`
- ❌ `currency_code` → Should go to `book_external_ids`
- ❌ `external_image_url` → Should go to `book_image_links`
- ❌ `s3-image-path` → Should be `s3_image_path` (underscore not hyphen)

### 2. Missing ID Generation
- ❌ Not generating UUIDv7 for books
- ❌ Not generating NanoIDs for other tables
- ❌ Not generating slugs

### 3. Missing Table Inserts
The migration doesn't populate:
- ❌ `book_external_ids` - Provider metadata
- ❌ `book_raw_data` - Raw JSON storage
- ❌ `authors` - Author records
- ❌ `book_authors_join` - Book-author relationships
- ❌ `book_dimensions` - Physical dimensions
- ❌ `book_image_links` - Image URLs
- ❌ `book_collections` - Categories
- ❌ `book_collections_join` - Book-category relationships

## What the Migration SHOULD Do

```javascript
// 1. Generate IDs
const bookId = generateUUIDv7(); // Not the Google Books ID!
const slug = generateSlug(title, firstAuthor);

// 2. Insert into books (ONLY canonical fields)
INSERT INTO books (
  id, title, subtitle, description, isbn10, isbn13,
  published_date, language, publisher, page_count,
  slug, s3_image_path
) VALUES (...)

// 3. Insert into book_external_ids (provider data)
INSERT INTO book_external_ids (
  id, book_id, source, external_id,
  provider_isbn10, provider_isbn13,
  info_link, preview_link, canonical_volume_link,
  average_rating, ratings_count,
  viewability, is_ebook, pdf_available, epub_available,
  saleability, list_price, retail_price, currency_code
) VALUES (generateNanoID(), bookId, 'GOOGLE_BOOKS', googleBooksId, ...)

// 4. Insert authors
for (const author of authors) {
  INSERT INTO authors (id, name) VALUES (generateNanoID(), author)
  ON CONFLICT (name) DO NOTHING;

  INSERT INTO book_authors_join (id, book_id, author_id, position)
  VALUES (generateNanoID(12), bookId, authorId, position);
}

// 5. Insert categories
for (const category of categories) {
  INSERT INTO book_collections (id, collection_type, source, display_name)
  VALUES (generateNanoID(8), 'CATEGORY', 'GOOGLE_BOOKS', category)
  ON CONFLICT DO NOTHING;

  INSERT INTO book_collections_join (id, collection_id, book_id)
  VALUES (generateNanoID(12), collectionId, bookId);
}

// 6. Insert image links
for (const [type, url] of Object.entries(imageLinks)) {
  INSERT INTO book_image_links (id, book_id, image_type, url, source)
  VALUES (generateNanoID(), bookId, type, url, 'GOOGLE_BOOKS');
}

// 7. Store raw JSON
INSERT INTO book_raw_data (id, book_id, raw_json_response, source)
VALUES (generateNanoID(), bookId, rawJson, 'GOOGLE_BOOKS');

// 8. Store dimensions if present
if (dimensions) {
  INSERT INTO book_dimensions (id, book_id, height_cm, width_cm)
  VALUES (generateNanoID(8), bookId, height, width);
}
```

## Quick Fix for Current Migration

To make it work temporarily with OLD schema:

```javascript
// Replace lines 106-132 with:
const bookSql = `
  INSERT INTO books (
    id, title, description, publisher, published_date,
    isbn10, isbn13, language, page_count
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
  )
  ON CONFLICT (id) DO UPDATE SET
    title = COALESCE(EXCLUDED.title, books.title),
    description = COALESCE(EXCLUDED.description, books.description),
    publisher = COALESCE(EXCLUDED.publisher, books.publisher),
    published_date = COALESCE(EXCLUDED.published_date, books.published_date),
    isbn10 = COALESCE(EXCLUDED.isbn10, books.isbn10),
    isbn13 = COALESCE(EXCLUDED.isbn13, books.isbn13),
    language = COALESCE(EXCLUDED.language, books.language),
    page_count = COALESCE(EXCLUDED.page_count, books.page_count)
`;

await client.query(bookSql, [
  bookId, title, description, publisher, pubDate,
  isbn10, isbn13, language, pageCount
]);
```

But really, the entire migration needs a rewrite to properly use the new normalized schema.