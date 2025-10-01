package com.williamcallahan.book_recommendation_engine.util;

/**
 * Central repository for shared literal values to reduce duplication.
 */
public final class ApplicationConstants {
    private ApplicationConstants() {
    }

    public static final class Cover {
        public static final String PLACEHOLDER_IMAGE_PATH = "/images/placeholder-book-cover.svg";

        private Cover() {
        }
    }

    public static final class Provider {
        public static final String GOOGLE_BOOKS = "GOOGLE_BOOKS";
        public static final String OPEN_LIBRARY = "OPEN_LIBRARY";

        private Provider() {
        }
    }

    public static final class Paging {
        public static final int DEFAULT_SEARCH_LIMIT = 10;
        public static final int MIN_SEARCH_LIMIT = 1;
        public static final int MAX_SEARCH_LIMIT = 100;

        public static final int DEFAULT_SIMILAR_LIMIT = 5;
        public static final int MAX_SIMILAR_LIMIT = 20;

        public static final int DEFAULT_AUTHOR_LIMIT = 10;
        public static final int MIN_AUTHOR_LIMIT = 1;
        public static final int MAX_AUTHOR_LIMIT = 100;

        public static final int DEFAULT_TIERED_LIMIT = 20;
        public static final int MAX_TIERED_LIMIT = 200;

        private Paging() {
        }
    }

    public static final class Search {
        public static final String DEFAULT_RECENT_FALLBACK_QUERY = "java programming";

        private Search() {
        }
    }

    public static final class Tag {
        public static final String QUALIFIER = "QUALIFIER";

        private Tag() {
        }
    }

    public static final class Urls {
        public static final String BASE_URL = "https://findmybook.net";
        public static final String OG_LOGO = BASE_URL + "/images/og-logo.png";
        public static final String DEFAULT_SOCIAL_IMAGE = BASE_URL + "/images/default-social-image.png";
        public static final String SITEMAP_PATH = BASE_URL + "/sitemap.xml";
        public static final String ROBOTS_PATH = BASE_URL + "/robots.txt";

        private Urls() {
        }
    }

    public static final class Database {
        public static final class Queries {
            // Book lookups
            public static final String BOOK_BY_ISBN13 = "SELECT id::text FROM books WHERE isbn13 = ? LIMIT 1";
            public static final String BOOK_BY_ISBN10 = "SELECT id::text FROM books WHERE isbn10 = ? LIMIT 1";
            public static final String BOOK_BY_ID = "SELECT id::text FROM books WHERE id = ?::uuid LIMIT 1";
            public static final String BOOK_BY_SLUG = "SELECT id FROM books WHERE slug = ? LIMIT 1";

            // Existence checks
            public static final String BOOK_EXISTS_BY_ISBN13 = "SELECT COUNT(*) > 0 FROM books WHERE isbn13 = ?";
            public static final String BOOK_EXISTS_BY_ISBN10 = "SELECT COUNT(*) > 0 FROM books WHERE isbn10 = ?";
            public static final String BOOK_EXISTS_BY_ID = "SELECT COUNT(*) > 0 FROM books WHERE id = ?";

            // Edition queries
            public static final String EDITIONS_BY_GROUP_KEY = "SELECT id, edition_number FROM books WHERE edition_group_key = ?";
            public static final String UPDATE_EDITION_GROUP = "UPDATE books SET edition_group_key = ? WHERE id = ?";

            // Author queries
            public static final String AUTHORS_BY_BOOK = "SELECT a.name FROM authors a JOIN book_authors ba ON a.id = ba.author_id WHERE ba.book_id = ?";
            public static final String INSERT_AUTHOR = "INSERT INTO authors (name) VALUES (?) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id";
            public static final String LINK_BOOK_AUTHOR = "INSERT INTO book_authors (book_id, author_id) VALUES (?, ?) ON CONFLICT DO NOTHING";

            // Category queries
            public static final String CATEGORIES_BY_BOOK = "SELECT c.display_name FROM categories c JOIN book_categories bc ON c.id = bc.category_id WHERE bc.book_id = ?";
            public static final String INSERT_CATEGORY = "INSERT INTO categories (display_name) VALUES (?) ON CONFLICT (display_name) DO UPDATE SET display_name = EXCLUDED.display_name RETURNING id";
            public static final String LINK_BOOK_CATEGORY = "INSERT INTO book_categories (book_id, category_id) VALUES (?, ?) ON CONFLICT DO NOTHING";

            // Cover queries
            public static final String UPDATE_COVER_URL = "UPDATE books SET cover_url = ?, cover_updated_at = NOW() WHERE id = ?";
            public static final String COVERS_BY_BOOK = "SELECT source, size, url FROM book_covers WHERE book_id = ? ORDER BY priority DESC";

            // Search queries
            public static final String SEARCH_BOOKS = "SELECT * FROM book_search_view WHERE search_vector @@ plainto_tsquery('english', ?) ORDER BY ts_rank(search_vector, plainto_tsquery('english', ?)) DESC LIMIT ?";
            public static final String REFRESH_SEARCH_VIEW = "SELECT refresh_book_search_view()";

            // Sitemap queries
            public static final String COUNT_BOOKS_WITH_SLUG = "SELECT COUNT(*) FROM books WHERE slug IS NOT NULL";
            public static final String BOOKS_FOR_SITEMAP = "SELECT id, slug, title, updated_at FROM books WHERE slug IS NOT NULL ORDER BY title LIMIT ? OFFSET ?";

            private Queries() {
            }
        }

        private Database() {
        }
    }

    public static final class ExternalServices {
        public static final String GOOGLE_BOOKS_API_BASE = "https://www.googleapis.com/books/v1/volumes";
        public static final String OPEN_LIBRARY_API_BASE = "https://openlibrary.org";
        public static final String AMAZON_LINK_TEMPLATE = "https://www.amazon.com/dp/";
        public static final String BARNES_NOBLE_LINK_TEMPLATE = "https://www.barnesandnoble.com/w/?ean=";
        public static final String BOOKSHOP_LINK_TEMPLATE = "https://bookshop.org/books/";

        private ExternalServices() {
        }
    }
}
