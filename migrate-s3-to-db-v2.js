#!/usr/bin/env node
/**
 * S3 to PostgreSQL migration script - Refactored Version
 * Cleaner, flatter logic while handling all corruption patterns
 */

const { Client } = require('pg');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('node:fs');
const crypto = require('node:crypto');

// ============================================================================
// CONFIGURATION
// ============================================================================

const args = process.argv.slice(2);
const getArg = (name, defaultValue) => {
  const arg = args.find(a => a.startsWith(`--${name}=`));
  return arg ? arg.split('=')[1] : defaultValue;
};

const CONFIG = {
  MAX_RECORDS: parseInt(getArg('max', '0'), 10),
  SKIP_RECORDS: parseInt(getArg('skip', '0'), 10),
  PREFIX: getArg('prefix', 'books/v1/'),
  DEBUG_MODE: getArg('debug', 'false') === 'true',
  BATCH_SIZE: 100
};

// ============================================================================
// ID GENERATORS
// ============================================================================

const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

function generateNanoId(size = 10) {
  const bytes = crypto.randomBytes(size);
  let id = '';
  for (let i = 0; i < size; i++) {
    id += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return id;
}

function generateUUIDv7() {
  const unixMillis = BigInt(Date.now());
  const timeHigh = Number((unixMillis >> 20n) & 0xffffffffn);
  const timeMid = Number((unixMillis >> 4n) & 0xffffn);
  const timeLow = Number(unixMillis & 0xfn);
  const random = crypto.randomBytes(10);
  const bytes = Buffer.alloc(16);

  bytes.writeUInt32BE(timeHigh, 0);
  bytes.writeUInt16BE(timeMid, 4);
  bytes[6] = ((timeLow & 0x0f) | 0x70);
  bytes[7] = random[0];
  bytes[8] = (random[1] & 0x3f) | 0x80;
  random.copy(bytes, 9, 2);

  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// ============================================================================
// JSON PARSING UTILITIES
// ============================================================================

/**
 * Parse potentially corrupted JSON data
 * Handles: concatenation, pre-processing, double-stringification
 */
class JsonParser {
  constructor(debugMode = false) {
    this.debug = debugMode;
  }

  /**
   * Main parse method - handles all corruption patterns
   */
  parse(bodyString, filename) {
    // Step 1: Validate input
    if (!bodyString || bodyString.length === 0) {
      throw new Error('Empty file');
    }

    // Strip null bytes (0x00) and other control characters except newlines, tabs, and carriage returns
    let containsNull = false;
    const cleaned = [...bodyString].map(ch => {
      const code = ch.charCodeAt(0);
      if (code === 0) {
        containsNull = true;
        return '';
      }
      // Remove control chars: 0x01-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F
      if ((code >= 1 && code <= 8) || code === 11 || code === 12 || (code >= 14 && code <= 31) || code === 127) {
        return '';
      }
      return ch;
    }).join('');
    if (containsNull) {
      console.warn(`[WARNING] File ${filename} contains null bytes, stripping them`);
    }
    if (cleaned !== bodyString) {
      console.warn(`[WARNING] File ${filename} contains control characters, stripping them`);
      bodyString = cleaned;
    }

    // Trim and re-assign to work with clean content
    bodyString = bodyString.trim();

    // Some files might have garbage before the JSON, try to find the first { or [
    const jsonStart = Math.min(
      bodyString.indexOf('{') === -1 ? Infinity : bodyString.indexOf('{'),
      bodyString.indexOf('[') === -1 ? Infinity : bodyString.indexOf('[')
    );

    if (jsonStart === Infinity || jsonStart > 100) {
      // If no JSON start found or it's too far into the file, it's probably not JSON
      throw new Error('File does not appear to be JSON (does not start with { or [)');
    }

    if (jsonStart > 0) {
      console.warn(`[WARNING] File ${filename} has ${jsonStart} bytes of garbage before JSON, stripping`);
      bodyString = bodyString.substring(jsonStart);
    }

    // Final check
    if (!bodyString.startsWith('{') && !bodyString.startsWith('[')) {
      throw new Error('File does not appear to be JSON after cleanup');
    }

    // Step 2: Split concatenated objects if needed
    const jsonObjects = this.splitConcatenated(bodyString, filename);

    // Step 3: Parse each object
    const parsedObjects = [];
    for (const jsonStr of jsonObjects) {
      try {
        const obj = JSON.parse(jsonStr);
        parsedObjects.push(obj);
      } catch (e) {
        console.error(`[ERROR] Failed to parse JSON fragment: ${e.message}`);
        if (this.debug) {
          console.error(`[DEBUG] Fragment start: ${jsonStr.substring(0, 100)}...`);
        }
      }
    }

    // Step 4: Extract from pre-processed format if needed
    const extractedObjects = parsedObjects.map(obj => this.extractFromPreProcessed(obj));

    // Step 5: Deduplicate
    return this.deduplicateBooks(extractedObjects);
  }

  /**
   * Split concatenated JSON objects (handles }{ pattern)
   */
  splitConcatenated(bodyString, filename) {
    // Check for concatenation pattern
    if (!bodyString.includes('}{')) {
      return [bodyString];
    }

    console.warn(`[WARNING] File ${filename} contains concatenated JSON objects`);

    const objects = [];
    let depth = 0;
    let current = '';

    for (let i = 0; i < bodyString.length; i++) {
      const char = bodyString[i];
      current += char;

      if (char === '{') {
        depth++;
      } else if (char === '}') {
        depth--;
        if (depth === 0 && current.length > 0) {
          objects.push(current);
          current = '';
          // Skip whitespace
          while (i + 1 < bodyString.length && /\s/.test(bodyString[i + 1])) {
            i++;
          }
        }
      }
    }

    if (current.trim().length > 0) {
      objects.push(current);
    }

    console.log(`[INFO] Split into ${objects.length} objects`);
    return objects;
  }

  /**
   * Extract actual book data from pre-processed format
   */
  extractFromPreProcessed(book) {
    // Check if this is pre-processed data
    // Indicators: title === id, has rawJsonResponse, no volumeInfo
    const isPreProcessed = book.rawJsonResponse &&
                          !book.volumeInfo &&
                          book.title === book.id;

    if (!isPreProcessed) {
      return book;
    }

    try {
      // Handle potential double-stringification
      let jsonStr = book.rawJsonResponse;
      if (typeof jsonStr === 'string' && jsonStr.startsWith('"') && jsonStr.endsWith('"')) {
        jsonStr = JSON.parse(jsonStr);
      }

      const innerData = JSON.parse(jsonStr);

      if (innerData.volumeInfo || innerData.kind === 'books#volume') {
        if (this.debug) {
          console.log(`[EXTRACT] Real title: "${innerData.volumeInfo?.title}" (was: "${book.title}")`);
        }
        return innerData;
      }
    } catch (e) {
      console.error(`[ERROR] Failed to extract from rawJsonResponse: ${e.message}`);
      // Return original book as fallback
      return book;
    }

    return book;
  }

  /**
   * Deduplicate books based on ISBN or title+author
   */
  deduplicateBooks(books) {
    if (books.length <= 1) return books;

    const seen = new Map();
    const unique = [];

    for (const book of books) {
      const key = this.getBookKey(book);
      if (!seen.has(key)) {
        seen.set(key, true);
        unique.push(book);
      } else if (this.debug) {
        console.log(`[DEDUP] Skipping duplicate: ${key}`);
      }
    }

    return unique;
  }

  /**
   * Generate unique key for deduplication
   */
  getBookKey(book) {
    const volumeInfo = book.volumeInfo || book;

    // Try ISBN first
    if (volumeInfo.industryIdentifiers) {
      for (const id of volumeInfo.industryIdentifiers) {
        if (id.type === 'ISBN_13') return `isbn13:${id.identifier}`;
        if (id.type === 'ISBN_10') return `isbn10:${id.identifier}`;
      }
    }

    // Fall back to title + first author
    const title = volumeInfo.title || '';
    const author = volumeInfo.authors?.[0] || '';
    return `${title}:${author}`.toLowerCase();
  }
}

// ============================================================================
// DATABASE OPERATIONS
// ============================================================================

/**
 * Handles all database operations for a single book
 */
class BookMigrator {
  constructor(client, debugMode = false) {
    this.client = client;
    this.debug = debugMode;
  }

  /**
   * Migrate a single book to the database
   */
  async migrate(googleBooksId, bookData) {
    // Sanitize Google Books ID for SQL safety
    const safeId = this.sanitizeGoogleBooksId(googleBooksId);

    // Extract all fields
    const fields = this.extractFields(bookData);

    // Find or create book
    const bookId = await this.findOrCreateBook(safeId, fields);

    // Insert related data in parallel where possible
    await Promise.all([
      this.insertExternalId(bookId, safeId, fields),
      this.insertRawData(bookId, bookData),
      this.insertImageLinks(bookId, fields.imageLinks),
      this.insertDimensions(bookId, fields.dimensions)
    ]);

    // Insert join table data (must be sequential for foreign keys)
    await this.insertAuthors(bookId, fields.authors);
    await this.insertCategories(bookId, fields.categories);

    return { bookId, title: fields.title };
  }

  /**
   * Sanitize Google Books ID (handle -- prefix)
   */
  sanitizeGoogleBooksId(id) {
    if (id.startsWith('--')) {
      const sanitized = id.replace(/^-+/, match => '_'.repeat(match.length));
      console.warn(`[SANITIZE] Converted ID "${id}" to "${sanitized}"`);
      return sanitized;
    }
    return id;
  }

  /**
   * Extract all fields from book data
   */
  extractFields(bookData) {
    const volumeInfo = bookData.volumeInfo || bookData;
    const saleInfo = bookData.saleInfo || {};
    const accessInfo = bookData.accessInfo || {};

    return {
      // Basic info
      title: volumeInfo.title || '',
      subtitle: volumeInfo.subtitle || null,
      description: volumeInfo.description || null,
      authors: volumeInfo.authors || [],
      categories: volumeInfo.categories || [],

      // Publishing info
      publisher: volumeInfo.publisher || null,
      publishedDate: this.normalizeDate(volumeInfo.publishedDate),
      language: volumeInfo.language || 'en',
      pageCount: volumeInfo.pageCount || volumeInfo.printedPageCount || null,

      // Identifiers
      isbn10: this.extractISBN(volumeInfo.industryIdentifiers, 'ISBN_10'),
      isbn13: this.extractISBN(volumeInfo.industryIdentifiers, 'ISBN_13'),

      // Images
      imageLinks: volumeInfo.imageLinks || {},

      // Physical
      dimensions: volumeInfo.dimensions || null,

      // Ratings
      averageRating: volumeInfo.averageRating || null,
      ratingsCount: volumeInfo.ratingsCount || null,

      // Links
      infoLink: volumeInfo.infoLink || null,
      previewLink: volumeInfo.previewLink || null,
      canonicalVolumeLink: volumeInfo.canonicalVolumeLink || null,
      webReaderLink: accessInfo.webReaderLink || null,

      // Sale/Access info
      saleability: saleInfo.saleability || 'NOT_FOR_SALE',
      isEbook: saleInfo.isEbook || false,
      listPrice: saleInfo.listPrice?.amount || saleInfo.retailPrice?.amount || null,
      currencyCode: saleInfo.listPrice?.currencyCode || saleInfo.retailPrice?.currencyCode || null,
      country: saleInfo.country || accessInfo.country || null,

      // Access details
      viewability: accessInfo.viewability || 'NO_PAGES',
      embeddable: accessInfo.embeddable || false,
      publicDomain: accessInfo.publicDomain || false,
      textToSpeechPermission: accessInfo.textToSpeechPermission || null,
      pdfAvailable: accessInfo.pdf?.isAvailable || false,
      epubAvailable: accessInfo.epub?.isAvailable || false,

      // Reading modes
      textReadable: volumeInfo.readingModes?.text || false,
      imageReadable: volumeInfo.readingModes?.image || false,

      // Other
      printType: volumeInfo.printType || 'BOOK',
      maturityRating: volumeInfo.maturityRating || 'NOT_MATURE',
      contentVersion: volumeInfo.contentVersion || null
    };
  }

  /**
   * Extract ISBN by type
   */
  extractISBN(identifiers, type) {
    if (!identifiers) return null;
    const id = identifiers.find(i => i.type === type);
    return id?.identifier || null;
  }

  /**
   * Normalize date to PostgreSQL format
   */
  normalizeDate(dateStr) {
    if (!dateStr) return null;

    // Handle year-only (1920) -> 1920-01-01
    if (dateStr.length === 4) {
      return `${dateStr}-01-01`;
    }

    // Handle year-month (1920-05) -> 1920-05-01
    if (dateStr.length === 7) {
      return `${dateStr}-01`;
    }

    return dateStr;
  }

  /**
   * Find existing book or create new one
   */
  async findOrCreateBook(googleBooksId, fields) {
    // Check by Google Books ID first
    const externalCheck = await this.client.query(
      `SELECT book_id FROM book_external_ids
       WHERE source = 'GOOGLE_BOOKS' AND external_id = $1`,
      [googleBooksId]
    );

    if (externalCheck.rows.length > 0) {
      return externalCheck.rows[0].book_id;
    }

    // Check by ISBN
    let existingBook = null;

    if (fields.isbn13) {
      const res = await this.client.query(
        'SELECT id FROM books WHERE isbn13 = $1 LIMIT 1',
        [fields.isbn13]
      );
      if (res.rows.length > 0) existingBook = res.rows[0];
    }

    if (!existingBook && fields.isbn10) {
      const res = await this.client.query(
        'SELECT id FROM books WHERE isbn10 = $1 LIMIT 1',
        [fields.isbn10]
      );
      if (res.rows.length > 0) existingBook = res.rows[0];
    }

    if (existingBook) {
      return existingBook.id;
    }

    // Create new book
    const bookId = generateUUIDv7();
    const slug = await this.generateUniqueSlug(fields.title, fields.authors[0]);

    await this.client.query(
      `INSERT INTO books (
        id, title, subtitle, description, isbn10, isbn13,
        published_date, language, publisher, page_count, slug,
        created_at, updated_at
      ) VALUES (
        $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        NOW(), NOW()
      )`,
      [bookId, fields.title, fields.subtitle, fields.description,
       fields.isbn10, fields.isbn13, fields.publishedDate,
       fields.language, fields.publisher, fields.pageCount, slug]
    );

    return bookId;
  }

  /**
   * Generate unique slug
   */
  async generateUniqueSlug(title, firstAuthor) {
    if (!title) return 'book';

    let slug = title.toLowerCase()
      .replace(/&/g, 'and')
      .replace(/['"''""]/g, '')
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/[\s_]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-+|-+$/g, '');

    if (slug.length > 60) {
      slug = slug.substring(0, 60);
      const lastDash = slug.lastIndexOf('-');
      if (lastDash > 30) slug = slug.substring(0, lastDash);
    }

    if (firstAuthor) {
      let authorSlug = firstAuthor.toLowerCase()
        .replace(/[^a-z0-9\s-]/g, '')
        .replace(/[\s_]+/g, '-')
        .substring(0, 30);
      slug = `${slug}-${authorSlug}`;
    }

    if (slug.length > 100) {
      slug = slug.substring(0, 100);
    }

    // Ensure uniqueness
    const result = await this.client.query(
      'SELECT ensure_unique_slug($1) as unique_slug',
      [slug]
    );

    return result.rows[0].unique_slug;
  }

  /**
   * Insert external ID record
   */
  async insertExternalId(bookId, googleBooksId, fields) {
    // Check if another external ID already has these ISBNs
    // If so, we'll store NULL for the ISBNs to avoid duplicate constraint violations
    // The ISBNs are already linked to the book via the books table
    let providerIsbn10 = fields.isbn10;
    let providerIsbn13 = fields.isbn13;

    if (fields.isbn13) {
      const existingIsbn13 = await this.client.query(
        `SELECT external_id FROM book_external_ids
         WHERE source = 'GOOGLE_BOOKS' AND provider_isbn13 = $1
         LIMIT 1`,
        [fields.isbn13]
      );
      if (existingIsbn13.rows.length > 0 && existingIsbn13.rows[0].external_id !== googleBooksId) {
        console.log(`[INFO] ISBN13 ${fields.isbn13} already linked via external ID ${existingIsbn13.rows[0].external_id}`);
        providerIsbn13 = null; // Don't duplicate the ISBN in external_ids table
      }
    }

    if (fields.isbn10) {
      const existingIsbn10 = await this.client.query(
        `SELECT external_id FROM book_external_ids
         WHERE source = 'GOOGLE_BOOKS' AND provider_isbn10 = $1
         LIMIT 1`,
        [fields.isbn10]
      );
      if (existingIsbn10.rows.length > 0 && existingIsbn10.rows[0].external_id !== googleBooksId) {
        console.log(`[INFO] ISBN10 ${fields.isbn10} already linked via external ID ${existingIsbn10.rows[0].external_id}`);
        providerIsbn10 = null; // Don't duplicate the ISBN in external_ids table
      }
    }

    await this.client.query(
      `INSERT INTO book_external_ids (
        id, book_id, source, external_id, provider_isbn10, provider_isbn13,
        info_link, preview_link, web_reader_link, canonical_volume_link,
        average_rating, ratings_count, is_ebook, pdf_available, epub_available,
        embeddable, public_domain, viewability, text_readable, image_readable,
        print_type, maturity_rating, content_version, text_to_speech_permission,
        saleability, country_code, list_price, retail_price, currency_code,
        created_at
      ) VALUES (
        $1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
        $29, NOW()
      )
      ON CONFLICT (source, external_id) DO UPDATE SET
        average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating),
        ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count),
        list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price),
        retail_price = COALESCE(EXCLUDED.retail_price, book_external_ids.retail_price)`,
      [
        generateNanoId(10), bookId, 'GOOGLE_BOOKS', googleBooksId,
        providerIsbn10, providerIsbn13, fields.infoLink, fields.previewLink,
        fields.webReaderLink, fields.canonicalVolumeLink, fields.averageRating,
        fields.ratingsCount, fields.isEbook, fields.pdfAvailable,
        fields.epubAvailable, fields.embeddable, fields.publicDomain,
        fields.viewability, fields.textReadable, fields.imageReadable,
        fields.printType, fields.maturityRating, fields.contentVersion,
        fields.textToSpeechPermission, fields.saleability, fields.country,
        fields.listPrice, fields.listPrice, fields.currencyCode
      ]
    );
  }

  /**
   * Insert raw JSON data
   */
  async insertRawData(bookId, bookData) {
    await this.client.query(
      `INSERT INTO book_raw_data (
        id, book_id, raw_json_response, source,
        fetched_at, contributed_at, created_at
      ) VALUES (
        $1, $2::uuid, $3::jsonb, $4, NOW(), NOW(), NOW()
      )
      ON CONFLICT (book_id, source) DO UPDATE SET
        raw_json_response = EXCLUDED.raw_json_response,
        fetched_at = NOW()`,
      [generateNanoId(10), bookId, JSON.stringify(bookData), 'GOOGLE_BOOKS']
    );
  }

  /**
   * Insert image links
   */
  async insertImageLinks(bookId, imageLinks) {
    if (!imageLinks || Object.keys(imageLinks).length === 0) return;

    for (const [imageType, url] of Object.entries(imageLinks)) {
      if (!url) continue;

      await this.client.query(
        `INSERT INTO book_image_links (
          id, book_id, image_type, url, source, created_at
        ) VALUES (
          $1, $2::uuid, $3, $4, $5, NOW()
        )
        ON CONFLICT (book_id, image_type) DO UPDATE SET
          url = EXCLUDED.url`,
        [generateNanoId(10), bookId, imageType, url, 'GOOGLE_BOOKS']
      );
    }
  }

  /**
   * Insert dimensions
   */
  async insertDimensions(bookId, dimensions) {
    if (!dimensions) return;

    const parseValue = (str) => {
      if (!str) return null;
      const match = str.match(/(\d+\.?\d*)/);
      return match ? parseFloat(match[1]) : null;
    };

    const height = parseValue(dimensions.height);
    const width = parseValue(dimensions.width);
    const thickness = parseValue(dimensions.thickness);

    if (!height && !width && !thickness) return;

    await this.client.query(
      `INSERT INTO book_dimensions (
        id, book_id, height_cm, width_cm, thickness_cm, created_at
      ) VALUES (
        $1, $2::uuid, $3, $4, $5, NOW()
      )
      ON CONFLICT (book_id) DO UPDATE SET
        height_cm = COALESCE(EXCLUDED.height_cm, book_dimensions.height_cm),
        width_cm = COALESCE(EXCLUDED.width_cm, book_dimensions.width_cm),
        thickness_cm = COALESCE(EXCLUDED.thickness_cm, book_dimensions.thickness_cm)`,
      [generateNanoId(8), bookId, height, width, thickness]
    );
  }

  /**
   * Insert authors
   */
  async insertAuthors(bookId, authors) {
    if (!authors || authors.length === 0) return;

    for (let i = 0; i < authors.length; i++) {
      const authorName = authors[i];
      if (!authorName?.trim()) continue;

      // Insert or get author
      const authorResult = await this.client.query(
        `INSERT INTO authors (id, name, normalized_name, created_at, updated_at)
         VALUES ($1, $2, $3, NOW(), NOW())
         ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
         RETURNING id`,
        [generateNanoId(10), authorName, this.normalizeAuthorName(authorName)]
      );

      const authorId = authorResult.rows[0].id;

      // Link book to author
      await this.client.query(
        `INSERT INTO book_authors_join (id, book_id, author_id, position, created_at)
         VALUES ($1, $2::uuid, $3, $4, NOW())
         ON CONFLICT (book_id, author_id) DO UPDATE SET position = EXCLUDED.position`,
        [generateNanoId(12), bookId, authorId, i]
      );
    }
  }

  /**
   * Normalize author name for deduplication
   */
  normalizeAuthorName(name) {
    return name.toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9\s]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /**
   * Insert categories
   */
  async insertCategories(bookId, categories) {
    if (!categories || categories.length === 0) return;

    for (const categoryName of categories) {
      if (!categoryName?.trim()) continue;

      // Insert or get category
      const normalized = categoryName.toLowerCase().replace(/[^a-z0-9\s]/g, '');

      let collectionId;
      const insertResult = await this.client.query(
        `INSERT INTO book_collections (
          id, collection_type, source, display_name, normalized_name,
          created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, NOW(), NOW()
        )
        ON CONFLICT (collection_type, source, normalized_name)
        WHERE collection_type = 'CATEGORY' AND normalized_name IS NOT NULL
        DO NOTHING
        RETURNING id`,
        [generateNanoId(8), 'CATEGORY', 'GOOGLE_BOOKS', categoryName, normalized]
      );

      if (insertResult.rows.length > 0) {
        collectionId = insertResult.rows[0].id;
      } else {
        // Find existing
        const findResult = await this.client.query(
          `SELECT id FROM book_collections
           WHERE display_name = $1 AND collection_type = $2 AND source = $3`,
          [categoryName, 'CATEGORY', 'GOOGLE_BOOKS']
        );
        if (findResult.rows.length > 0) {
          collectionId = findResult.rows[0].id;
        }
      }

      if (collectionId) {
        // Link book to collection
        await this.client.query(
          `INSERT INTO book_collections_join (
            id, collection_id, book_id, created_at, updated_at
          ) VALUES (
            $1, $2, $3::uuid, NOW(), NOW()
          )
          ON CONFLICT (collection_id, book_id) DO NOTHING`,
          [generateNanoId(12), collectionId, bookId]
        );
      }
    }
  }
}

// ============================================================================
// MAIN MIGRATION LOGIC
// ============================================================================

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

function loadEnvFile() {
  try {
    if (!fs.existsSync('.env')) return;

    const envContent = fs.readFileSync('.env', 'utf8');
    envContent.split('\n').forEach(line => {
      if (line.startsWith('#')) return;
      const [key, ...valueParts] = line.split('=');
      if (key && valueParts.length > 0 && !process.env[key]) {
        process.env[key] = valueParts.join('=').trim();
      }
    });
  } catch {
    // Ignore errors
  }
}

function parsePostgresUrl(pgUrl) {
  if (!pgUrl) throw new Error('SPRING_DATASOURCE_URL not set');

  const parsed = new URL(pgUrl);
  return {
    host: parsed.hostname,
    port: parsed.port || 5432,
    database: parsed.pathname.slice(1) || 'postgres',
    user: parsed.username,
    password: parsed.password,
    ssl: parsed.searchParams.get('sslmode') === 'require' ? { rejectUnauthorized: false } : false
  };
}

/**
 * Main migration function
 */
async function migrate() {
  console.log('🚀 Starting S3 to PostgreSQL migration (v2 - Refactored)');

  loadEnvFile();

  const dbParams = parsePostgresUrl(process.env.SPRING_DATASOURCE_URL);
  const s3Bucket = process.env.S3_BUCKET;

  if (!s3Bucket) throw new Error('S3_BUCKET not set');

  console.log(`📦 Database: ${dbParams.host}/${dbParams.database}`);
  console.log(`☁️  S3 Bucket: ${s3Bucket}`);
  console.log(`📝 Config: max=${CONFIG.MAX_RECORDS || 'unlimited'}, skip=${CONFIG.SKIP_RECORDS}, debug=${CONFIG.DEBUG_MODE}`);

  // Initialize clients
  const pgClient = new Client(dbParams);
  await pgClient.connect();

  const s3Client = new S3Client({
    endpoint: process.env.S3_SERVER_URL,
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY_ID,
      secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_REGION || 'us-west-2',
    forcePathStyle: true
  });

  // Initialize helpers
  const jsonParser = new JsonParser(CONFIG.DEBUG_MODE);
  const bookMigrator = new BookMigrator(pgClient, CONFIG.DEBUG_MODE);

  // Statistics
  let processed = 0;
  let skipped = 0;
  let errors = 0;
  let continuationToken = null;

  do {
    const command = new ListObjectsV2Command({
      Bucket: s3Bucket,
      Prefix: CONFIG.PREFIX,
      MaxKeys: CONFIG.BATCH_SIZE,
      ContinuationToken: continuationToken
    });

    const result = await s3Client.send(command);
    if (!result.Contents) break;

    for (const obj of result.Contents) {
      const key = obj.Key;
      if (!key.endsWith('.json')) continue;

      // Handle skipping
      if (skipped < CONFIG.SKIP_RECORDS) {
        skipped++;
        continue;
      }

      // Check max limit
      if (CONFIG.MAX_RECORDS > 0 && processed >= CONFIG.MAX_RECORDS) {
        console.log(`\n✅ Reached max records limit: ${CONFIG.MAX_RECORDS}`);
        break;
      }

      try {
        // Fetch from S3
        const getCommand = new GetObjectCommand({ Bucket: s3Bucket, Key: key });
        const s3Object = await s3Client.send(getCommand);
        const bodyString = await streamToString(s3Object.Body);

        console.log(`\n📖 Processing: ${key}`);

        // Parse JSON (handles all corruption patterns)
        const books = jsonParser.parse(bodyString, key);
        console.log(`   Found ${books.length} unique book(s)`);

        // Extract base Google Books ID
        let baseId = key.split('/').pop().replace('.json', '');

        // Process each book
        for (let i = 0; i < books.length; i++) {
          const book = books[i];
          const googleBooksId = books.length > 1 ? `${baseId}_${i}` : baseId;

          await pgClient.query('BEGIN');

          try {
            const result = await bookMigrator.migrate(googleBooksId, book);
            await pgClient.query('COMMIT');
            console.log(`   ✅ Migrated: ${result.title} (${result.bookId})`);
          } catch (e) {
            await pgClient.query('ROLLBACK');
            console.error(`   ❌ Failed: ${e.message}`);
            if (CONFIG.DEBUG_MODE) {
              console.error(e.stack);
            }
            errors++;
          }
        }

        processed++;

      } catch (e) {
        console.error(`❌ Failed to process ${key}: ${e.message}`);
        errors++;
      }
    }

    continuationToken = result.NextContinuationToken;

  } while (continuationToken && (CONFIG.MAX_RECORDS === 0 || processed < CONFIG.MAX_RECORDS));

  // Refresh materialized view
  console.log('\n🔄 Refreshing search view...');
  await pgClient.query('SELECT refresh_book_search_view()');

  await pgClient.end();

  // Final summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 MIGRATION COMPLETE');
  console.log('='.repeat(60));
  console.log(`✅ Processed: ${processed}`);
  console.log(`❌ Errors: ${errors}`);
  console.log(`⏭️  Skipped: ${skipped}`);
  console.log('='.repeat(60));
}

// Run migration
if (require.main === module) {
  migrate().catch(e => {
    console.error('💥 Migration failed:', e);
    process.exit(1);
  });
}

module.exports = { JsonParser, BookMigrator };