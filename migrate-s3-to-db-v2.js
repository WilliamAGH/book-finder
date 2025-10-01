#!/usr/bin/env node
/**
 * S3 to PostgreSQL migration script - Refactored Version
 * Cleaner, flatter logic while handling all corruption patterns
 */

const { Client } = require('pg');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('node:fs');
const crypto = require('node:crypto');
const zlib = require('node:zlib');

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
// HELPERS
// ============================================================================

function normalizeCollectionName(name) {
  if (!name) return null;
  return name.toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function titleizeFromSlug(slug) {
  if (!slug) return '';
  return slug.split('-')
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function sanitizeIsbn(raw) {
  if (!raw) return null;
  const cleaned = raw.replace(/[^0-9Xx]/g, '').toUpperCase();
  return cleaned.length === 0 ? null : cleaned;
}

function extractIsoDate(value) {
  if (!value) return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  const datePart = trimmed.includes('T') ? trimmed.split('T')[0] : trimmed;
  return /^\d{4}-\d{2}-\d{2}$/.test(datePart) ? datePart : null;
}

function toIntegerOrNull(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

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
  const bytes = Buffer.alloc(16);

  // 48-bit Unix millisecond timestamp (big-endian) occupies bytes 0-5
  bytes[0] = Number((unixMillis >> 40n) & 0xffn);
  bytes[1] = Number((unixMillis >> 32n) & 0xffn);
  bytes[2] = Number((unixMillis >> 24n) & 0xffn);
  bytes[3] = Number((unixMillis >> 16n) & 0xffn);
  bytes[4] = Number((unixMillis >> 8n) & 0xffn);
  bytes[5] = Number(unixMillis & 0xffn);

  // Remaining bytes are random; overlay version/variant bits as required by RFC 9562 draft
  const random = crypto.randomBytes(10);
  random.copy(bytes, 6);

  bytes[6] = (bytes[6] & 0x0f) | 0x70; // version 7 in high nibble
  bytes[7] = (bytes[7] & 0x3f) | 0x80; // variant 2 (RFC 4122)

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
   * Find the start of valid JSON in a corrupted string
   */
  findJsonStart(str) {
    // First try to find any { or [
    const firstBrace = str.indexOf('{');
    const firstBracket = str.indexOf('[');

    let jsonStart = -1;
    if (firstBrace >= 0 && (firstBracket === -1 || firstBrace < firstBracket)) {
      jsonStart = firstBrace;
    } else if (firstBracket >= 0) {
      jsonStart = firstBracket;
    }

    // If we found a { or [, return it
    if (jsonStart >= 0 && jsonStart < 1000) {
      return { index: jsonStart, name: 'JSON start' };
    }

    // Otherwise look for specific patterns
    const patterns = [
      { pattern: '{"id":', name: 'id object' },
      { pattern: '{"kind":', name: 'kind object' },
      { pattern: '{"title":', name: 'title object' },
      { pattern: '{"volumeInfo":', name: 'volumeInfo object' },
      { pattern: '{ "id":', name: 'id object with space' },
      { pattern: '{ "kind":', name: 'kind object with space' },
      { pattern: '[{"', name: 'array of objects' }
    ];

    let bestMatch = { index: -1, name: '' };

    for (const { pattern, name } of patterns) {
      const idx = str.indexOf(pattern);
      if (idx >= 0 && (bestMatch.index === -1 || idx < bestMatch.index)) {
        bestMatch = { index: idx, name };
      }
    }

    return bestMatch;
  }

  /**
   * Main parse method - handles all corruption patterns
   */
  parse(bodyString, filename) {
    // Step 1: Validate input
    if (!bodyString || bodyString.length === 0) {
      throw new Error('Empty file');
    }

    // Remove UTF-8 BOM if present
    if (bodyString.charCodeAt(0) === 0xFEFF) {
      console.warn(`[WARNING] File ${filename} starts with BOM (\uFEFF), stripping it`);
      bodyString = bodyString.slice(1);
    }

    // Strip control characters (without embedding them literally in source)
    let hadControlChars = false;
    let cleanedBuilder = '';
    for (let i = 0; i < bodyString.length; i++) {
      const code = bodyString.charCodeAt(i);
      // Keep tab(9), LF(10), CR(13), printable (>=32, except DEL 127)
      if (code === 9 || code === 10 || code === 13 || (code >= 32 && code !== 127)) {
        cleanedBuilder += bodyString[i];
      } else {
        hadControlChars = true;
      }
    }
    if (hadControlChars) {
      console.warn(`[WARNING] File ${filename} contains control characters, stripping them`);
      bodyString = cleanedBuilder;
    }

    // Trim whitespace
    bodyString = bodyString.trim();

    // Check if it starts with valid JSON; if not, find the first likely start
    if (!bodyString.startsWith('{') && !bodyString.startsWith('[')) {
      const jsonMatch = this.findJsonStart(bodyString);
      if (jsonMatch.index === -1) {
        throw new Error('No valid JSON patterns found in file');
      }
      console.warn(`[WARNING] Found ${jsonMatch.name} at position ${jsonMatch.index}, stripping ${jsonMatch.index} bytes of corruption`);
      bodyString = bodyString.substring(jsonMatch.index);
    }

    // Step 2: Extract all top-level JSON fragments (handles concatenation and trailing junk)
    const fragments = this.extractJsonFragments(bodyString, filename);

    // Step 3: Parse each fragment and normalize to book objects
    const parsedObjects = [];
    for (const jsonStr of fragments) {
      try {
        const obj = JSON.parse(jsonStr);
        if (Array.isArray(obj)) {
          parsedObjects.push(...obj);
        } else if (obj && Array.isArray(obj.items)) {
          parsedObjects.push(...obj.items);
        } else {
          parsedObjects.push(obj);
        }
      } catch (e) {
        console.error(`[ERROR] Failed to parse JSON fragment: ${e.message}`);
        if (this.debug) {
          console.error(`[DEBUG] Fragment start: ${jsonStr.substring(0, 200)}...`);
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
   * Note: kept for backward compatibility but superseded by extractJsonFragments()
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
   * Extract all top-level JSON fragments from a string.
   * Robust against trailing garbage and multiple concatenated JSON values,
   * while respecting strings and escape sequences.
   */
  extractJsonFragments(bodyString, filename) {
    const fragments = [];
    let i = 0;
    const n = bodyString.length;

    const isWhitespace = (ch) => ch === ' ' || ch === '\n' || ch === '\r' || ch === '\t';

    while (i < n) {
      // Skip until next JSON start
      while (i < n && !('{['.includes(bodyString[i]))) {
        // If we encounter whitespace, just skip
        if (!isWhitespace(bodyString[i])) {
          // non-JSON byte outside fragments; will be ignored
        }
        i++;
      }
      if (i >= n) break;

      // Found start of fragment
      const start = i;
      let depth = 0;
      let inString = false;
      let escapeNext = false;

      for (; i < n; i++) {
        const ch = bodyString[i];

        if (inString) {
          if (escapeNext) {
            escapeNext = false;
          } else if (ch === '\\') {
            escapeNext = true;
          } else if (ch === '"') {
            inString = false;
          }
          continue;
        }

        if (ch === '"') {
          inString = true;
          continue;
        }

        if (ch === '{' || ch === '[') {
          depth++;
        } else if (ch === '}' || ch === ']') {
          depth--;
          if (depth === 0) {
            // End of this fragment
            const end = i + 1;
            fragments.push(bodyString.slice(start, end));
            i = end; // move cursor past this fragment
            break;
          }
        }
      }

      // If we reached end-of-string while still inside a fragment (truncated JSON)
      if (i >= n && depth > 0) {
        console.warn(`[WARNING] File ${filename} appears truncated; attempting to parse first ${Math.min(n - start, 200)} chars`);
        fragments.push(bodyString.slice(start));
        break;
      }
    }

    // Log if there was trailing junk
    const lastFragment = fragments[fragments.length - 1];
    if (lastFragment) {
      const tailIndex = bodyString.lastIndexOf(lastFragment) + lastFragment.length;
      const trailing = bodyString.slice(tailIndex);
      if (trailing.trim().length > 0) {
        console.warn(`[WARNING] File ${filename} contains trailing non-JSON data after a valid JSON value; ignoring trailing ${trailing.length} bytes`);
      }
    }

    // Fallback: if no fragments extracted, try legacy splitConcatenated
    if (fragments.length === 0) {
      return this.splitConcatenated(bodyString, filename);
    }

    return fragments;
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
    this.contextLabel = null;
  }

  setContextLabel(label) {
    this.contextLabel = label;
  }

  log(message) {
    if (this.contextLabel) {
      console.log(`   [#${this.contextLabel}] ${message}`);
    } else {
      console.log(`   ${message}`);
    }
  }

  /**
   * Migrate a single book to the database
   */
  async migrate(googleBooksId, bookData, contextLabel) {
    this.setContextLabel(contextLabel);
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
    await this.insertQualifierCollections(bookId, fields.qualifiers);

    const result = { bookId, title: fields.title };
    this.setContextLabel(null);
    return result;
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
      qualifiers: bookData.qualifiers || bookData.Qualifiers || {},

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
      listPriceAmount: saleInfo.listPrice?.amount ?? null,
      retailPriceAmount: saleInfo.retailPrice?.amount ?? null,
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
      this.log(`↻ Reusing existing book ${externalCheck.rows[0].book_id} via external id ${googleBooksId}`);
      this.setContextLabel(null);
      return externalCheck.rows[0].book_id;
    }

    // Check by ISBN
    let existingBook = null;

    if (fields.isbn13) {
      const res = await this.client.query(
        'SELECT id FROM books WHERE isbn13 = $1 LIMIT 1',
        [fields.isbn13]
      );
      if (res.rows.length > 0) {
        this.log(`↻ Reusing existing book ${res.rows[0].id} via ISBN13 ${fields.isbn13}`);
        this.setContextLabel(null);
        existingBook = res.rows[0];
      }
    }

    if (!existingBook && fields.isbn10) {
      const res = await this.client.query(
        'SELECT id FROM books WHERE isbn10 = $1 LIMIT 1',
        [fields.isbn10]
      );
      if (res.rows.length > 0) {
        this.log(`↻ Reusing existing book ${res.rows[0].id} via ISBN10 ${fields.isbn10}`);
        this.setContextLabel(null);
        existingBook = res.rows[0];
      }
    }

    if (existingBook) {
      this.setContextLabel(null);
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

    this.log(`✨ Inserted canonical book ${bookId} (slug ${slug})`);

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
    const existingExternalId = await this.client.query(
      `SELECT id FROM book_external_ids
       WHERE source = 'GOOGLE_BOOKS' AND external_id = $1
       LIMIT 1`,
      [googleBooksId]
    );

    const externalIdReuse = existingExternalId.rows.length > 0;

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
        this.log(`[INFO] ISBN13 ${fields.isbn13} already linked via external ID ${existingIsbn13.rows[0].external_id}`);
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
        this.log(`[INFO] ISBN10 ${fields.isbn10} already linked via external ID ${existingIsbn10.rows[0].external_id}`);
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
        book_id = EXCLUDED.book_id,
        info_link = COALESCE(EXCLUDED.info_link, book_external_ids.info_link),
        preview_link = COALESCE(EXCLUDED.preview_link, book_external_ids.preview_link),
        web_reader_link = COALESCE(EXCLUDED.web_reader_link, book_external_ids.web_reader_link),
        canonical_volume_link = COALESCE(EXCLUDED.canonical_volume_link, book_external_ids.canonical_volume_link),
        average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating),
        ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count),
        list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price),
        retail_price = COALESCE(EXCLUDED.retail_price, book_external_ids.retail_price),
        currency_code = COALESCE(EXCLUDED.currency_code, book_external_ids.currency_code)`,
      [
        generateNanoId(10), bookId, 'GOOGLE_BOOKS', googleBooksId,
        providerIsbn10, providerIsbn13, 
        this.normalizeToHttps(fields.infoLink), 
        this.normalizeToHttps(fields.previewLink),
        this.normalizeToHttps(fields.webReaderLink), 
        this.normalizeToHttps(fields.canonicalVolumeLink), 
        fields.averageRating,
        fields.ratingsCount, fields.isEbook, fields.pdfAvailable,
        fields.epubAvailable, fields.embeddable, fields.publicDomain,
        fields.viewability, fields.textReadable, fields.imageReadable,
        fields.printType, fields.maturityRating, fields.contentVersion,
        fields.textToSpeechPermission, fields.saleability, fields.country,
        fields.listPriceAmount, fields.retailPriceAmount, fields.currencyCode
      ]
    );

    if (externalIdReuse) {
      this.log(`↻ Reusing existing book_external_ids row for ${googleBooksId}`);
    } else {
      this.log(`✨ Added book_external_ids row for ${googleBooksId}`);
    }
  }

  /**
   * Insert raw JSON data
   */
  async insertRawData(bookId, bookData) {
    const existingRaw = await this.client.query(
      `SELECT id FROM book_raw_data
       WHERE book_id = $1::uuid AND source = $2
       LIMIT 1`,
      [bookId, 'GOOGLE_BOOKS']
    );

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

    if (existingRaw.rows.length > 0) {
      this.log(`↻ Reusing existing book_raw_data row for book ${bookId}`);
    } else {
      this.log(`✨ Added book_raw_data row for book ${bookId}`);
    }
  }

  /**
   * Insert image links
   */
  async insertImageLinks(bookId, imageLinks) {
    if (!imageLinks || Object.keys(imageLinks).length === 0) return;

    for (const [imageType, url] of Object.entries(imageLinks)) {
      if (!url) continue;
      
      // Normalize HTTP to HTTPS for Google Books URLs
      const normalizedUrl = url.startsWith('http://') ? url.replace('http://', 'https://') : url;

      const existingImage = await this.client.query(
        `SELECT id FROM book_image_links
         WHERE book_id = $1::uuid AND image_type = $2
         LIMIT 1`,
        [bookId, imageType]
      );

      await this.client.query(
        `INSERT INTO book_image_links (
          id, book_id, image_type, url, source, created_at
        ) VALUES (
          $1, $2::uuid, $3, $4, $5, NOW()
        )
        ON CONFLICT (book_id, image_type) DO UPDATE SET
          url = EXCLUDED.url`,
        [generateNanoId(10), bookId, imageType, normalizedUrl, 'GOOGLE_BOOKS']
      );

      if (existingImage.rows.length > 0) {
        this.log(`↻ Reusing existing book_image_links row (${imageType}) for book ${bookId}`);
      } else {
        this.log(`✨ Added book_image_links row (${imageType}) for book ${bookId}`);
      }
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

    const existingDimensions = await this.client.query(
      `SELECT book_id FROM book_dimensions
       WHERE book_id = $1::uuid
       LIMIT 1`,
      [bookId]
    );

    await this.client.query(
      `INSERT INTO book_dimensions (
        book_id, height, width, thickness, created_at, updated_at
      ) VALUES (
        $1::uuid, $2, $3, $4, NOW(), NOW()
      )
      ON CONFLICT (book_id) DO UPDATE SET
        height = COALESCE(EXCLUDED.height, book_dimensions.height),
        width = COALESCE(EXCLUDED.width, book_dimensions.width),
        thickness = COALESCE(EXCLUDED.thickness, book_dimensions.thickness),
        updated_at = NOW()`,
      [bookId, height, width, thickness]
    );

    if (existingDimensions.rows.length > 0) {
      this.log(`↻ Reusing existing book_dimensions row for book ${bookId}`);
    } else {
      this.log(`✨ Added book_dimensions row for book ${bookId}`);
    }
  }

  /**
   * Insert authors
   */
  async insertAuthors(bookId, authors) {
    if (!authors || authors.length === 0) return;

    for (let i = 0; i < authors.length; i++) {
      const authorName = authors[i];
      if (!authorName?.trim()) continue;

      const normalizedName = this.normalizeAuthorName(authorName);
      const existingAuthor = await this.client.query(
        'SELECT id FROM authors WHERE name = $1 LIMIT 1',
        [authorName]
      );

      // Insert or get author
      const authorResult = await this.client.query(
        `INSERT INTO authors (id, name, normalized_name, created_at, updated_at)
         VALUES ($1, $2, $3, NOW(), NOW())
         ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
         RETURNING id`,
        [generateNanoId(10), authorName, normalizedName]
      );

      const authorId = authorResult.rows[0].id;

      if (existingAuthor.rows.length > 0) {
        this.log(`↻ Reusing existing author ${authorId} (${authorName})`);
      } else {
        this.log(`✨ Inserted author ${authorId} (${authorName})`);
      }

      const existingJoin = await this.client.query(
        `SELECT id FROM book_authors_join
         WHERE book_id = $1::uuid AND author_id = $2
         LIMIT 1`,
        [bookId, authorId]
      );

      // Link book to author
      await this.client.query(
        `INSERT INTO book_authors_join (id, book_id, author_id, position, created_at)
         VALUES ($1, $2::uuid, $3, $4, NOW())
         ON CONFLICT (book_id, author_id) DO UPDATE SET position = EXCLUDED.position`,
        [generateNanoId(12), bookId, authorId, i]
      );

      if (existingJoin.rows.length > 0) {
        this.log(`↻ Reusing existing book_authors_join row for author ${authorId} and book ${bookId}`);
      } else {
        this.log(`✨ Linked author ${authorId} to book ${bookId}`);
      }
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
   * Normalize HTTP URLs to HTTPS for security
   */
  normalizeToHttps(url) {
    if (!url) return url;
    return url.startsWith('http://') ? url.replace('http://', 'https://') : url;
  }

  /**
   * Insert categories
   */
  async insertCategories(bookId, categories) {
    if (!categories || categories.length === 0) return;

    for (const categoryName of categories) {
      if (!categoryName?.trim()) continue;

      const normalized = normalizeCollectionName(categoryName);
      const existingCategory = await this.client.query(
        `SELECT id FROM book_collections
         WHERE collection_type = 'CATEGORY' AND source = 'GOOGLE_BOOKS' AND normalized_name = $1
         LIMIT 1`,
        [normalized]
      );

      let collectionId;
      if (existingCategory.rows.length > 0) {
        collectionId = existingCategory.rows[0].id;
        this.log(`↻ Reusing category collection ${collectionId} (${categoryName})`);
        await this.client.query(
          `UPDATE book_collections
           SET display_name = $1, updated_at = NOW()
           WHERE id = $2`,
          [categoryName, collectionId]
        );
      } else {
        const insertedCategory = await this.client.query(
          `INSERT INTO book_collections (
            id, collection_type, source, display_name, normalized_name,
            created_at, updated_at
          ) VALUES (
            $1, 'CATEGORY', 'GOOGLE_BOOKS', $2, $3, NOW(), NOW()
          )
          RETURNING id`,
          [generateNanoId(8), categoryName, normalized]
        );
        collectionId = insertedCategory.rows[0].id;
        this.log(`✨ Created category collection ${collectionId} (${categoryName})`);
      }

      const existingJoin = await this.client.query(
        `SELECT id FROM book_collections_join
         WHERE collection_id = $1 AND book_id = $2::uuid
         LIMIT 1`,
        [collectionId, bookId]
      );

      await this.client.query(
        `INSERT INTO book_collections_join (
            id, collection_id, book_id, created_at, updated_at
         ) VALUES (
            $1, $2, $3::uuid, NOW(), NOW()
         )
         ON CONFLICT (collection_id, book_id) DO UPDATE SET
            updated_at = NOW()`,
        [generateNanoId(12), collectionId, bookId]
      );

      if (existingJoin.rows.length > 0) {
        this.log(`↻ Reusing category membership for collection ${collectionId} → book ${bookId}`);
      } else {
        this.log(`✨ Linked book ${bookId} to category ${collectionId}`);
      }
    }
  }

  async insertQualifierCollections(bookId, qualifiers) {
    if (!qualifiers || Object.keys(qualifiers).length === 0) {
      return;
    }

    const nytQualifier = qualifiers.nytBestseller;
    if (!nytQualifier) {
      return;
    }

    const qualifierDetails = typeof nytQualifier === 'object' && nytQualifier !== null ? nytQualifier : {};
    const listCodeRaw = qualifierDetails.list || qualifierDetails.listCode || 'nyt-hardcover-fiction';
    const normalizedList = normalizeCollectionName(listCodeRaw) || 'nyt-bestsellers';
    let resolvedDisplay = qualifierDetails.displayName && qualifierDetails.displayName.trim().length > 0
      ? qualifierDetails.displayName.trim()
      : titleizeFromSlug(normalizedList);
    if (!resolvedDisplay.toLowerCase().startsWith('nyt')) {
      resolvedDisplay = `NYT ${resolvedDisplay}`;
    }
    const displayName = resolvedDisplay.trim();

    const rank = Number.isFinite(qualifierDetails.rank) ? qualifierDetails.rank : (Number.isFinite(qualifierDetails.position) ? qualifierDetails.position : null);
    const weeksOnList = Number.isFinite(qualifierDetails.weeksOnList) ? qualifierDetails.weeksOnList : null;

    const existingCollection = await this.client.query(
      `SELECT id FROM book_collections
       WHERE collection_type = 'BESTSELLER_LIST' AND source = 'NYT' AND normalized_name = $1
       LIMIT 1`,
      [normalizedList]
    );

    let collectionId;
    if (existingCollection.rows.length > 0) {
      collectionId = existingCollection.rows[0].id;
      this.log(`↻ Reusing NYT qualifier collection ${collectionId} (${displayName})`);
      await this.client.query(
        `UPDATE book_collections
         SET provider_list_code = $1,
             display_name = $2,
             updated_at = NOW()
         WHERE id = $3`,
        [normalizedList, displayName, collectionId]
      );
    } else {
      const inserted = await this.client.query(
        `INSERT INTO book_collections (
             id, collection_type, source, provider_list_code, display_name, normalized_name,
             created_at, updated_at
         ) VALUES (
             $1, 'BESTSELLER_LIST', 'NYT', $2, $3, $4, NOW(), NOW()
         )
         RETURNING id`,
        [generateNanoId(8), normalizedList, displayName, normalizedList]
      );
      collectionId = inserted.rows[0].id;
      this.log(`✨ Created NYT qualifier collection ${collectionId} (${displayName})`);
    }

    const existingJoin = await this.client.query(
      `SELECT id FROM book_collections_join
       WHERE collection_id = $1 AND book_id = $2::uuid
       LIMIT 1`,
      [collectionId, bookId]
    );

    await this.client.query(
      `INSERT INTO book_collections_join (
            id, collection_id, book_id, position, weeks_on_list, created_at, updated_at
       ) VALUES (
            $1, $2, $3::uuid, $4, $5, NOW(), NOW()
       )
       ON CONFLICT (collection_id, book_id) DO UPDATE SET
            position = COALESCE(EXCLUDED.position, book_collections_join.position),
            weeks_on_list = COALESCE(EXCLUDED.weeks_on_list, book_collections_join.weeks_on_list),
            updated_at = NOW()`,
      [generateNanoId(12), collectionId, bookId, rank ?? null, weeksOnList ?? null]
    );

    if (existingJoin.rows.length > 0) {
      this.log(`↻ Reusing NYT qualifier membership for book ${bookId}`);
    } else {
      this.log(`✨ Linked book ${bookId} to NYT qualifier list ${collectionId}`);
    }
  }
}

class NytListMigrator {
  constructor(client, debugMode = false) {
    this.client = client;
    this.debug = debugMode;
  }

  async migrateList(s3Key, bodyString, ordinal) {
    console.log(`\n📚 Processing list [#L${ordinal}]: ${s3Key}`);

    let listJson;
    try {
      listJson = JSON.parse(bodyString);
    } catch (error) {
      console.error(`   ❌ Failed to parse NYT list JSON (${s3Key}): ${error.message}`);
      throw error;
    }

    const listCodeRaw = listJson.list_name_encoded || listJson.listNameEncoded || null;
    if (!listCodeRaw) {
      console.warn(`   ⚠️ [#L${ordinal}] Skipping list without list_name_encoded field.`);
      return;
    }

    const normalizedList = normalizeCollectionName(listCodeRaw) || normalizeCollectionName(listJson.display_name || listCodeRaw);
    const providerListId = listJson.list_id || listJson.listId || null;
    const displayName = listJson.display_name || titleizeFromSlug(normalizedList || listCodeRaw);
    const description = listJson.list_name || displayName;
    const bestsellersDate = extractIsoDate(listJson.bestsellers_date) || extractIsoDate(listJson.bestsellersDate);
    const effectivePublishedDate = extractIsoDate(listJson.published_date) || extractIsoDate(listJson.publishedDate) || bestsellersDate;
    const rawJson = JSON.stringify(listJson);

    const collectionId = await this.upsertCollection({
      providerListId,
      listCode: listCodeRaw,
      normalizedList,
      displayName,
      description,
      bestsellersDate,
      publishedDate: effectivePublishedDate,
      rawJson,
      ordinal
    });

    const booksArray = Array.isArray(listJson.books) ? listJson.books : [];
    if (booksArray.length === 0) {
      console.log(`   ℹ️  [#L${ordinal}] List contains no book entries.`);
      return;
    }

    let linked = 0;
    let skipped = 0;
    let entry = 0;
    for (const bookNode of booksArray) {
      entry += 1;
      const linkedBook = await this.upsertMembership(collectionId, normalizedList, bookNode, `${ordinal}.${entry}`);
      if (linkedBook) {
        linked += 1;
      } else {
        skipped += 1;
      }
    }

    console.log(`   ✅ [#L${ordinal}] Linked ${linked} book(s); ${skipped} skipped (missing canonical entry).`);
  }

  async upsertCollection({ providerListId, listCode, normalizedList, displayName, description, bestsellersDate, publishedDate, rawJson, ordinal }) {
    const insertId = generateNanoId(8);
    let _inserted = false;

    const insertResult = await this.client.query(
      `INSERT INTO book_collections (
          id, collection_type, source, provider_list_id, provider_list_code,
          display_name, normalized_name, description, bestsellers_date, published_date,
          raw_data_json, created_at, updated_at
      ) VALUES (
          $1, 'BESTSELLER_LIST', 'NYT', $2, $3, $4, $5, $6,
          $7::date, $8::date, $9::jsonb, NOW(), NOW()
      )
      ON CONFLICT (source, provider_list_code, published_date) DO NOTHING
      RETURNING id`,
      [insertId, providerListId, listCode, displayName, normalizedList, description, bestsellersDate, publishedDate, rawJson]
    );

    let collectionId;
    if (insertResult.rowCount > 0) {
      _inserted = true;
      collectionId = insertResult.rows[0].id;
      console.log(`   ✨ [#L${ordinal}] Created bestseller collection ${displayName} (${listCode}) → ${collectionId}`);
    } else {
      const existing = await this.client.query(
        `SELECT id FROM book_collections
         WHERE source = 'NYT' AND provider_list_code = $1 AND published_date = $2::date
         LIMIT 1`,
        [listCode, publishedDate]
      );
      if (existing.rowCount === 0) {
        throw new Error(`Unable to resolve collection id for list ${listCode} (${publishedDate}).`);
      }
      collectionId = existing.rows[0].id;
      console.log(`   ↻ [#L${ordinal}] Reusing bestseller collection ${displayName} (${listCode}) → ${collectionId}`);
    }

    await this.client.query(
      `UPDATE book_collections
       SET provider_list_id = $1,
           display_name = $2,
           normalized_name = $3,
           description = $4,
           bestsellers_date = $5::date,
           published_date = $6::date,
           raw_data_json = $7::jsonb,
           updated_at = NOW()
       WHERE id = $8`,
      [providerListId, displayName, normalizedList, description, bestsellersDate, publishedDate, rawJson, collectionId]
    );

    return collectionId;
  }

  async upsertMembership(collectionId, normalizedList, bookNode, ordinalLabel) {
    const isbn13 = sanitizeIsbn(bookNode.primary_isbn13 || bookNode.primaryIsbn13);
    const isbn10 = sanitizeIsbn(bookNode.primary_isbn10 || bookNode.primaryIsbn10);
    const canonicalId = await this.resolveCanonicalBookId(isbn13, isbn10);

    if (!canonicalId) {
      const identifier = isbn13 || isbn10 || 'unknown ISBN';
      console.warn(`   ⚠️ [#L${ordinalLabel}] Skipping NYT entry; canonical book not found for ${identifier}.`);
      return false;
    }

    const rank = toIntegerOrNull(bookNode.rank);
    const weeksOnList = toIntegerOrNull(bookNode.weeks_on_list || bookNode.weeksOnList);
    const rankLastWeek = toIntegerOrNull(bookNode.rank_last_week || bookNode.rankLastWeek);
    const peakPosition = toIntegerOrNull(bookNode.audiobook_rank || bookNode.audiobookRank);
    const providerRef = bookNode.amazon_product_url || bookNode.amazonProductUrl || null;
    const rawItemJson = JSON.stringify(bookNode);

    const membershipId = generateNanoId(12);
    const insertResult = await this.client.query(
      `INSERT INTO book_collections_join (
          id, collection_id, book_id, position, weeks_on_list, rank_last_week,
          peak_position, provider_isbn13, provider_isbn10, provider_book_ref,
          raw_item_json, created_at, updated_at
       ) VALUES (
          $1, $2, $3::uuid, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, NOW(), NOW()
       )
       ON CONFLICT (collection_id, book_id) DO NOTHING
       RETURNING id`,
      [membershipId, collectionId, canonicalId, rank ?? null, weeksOnList ?? null, rankLastWeek ?? null, peakPosition ?? null, isbn13, isbn10, providerRef, rawItemJson]
    );

    if (insertResult.rowCount > 0) {
      console.log(`   ✨ [#L${ordinalLabel}] Added book ${canonicalId} to NYT ${normalizedList} (rank ${rank ?? 'n/a'})`);
    } else {
      console.log(`   ↻ [#L${ordinalLabel}] Reusing bestseller membership for book ${canonicalId} (rank ${rank ?? 'n/a'})`);
    }

    await this.client.query(
      `UPDATE book_collections_join
       SET position = $3,
           weeks_on_list = $4,
           rank_last_week = $5,
           peak_position = $6,
           provider_isbn13 = $7,
           provider_isbn10 = $8,
           provider_book_ref = $9,
           raw_item_json = $10::jsonb,
           updated_at = NOW()
       WHERE collection_id = $1 AND book_id = $2::uuid`,
      [collectionId, canonicalId, rank ?? null, weeksOnList ?? null, rankLastWeek ?? null, peakPosition ?? null, isbn13, isbn10, providerRef, rawItemJson]
    );

    return true;
  }

  async resolveCanonicalBookId(isbn13, isbn10) {
    if (isbn13) {
      const bookByIsbn13 = await this.client.query('SELECT id FROM books WHERE isbn13 = $1 LIMIT 1', [isbn13]);
      if (bookByIsbn13.rowCount > 0) {
        return bookByIsbn13.rows[0].id;
      }
      const externalByIsbn13 = await this.client.query(
        `SELECT book_id FROM book_external_ids
         WHERE provider_isbn13 = $1 OR external_id = $1
         ORDER BY created_at DESC
         LIMIT 1`,
        [isbn13]
      );
      if (externalByIsbn13.rowCount > 0) {
        return externalByIsbn13.rows[0].book_id;
      }
    }

    if (isbn10) {
      const bookByIsbn10 = await this.client.query('SELECT id FROM books WHERE isbn10 = $1 LIMIT 1', [isbn10]);
      if (bookByIsbn10.rowCount > 0) {
        return bookByIsbn10.rows[0].id;
      }
      const externalByIsbn10 = await this.client.query(
        `SELECT book_id FROM book_external_ids
         WHERE provider_isbn10 = $1 OR external_id = $1
         ORDER BY created_at DESC
         LIMIT 1`,
        [isbn10]
      );
      if (externalByIsbn10.rowCount > 0) {
        return externalByIsbn10.rows[0].book_id;
      }
    }

    return null;
  }
}

async function migrateNytLists({ s3Client, bucket, batchSize, migrator }) {
  let continuationToken = null;
  let processed = 0;
  let errors = 0;

  do {
    const command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: 'lists/nyt/',
      MaxKeys: batchSize,
      ContinuationToken: continuationToken
    });

    const result = await s3Client.send(command);
    if (!result.Contents || result.Contents.length === 0) {
      break;
    }

    for (const obj of result.Contents) {
      const key = obj.Key;
      if (!key.endsWith('.json')) {
        continue;
      }

      try {
        const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
        const s3Object = await s3Client.send(getCommand);
        const bodyString = await streamToString(s3Object.Body, {
          key,
          contentEncoding: s3Object.ContentEncoding,
          contentType: s3Object.ContentType
        });
        const ordinal = processed + 1;
        await migrator.migrateList(key, bodyString, ordinal);
        processed += 1;
      } catch (error) {
        console.error(`❌ Failed to process NYT list ${key}: ${error.message}`);
        if (migrator.debug && error.stack) {
          console.error(error.stack);
        }
        errors += 1;
      }
    }

    continuationToken = result.NextContinuationToken;
  } while (continuationToken);

  return { processed, errors };
}

// ============================================================================
// MAIN MIGRATION LOGIC
// ============================================================================

async function streamToString(stream, options = {}) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  let buffer = Buffer.concat(chunks);
  const encoding = (options.contentEncoding || '').toLowerCase();
  const contentType = (options.contentType || '').toLowerCase();
  const key = options.key || 'unknown';

  const looksCompressed =
    encoding.includes('gzip') ||
    encoding.includes('x-gzip') ||
    contentType === 'application/x-gzip' ||
    (buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b);

  if (looksCompressed) {
    try {
      buffer = zlib.gunzipSync(buffer);
    } catch (err) {
      throw new Error(`Unable to gunzip S3 object ${key}: ${err.message}`);
    }
  }

  return buffer.toString('utf-8');
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
  const sslMode = (parsed.searchParams.get('sslmode') || '').toLowerCase();
  const allowInsecure = process.env.PGSSL_ALLOW_INSECURE === 'true';

  const buildSecureSslConfig = () => {
    const sslConfig = { rejectUnauthorized: true };
    const rootCertPath = process.env.PGSSLROOTCERT;
    if (rootCertPath) {
      try {
        sslConfig.ca = fs.readFileSync(rootCertPath);
      } catch (err) {
        console.warn(`[DB] Failed to load PGSSLROOTCERT from ${rootCertPath}: ${err.message}`);
      }
    }
    return sslConfig;
  };

  let ssl;
  if (['require', 'verify-full', 'verify-ca'].includes(sslMode)) {
    ssl = buildSecureSslConfig();
  } else if (sslMode === 'disable') {
    ssl = allowInsecure ? false : buildSecureSslConfig();
  } else if (sslMode === 'prefer' || sslMode === 'allow') {
    ssl = allowInsecure ? false : buildSecureSslConfig();
  } else {
    // Default to secure unless explicitly opted out for local development
    ssl = allowInsecure ? false : buildSecureSslConfig();
  }

  return {
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : 5432,
    database: parsed.pathname.slice(1) || 'postgres',
    user: parsed.username,
    password: parsed.password,
    ssl
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
  const nytListMigrator = new NytListMigrator(pgClient, CONFIG.DEBUG_MODE);

  // Statistics
  let processedBooks = 0;
  let processedLists = 0;
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

      // Only apply skip/limit logic to canonical book objects
      if (key.startsWith('books/')) {
        if (skipped < CONFIG.SKIP_RECORDS) {
          skipped++;
          continue;
        }

        if (CONFIG.MAX_RECORDS > 0 && processedBooks >= CONFIG.MAX_RECORDS) {
          console.log(`\n✅ Reached max records limit: ${CONFIG.MAX_RECORDS}`);
          break;
        }
      }

      try {
        // Fetch from S3
        const getCommand = new GetObjectCommand({ Bucket: s3Bucket, Key: key });
        const s3Object = await s3Client.send(getCommand);
        const bodyString = await streamToString(s3Object.Body, {
          key,
          contentEncoding: s3Object.ContentEncoding,
          contentType: s3Object.ContentType
        });

        if (key.startsWith('books/')) {
          const recordIndex = processedBooks + 1;
          console.log(`\n📖 Processing [#${recordIndex}]: ${key}`);

          const books = jsonParser.parse(bodyString, key);
          console.log(`   Found ${books.length} unique book(s)`);

          let baseId = key.split('/').pop().replace('.json', '');

          for (let i = 0; i < books.length; i++) {
            const book = books[i];
            const googleBooksId = books.length > 1 ? `${baseId}_${i}` : baseId;
            const bookOrdinalLabel = books.length > 1 ? `${recordIndex}.${i + 1}` : `${recordIndex}`;

            await pgClient.query('BEGIN');

            try {
            const result = await bookMigrator.migrate(googleBooksId, book, bookOrdinalLabel);
              await pgClient.query('COMMIT');
              console.log(`   ✅ [#${bookOrdinalLabel}] Migrated: ${result.title} (${result.bookId})`);
            } catch (e) {
              await pgClient.query('ROLLBACK');
              console.error(`   ❌ Failed: ${e.message}`);
              if (CONFIG.DEBUG_MODE) {
                console.error(e.stack);
              }
              errors++;
            }
          }

          processedBooks++;
        }

      } catch (e) {
        console.error(`❌ Failed to process ${key}: ${e.message}`);
        errors++;
      }
    }

    continuationToken = result.NextContinuationToken;

  } while (continuationToken && (CONFIG.MAX_RECORDS === 0 || processedBooks < CONFIG.MAX_RECORDS));

  // Process NYT list snapshots
  const listResult = await migrateNytLists({
    s3Client,
    bucket: s3Bucket,
    batchSize: CONFIG.BATCH_SIZE,
    migrator: nytListMigrator
  });
  processedLists += listResult.processed;
  errors += listResult.errors;

  // Refresh materialized view
  console.log('\n🔄 Refreshing search view...');
  await pgClient.query('SELECT refresh_book_search_view()');

  await pgClient.end();

  // Final summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 MIGRATION COMPLETE');
  console.log('='.repeat(60));
  console.log(`✅ Book files processed: ${processedBooks}`);
  console.log(`📚 NYT list files processed: ${processedLists}`);
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
