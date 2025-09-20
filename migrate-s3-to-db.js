#!/usr/bin/env node
/**
 * S3 to PostgreSQL migration script for new normalized schema
 * Generates proper IDs (UUIDv7 for books, NanoID for others)
 * Populates all normalized tables correctly
 */

const { Client } = require('pg');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('node:fs');
const crypto = require('node:crypto');
const path = require('node:path');

// Parse command line args
const args = process.argv.slice(2);
const getArg = (name, defaultValue) => {
  const arg = args.find(a => a.startsWith(`--${name}=`));
  return arg ? arg.split('=')[1] : defaultValue;
};

const MAX_RECORDS = parseInt(getArg('max', '0'));
const SKIP_RECORDS = parseInt(getArg('skip', '0'));
const PREFIX = getArg('prefix', 'books/v1/');
const DEBUG_MODE = getArg('debug', 'false') === 'true';

// ============================================================================
// ID GENERATION (matching Java IdGenerator)
// ============================================================================

// NanoID generation
const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

function generateNanoId(size = 10) {
  const bytes = crypto.randomBytes(size);
  let id = '';
  for (let i = 0; i < size; i++) {
    id += ALPHABET[bytes[i] % ALPHABET.length];
  }
  return id;
}

// UUIDv7 generation (time-ordered)
function generateUUIDv7() {
  const unixMillis = BigInt(Date.now());
  const timeHigh = Number((unixMillis >> 20n) & 0xffffffffn);
  const timeMid = Number((unixMillis >> 4n) & 0xffffn);
  const timeLow = Number(unixMillis & 0xfn);

  const random = crypto.randomBytes(10); // 80 bits of randomness

  const bytes = Buffer.alloc(16);
  bytes.writeUInt32BE(timeHigh, 0);            // 32 bits
  bytes.writeUInt16BE(timeMid, 4);             // next 16 bits

  // timeLow (4 bits) + version (4 bits => 0111)
  bytes[6] = ((timeLow & 0x0f) | 0x70);
  bytes[7] = random[0];                       // remaining 8 bits of randA

  // Variant 2 bits 10xxxxxx
  bytes[8] = (random[1] & 0x3f) | 0x80;
  random.copy(bytes, 9, 2);                   // rest of random bytes

  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

// Slug generation (simplified version)
function generateSlug(title, firstAuthor) {
  if (!title) return null;

  let slug = title.toLowerCase();

  // Basic replacements
  slug = slug.replace(/&/g, 'and');
  slug = slug.replace(/['"'""\u201C\u201D]/g, '');
  slug = slug.replace(/[^a-z0-9\s-]/g, '');
  slug = slug.replace(/[\s_]+/g, '-');
  slug = slug.replace(/-+/g, '-');
  slug = slug.replace(/^-+|-+$/g, '');

  // Truncate to 60 chars for title part
  if (slug.length > 60) {
    const lastDash = slug.lastIndexOf('-', 60);
    if (lastDash > 30) {
      slug = slug.substring(0, lastDash);
    } else {
      slug = slug.substring(0, 60);
    }
  }

  // Add author if provided
  if (firstAuthor) {
    let authorSlug = firstAuthor.toLowerCase();
    authorSlug = authorSlug.replace(/[^a-z0-9\s-]/g, '');
    authorSlug = authorSlug.replace(/[\s_]+/g, '-');
    authorSlug = authorSlug.replace(/-+/g, '-');
    authorSlug = authorSlug.replace(/^-+|-+$/g, '');

    if (authorSlug.length > 30) {
      const lastDash = authorSlug.lastIndexOf('-', 30);
      if (lastDash > 15) {
        authorSlug = authorSlug.substring(0, lastDash);
      } else {
        authorSlug = authorSlug.substring(0, 30);
      }
    }

    slug = `${slug}-${authorSlug}`;
  }

  // Final truncation to 100 chars
  if (slug.length > 100) {
    const lastDash = slug.lastIndexOf('-', 100);
    if (lastDash > 50) {
      slug = slug.substring(0, lastDash);
    } else {
      slug = slug.substring(0, 100);
    }
  }

  return slug || 'book';
}

const DIACRITICS_REGEX = /[\u0300-\u036f]/g;
const EDITION_PATTERN = /(\d+)(?:st|nd|rd|th)?\s*(?:edition|ed\b)/i;

function normalizeForKey(value) {
  if (!value) return '';
  return value
    .normalize('NFKD')
    .replace(DIACRITICS_REGEX, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function buildEditionGroupKey(title, authors = []) {
  const normalizedTitle = normalizeForKey(title);
  if (!normalizedTitle) {
    return null;
  }
  const primaryAuthor = Array.isArray(authors) && authors.length > 0 ? normalizeForKey(authors[0]) : '';
  return primaryAuthor ? `${normalizedTitle}__${primaryAuthor}` : normalizedTitle;
}

function parseEditionCandidate(candidate) {
  if (candidate === null || candidate === undefined) return null;
  if (typeof candidate === 'number' && Number.isFinite(candidate)) {
    const value = Math.floor(candidate);
    return value > 0 ? value : null;
  }
  if (typeof candidate === 'string') {
    const trimmed = candidate.trim();
    if (!trimmed) return null;

    const directNumber = trimmed.match(/^(\d{1,3})$/);
    if (directNumber) {
      const value = parseInt(directNumber[1], 10);
      return value > 0 ? value : null;
    }

    const editionMatch = trimmed.match(EDITION_PATTERN);
    if (editionMatch) {
      const value = parseInt(editionMatch[1], 10);
      return value > 0 ? value : null;
    }

    const trailingDigits = trimmed.match(/(\d+)(?:\.\d+)*$/);
    if (trailingDigits) {
      const value = parseInt(trailingDigits[1], 10);
      return value > 0 ? value : null;
    }
  }
  return null;
}

function extractEditionNumber(volumeInfo = {}, metadata = {}, sourceData = {}) {
  const candidates = [];

  const pushCandidate = (value) => {
    const parsed = parseEditionCandidate(value);
    if (parsed !== null && parsed !== undefined) {
      candidates.push(parsed);
    }
  };

  pushCandidate(volumeInfo.edition);
  pushCandidate(volumeInfo.editionInformation);
  pushCandidate(volumeInfo.editionInfo);
  pushCandidate(volumeInfo.contentVersion);
  pushCandidate(metadata?.editionNumber ?? metadata?.edition_number);
  pushCandidate(metadata?.edition);
  pushCandidate(metadata?.['edition-number']);
  pushCandidate(sourceData?.editionNumber ?? sourceData?.edition);

  if (typeof volumeInfo.subtitle === 'string') {
    pushCandidate(volumeInfo.subtitle);
  }
  if (typeof volumeInfo.title === 'string') {
    pushCandidate(volumeInfo.title);
  }
  if (typeof sourceData?.title === 'string') {
    pushCandidate(sourceData.title);
  }

  return candidates.length > 0 ? candidates[0] : null;
}

// ============================================================================
// ERROR LOGGING
// ============================================================================

class ErrorLogger {
  constructor(logDir = './migration-logs') {
    this.logDir = logDir;
    this.timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    this.logFile = path.join(logDir, `migration-errors-${this.timestamp}.log`);
    this.summaryFile = path.join(logDir, `migration-summary-${this.timestamp}.json`);

    // Create log directory if it doesn't exist
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }

    // Initialize log file with header
    const header = `==============================================\nMigration Error Log\nStarted: ${new Date().toISOString()}\n==============================================\n\n`;
    fs.writeFileSync(this.logFile, header);
  }

  logError(key, error, context = {}) {
    const timestamp = new Date().toISOString();
    const errorEntry = `[${timestamp}] ERROR in ${key}\nMessage: ${error.message}\nContext: ${JSON.stringify(context, null, 2)}\nStack: ${error.stack || 'N/A'}\n${'='.repeat(50)}\n\n`;

    fs.appendFileSync(this.logFile, errorEntry);
  }

  writeSummary(stats) {
    const summary = {
      timestamp: new Date().toISOString(),
      ...stats,
      logFile: this.logFile
    };

    fs.writeFileSync(this.summaryFile, JSON.stringify(summary, null, 2));
    console.log(`\n📝 Error log saved to: ${this.logFile}`);
    console.log(`📄 Summary saved to: ${this.summaryFile}`);
  }
}

// ============================================================================
// DATABASE HELPERS
// ============================================================================

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

function loadEnvFile() {
  try {
    if (fs.existsSync('.env')) {
      const envContent = fs.readFileSync('.env', 'utf8');
      envContent.split('\n').forEach(line => {
        if (line.startsWith('#')) return;
        const [key, ...valueParts] = line.split('=');
        if (key && valueParts.length > 0) {
          const value = valueParts.join('=').trim();
          if (value && !process.env[key]) {
            process.env[key] = value;
          }
        }
      });
    }
  } catch (e) {
    console.log('[ENV] Could not load .env file:', e.message);
  }
}

// Edition sync function removed - using work_clusters system
function syncEditionLinks_REMOVED() {
  return;
}

// Original function commented out for reference
/*
async function syncEditionLinks(client, {
  bookId,
  editionGroupKey,
  editionNumber,
  tablesWritten,
  debugInfo
}) {
  const operations = debugInfo?.operations;

  const pushTable = (label) => {
    if (!tablesWritten.includes(label)) {
      tablesWritten.push(label);
    }
  };

  if (!editionGroupKey) {
    const prune = await client.query(
      'DELETE FROM book_editions WHERE book_id = $1 OR related_book_id = $1 RETURNING id',
      [bookId]
    );
    if (prune.rowCount > 0) {
      pushTable('book_editions');
      if (operations) operations.book_editions = `PRUNE:${prune.rowCount}`;
    }
    return;
  }

  // Edition system has been removed - skip sibling processing
  const siblingResult = { rows: [] };

  if (siblingResult.rows.length === 0) {
    return;
  }

  const siblings = siblingResult.rows.map((row) => ({
    id: row.id,
    editionNumber: Number(row.edition_number) || 1
  }));

  const self = siblings.find((row) => row.id === bookId);
  if (self && editionNumber != null) {
    self.editionNumber = Number(editionNumber) || 1;
  }

  const bookIds = siblings.map((row) => row.id);

  if (siblings.length <= 1) {
    const prune = await client.query(
      'DELETE FROM book_editions WHERE book_id = ANY($1::uuid[]) OR related_book_id = ANY($1::uuid[]) RETURNING id',
      [bookIds]
    );
    if (prune.rowCount > 0) {
      pushTable('book_editions');
      if (operations) operations.book_editions = `PRUNE:${prune.rowCount}`;
    }
    return;
  }

  siblings.sort((a, b) => {
    const diff = (b.editionNumber || 1) - (a.editionNumber || 1);
    if (diff !== 0) return diff;
    return a.id.localeCompare(b.id);
  });

  const primary = siblings[0];
  const alternates = siblings.slice(1);

  await client.query(
    'DELETE FROM book_editions WHERE book_id = ANY($1::uuid[]) OR related_book_id = ANY($1::uuid[])',
    [bookIds]
  );

  let linksCreated = 0;
  for (const alternate of alternates) {
    await client.query(
      `INSERT INTO book_editions (
         id, book_id, related_book_id, link_source, relationship_type,
         created_at, updated_at
       ) VALUES (
         $1, $2::uuid, $3::uuid, $4, $5, NOW(), NOW()
       ) ON CONFLICT (book_id, related_book_id) DO UPDATE SET
         link_source = EXCLUDED.link_source,
         relationship_type = EXCLUDED.relationship_type,
         updated_at = NOW()`,
      [
        generateNanoId(12),
        primary.id,
        alternate.id,
        'S3_MIGRATION',
        'ALTERNATE_EDITION'
      ]
    );
    linksCreated += 1;
  }

  if (linksCreated > 0) {
    pushTable('book_editions');
    if (operations) operations.book_editions = `LINK:${linksCreated}`;
  }
}
*/

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

// ============================================================================
// NORMALIZATION HELPERS
// ============================================================================

function normalizeAuthorName(name) {
  if (!name || !name.trim()) return null;

  let normalized = name.toLowerCase();

  // Remove accents/diacritics
  normalized = normalized
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');

  // Handle suffixes (Jr., Sr., III, etc.)
  normalized = normalized
    .replace(/\s+jr\.?(?:\s|$)/gi, ' jr')
    .replace(/\s+sr\.?(?:\s|$)/gi, ' sr')
    .replace(/\s+ii+(?:\s|$)/gi, ' ii')
    .replace(/\s+iv(?:\s|$)/gi, ' iv')
    .replace(/\s+ph\.?d\.?(?:\s|$)/gi, ' phd')
    .replace(/\s+m\.?d\.?(?:\s|$)/gi, ' md');

  // Remove possessives
  normalized = normalized.replace(/['']s\b/g, '');

  // Handle corporate suffixes
  normalized = normalized
    .replace(/\s+inc\.?(?:\s|$)/gi, ' inc')
    .replace(/\s+corp\.?(?:\s|$)/gi, ' corp')
    .replace(/\s+ltd\.?(?:\s|$)/gi, ' ltd')
    .replace(/\s+llc\.?(?:\s|$)/gi, ' llc')
    .replace(/\s+co\.?(?:\s|$)/gi, ' co');

  // Remove editorial annotations
  normalized = normalized
    .replace(/\[from old catalog\]/gi, '')
    .replace(/\(editor\)/gi, '')
    .replace(/\(author\)/gi, '')
    .replace(/\(translator\)/gi, '')
    .replace(/\(ed\.\)/gi, '');

  // Handle "Last, First" → "first last"
  if (normalized.match(/^[a-z]+,\s+[a-z]+/)) {
    const parts = normalized.split(',').map(p => p.trim());
    if (parts.length === 2) {
      normalized = `${parts[1]} ${parts[0]}`;
    }
  }

  // Remove punctuation and normalize spaces
  normalized = normalized
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  return normalized;
}

// ============================================================================
// DEDUPLICATION HELPERS
// ============================================================================

function extractBookKey(volumeInfo) {
  // Create a unique key for deduplication
  const isbn13 = volumeInfo.industryIdentifiers?.find(id => id.type === 'ISBN_13')?.identifier;
  const isbn10 = volumeInfo.industryIdentifiers?.find(id => id.type === 'ISBN_10')?.identifier;
  const title = volumeInfo.title || '';
  const firstAuthor = (volumeInfo.authors && volumeInfo.authors[0]) || '';

  // Primary key is ISBN if available, otherwise title+author
  if (isbn13) return `isbn13:${isbn13}`;
  if (isbn10) return `isbn10:${isbn10}`;
  if (title && firstAuthor) return `title:${title.toLowerCase()}:${firstAuthor.toLowerCase()}`;
  if (title) return `title:${title.toLowerCase()}`;
  return null;
}

function mergeBookRecords(existing, newRecord) {
  // Merge two book records, preferring non-null values from either
  const volumeInfo = existing.volumeInfo || existing;
  const newVolumeInfo = newRecord.volumeInfo || newRecord;

  return {
    ...existing,
    volumeInfo: {
      ...volumeInfo,
      // Take the longer/better description
      description: (newVolumeInfo.description && newVolumeInfo.description.length > (volumeInfo.description || '').length)
        ? newVolumeInfo.description
        : volumeInfo.description,
      // Merge authors (unique)
      authors: [...new Set([...(volumeInfo.authors || []), ...(newVolumeInfo.authors || [])])],
      // Merge categories (unique)
      categories: [...new Set([...(volumeInfo.categories || []), ...(newVolumeInfo.categories || [])])],
      // Take the best image links (prefer larger)
      imageLinks: {
        ...volumeInfo.imageLinks,
        ...newVolumeInfo.imageLinks,
        // Prefer extraLarge > large > medium > thumbnail > small
        ...(newVolumeInfo.imageLinks?.extraLarge ? { extraLarge: newVolumeInfo.imageLinks.extraLarge } : {}),
        ...(newVolumeInfo.imageLinks?.large ? { large: newVolumeInfo.imageLinks.large } : {})
      },
      // Fill in missing fields
      subtitle: volumeInfo.subtitle || newVolumeInfo.subtitle,
      publisher: volumeInfo.publisher || newVolumeInfo.publisher,
      publishedDate: volumeInfo.publishedDate || newVolumeInfo.publishedDate,
      pageCount: volumeInfo.pageCount || newVolumeInfo.pageCount || volumeInfo.printedPageCount || newVolumeInfo.printedPageCount,
      language: volumeInfo.language || newVolumeInfo.language,
      // Merge industry identifiers (unique)
      industryIdentifiers: [
        ...new Map([
          ...(volumeInfo.industryIdentifiers || []),
          ...(newVolumeInfo.industryIdentifiers || [])
        ].map(id => [`${id.type}:${id.identifier}`, id])).values()
      ],
      // Average the ratings if both exist
      averageRating: (volumeInfo.averageRating && newVolumeInfo.averageRating)
        ? (volumeInfo.averageRating + newVolumeInfo.averageRating) / 2
        : volumeInfo.averageRating || newVolumeInfo.averageRating,
      ratingsCount: Math.max(volumeInfo.ratingsCount || 0, newVolumeInfo.ratingsCount || 0)
    },
    saleInfo: existing.saleInfo || newRecord.saleInfo,
    accessInfo: existing.accessInfo || newRecord.accessInfo
  };
}

function deduplicateJsonArray(bookArray) {
  // Handle case where JSON contains an array of book objects
  if (!Array.isArray(bookArray)) {
    return [bookArray];
  }

  const bookMap = new Map();
  const debugDedupInfo = [];

  for (const book of bookArray) {
    const volumeInfo = book.volumeInfo || book;
    const key = extractBookKey(volumeInfo);

    if (!key) {
      // No good key, include it anyway
      bookMap.set(Math.random().toString(), book);
      debugDedupInfo.push({ action: 'NO_KEY', title: volumeInfo.title });
      continue;
    }

    if (bookMap.has(key)) {
      // Merge with existing
      const merged = mergeBookRecords(bookMap.get(key), book);
      bookMap.set(key, merged);
      debugDedupInfo.push({ action: 'MERGED', key, title: volumeInfo.title });
    } else {
      // New book
      bookMap.set(key, book);
      debugDedupInfo.push({ action: 'NEW', key, title: volumeInfo.title });
    }
  }

  if (DEBUG_MODE && debugDedupInfo.length > 1) {
    console.log('\n🔀 JSON DEDUPLICATION:');
    debugDedupInfo.forEach(info => {
      console.log(`  ${info.action}: ${info.title} ${info.key ? `[${info.key}]` : ''}`);
    });
  }

  return Array.from(bookMap.values());
}

// ============================================================================
// MAIN MIGRATION LOGIC
// ============================================================================

async function migrateBookToDb(client, googleBooksId, bookData, _rawJson) {
  const tablesWritten = [];
  const debugInfo = DEBUG_MODE ? {
    source: {},
    operations: {},
    mapped: {}
  } : null;

  // DEDUPLICATION STRATEGY:
  // 1. First check if this Google Books ID already exists
  // 2. Then check if we have a book with matching ISBN (could be different provider/edition)
  // 3. If found by ISBN, determine if it's the same book or a different edition
  // 4. Only create new book record if truly new

  // CRITICAL FIX: Handle pre-processed S3 data where actual content is in rawJsonResponse
  let actualBookData = bookData;

  // Check if this is pre-processed data with the real content in rawJsonResponse
  if (bookData.rawJsonResponse && !bookData.volumeInfo) {
    try {
      const rawData = JSON.parse(bookData.rawJsonResponse);
      if (rawData.volumeInfo) {
        console.log(`[FIX] Using rawJsonResponse for actual data (title was "${bookData.title}", real title: "${rawData.volumeInfo.title}")`);
        actualBookData = rawData;
      }
    } catch (e) {
      console.error(`[ERROR] Failed to parse rawJsonResponse: ${e.message}`);
    }
  }

  // Now extract from the correct data source
  const volumeInfo = actualBookData.volumeInfo || actualBookData;
  const saleInfo = actualBookData.saleInfo || {};
  const accessInfo = actualBookData.accessInfo || {};
  const metadata = actualBookData._metadata || bookData._metadata || {};

  // Extract basic fields from volumeInfo
  const title = volumeInfo.title || '';
  const subtitle = volumeInfo.subtitle || null;
  const description = volumeInfo.description || null;
  const publisher = volumeInfo.publisher || null;
  const publishedDate = volumeInfo.publishedDate || null;
  const language = volumeInfo.language || 'en';
  const pageCount = volumeInfo.pageCount || volumeInfo.printedPageCount || null;

  // Extract ISBNs from industryIdentifiers
  let isbn10 = null;
  let isbn13 = null;
  if (volumeInfo.industryIdentifiers) {
    for (const id of volumeInfo.industryIdentifiers) {
      if (id.type === 'ISBN_10') isbn10 = id.identifier;
      if (id.type === 'ISBN_13') isbn13 = id.identifier;
    }
  }

  // Extract authors and categories early for debug
  const authors = volumeInfo.authors || [];
  const categories = volumeInfo.categories || [];

  // Edition management removed - using work_clusters system now

  // Store source data for debug
  if (DEBUG_MODE) {
    debugInfo.source = {
      google_books_id: googleBooksId,
      title: title || '(empty)',
      isbn13: isbn13 || '(none)',
      isbn10: isbn10 || '(none)',
      authors: authors.join(', ') || '(none)',
      publisher: publisher || '(none)',
      published_date: publishedDate || '(none)',
      categories: categories.join(', ') || '(none)'
      // edition_number and edition_group_key removed from schema
    };
  }

  // ============================================================================
  // DEDUPLICATION LOGIC
  // ============================================================================

  let bookId = null;
  let isNewBook = false;
  let existingSlug = null;

  const fetchSlugForBook = async (id) => {
    if (!id) return null;
    const res = await client.query('SELECT slug FROM books WHERE id = $1', [id]);
    return res.rows[0]?.slug || null;
  };

  const externalIdCheck = await client.query(
    `SELECT book_id FROM book_external_ids
     WHERE source = 'GOOGLE_BOOKS' AND external_id = $1`,
    [googleBooksId]
  );

  const foundByExternalId = externalIdCheck.rows.length > 0;
  let candidate = null;

  if (foundByExternalId) {
    bookId = externalIdCheck.rows[0].book_id;
    existingSlug = await fetchSlugForBook(bookId);
    console.log(`[EXISTING] Found by Google Books ID: ${googleBooksId} -> Book ${bookId}`);
  } else {

    if (!candidate && isbn13) {
      const res = await client.query('SELECT id, slug FROM books WHERE isbn13 = $1 LIMIT 1', [isbn13]);
      if (res.rows.length > 0) candidate = res.rows[0];

      if (!candidate) {
        const providerRes = await client.query(
          `SELECT book_id AS id FROM book_external_ids WHERE provider_isbn13 = $1 LIMIT 1`,
          [isbn13]
        );
        if (providerRes.rows.length > 0) {
          candidate = {
            id: providerRes.rows[0].id,
            slug: await fetchSlugForBook(providerRes.rows[0].id)
          };
        }
      }
    }

    if (!candidate && isbn10) {
      const res = await client.query('SELECT id, slug FROM books WHERE isbn10 = $1 LIMIT 1', [isbn10]);
      if (res.rows.length > 0) candidate = res.rows[0];

      if (!candidate) {
        const providerRes = await client.query(
          `SELECT book_id AS id FROM book_external_ids WHERE provider_isbn10 = $1 LIMIT 1`,
          [isbn10]
        );
        if (providerRes.rows.length > 0) {
          candidate = {
            id: providerRes.rows[0].id,
            slug: await fetchSlugForBook(providerRes.rows[0].id)
          };
        }
      }
    }

    if (candidate) {
      bookId = candidate.id;
      existingSlug = candidate.slug || await fetchSlugForBook(candidate.id);
      console.log(`[EXISTING] Matched existing canonical book ${bookId} via ISBN/provider lookup.`);
    } else {
      bookId = generateUUIDv7();
      isNewBook = true;
      console.log(`[NEW BOOK] No existing match for: ${title}`);
    }
  }

  // Track deduplication result for debug
  if (DEBUG_MODE) {
    debugInfo.dedup = {
      book_id: bookId,
      is_new: isNewBook,
      match_type: foundByExternalId ? 'EXTERNAL_ID' :
                  candidate ? 'ISBN_MATCH' :
                  'NEW_BOOK'
    };
    debugInfo.operations = {}; // Will track each table operation
  }

  // Provider-specific fields (will go to book_external_ids)
  const averageRating = volumeInfo.averageRating || null;
  const ratingsCount = volumeInfo.ratingsCount || null;
  const infoLink = volumeInfo.infoLink || null;
  const previewLink = volumeInfo.previewLink || null;
  const canonicalVolumeLink = volumeInfo.canonicalVolumeLink || null;
  const webReaderLink = accessInfo.webReaderLink || null;

  // Sale info
  const saleability = saleInfo.saleability || 'NOT_FOR_SALE';
  const isEbook = saleInfo.isEbook || false;
  const listPrice = saleInfo.listPrice?.amount || saleInfo.retailPrice?.amount || null;
  const currencyCode = saleInfo.listPrice?.currencyCode || saleInfo.retailPrice?.currencyCode || null;

  // Access info
  const viewability = accessInfo.viewability || 'NO_PAGES';
  const embeddable = accessInfo.embeddable || false;
  const publicDomain = accessInfo.publicDomain || false;
  const textToSpeechPermission = accessInfo.textToSpeechPermission || null;
  const pdfAvailable = accessInfo.pdf?.isAvailable || false;
  const epubAvailable = accessInfo.epub?.isAvailable || false;

  // Reading modes
  const textReadable = volumeInfo.readingModes?.text || false;
  const imageReadable = volumeInfo.readingModes?.image || false;

  // Other metadata
  const printType = volumeInfo.printType || 'BOOK';
  const maturityRating = volumeInfo.maturityRating || 'NOT_MATURE';
  const contentVersion = volumeInfo.contentVersion || null;
  const country = saleInfo.country || accessInfo.country || null;

  // S3 image paths will be stored in book_image_links table after download

  // Image links
  const imageLinks = volumeInfo.imageLinks || {};

  // Edition info already extracted above for debug

  // Generate slug (only for new books)
  let slug = existingSlug;  // Use existing slug if we found the book

  if (!slug && isNewBook) {
    const firstAuthor = authors.length > 0 ? authors[0] : null;
    const baseSlug = generateSlug(title, firstAuthor);

    // Ensure slug uniqueness
    slug = baseSlug;
    if (baseSlug) {
      const slugResult = await client.query(
        'SELECT ensure_unique_slug($1) as unique_slug',
        [baseSlug]
      );
      slug = slugResult.rows[0].unique_slug;
    }
  }

  // Convert published date
  let pubDate = null;
  if (publishedDate) {
    try {
      if (publishedDate.length === 4) {
        pubDate = `${publishedDate}-01-01`;
      } else if (publishedDate.length === 7) {
        pubDate = `${publishedDate}-01`;
      } else {
        pubDate = publishedDate;
      }
    } catch (_e) {
      pubDate = null;
    }
  }

  try {
    // Transaction is now handled by the caller
    // 1. Insert/Update books table (only if new or needs update)
    if (isNewBook) {
      // Insert new book
      const bookSql = `
        INSERT INTO books (
          id, title, subtitle, description, isbn10, isbn13,
          published_date, language, publisher, page_count,
          slug, created_at, updated_at
        ) VALUES (
          $1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10,
          $11, NOW(), NOW()
        )
      `;

      await client.query(bookSql, [
        bookId, title, subtitle, description, isbn10, isbn13,
        pubDate, language, publisher, pageCount,
        slug
      ]);
      tablesWritten.push('books');
      if (DEBUG_MODE) debugInfo.operations.books = 'INSERT';
    } else {
      // Update existing book (merge better data if available)
      const updateSql = `
        UPDATE books SET
          subtitle = COALESCE(NULLIF($2, ''), subtitle),
          description = COALESCE(NULLIF($3, ''), description),
          isbn10 = COALESCE(NULLIF($4, ''), isbn10),
          isbn13 = COALESCE(NULLIF($5, ''), isbn13),
          published_date = COALESCE($6, published_date),
          language = COALESCE(NULLIF($7, ''), language),
          publisher = COALESCE(NULLIF($8, ''), publisher),
          page_count = COALESCE($9, page_count),
          updated_at = NOW()
        WHERE id = $1::uuid
      `;

      await client.query(updateSql, [
        bookId, subtitle, description, isbn10, isbn13,
        pubDate, language, publisher, pageCount
      ]);
      tablesWritten.push('books-updated');
      if (DEBUG_MODE) debugInfo.operations.books = 'UPDATE';
    }

    // Edition sync removed - using work_clusters system now
    // await syncEditionLinks(...)

    // 2. Insert into book_external_ids (provider-specific data)
    const externalIdSql = `
      INSERT INTO book_external_ids (
        id, book_id, source, external_id,
        provider_isbn10, provider_isbn13,
        info_link, preview_link, web_reader_link, canonical_volume_link,
        average_rating, ratings_count,
        is_ebook, pdf_available, epub_available,
        embeddable, public_domain, viewability,
        text_readable, image_readable,
        print_type, maturity_rating, content_version,
        text_to_speech_permission,
        saleability, country_code,
        list_price, retail_price, currency_code,
        created_at
      ) VALUES (
        $1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23,
        $24, $25, $26, $27, $28, $29, NOW()
      )
      ON CONFLICT (source, external_id) DO UPDATE SET
        average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating),
        ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count),
        list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price),
        retail_price = COALESCE(EXCLUDED.retail_price, book_external_ids.retail_price)
    `;

    await client.query(externalIdSql, [
      generateNanoId(10), bookId, 'GOOGLE_BOOKS', googleBooksId,
      isbn10, isbn13,
      infoLink, previewLink, webReaderLink, canonicalVolumeLink,
      averageRating, ratingsCount,
      isEbook, pdfAvailable, epubAvailable,
      embeddable, publicDomain, viewability,
      textReadable, imageReadable,
      printType, maturityRating, contentVersion,
      textToSpeechPermission,
      saleability, country,
      listPrice, listPrice, currencyCode
    ]);
    tablesWritten.push('book_external_ids');

    // 3. Store raw JSON data
    const rawDataSql = `
      INSERT INTO book_raw_data (
        id, book_id, raw_json_response, source,
        fetched_at, contributed_at, created_at
      ) VALUES (
        $1, $2::uuid, $3::jsonb, $4, NOW(), NOW(), NOW()
      )
      ON CONFLICT (book_id, source) DO UPDATE SET
        raw_json_response = EXCLUDED.raw_json_response,
        fetched_at = NOW()
    `;

    await client.query(rawDataSql, [
      generateNanoId(10), bookId, JSON.stringify(bookData), 'GOOGLE_BOOKS'
    ]);
    tablesWritten.push('book_raw_data');

    // 4. Handle authors
    if (authors.length > 0) {
      for (let i = 0; i < authors.length; i++) {
        const authorName = authors[i];
        if (!authorName || !authorName.trim()) continue;

        // Insert or get author
        const authorSql = `
          INSERT INTO authors (id, name, normalized_name, created_at, updated_at)
          VALUES ($1, $2, $3, NOW(), NOW())
          ON CONFLICT (name) DO UPDATE SET
            updated_at = NOW()
          RETURNING id
        `;

        const authorResult = await client.query(authorSql, [
          generateNanoId(10),
          authorName,
          normalizeAuthorName(authorName)
        ]);

        const authorId = authorResult.rows[0].id;

        // Link book to author
        const bookAuthorSql = `
          INSERT INTO book_authors_join (id, book_id, author_id, position, created_at)
          VALUES ($1, $2::uuid, $3, $4, NOW())
          ON CONFLICT (book_id, author_id) DO UPDATE SET
            position = EXCLUDED.position
        `;

        await client.query(bookAuthorSql, [
          generateNanoId(12), // 12 chars for high-volume join table
          bookId,
          authorId,
          i
        ]);
      }
      tablesWritten.push('authors', 'book_authors_join');
    }

    // 5. Handle categories
    if (categories.length > 0) {
      for (const categoryName of categories) {
        if (!categoryName || !categoryName.trim()) continue;

        // Insert or get collection (category)
        const collectionSql = `
          INSERT INTO book_collections (
            id, collection_type, source, display_name, normalized_name,
            created_at, updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, NOW(), NOW()
          )
          ON CONFLICT (collection_type, source, normalized_name)
          WHERE collection_type = 'CATEGORY' AND normalized_name IS NOT NULL
          DO NOTHING
          RETURNING id
        `;

        let collectionId;
        try {
          const collectionResult = await client.query(collectionSql, [
            generateNanoId(8), // 8 chars for low-volume collections
            'CATEGORY',
            'GOOGLE_BOOKS',
            categoryName,
            categoryName.toLowerCase().replace(/[^a-z0-9\s]/g, '')
          ]);

          if (collectionResult.rows.length > 0) {
            collectionId = collectionResult.rows[0].id;
          } else {
            // If insert failed due to conflict, find the existing one
            const findResult = await client.query(
              'SELECT id FROM book_collections WHERE display_name = $1 AND collection_type = $2 AND source = $3',
              [categoryName, 'CATEGORY', 'GOOGLE_BOOKS']
            );
            if (findResult.rows.length > 0) {
              collectionId = findResult.rows[0].id;
            }
          }
        } catch (e) {
          // Collection might already exist, try to find it
          const findResult = await client.query(
            'SELECT id FROM book_collections WHERE display_name = $1 AND collection_type = $2 AND source = $3',
            [categoryName, 'CATEGORY', 'GOOGLE_BOOKS']
          );
          if (findResult.rows.length > 0) {
            collectionId = findResult.rows[0].id;
          }
        }

        if (collectionId) {
          // Link book to collection
          const bookCollectionSql = `
            INSERT INTO book_collections_join (
              id, collection_id, book_id, created_at, updated_at
            ) VALUES (
              $1, $2, $3::uuid, NOW(), NOW()
            )
            ON CONFLICT (collection_id, book_id) DO NOTHING
          `;

          await client.query(bookCollectionSql, [
            generateNanoId(12), // 12 chars for high-volume join table
            collectionId,
            bookId
          ]);
        }
      }
      tablesWritten.push('book_collections', 'book_collections_join');
    }

    // 6. Handle image links from volumeInfo.imageLinks and map to S3 if exists
    if (imageLinks && Object.keys(imageLinks).length > 0) {
      // Generate the expected S3 path using the same pattern as S3BookCoverService
      // Pattern: images/book-covers/{bookId}-lg-google-books-api.jpg
      const s3Key = `images/book-covers/${googleBooksId}-lg-google-books-api.jpg`;

      // We'll mark the largest available image as having the S3 path
      let hasS3Image = false;

      for (const [imageType, url] of Object.entries(imageLinks)) {
        if (url) {
          let s3ImagePath = null;
          let s3UploadedAt = null;

          // Map S3 path to the best quality image (prefer extraLarge > large > medium)
          if (!hasS3Image && (imageType === 'extraLarge' || imageType === 'large' || imageType === 'medium')) {
            // This would be where the S3BookCoverService stores the image
            s3ImagePath = s3Key;
            // In production, we'd check S3 to see if it actually exists
            // For now, we'll mark it as potentially existing
            hasS3Image = true;

            if (DEBUG_MODE) {
              console.log(`[S3 MAP] Potential S3 image path for ${imageType}: ${s3Key}`);
            }
          }

          const imageSql = `
            INSERT INTO book_image_links (
              id, book_id, image_type, url, source, s3_image_path, s3_uploaded_at, created_at
            ) VALUES (
              $1, $2::uuid, $3, $4, $5, $6, $7::timestamptz, NOW()
            )
            ON CONFLICT (book_id, image_type) DO UPDATE SET
              url = EXCLUDED.url,
              s3_image_path = COALESCE(EXCLUDED.s3_image_path, book_image_links.s3_image_path),
              s3_uploaded_at = COALESCE(EXCLUDED.s3_uploaded_at, book_image_links.s3_uploaded_at)
          `;

          await client.query(imageSql, [
            generateNanoId(10),
            bookId,
            imageType,
            url,
            'GOOGLE_BOOKS',
            s3ImagePath,
            s3UploadedAt
          ]);
        }
      }
      tablesWritten.push('book_image_links');
    }

    // 7. Handle dimensions if present
    const dimensions = volumeInfo.dimensions;
    if (dimensions) {
      const dims = dimensions;
      let height = null, width = null, thickness = null;

      // Parse dimensions (e.g., "23.00 cm" -> 23.00)
      if (dims.height) {
        const match = dims.height.match(/(\d+\.?\d*)/);
        if (match) height = parseFloat(match[1]);
      }
      if (dims.width) {
        const match = dims.width.match(/(\d+\.?\d*)/);
        if (match) width = parseFloat(match[1]);
      }
      if (dims.thickness) {
        const match = dims.thickness.match(/(\d+\.?\d*)/);
        if (match) thickness = parseFloat(match[1]);
      }

      if (height || width || thickness) {
        const dimSql = `
          INSERT INTO book_dimensions (
            id, book_id, height_cm, width_cm, thickness_cm, created_at
          ) VALUES (
            $1, $2::uuid, $3, $4, $5, NOW()
          )
          ON CONFLICT (book_id) DO UPDATE SET
            height_cm = COALESCE(EXCLUDED.height_cm, book_dimensions.height_cm),
            width_cm = COALESCE(EXCLUDED.width_cm, book_dimensions.width_cm),
            thickness_cm = COALESCE(EXCLUDED.thickness_cm, book_dimensions.thickness_cm)
        `;

        await client.query(dimSql, [
          generateNanoId(8), // 8 chars for low-volume dimensions
          bookId,
          height,
          width,
          thickness
        ]);
        tablesWritten.push('book_dimensions');
      }
    }

    // Track remaining operations for debug
    if (DEBUG_MODE) {
      if (!debugInfo.operations.book_editions) debugInfo.operations.book_editions = 'SKIP';
      if (!debugInfo.operations.book_external_ids) debugInfo.operations.book_external_ids = 'INSERT/UPDATE';
      if (!debugInfo.operations.book_raw_data) debugInfo.operations.book_raw_data = 'INSERT/UPDATE';
      if (!debugInfo.operations.authors) debugInfo.operations.authors = authors.length > 0 ? 'INSERT/UPDATE' : 'SKIP';
      if (!debugInfo.operations.book_dimensions) debugInfo.operations.book_dimensions = dimensions ? 'INSERT/UPDATE' : 'SKIP';
      if (!debugInfo.operations.book_image_links) debugInfo.operations.book_image_links = Object.keys(imageLinks).length > 0 ? 'INSERT' : 'SKIP';
      if (!debugInfo.operations.book_collections) debugInfo.operations.book_collections = categories.length > 0 ? 'INSERT' : 'SKIP';

      // Store mapped output
      debugInfo.mapped = {
        book_id: bookId,
        slug: slug || '(none)',
        isbn13: isbn13 || '(none)',
        isbn10: isbn10 || '(none)',
        authors_count: authors.length,
        categories_count: categories.length,
        image_types: Object.keys(imageLinks).join(', ') || '(none)'
      };
    }

    return { bookId, tablesWritten, debugInfo };
  } catch (e) {
    // Re-throw with additional context
    const errorContext = {
      googleBooksId,
      title: volumeInfo.title,
      isbn10,
      isbn13
    };
    e.context = errorContext;
    throw e;
  }
}

// ============================================================================
// DEBUG DISPLAY HELPERS
// ============================================================================

function displayDebugTable(debugInfo) {
  if (!debugInfo) return;

  console.log(`\n${'='.repeat(120)}`);
  console.log('DEBUG: MIGRATION MAPPING AND OPERATIONS');
  console.log('='.repeat(120));

  // Source data section
  console.log('\n📥 SOURCE DATA (from S3/Google Books):');
  console.log('-'.repeat(120));
  const sourceTable = [
    ['Field', 'Value'],
    ['─────', '─────']
  ];
  for (const [key, value] of Object.entries(debugInfo.source)) {
    sourceTable.push([key.replace(/_/g, ' ').toUpperCase(), String(value)]);
  }
  printTable(sourceTable);

  // Deduplication result
  console.log('\n🔍 DEDUPLICATION RESULT:');
  console.log('-'.repeat(120));
  const dedupTable = [
    ['Book ID', 'Is New?', 'Match Type'],
    ['───────', '───────', '──────────']
  ];
  dedupTable.push([
    debugInfo.dedup.book_id,
    debugInfo.dedup.is_new ? 'YES' : 'NO',
    debugInfo.dedup.match_type
  ]);
  printTable(dedupTable);

  // Operations performed
  console.log('\n⚙️  OPERATIONS PERFORMED:');
  console.log('-'.repeat(120));
  const opsTable = [
    ['Table', 'Operation'],
    ['─────', '─────────']
  ];
  for (const [table, op] of Object.entries(debugInfo.operations)) {
    const emoji = op === 'INSERT' ? '✅' :
                  op === 'UPDATE' ? '🔄' :
                  op === 'SKIP' ? '⏭️' : '❓';
    opsTable.push([table, `${emoji} ${op}`]);
  }
  printTable(opsTable);

  // Mapped output
  console.log('\n📤 MAPPED OUTPUT (in database):');
  console.log('-'.repeat(120));
  const mappedTable = [
    ['Field', 'Value'],
    ['─────', '─────']
  ];
  for (const [key, value] of Object.entries(debugInfo.mapped)) {
    mappedTable.push([key.replace(/_/g, ' ').toUpperCase(), String(value)]);
  }
  printTable(mappedTable);

  console.log(`\n${'='.repeat(120)}\n`);
}

function printTable(rows) {
  if (rows.length === 0) return;

  // Calculate column widths
  const columnWidths = [];
  for (let i = 0; i < rows[0].length; i++) {
    columnWidths[i] = Math.max(...rows.map(row => String(row[i]).length));
  }

  // Print rows
  rows.forEach((row, index) => {
    const formattedRow = row.map((cell, i) =>
      String(cell).padEnd(columnWidths[i])
    ).join(' │ ');
    console.log(`  ${formattedRow}`);
  });
}

// ============================================================================
// MAIN MIGRATION RUNNER
// ============================================================================

async function migrateBooksFromS3() {
  loadEnvFile();

  const dbParams = parsePostgresUrl(process.env.SPRING_DATASOURCE_URL);
  const s3Bucket = process.env.S3_BUCKET;

  if (!s3Bucket) throw new Error('S3_BUCKET not set');

  console.log(`[MIGRATION] Connecting to DB: ${dbParams.host}:${dbParams.port}/${dbParams.database}`);
  console.log(`[MIGRATION] S3 bucket: ${s3Bucket}, prefix: ${PREFIX}`);
  console.log(`[MIGRATION] Max records: ${MAX_RECORDS || 'unlimited'}, skip: ${SKIP_RECORDS}`);
  if (DEBUG_MODE) {
    console.log(`[MIGRATION] Debug mode: ENABLED`);
  }

  // Connect to DB
  const client = new Client(dbParams);
  await client.connect();

  // Configure S3
  const s3 = new S3Client({
    endpoint: process.env.S3_SERVER_URL,
    credentials: {
      accessKeyId: process.env.S3_ACCESS_KEY_ID,
      secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    },
    region: process.env.AWS_REGION || 'us-west-2',
    forcePathStyle: true
  });

  let processed = 0;
  let skipped = 0;
  let errors = 0;
  let booksInserted = 0;
  let booksUpdated = 0;
  let booksMerged = 0;
  let continuationToken = null;

  // Track error types for quality control
  const errorStats = {
    corrupt_json: 0,
    initialization_error: 0,
    database_error: 0,
    connection_error: 0,
    other: 0
  };
  const errorDetails = [];

  // Retry queue for failed records
  const retryQueue = [];

  // Initialize error logger
  const errorLogger = new ErrorLogger();

  do {
    const command = new ListObjectsV2Command({
      Bucket: s3Bucket,
      Prefix: PREFIX,
      MaxKeys: 100,
      ContinuationToken: continuationToken
    });

    const result = await s3.send(command);

    if (!result.Contents) break;

    for (const obj of result.Contents) {
      const key = obj.Key;
      if (!key.endsWith('.json')) continue;

      // Handle skipping
      if (skipped < SKIP_RECORDS) {
        skipped++;
        continue;
      }

      // Check max records limit
      if (MAX_RECORDS > 0 && processed >= MAX_RECORDS) {
        console.log(`[MIGRATION] Reached max records limit: ${MAX_RECORDS}`);

        // Refresh materialized view before exiting
        console.log('[MIGRATION] Refreshing search view...');
        await client.query('SELECT refresh_book_search_view()');

        await client.end();
        console.log(`[MIGRATION] Complete! Processed: ${processed}, Errors: ${errors}, Skipped: ${skipped}`);
        return;
      }

      let bodyString = null;
      try {
        // Fetch JSON from S3
        const getCommand = new GetObjectCommand({ Bucket: s3Bucket, Key: key });
        const s3Object = await s3.send(getCommand);
        bodyString = await streamToString(s3Object.Body);

        console.log(`[PROCESSING] ${key}`);

        // Check for common corruption patterns before parsing
        if (!bodyString || bodyString.length === 0) {
          throw new Error('Empty file');
        }

        // Check for binary/corrupted data (common pattern: � characters)
        if (bodyString.includes('\ufffd') || bodyString.includes('\x00')) {
          throw new Error('File contains binary/corrupted data');
        }

        // Try to detect truncated JSON
        const trimmed = bodyString.trim();
        if (trimmed.length > 0 && !trimmed.endsWith('}') && !trimmed.endsWith(']')) {
          console.warn(`[WARNING] File ${key} may be truncated (doesn't end with } or ])`);
        }

        let parsedData = JSON.parse(bodyString);

        // Handle different JSON structures:
        // 1. Single book object
        // 2. Array of book objects (duplicates within same file)
        // 3. Nested structure with items array
        let bookDataArray = [];

        if (Array.isArray(parsedData)) {
          // Direct array of books
          bookDataArray = parsedData;
        } else if (parsedData.items && Array.isArray(parsedData.items)) {
          // Google Books API response format with items array
          bookDataArray = parsedData.items;
        } else if (parsedData.volumeInfo || parsedData.title) {
          // Single book object
          bookDataArray = [parsedData];
        } else {
          // Unknown structure, try to use as-is
          bookDataArray = [parsedData];
        }

        // Deduplicate books within the same JSON file
        const dedupedBooks = deduplicateJsonArray(bookDataArray);

        if (dedupedBooks.length > 1) {
          console.log(`[MULTI] Found ${dedupedBooks.length} unique books in ${key}`);
        }

        // Extract Google Books ID from key (e.g., books/v1/ABC123.json -> ABC123)
        const baseGoogleBooksId = key.split('/').pop().replace('.json', '');

        // Process each unique book in the file
        for (let bookIndex = 0; bookIndex < dedupedBooks.length; bookIndex++) {
          const bookData = dedupedBooks[bookIndex];

          // For multiple books in same file, append index to ID
          const googleBooksId = dedupedBooks.length > 1
            ? `${baseGoogleBooksId}_${bookIndex}`
            : baseGoogleBooksId;

          console.log(`[BOOK] Google ID: ${googleBooksId}, Title: ${bookData.volumeInfo?.title || bookData.title || 'N/A'}`);

          // Create separate transaction for each book
          try {
            await client.query('BEGIN');

            // Migrate to database
            const { bookId, tablesWritten, debugInfo } = await migrateBookToDb(
              client,
              googleBooksId,
              bookData,
              bookData  // Pass the same data since it's already the full structure
            );

            await client.query('COMMIT');

            console.log(`[SUCCESS] Book UUID: ${bookId}, Tables: ${tablesWritten.join(', ')}`);

            // Track operation types
            if (debugInfo && debugInfo.operations) {
              if (debugInfo.operations.books === 'INSERT') {
                booksInserted++;
              } else if (debugInfo.operations.books === 'UPDATE') {
                booksUpdated++;
              }
            }

            // Display debug table if in debug mode
            if (DEBUG_MODE) {
              displayDebugTable(debugInfo);
            }

          } catch (bookError) {
            await client.query('ROLLBACK');

            console.error(`[ERROR] Failed to migrate book ${googleBooksId}: ${bookError.message}`);

            // Categorize error
            if (bookError.message.includes('duplicate key')) {
              errorStats.database_error++;
            } else if (bookError.message.includes('Cannot access')) {
              errorStats.initialization_error++;
            } else if (bookError.message.includes('connection') || bookError.message.includes('timeout')) {
              errorStats.connection_error++;
              // Add to retry queue if connection issue
              if (retryQueue.length < 50) {
                retryQueue.push({ key, bookData, googleBooksId, attempt: 1 });
              }
            } else {
              errorStats.other++;
            }

            // Store error details
            if (errorDetails.length < 10) {
              errorDetails.push({
                file: `${key}[${bookIndex}]`,
                error: bookError.message.substring(0, 100)
              });
            }

            // Log to file
            errorLogger.logError(`${key}[${bookIndex}]`, bookError, {
              googleBooksId,
              title: bookData.volumeInfo?.title || bookData.title,
              ...(bookError.context || {})
            });

            errors++;
            continue; // Continue with next book in the file
          }
        }

        processed++;

        console.log(''); // Add a newline after processing file

      } catch (e) {
        const errorMsg = e.message;
        console.error(`[ERROR] Failed to process ${key}: ${errorMsg}`);

        // Categorize error for quality control
        if (errorMsg.includes('Unexpected token') || errorMsg.includes('Unexpected non-whitespace')) {
          errorStats.corrupt_json++;
        } else if (errorMsg.includes('Cannot access') && errorMsg.includes('before initialization')) {
          errorStats.initialization_error++;
        } else if (errorMsg.includes('duplicate key') || errorMsg.includes('violates')) {
          errorStats.database_error++;
        } else {
          errorStats.other++;
        }

        // Store error details for summary
        if (errorDetails.length < 10) { // Keep first 10 for review
          errorDetails.push({
            file: key,
            error: errorMsg.substring(0, 100)
          });
        }

        // Log error to file
        errorLogger.logError(key, e, {
          phase: 'json-parsing',
          fileSize: bodyString ? bodyString.length : 0
        });

        if (process.env.DEBUG) {
          console.error(e.stack);
        }
        errors++;
      }
    }

    continuationToken = result.NextContinuationToken;

  } while (continuationToken);

  // Process retry queue
  if (retryQueue.length > 0) {
    console.log(`\n[RETRY] Processing ${retryQueue.length} failed records...`);

    for (const retry of retryQueue) {
      console.log(`[RETRY] Attempt ${retry.attempt} for ${retry.key}`);

      try {
        await client.query('BEGIN');

        const { bookId } = await migrateBookToDb(
          client,
          retry.googleBooksId,
          retry.bookData,
          retry.bookData
        );

        await client.query('COMMIT');
        console.log(`[RETRY SUCCESS] Book UUID: ${bookId}`);
        processed++;
        errors--; // Decrement error count since we succeeded

      } catch (retryError) {
        await client.query('ROLLBACK');
        console.error(`[RETRY FAILED] ${retry.key}: ${retryError.message}`);
      }
    }
  }

  // Refresh materialized view at the end
  console.log('[MIGRATION] Refreshing search view...');
  await client.query('SELECT refresh_book_search_view()');

  await client.end();

  // Display quality control summary
  console.log(`\n${'='.repeat(80)}`);
  console.log('MIGRATION COMPLETE - QUALITY CONTROL SUMMARY');
  console.log('='.repeat(80));

  console.log(`\n📊 OVERALL STATISTICS:`);
  console.log(`  ✅ Successfully Processed: ${processed}`);
  console.log(`  ❌ Errors: ${errors}`);
  console.log(`  ⏭️  Skipped: ${skipped}`);
  console.log(`  📁 Total Files Attempted: ${processed + errors + skipped}`);

  if (booksInserted > 0 || booksUpdated > 0) {
    console.log(`\n📚 BOOK OPERATIONS:`);
    if (booksInserted > 0) console.log(`  ➕ Books Inserted: ${booksInserted}`);
    if (booksUpdated > 0) console.log(`  🔄 Books Updated: ${booksUpdated}`);
    if (booksMerged > 0) console.log(`  🤝 Books Merged: ${booksMerged}`);
  }

  if (retryQueue.length > 0) {
    console.log(`  🔄 Retries Attempted: ${retryQueue.length}`);
    const successfulRetries = retryQueue.length - errors;
    if (successfulRetries > 0) {
      console.log(`  ✨ Successful Retries: ${successfulRetries}`);
    }
  }

  if (errors > 0) {
    console.log(`\n❌ ERROR BREAKDOWN:`);
    console.log(`  🗑️  Corrupt JSON files: ${errorStats.corrupt_json}`);
    console.log(`  🐛 Initialization errors: ${errorStats.initialization_error}`);
    console.log(`  🗄️  Database errors: ${errorStats.database_error}`);
    console.log(`  🌐 Connection errors: ${errorStats.connection_error}`);
    console.log(`  ❓ Other errors: ${errorStats.other}`);

    if (errorDetails.length > 0) {
      console.log(`\n📋 SAMPLE ERROR DETAILS (first ${errorDetails.length}):`);
      errorDetails.forEach((detail) => {
        console.log(`  • ${detail.file}`);
        console.log(`    Error: ${detail.error}`);
      });
    }

    // Calculate error rate
    const errorRate = ((errors / (processed + errors)) * 100).toFixed(2);
    console.log(`\n⚠️  ERROR RATE: ${errorRate}%`);

    if (errorStats.corrupt_json > 0) {
      console.log(`\n💡 RECOMMENDATION: ${errorStats.corrupt_json} corrupt JSON files detected.`);
      console.log(`   Consider re-downloading from S3 or checking S3 bucket integrity.`);
    }
  }

  // Write summary to log files
  errorLogger.writeSummary({
    processed,
    errors,
    skipped,
    errorStats,
    errorDetails: errorDetails.slice(0, 50), // Keep more in file
    retryAttempts: retryQueue.length,
    booksInserted,
    booksUpdated,
    booksMerged
  });

  console.log(`\n${'='.repeat(80)}`);
}

// Run migration
migrateBooksFromS3().catch(e => {
  console.error('[ERROR] Migration failed:', e);
  process.exit(1);
});
