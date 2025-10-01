# External API Logging Guide

## Overview

Comprehensive console logging has been added for all external API calls during opportunistic backfill/supplementation. All logs use the `[EXTERNAL-API]` prefix for easy filtering.

## Log Format

All console logs follow this pattern:

```text
[EXTERNAL-API] [Component] Action: details
```

## What You'll See in Console

### 1. Tiered Search Start/Complete

**Search Initiation:**

```text
[EXTERNAL-API] [TIERED-SEARCH] START: query='elin hilderbrand', postgresResults=0, desiredTotal=20, needFromExternal=20
```

**Search Completion:**

```text
[EXTERNAL-API] [TIERED-SEARCH] COMPLETE: query='elin hilderbrand', postgresResults=0, externalResults=15, totalResults=15
```

### 2. Search Strategy Detection

**Author Name Detection:**

```text
[EXTERNAL-API] [SEARCH-STRATEGY] Using 'inauthor:elin hilderbrand' for query='elin hilderbrand'
```

### 3. API Call Attempts

**Primary Search (Authenticated):**

```text
[EXTERNAL-API] [GoogleBooks] AUTHENTICATED PRIMARY_SEARCH ATTEMPT: for query='inauthor:elin hilderbrand'
[EXTERNAL-API] [PAGED-SEARCH] Authenticated paged search: query='inauthor:elin hilderbrand', maxResults=20, orderBy=relevance
```

**Fallback Search (Unauthenticated):**

```text
[EXTERNAL-API] [GoogleBooks] UNAUTHENTICATED FALLBACK_SEARCH ATTEMPT: for query='inauthor:elin hilderbrand'
[EXTERNAL-API] [PAGED-SEARCH] Unauthenticated paged search: query='inauthor:elin hilderbrand', maxResults=20, orderBy=relevance
```

**OpenLibrary Fallback:**

```text
[EXTERNAL-API] [OpenLibrary] UNAUTHENTICATED FALLBACK_SEARCH ATTEMPT: for query='elin hilderbrand'
```

### 4. Circuit Breaker Events

**When Circuit Breaker Blocks Authenticated Calls:**

```text
[EXTERNAL-API] [GoogleBooks] CIRCUIT-BREAKER-OPEN: Blocking authenticated call for query='elin hilderbrand' (unauthenticated fallback will be attempted)
```

### 5. API Call Results

**Successful Response:**

```text
[EXTERNAL-API] [GoogleBooks] SUCCESS: PRIMARY_SEARCH returned 15 result(s) for query='inauthor:elin hilderbrand'
[EXTERNAL-API] [PAGED-SEARCH] Authenticated paged search complete: query='inauthor:elin hilderbrand', results=15
[EXTERNAL-API] [GoogleBooks] Authenticated search response: query='inauthor:elin hilderbrand', startIndex=0, itemsInPage=15
```

**No Results:**

```text
[EXTERNAL-API] [GoogleBooks] SUCCESS: PRIMARY_SEARCH returned 0 result(s) for query='some unknown book'
```

**Failure:**

```text
[EXTERNAL-API] [GoogleBooks] FAILURE: PRIMARY_SEARCH failed for query='problematic query' - HTTP 429: Rate limit exceeded
[EXTERNAL-API] [PAGED-SEARCH] Authenticated paged search ERROR: query='problematic query', error=Rate limit exceeded
```

### 6. HTTP Request/Response Details

**Request:**

```text
[EXTERNAL-API] [HTTP] AUTHENTICATED GET request to: https://www.googleapis.com/books/v1/volumes?q=inauthor:elin+hilderbrand&startIndex=0&maxResults=40
```

**Response:**

```text
[EXTERNAL-API] [HTTP] Response: status=200, url=https://www.googleapis.com/books/v1/volumes..., bodySize=15234 bytes
```

### 7. Fallback Disabled Events

```text
[EXTERNAL-API] [GoogleBooks] DISABLED: External fallback disabled for query='test query'
```

## Typical Search Flow Example

Here's what a complete successful search with supplementation looks like in the console:

```text
[EXTERNAL-API] [TIERED-SEARCH] START: query='elin hilderbrand', postgresResults=0, desiredTotal=20, needFromExternal=20
[EXTERNAL-API] [SEARCH-STRATEGY] Using 'inauthor:elin hilderbrand' for query='elin hilderbrand'
[EXTERNAL-API] [GoogleBooks] AUTHENTICATED PRIMARY_SEARCH ATTEMPT: for query='inauthor:elin hilderbrand'
[EXTERNAL-API] [PAGED-SEARCH] Authenticated paged search: query='inauthor:elin hilderbrand', maxResults=20, orderBy=relevance
[EXTERNAL-API] [HTTP] AUTHENTICATED GET request to: https://www.googleapis.com/books/v1/volumes?q=inauthor:elin+hilderbrand...
[EXTERNAL-API] [HTTP] Response: status=200, url=https://www.googleapis.com/books/v1/volumes..., bodySize=23456 bytes
[EXTERNAL-API] [GoogleBooks] Authenticated search response: query='inauthor:elin hilderbrand', startIndex=0, itemsInPage=20
[EXTERNAL-API] [PAGED-SEARCH] Authenticated paged search complete: query='inauthor:elin hilderbrand', results=20
[EXTERNAL-API] [GoogleBooks] SUCCESS: PRIMARY_SEARCH returned 20 result(s) for query='inauthor:elin hilderbrand'
[EXTERNAL-API] [TIERED-SEARCH] COMPLETE: query='elin hilderbrand', postgresResults=0, externalResults=20, totalResults=20
```

## Graceful Degradation Flow Example

When authenticated calls are blocked by circuit breaker:

```text
[EXTERNAL-API] [TIERED-SEARCH] START: query='book search', postgresResults=5, desiredTotal=20, needFromExternal=15
[EXTERNAL-API] [GoogleBooks] AUTHENTICATED PRIMARY_SEARCH ATTEMPT: for query='book search'
[EXTERNAL-API] [GoogleBooks] CIRCUIT-BREAKER-OPEN: Blocking authenticated call for query='book search' (unauthenticated fallback will be attempted)
[EXTERNAL-API] [GoogleBooks] SUCCESS: PRIMARY_SEARCH returned 0 result(s) for query='book search'
[EXTERNAL-API] [GoogleBooks] UNAUTHENTICATED FALLBACK_SEARCH ATTEMPT: for query='book search'
[EXTERNAL-API] [PAGED-SEARCH] Unauthenticated paged search: query='book search', maxResults=15, orderBy=relevance
[EXTERNAL-API] [HTTP] UNAUTHENTICATED GET request to: https://www.googleapis.com/books/v1/volumes?q=book+search...
[EXTERNAL-API] [HTTP] Response: status=200, url=https://www.googleapis.com/books/v1/volumes..., bodySize=12345 bytes
[EXTERNAL-API] [GoogleBooks] Unauthenticated search response: query='book search', startIndex=0, itemsInPage=10
[EXTERNAL-API] [PAGED-SEARCH] Unauthenticated paged search complete: query='book search', results=10
[EXTERNAL-API] [GoogleBooks] SUCCESS: FALLBACK_SEARCH returned 10 result(s) for query='book search'
[EXTERNAL-API] [TIERED-SEARCH] COMPLETE: query='book search', postgresResults=5, externalResults=10, totalResults=15
```

## Filtering Logs

### View Only External API Logs

```bash
# In console/terminal
grep "\[EXTERNAL-API\]" application.log

# Real-time monitoring
tail -f application.log | grep "\[EXTERNAL-API\]"
```

### View Only Errors

```bash
grep "\[EXTERNAL-API\].*ERROR\|FAILURE" application.log
```

### View Only Successful API Calls

```bash
grep "\[EXTERNAL-API\].*SUCCESS" application.log
```

### View Circuit Breaker Events

```bash
grep "\[EXTERNAL-API\].*CIRCUIT-BREAKER" application.log
```

### View Search Flow (Start to Complete)

```bash
grep "\[EXTERNAL-API\].*TIERED-SEARCH" application.log
```

## Key Indicators

### ✅ Healthy Operation

- See PRIMARY_SEARCH SUCCESS with results > 0
- See TIERED-SEARCH COMPLETE with merged results
- HTTP responses with status=200
- No CIRCUIT-BREAKER-OPEN messages

### ⚠️ Degraded Operation (But Working)

- CIRCUIT-BREAKER-OPEN messages
- PRIMARY_SEARCH returning 0 results
- FALLBACK_SEARCH attempts being made
- FALLBACK_SEARCH SUCCESS with results > 0

### ❌ Complete Failure

- All searches returning 0 results
- Multiple FAILURE messages
- HTTP errors (429, 500, etc.)
- OpenLibrary also failing

## Troubleshooting with Logs

### Problem: No external results ever returned

**Look for:**

```text
[EXTERNAL-API] [GoogleBooks] DISABLED
```

**Solution:** Check `app.features.external-fallback.enabled` in `application.yml`

### Problem: Circuit breaker constantly open

**Look for:**

```text
[EXTERNAL-API] [GoogleBooks] CIRCUIT-BREAKER-OPEN
```

**Solution:** Check for rate limit errors (HTTP 429) in logs, may need to wait for circuit breaker cooldown

### Problem: Author searches not working

**Look for:**

```text
[EXTERNAL-API] [SEARCH-STRATEGY] Using 'inauthor:...
```

**If missing:** Author detection heuristic may not be recognizing the query as an author name

## Integration Points

All logging is implemented in:

1. **ExternalApiLogger.java** - Centralized logging utility
2. **TieredBookSearchService.java** - Tiered search orchestration
3. **GoogleApiFetcher.java** - Low-level API calls
4. **BookDataOrchestrator.java** - Overall data orchestration

## Next Steps

Once the system is stable and working 100%, you can:

1. Reduce logging verbosity by removing `System.out.println()` calls
2. Change log levels from `INFO` to `DEBUG` for routine operations
3. Keep `WARN`/`ERROR` level logs for genuine issues
