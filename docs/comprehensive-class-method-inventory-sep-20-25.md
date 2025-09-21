# Comprehensive Class and Method Inventory
**Date:** September 20, 2025
**Purpose:** Identify duplicate code/logic for DRY improvements and centralization opportunities

## Table of Contents
1. [Main Application](#main-application)
2. [Configuration Classes](#configuration-classes)
3. [Controllers](#controllers)
4. [Services](#services)
5. [Schedulers](#schedulers)
6. [Utilities](#utilities)
7. [Models & DTOs](#models--dtos)
8. [Repositories](#repositories)
9. [Filters & Support](#filters--support)
10. [Test Classes](#test-classes)
11. [Duplication Analysis](#duplication-analysis)

---

## Main Application

### BookRecommendationEngineApplication
**Path:** `src/main/java/.../BookRecommendationEngineApplication.java`
- **Purpose:** Spring Boot application entry point
- **Methods:**
  - `main(String[] args)` - Application startup
- **Potential Duplication:** Standard Spring Boot app, no duplication

---

## Configuration Classes

### AppRateLimiterConfig
**Path:** `src/main/java/.../config/AppRateLimiterConfig.java`
- **Purpose:** Rate limiter configuration for external APIs (production only)
- **Profile:** `!dev`
- **Methods:**
  - `googleBooksRateLimiter()` - Creates rate limiter for Google Books API (configurable limit/minute)
  - `openLibraryRateLimiter()` - Creates rate limiter for OpenLibrary (1 req/5sec)
  - `longitoodRateLimiter()` - Creates rate limiter for Longitood (100 req/min)
- **Patterns:** All three methods follow same pattern - create RateLimiterConfig, build limiter, log initialization
- **Duplication:** Repetitive rate limiter creation pattern

### AsyncConfig
**Path:** `src/main/java/.../config/AsyncConfig.java`
- **Purpose:** Configure async request handling and thread pools
- **Implements:** WebMvcConfigurer
- **Methods:**
  - `configureAsyncSupport()` - Sets 60sec timeout, assigns task executor
  - `mvcTaskExecutor()` - Creates thread pool (20 core, 100 max, 500 queue)
  - `imageProcessingExecutor()` - Creates CPU-bound thread pool (processor-based sizing)
- **Patterns:** Thread pool creation with naming prefix
- **Duplication:** Similar thread pool setup pattern in both executor methods

### CacheComponentsConfig
**Path:** `src/main/java/.../config/CacheComponentsConfig.java`
- **Purpose:** Cache infrastructure configuration
- **Methods:**
  - `bookDetailCache()` - Caffeine cache for Book objects (20k max, 6hr expiry)
  - `bookDetailCacheMap()` - ConcurrentHashMap for book caching
  - `cacheManager()` - Primary CacheManager with Caffeine config
- **Patterns:** Caffeine cache builder pattern with stats recording
- **Duplication:** Cache configuration duplicated between bookDetailCache and cacheManager

### CacheFactory
**Path:** `src/main/java/.../config/CacheFactory.java`
- **Purpose:** Factory for creating Caffeine caches with consistent configuration
- **Annotations:** @Component, @Configuration
- **Methods:**
  - `createCache(name, maxSize, ttl)` - Creates cache with size limit and TTL
  - `createCacheWithTtl(name, ttl)` - Creates cache with TTL only
  - `createCacheWithSize(name, maxSize)` - Creates cache with size limit only
  - `bookCache()` - Bean: 10k max, 1hr TTL
  - `bookListCache()` - Bean: 1k max, 30min TTL
  - `stringSetCache()` - Bean: 5k max, 2hr TTL
  - `urlMappingCache()` - Bean: 10k max, 24hr TTL
  - `validationCache()` - Bean: 5k max, 1hr TTL
  - `imageCache()` - Bean: 1k max, 15min TTL
- **Patterns:** All factory methods use Caffeine builder with recordStats()
- **Duplication:** Factory methods duplicate Caffeine builder pattern

### CustomBasicAuthenticationEntryPoint
**Path:** `src/main/java/.../config/CustomBasicAuthenticationEntryPoint.java`
- **Purpose:** Custom HTTP Basic authentication entry point with JSON error responses
- **Extends:** BasicAuthenticationEntryPoint
- **Methods:**
  - `commence()` - Returns JSON error response with authentication instructions
  - `afterPropertiesSet()` - Sets realm name to "BookRecommendationAdmin"
- **Features:** Custom JSON response format, detailed authentication guidance
- **Duplication:** Standard authentication entry point pattern

### DatabaseUrlEnvironmentPostProcessor
**Path:** `src/main/java/.../config/DatabaseUrlEnvironmentPostProcessor.java`
- **Purpose:** Converts postgres:// URLs to JDBC format, extracts credentials
- **Implements:** EnvironmentPostProcessor, Ordered
- **Inner Class:** `JdbcParseResult` - holds jdbcUrl, username, password
- **Methods:**
  - `normalizePostgresUrl(String)` - Static parser for postgres URLs to JDBC format
  - `postProcessEnvironment()` - Spring hook to transform datasource URLs
  - `getOrder()` - Returns HIGHEST_PRECEDENCE
- **Complex Logic:** Manual URL parsing with host/port/database/query extraction
- **Duplication:** None identified

### DevModeConfig
**Path:** `src/main/java/.../config/DevModeConfig.java`
- **Purpose:** Development mode configuration to minimize API calls and enhance caching
- **Profile:** dev
- **Dependencies:** Caffeine, Resilience4j RateLimiter
- **Methods:**
  - `devCacheManager()` - Enhanced cache manager with longer TTLs
  - `googleBooksRateLimiter()` - Rate limiter for Google Books API
  - `logCacheStats()` - @Scheduled cache statistics logging
- **Features:** Configurable cache TTLs, request limiting, cache monitoring
- **Duplication:** Cache configuration patterns similar to CacheComponentsConfig

### FeatureFlagConfig
**Path:** `src/main/java/.../config/FeatureFlagConfig.java`
- **Purpose:** Centralized feature flag configuration for runtime toggles
- **Methods:**
  - `isYearFilteringEnabled()` - @Bean for year filtering feature state
  - `isEmbeddingServiceEnabled()` - @Bean for embedding service feature state
- **Configuration:** Property-based toggles with sensible defaults
- **Duplication:** Simple bean factory pattern

### GoogleBooksConfig
**Path:** `src/main/java/.../config/GoogleBooksConfig.java`
- **Purpose:** Google Books API WebClient configuration
- **Methods:**
  - `googleBooksWebClient()` - Configured WebClient with 16MB buffer
  - `getGoogleBooksApiBaseUrl()` - Getter for base URL
  - `getMaxResults()` - Getter for max results limit
  - `getConnectTimeout()` - Getter for connection timeout
  - `getReadTimeout()` - Getter for read timeout
- **Features:** Large buffer size for responses, property-based configuration
- **Duplication:** Similar WebClient configuration to WebClientConfig

### NoDatabaseConfig
**Path:** `src/main/java/.../config/NoDatabaseConfig.java`
- **Purpose:** Disables database auto-configuration when no datasource URL is present
- **Conditional:** @ConditionalOnExpression checking empty datasource URL
- **Features:** Excludes DataSource, JPA, Hibernate auto-configuration
- **Duplication:** Standard conditional configuration pattern

### S3Config
**Path:** `src/main/java/.../config/S3Config.java`
- **Purpose:** S3 client configuration for book cover storage
- **Conditional:** S3EnvironmentCondition
- **Fields:** accessKeyId, secretAccessKey, s3ServerUrl, s3Region (us-west-2 default)
- **Methods:**
  - `s3Client()` - Creates S3Client with credentials and endpoint override
- **Features:** Supports MinIO/local S3, validates config, graceful degradation
- **Duplication:** None identified

### S3EnvironmentCondition
**Path:** `src/main/java/.../config/S3EnvironmentCondition.java`
- **Purpose:** Custom condition checking for S3 environment variables
- **Implements:** Condition
- **Methods:**
  - `matches()` - Validates S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET presence
- **Features:** One-time logging, environment variable validation
- **Duplication:** Standard Spring condition pattern

### S3HealthIndicator
**Path:** `src/main/java/.../config/S3HealthIndicator.java`
- **Purpose:** Reactive health indicator for S3 bucket accessibility
- **Implements:** ReactiveHealthIndicator
- **Dependencies:** S3Client (optional)
- **Methods:**
  - `health()` - Reactive health check using S3 headBucket operation
- **Features:** 5-second timeout, detailed error reporting, graceful degradation
- **Duplication:** Standard health indicator pattern

### SearchPageHealthIndicator
**Path:** `src/main/java/.../config/SearchPageHealthIndicator.java`
- **Purpose:** Health indicator for search page availability with dynamic configuration
- **Implements:** ReactiveHealthIndicator, ApplicationListener<WebServerInitializedEvent>
- **Dependencies:** WebPageHealthIndicator delegate
- **Methods:**
  - `onApplicationEvent()` - Configures health check after server startup
  - `health()` - Delegates to WebPageHealthIndicator
- **Features:** Dynamic port configuration, test query "healthcheck"
- **Duplication:** Similar pattern to other health indicators

### SecurityConfig
**Path:** `src/main/java/.../config/SecurityConfig.java`
- **Purpose:** Spring Security configuration with role-based access
- **Annotations:** @EnableWebSecurity, @EnableMethodSecurity
- **Conditional:** WebApplication.Type.SERVLET
- **Methods:**
  - `securityFilterChain(HttpSecurity)` - Configures auth rules, admin paths require ADMIN role
  - `configureSecurity(HttpSecurity)` - Sets CSP headers with dynamic directives for Clicky, Google Books, CDN
  - `passwordEncoder()` - BCryptPasswordEncoder bean
  - `userDetailsService()` - In-memory users (admin, user) with encoded passwords
- **Features:** CSP header management, referrer policy, form login, HTTP basic auth
- **Duplication:** CSP directive building logic is complex and repetitive

### SitemapProperties
**Path:** `src/main/java/.../config/SitemapProperties.java`
- **Purpose:** Configuration properties for sitemap generation
- **Annotations:** @ConfigurationProperties(prefix = "sitemap")
- **Properties:** baseUrl, htmlPageSize, xmlPageSize, schedulerEnabled, schedulerCron, schedulerCoverSampleSize, schedulerExternalHydrationSize, s3AccumulatedIdsKey
- **Methods:** Standard getters and setters for all properties
- **Features:** Default values, strongly typed configuration
- **Duplication:** Standard configuration properties pattern

### WebClientConfig
**Path:** `src/main/java/.../config/WebClientConfig.java`
- **Purpose:** WebClient configuration with timeouts
- **Methods:**
  - `webClientBuilder()` - Creates WebClient.Builder with 5sec timeouts, 10MB buffer
- **Configuration:** Connect timeout 5000ms, read/write timeout 5sec, response timeout 5sec
- **Duplication:** Timeout values could be externalized, similar to GoogleBooksConfig

### WebConfig
**Path:** `src/main/java/.../config/WebConfig.java`
- **Purpose:** Web MVC configuration for static resource handling
- **Implements:** WebMvcConfigurer
- **Methods:**
  - `addResourceHandlers()` - Configures resource handlers for cover cache and static resources
- **Features:** Dynamic cover cache directory mapping, 30-day browser caching, file system resource serving
- **Duplication:** Standard resource handler configuration

### WebPageHealthIndicator
**Path:** `src/main/java/.../config/WebPageHealthIndicator.java`
- **Purpose:** Reusable health indicator for web page availability checks
- **Classes:** WebPageHealthIndicator, HomepageHealthIndicator, BookDetailPageHealthIndicator
- **Methods:**
  - `checkPage()` - Performs HTTP health check with timeout
- **Features:** Configurable error handling, 5-second timeout, detailed error reporting
- **Duplication:** Health indicator pattern reused across multiple page types

### WebSocketConfig
**Path:** `src/main/java/.../config/WebSocketConfig.java`
- **Purpose:** WebSocket configuration for real-time communication
- **Implements:** WebSocketMessageBrokerConfigurer
- **Annotations:** @EnableWebSocketMessageBroker
- **Methods:**
  - `configureMessageBroker()` - Sets up in-memory broker for /topic destinations
  - `registerStompEndpoints()` - Registers /ws endpoint with SockJS fallback
- **Features:** CORS support, real-time notifications
- **Duplication:** Standard WebSocket configuration pattern

---

## Controllers

### HomeController
**Path:** `src/main/java/.../controller/HomeController.java`
- **Purpose:** Main web controller for user-facing pages (home, search, book details)
- **Fields:**
  - 15+ service dependencies (bookDataOrchestrator, googleBooksService, recentlyViewedService, etc.)
  - Multiple @Value config fields for affiliates, SEO, covers
  - Static lists for explore queries, regex patterns for ISBN
- **Key Methods:**
  - `home(Model)` - Displays homepage with bestsellers & recently viewed (max 8 each)
  - `search(query, year, Model)` - Search page with year filtering support
  - `bookDetail(id, query, page, sort, view, Model)` - Book detail page with SEO, affiliates, similar books
  - `bookDetailByIsbn(isbn)` - ISBN lookup with redirect to canonical URL
  - `bookDetailByIsbn13(isbn13)` - ISBN-13 specific lookup
  - `bookDetailByIsbn10(isbn10)` - ISBN-10 specific lookup
  - `explore()` - Random search query redirect
- **Helper Methods:**
  - `isActualCover(Book)` - Filters placeholder images
  - `deduplicateBooksById(List<Book>)` - Remove duplicate books by ID
  - `processBooksCovers(List<Book>)` - Batch cover image processing
  - `generateKeywords(Book)` - SEO keyword generation
  - `fetchCanonicalBook(identifier)` - Multi-strategy book lookup (slug, ID, UUID)
  - `fetchAdditionalHomepageBooks(query, limit)` - Fill homepage when not enough recent books
  - `isValidIsbn(isbn)` - ISBN validation
  - `redirectTo(path)` - 303 redirect helper
- **Patterns:** Heavy use of Reactive (Mono/Flux), complex fallback chains, SEO optimization
- **Duplication:**
  - Affiliate link generation repeated in multiple places
  - Cover processing logic duplicated between methods
  - ISBN validation patterns could be centralized

### BookController
**Path:** `src/main/java/.../controller/BookController.java`
- **Purpose:** REST API for book operations (Postgres-first)
- **Base Path:** `/api/books`
- **Dependencies:** BookDataOrchestrator, RecommendationService
- **Endpoints:**
  - `GET /search` - Search books with pagination (startIndex, maxResults)
  - `GET /authors/search` - Search authors by query
  - `GET /{identifier}` - Get book by ID or slug
  - `GET /{identifier}/similar` - Get similar book recommendations
- **Helper Methods:**
  - `fetchBook(identifier)` - Try ID then slug lookup
  - `buildSearchResponse()` - Paginated search response builder
  - `toSearchHit(Book)` - Convert to SearchHitDto with relevance
  - `buildAuthorResponse()` - Author search response builder
  - `toAuthorHit()` - Convert AuthorResult to DTO
- **Inner Records:** SearchResponse, SearchHitDto, AuthorSearchResponse, AuthorHitDto
- **Patterns:** Clean REST design, consistent error handling, DTO mapping
- **Duplication:** fetchBook logic similar to HomeController's fetchCanonicalBook

### AdminController
**Path:** `src/main/java/.../controller/AdminController.java`
- **Purpose:** REST Controller for administrative operations (S3 cleanup, scheduler triggers, circuit breaker management)
- **Dependencies:** S3CoverCleanupService, NewYorkTimesBestsellerScheduler, BookCacheWarmingScheduler, ApiCircuitBreakerService
- **Endpoints:**
  - `GET /admin/s3-cleanup/dry-run` - S3 cover cleanup dry run with batch limits
  - `POST /admin/s3-cleanup/move-flagged` - Move flagged S3 images to quarantine
  - `POST /admin/trigger-nyt-bestsellers` - Trigger NYT bestseller processing
  - `POST /admin/trigger-cache-warming` - Trigger book cache warming
  - `GET /admin/circuit-breaker/status` - Get circuit breaker status
  - `POST /admin/circuit-breaker/reset` - Reset circuit breaker
- **Patterns:** Comprehensive error handling, optional service dependencies, configurable batch processing
- **Duplication:** Similar error handling patterns across endpoints, parameter validation repeated

### BookCoverController
**Path:** `src/main/java/.../controller/BookCoverController.java`
- **Purpose:** REST API for book cover image operations and retrieval
- **Dependencies:** BookDataOrchestrator, GoogleBooksService, BookImageOrchestrationService
- **Endpoints:**
  - `GET /api/covers/{id}` - Get best cover URL with source preference
- **Methods:**
  - `firstNonBlank(String...)` - Find first non-blank string
  - `parsePreferredSource(String)` - Parse CoverImageSource enum
- **Patterns:** Reactive programming (Mono/Flux), DeferredResult for async processing, complex fallback chains
- **Error Handling:** @ExceptionHandler for IllegalArgumentException, AsyncRequestTimeoutException
- **Duplication:** Cover URL fallback logic could be centralized, timeout handling patterns repeated

### BookCoverPreferenceController
**Path:** `src/main/java/.../controller/BookCoverPreferenceController.java`
- **Purpose:** Global controller advice for managing book cover image source preferences
- **Annotations:** @ControllerAdvice
- **Methods:**
  - `addCoverSourcePreference()` - @ModelAttribute for cover source preference
  - `addCoverSourceOptions()` - @ModelAttribute for cover source options array
- **Patterns:** Controller advice pattern, automatic model population
- **Duplication:** Similar pattern to ImageResolutionPreferenceController

### ApiMetricsController
**Path:** `src/main/java/.../controller/ApiMetricsController.java`
- **Purpose:** REST controller for exposing API usage metrics (admin access only)
- **Dependencies:** ApiRequestMonitor
- **Security:** @PreAuthorize("hasRole('ADMIN')")
- **Endpoints:**
  - `GET /admin/api-metrics/report` - Human-readable metrics report
  - `GET /admin/api-metrics` - JSON format metrics
  - `GET /admin/api-metrics/hourly-count` - Current hour request count
  - `GET /admin/api-metrics/daily-count` - Current day request count
  - `GET /admin/api-metrics/total-count` - Total request count
- **Patterns:** Multiple format outputs (text/JSON), granular metric endpoints
- **Duplication:** Simple delegation to service methods

### ErrorDiagnosticsController
**Path:** `src/main/java/.../controller/ErrorDiagnosticsController.java`
- **Purpose:** Global error controller implementing Spring Boot's ErrorController interface
- **Dependencies:** ErrorAttributes
- **Methods:**
  - `handleError(WebRequest, Model)` - Processes all application errors
- **Features:** Stack trace inclusion, exception type extraction, comprehensive error attributes
- **Patterns:** Spring Boot error handling, detailed error diagnostics
- **Duplication:** Standard Spring Boot error controller pattern

### ImageResolutionPreferenceController
**Path:** `src/main/java/.../controller/ImageResolutionPreferenceController.java`
- **Purpose:** Global controller advice for managing image resolution preferences
- **Annotations:** @ControllerAdvice
- **Methods:**
  - `addResolutionPreference()` - @ModelAttribute for resolution preference
  - `addResolutionOptions()` - @ModelAttribute for resolution options array
- **Patterns:** Controller advice pattern, automatic model population
- **Duplication:** Nearly identical pattern to BookCoverPreferenceController

### RobotsController
**Path:** `src/main/java/.../controller/RobotsController.java`
- **Purpose:** Serves robots.txt with dynamic content based on environment
- **Methods:**
  - `getRobotsTxt()` - Returns environment-specific robots.txt content
- **Logic:** Permissive for production main branch, restrictive for all other environments
- **Configuration:** Uses @Value for coolify.url and coolify.branch
- **Patterns:** Environment-aware configuration, string templating
- **Duplication:** Environment detection logic could be centralized

### SitemapController
**Path:** `src/main/java/.../controller/SitemapController.java`
- **Purpose:** Serves sitemap pages and XML sitemap feeds
- **Dependencies:** SitemapService, SitemapProperties
- **Endpoints:**
  - `GET /sitemap` - Redirect to normalized sitemap URLs
  - `GET /sitemap/{view}/{letter}/{page}` - Dynamic sitemap pages
  - `GET /sitemap.xml` - XML sitemap index
  - `GET /sitemap-xml/books/{page}.xml` - Books XML sitemap
  - `GET /sitemap-xml/authors/{page}.xml` - Authors XML sitemap
- **Helper Methods:**
  - `normalizeView(String)` - Normalize view parameter
  - `populateSitemapModel()` - Populate model for sitemap template
  - `buildBookUrlSet()` - Generate XML for book URLs
  - `buildAuthorUrlSet()` - Generate XML for author URLs
  - `escapeXml(String)` - XML entity escaping
- **Patterns:** URL normalization, XML generation, pagination
- **Duplication:** XML building patterns could be abstracted, URL normalization logic

### ThemePreferenceController
**Path:** `src/main/java/.../controller/ThemePreferenceController.java`
- **Purpose:** REST controller for managing user theme preferences (light/dark mode)
- **Base Path:** `/api/theme`
- **Methods:**
  - `getThemePreference(HttpServletRequest)` - GET endpoint for current theme preference
  - `updateThemePreference(Map, HttpServletResponse)` - POST endpoint for updating theme
  - `getThemeCookieValue(HttpServletRequest)` - Helper to extract theme from cookies
- **Features:** Cookie-based persistence, system theme fallback, secure cookie management
- **Cookie:** "preferred_theme" with 1-year expiration
- **Duplication:** Cookie handling patterns could be abstracted

---

## Services

### BookDataOrchestrator
- **Path:** `src/main/java/.../service/BookDataOrchestrator.java`
- **Purpose:** Orchestrates book data retrieval through a tiered fetch strategy (DB → S3 → APIs)
- **Dependencies:** S3RetryService, GoogleApiFetcher, ObjectMapper, OpenLibraryBookDataService, BookDataAggregatorService, BookCollectionPersistenceService, BookSearchService, BookS3CacheService, PostgresBookRepository, CanonicalBookPersistenceService, TieredBookSearchService
- **Source of Truth:** Canonical tiered lookups (`getBookByIdTiered`, `fetchCanonicalBookReactive`) and downstream persistence wiring.
- **Followers:** `BookApiProxy`, `RecommendationService`, controllers delegate to these methods instead of duplicating DB/S3/API ladders.
- **Key Methods:**
  - `refreshSearchView()` - Refreshes materialized search view
  - `getBookFromDatabase(String)` - Direct Postgres lookup without fallbacks
  - `getBookFromDatabaseBySlug(String)` - Postgres lookup by slug
  - `getBookByIdTiered(String)` - Multi-tier fetch (DB → S3 → APIs)
  - `getBookBySlugTiered(String)` - Slug-based tiered lookup
  - `searchBooksTiered()` - Tiered search across data sources
  - `searchAuthors()` - Author search with tiered approach
  - `migrateBooksFromS3()` - Bulk S3 to DB migration
  - `migrateListsFromS3()` - S3 list migration
- **Private Methods:**
  - `fetchFromApisAndAggregate()` - Aggregates data from multiple APIs
  - `persistBook()` - Saves book with enrichment options
  - `parseBookJsonPayload()` - JSON parsing for migration
- **Patterns:** Reactive programming, tiered caching, fallback chains, search view refresh
- **Duplication:** Similar tiered patterns across different lookup methods

### GoogleBooksService
**Path:** `src/main/java/.../service/GoogleBooksService.java`
- **Purpose:** Service for interacting with Google Books API with circuit breaking and rate limiting
- **Dependencies:** ObjectMapper, ApiRequestMonitor, GoogleApiFetcher, BookDataOrchestrator
- **Resilience:** @CircuitBreaker, @TimeLimiter, @RateLimiter annotations
- **Source of Truth:** Google API fetch orchestration + cover provenance evaluation before data reaches orchestrator.
- **Followers:** `TieredBookSearchService`, `BookApiProxy`, schedulers reuse these resilient fetchers rather than hand-rolling WebClient calls.
- **Key Methods:**
  - `searchBooks()` - Single page search with resilience patterns
  - `searchBooksAsyncReactive()` - Comprehensive multi-page search with reactive API
  - `searchBooksByTitle()`, `searchBooksByAuthor()`, `searchBooksByISBN()` - Specialized searches
  - `getBookById()` - Individual book retrieval by Google ID
  - `getSimilarBooks()` - Find similar books by author/title
  - `fetchGoogleBookIdByIsbn()` - ISBN to Google ID mapping
  - `fetchGoogleBookIdsForMultipleIsbns()` - Batch ISBN processing
  - `fetchMultipleBooksByIdsTiered()` - Bulk book details via orchestrator
- **Fallback Methods:**
  - `searchBooksFallback()`, `searchBooksRateLimitFallback()` - Circuit breaker fallbacks
  - `getBookByIdFallback()`, `getBookByIdRateLimitFallback()` - Rate limit handling
- **Patterns:** Circuit breaker pattern, reactive streams, batch processing with delays
- **Duplication:** Similar fallback method patterns, resilience annotation patterns

### RecommendationService
- **Path:** `src/main/java/.../service/RecommendationService.java`
- **Purpose:** Generates book recommendations using multi-faceted similarity criteria
- **Dependencies:** BookDataOrchestrator, BookRecommendationPersistenceService
- **Strategy:** Multi-strategy approach (author, category, text matching)
- **Source of Truth:** Delegates all lookups/searches to `BookDataOrchestrator` + `TieredBookSearchService`; contains only scoring/aggregation rules.
- **Followers:** Reused by `BookApiProxy` and controllers consuming recommendation flows.
- **Key Methods:**
  - `getSimilarBooks(String, int)` - Main recommendation method with caching
  - `fetchRecommendationsFromApiAndUpdateCache()` - API-based recommendation generation
- **Private Methods:**
  - `findBooksByAuthorsReactive()` - Author-based recommendations (score 4.0)
  - `findBooksByCategoriesReactive()` - Category-based recommendations
  - `findBooksByTextReactive()` - Keyword-based recommendations
  - `calculateCategoryOverlapScore()` - Category similarity scoring
  - `normalizeCategories()` - Category standardization
  - `fetchCanonicalBook()` - Canonical book resolution
  - `searchBooksTiered()` - Tiered search with fallbacks
- **Inner Classes:** `ScoredBook` - Book with similarity score and reasons
- **Patterns:** Reactive streams, scoring algorithms, caching strategies
- **Duplication:** Similar tiered search patterns, scoring mechanisms

### BookSearchService
**Path:** `src/main/java/.../service/BookSearchService.java`
- **Purpose:** Direct PostgreSQL search service using database functions
- **Dependencies:** JdbcTemplate
- **Record Classes:** `SearchResult`, `AuthorResult`, `IsbnSearchResult`
- **Key Methods:**
  - `searchBooks(String, Integer)` - Full-text search with relevance scoring
  - `searchByIsbn(String)` - ISBN-specific search
  - `searchAuthors(String, Integer)` - Author search with book counts
  - `refreshMaterializedView()` - Search view refresh
- **Helper Methods:**
  - `normaliseQuery()` - Query sanitization
  - `normaliseIsbn()` - ISBN sanitization
  - `safeLimit()` - Result limiting with bounds checking
- **Patterns:** Database function calls, result mapping, data sanitization
- **Duplication:** Similar normalization patterns, result limiting logic

### BookApiProxy
- **Path:** `src/main/java/.../service/BookApiProxy.java`
- **Purpose:** Smart API proxy with multi-level caching to minimize external calls
- **Dependencies:** GoogleBooksService, BookDataOrchestrator, ObjectMapper, GoogleBooksMockService
- **Caching Layers:** In-memory cache, local file cache, mock service, S3 cache, database tier
- **Source of Truth:** Uses `BookDataOrchestrator` for canonical hydration/search before touching external APIs; only owns cache orchestration now.
- **Followers:** Search endpoints and cover flows that need cached data reuse the proxy’s simplified API, but rely on orchestrator results.
- **Key Methods:**
  - `getBookById(String)` - Smart book retrieval with caching
  - `searchBooks(String, String)` - Cached search functionality
- **Private Methods:**
  - `processBookRequest()` - Multi-layer book request processing
  - `processSearchRequest()` - Multi-layer search request processing
  - `getBookFromLocalCache()` - Local file cache retrieval
  - `saveBookToLocalCache()` - Local file cache storage
  - `sanitizeSearchResults()` - Result filtering and limiting
- **Features:** Request deduplication, memory management, configurable caching strategies
- **Patterns:** Cache-aside pattern, async processing, request merging
- **Duplication:** Similar caching patterns across book and search methods

### GoogleApiFetcher
**Path:** `src/main/java/.../service/GoogleApiFetcher.java`
- **Purpose:** Low-level Google Books API client with retry logic and circuit breaking
- **Dependencies:** WebClient, ApiRequestMonitor, ApiCircuitBreakerService
- **Key Methods:**
  - `fetchVolumeByIdAuthenticated()` - Authenticated volume retrieval
  - `fetchVolumeByIdUnauthenticated()` - Unauthenticated volume retrieval
  - `searchVolumesAuthenticated()` - Authenticated search
  - `searchVolumesUnauthenticated()` - Unauthenticated search
  - `streamSearchItems()` - Streaming search results across pages
- **Private Methods:**
  - `fetchVolumeByIdInternal()` - Internal volume fetch implementation
  - `searchVolumesInternal()` - Internal search implementation
  - `performGetJson()` - HTTP request execution with retry
  - `getQueryTypeForMonitoring()` - Query categorization for metrics
- **Features:** Circuit breaker integration, retry logic, API key management, rate limiting
- **Patterns:** Retry with backoff, circuit breaker pattern, reactive error handling
- **Duplication:** Similar retry patterns across authenticated/unauthenticated methods

### RecentBookViewRepository
**Path:** `src/main/java/.../service/RecentBookViewRepository.java`
- **Purpose:** Repository for persisting and aggregating recent book view activity
- **Dependencies:** JdbcTemplate (optional)
- **Record Classes:** `ViewStats` - aggregated view statistics
- **Key Methods:**
  - `recordView()` - Records individual book view
  - `fetchStatsForBook()` - Gets view statistics for single book
  - `fetchMostRecentViews()` - Gets recently viewed books with stats
  - `isEnabled()` - Checks if database is available
- **Features:** Time-windowed aggregations (24h, 7d, 30d), graceful degradation when DB unavailable
- **Patterns:** Repository pattern, optional dependency handling, time-based analytics
- **Duplication:** Similar database query patterns, null checking

### RecentlyViewedService
**Path:** `src/main/java/.../service/RecentlyViewedService.java`
- **Purpose:** Manages recently viewed books with in-memory storage and database integration
- **Dependencies:** GoogleBooksService, BookDataOrchestrator, DuplicateBookService, RecentBookViewRepository
- **Storage:** LinkedList for in-memory storage with size limits
- **Key Methods:**
  - `addToRecentlyViewed()` - Adds book with canonical ID resolution
  - `getRecentlyViewedBooksReactive()` - Reactive recently viewed books retrieval
  - `getRecentlyViewedBooks()` - Blocking version of reactive method
  - `fetchDefaultBooksAsync()` - Fallback recommendations when no history
  - `clearRecentlyViewedBooks()` - Clears in-memory history
- **Private Methods:**
  - `prepareDefaultBooks()` - Filters and sorts default recommendations
  - `loadFromRepositoryMono()` - Loads from database repository
  - `applyViewStats()` - Applies view statistics to book objects
  - `compareByLastViewedThenPublished()` - Custom sorting logic
  - `isValidCoverImage()` - Cover image validation
- **Features:** Thread-safe operations, canonical ID resolution, fallback strategies
- **Patterns:** Reactive programming, thread-safe collections, tiered data sources
- **Duplication:** Similar tiered lookup patterns, thread safety patterns

### NewYorkTimesService
**Path:** `src/main/java/.../service/NewYorkTimesService.java`
- **Purpose:** Service for interacting with the New York Times Books API to fetch bestseller lists
- **Dependencies:** WebClient, JdbcTemplate
- **Key Methods:**
  - `fetchBestsellerListOverview()` - Fetches full bestseller overview from NYT API
  - `getCurrentBestSellers(String, int)` - Gets current bestsellers from database by list code
- **Features:** Complex SQL joins with image selection, reactive error handling, caching support
- **Database Query:** Complex JOIN across book_collections, book_collections_join, books, and book_image_links tables
- **Patterns:** Database-first approach with API fallback, image prioritization logic
- **Duplication:** Database query patterns similar to other services

### OpenLibraryBookDataService
**Path:** `src/main/java/.../service/OpenLibraryBookDataService.java`
- **Purpose:** Service for interacting with OpenLibrary API to fetch book information
- **Dependencies:** WebClient
- **Resilience:** @CircuitBreaker, @RateLimiter, @TimeLimiter annotations
- **Key Methods:**
  - `fetchBookByIsbn(String)` - Fetches book data by ISBN
  - `searchBooksByTitle(String)` - Searches books by title
- **Private Methods:**
  - `parseOpenLibraryBook()` - Converts OpenLibrary JSON to Book object
  - `parseOpenLibrarySearchDoc()` - Converts search result JSON to Book
- **Fallback Methods:**
  - `fetchBookFallback()`, `searchBooksFallback()` - Circuit breaker fallbacks
- **Features:** Complex date parsing with multiple formats, ISBN handling, cover image URL construction
- **Patterns:** Resilience patterns, JSON parsing with multiple fallbacks
- **Duplication:** Similar resilience patterns to GoogleBooksService, date parsing logic could be centralized

### BookSitemapService
**Path:** `src/main/java/.../service/BookSitemapService.java`
- **Purpose:** Builds sitemap snapshots from Postgres and synchronizes supporting artifacts
- **Dependencies:** SitemapService, SitemapProperties, ObjectMapper, BookDataOrchestrator, S3StorageService
- **Record Classes:** `SitemapSnapshot`, `SnapshotSyncResult`, `ExternalHydrationSummary`
- **Key Methods:**
  - `synchronizeSnapshot()` - Orchestrates snapshot build and upload
  - `buildSnapshot()` - Creates sitemap snapshot from database
  - `uploadSnapshot()` - Uploads snapshot to S3
  - `hydrateExternally()` - External hydration with timeout and retry logic
- **Private Methods:**
  - `buildSnapshotPayload()` - Creates JSON payload for S3 upload
- **Features:** S3 integration, external hydration with timeouts, comprehensive error handling
- **Patterns:** Builder pattern, aggregation across pages, timeout handling
- **Duplication:** S3 upload patterns, JSON serialization logic

### ApiRequestMonitor
**Path:** `src/main/java/.../service/ApiRequestMonitor.java`
- **Purpose:** Service for monitoring API request metrics and usage patterns
- **Storage:** ConcurrentHashMap for thread-safe metrics storage
- **Key Methods:**
  - `recordSuccessfulRequest(String)` - Records successful API calls
  - `recordFailedRequest(String, String)` - Records failed API calls with error details
  - `recordMetric(String, String)` - Records custom metrics with details
  - `generateReport()` - Creates human-readable metrics report
  - `getMetricsMap()` - Returns metrics as structured map
- **Scheduled Methods:**
  - `resetHourlyCounters()` - @Scheduled hourly reset (cron: "0 0 * * * ?")
  - `resetDailyCounters()` - @Scheduled daily reset (cron: "0 0 0 * * ?")
- **Private Methods:**
  - `checkAndUpdateTimePeriods()` - Hour/day boundary detection
- **Features:** Thread-safe counters, time-windowed metrics, custom metric tracking with limits
- **Patterns:** Observer pattern, scheduled tasks, concurrent data structures
- **Duplication:** Time-based metric patterns could be abstracted

### BookLookupService
**Path:** `src/main/java/.../service/BookLookupService.java`
- **Purpose:** Centralized service for book lookup operations to eliminate duplicate ISBN query patterns
- **Dependencies:** JdbcTemplate
- **Source of Truth:** ISBN/external ID resolution (`findBookIdByIsbn*`, `resolveCanonicalBookId`).
- **Followers:** Persistence services, schedulers, and `BookDataOrchestrator` consumers (controllers, schedulers) now route through this helper.
- **Key Methods:**
  - `findBookIdByIsbn13(String)` - Finds book ID by ISBN13 (books table + external IDs fallback)
  - `findBookIdByIsbn10(String)` - Finds book ID by ISBN10 (books table + external IDs fallback)
  - `findBookIdByExternalId(String, String)` - Finds book ID by external provider ID
  - `resolveCanonicalBookId(String, String)` - Canonical ID resolution with ISBN13/ISBN10 fallback
  - `findBookById(String)` - Checks if book exists by ID
  - `queryForId(String, Object...)` - Backward compatibility helper method
- **Features:** Consistent lookup patterns, fallback strategies, null-safe operations
- **Patterns:** Repository pattern, fallback chains, method delegation
- **Duplication:** Centralizes common lookup patterns found throughout the codebase

### BookS3CacheService
**Path:** `src/main/java/.../service/BookS3CacheService.java`
- **Purpose:** Encapsulates S3 cache update heuristics for intelligent cache management
- **Dependencies:** S3RetryService, ObjectMapper
- **Key Methods:**
  - `updateCache(Book, JsonNode, String, String)` - Main cache update orchestration
- **Private Methods:**
  - `shouldUpdateS3()` - Intelligent cache update decision logic
  - `countNonNullKeyFields()` - Counts populated fields for cache decision heuristics
- **Features:** Smart cache update decisions based on data completeness and quality
- **Heuristics:** Description length comparison (10% improvement threshold), field completeness scoring
- **Patterns:** Strategy pattern for cache decisions, reactive error handling
- **Duplication:** Data quality assessment patterns could be reused

### CanonicalBookPersistenceService
**Path:** `src/main/java/.../service/CanonicalBookPersistenceService.java`
- **Purpose:** Handles canonical Book persistence into Postgres with comprehensive relationship management
- **Dependencies:** JdbcTemplate, ObjectMapper, BookSupplementalPersistenceService, TransactionTemplate
- **Record Classes:** `EditionLinkRecord`
- **Key Methods:**
  - `enrichAndSave(Book, JsonNode)` - Main enrichment and save orchestration
  - `saveBook(Book, JsonNode)` - Book saving with transaction support
- **Private Methods:**
  - `persistCanonicalBook()` - Core persistence logic
  - `resolveCanonicalBookId()` - Canonical ID resolution with multiple fallbacks
  - `upsertBookRecord()` - Main book table upsert with COALESCE logic
  - `upsertExternalMetadata()` - External ID metadata persistence
  - `persistImageLinks()` - Image link management
  - `synchronizeEditionRelationships()` - Complex edition relationship management
  - `persistDimensions()` - Physical dimension persistence
  - `persistRawJson()` - Raw JSON payload storage
- **Features:** Comprehensive book persistence, relationship management, UUID validation, slug generation
- **Patterns:** Transactional operations, upsert patterns, relationship synchronization
- **Duplication:** Similar ID resolution patterns to BookLookupService, UUID validation repeated

### PostgresBookRepository
**Path:** `src/main/java/.../service/PostgresBookRepository.java`
- **Purpose:** Repository for hydrating full Book aggregates directly from Postgres
- **Dependencies:** JdbcTemplate, ObjectMapper
- **Record Classes:** `CoverCandidate`
- **Key Methods:**
  - `fetchByCanonicalId(String)` - Fetch by UUID with full hydration
  - `fetchBySlug(String)` - Fetch by URL slug
  - `fetchByIsbn13(String)`, `fetchByIsbn10(String)` - ISBN-based lookups
  - `fetchByExternalId(String)` - External ID lookup with multiple fallbacks
- **Private Methods:**
  - `loadAggregate(UUID)` - Main book loading with full relationship hydration
  - `hydrateAuthors()`, `hydrateCategories()`, `hydrateCollections()` - Relationship hydration
  - `hydrateDimensions()`, `hydrateRawPayload()`, `hydrateTags()` - Additional data hydration
  - `hydrateEditions()`, `hydrateCover()`, `hydrateRecommendations()` - Complex relationship loading
  - `hydrateProviderMetadata()` - External provider data loading
- **Features:** Complex SQL joins, full object graph hydration, cover image prioritization
- **Patterns:** Repository pattern, lazy loading, complex aggregation queries
- **Duplication:** Similar lookup patterns to other repository services, ISBN resolution logic

### S3StorageService
**Path:** `src/main/java/.../service/S3StorageService.java`
- **Purpose:** Service for handling file storage operations in S3 with async support
- **Dependencies:** S3Client
- **Conditional:** @Conditional(S3EnvironmentCondition.class)
- **Key Methods:**
  - `uploadFileAsync()` - Async file upload with public URL generation
  - `uploadJsonAsync()` - JSON upload with GZIP compression for Google Books cache
  - `fetchJsonAsync()` - JSON fetch with automatic GZIP decompression
  - `uploadGenericJsonAsync()` - Generic JSON upload with optional compression
  - `fetchGenericJsonAsync()` - Generic JSON fetch with compression handling
  - `listObjects()` - S3 object listing with pagination support
  - `downloadFileAsBytes()` - File download as byte array
  - `copyObject()`, `deleteObject()` - S3 object operations
- **Features:** CDN integration, compression support, reactive programming, comprehensive error handling
- **Patterns:** Async/reactive patterns, builder pattern for requests, graceful degradation
- **Duplication:** Compression logic, async patterns, S3 error handling

### SitemapService
**Path:** `src/main/java/.../service/SitemapService.java`
- **Purpose:** Coordinates Postgres-backed sitemap data access for both HTML and XML rendering
- **Dependencies:** SitemapRepository, SitemapProperties
- **Record Classes:** `SitemapOverview`, `PagedResult<T>`, `BookSitemapItem`, `AuthorSection`, `AuthorListingDescriptor`, `AuthorListingXmlItem`
- **Key Methods:**
  - `getOverview()` - Sitemap overview with letter counts
  - `getBooksByLetter()` - Paginated books by letter bucket
  - `getAuthorsByLetter()` - Paginated authors with their books
  - `getBooksXmlPageCount()`, `getBooksForXmlPage()` - XML sitemap generation
  - `getAuthorXmlPageCount()`, `getAuthorListingsForXmlPage()` - Author XML listings
  - `listAuthorListingDescriptors()` - Author listing page descriptors
- **Private Methods:**
  - `normalizeBucket()` - Letter bucket normalization (A-Z, 0-9)
  - `countBooksByBucket()`, `countAuthorsByBucket()` - Bucket counting
- **Features:** Letter-based organization, pagination, XML generation support, comprehensive mapping
- **Patterns:** Builder pattern, pagination handling, letter bucket organization
- **Duplication:** Pagination patterns, bucket counting logic

### ApiCircuitBreakerService
**Path:** `src/main/java/.../service/ApiCircuitBreakerService.java`
- **Purpose:** Circuit breaker service to prevent API calls when rate limits are exceeded
- **State Management:** CircuitState enum (CLOSED, OPEN, HALF_OPEN)
- **Storage:** AtomicReference and AtomicInteger for thread-safe state management
- **Key Methods:**
  - `isApiCallAllowed()` - Checks if API calls are allowed based on circuit state
  - `recordSuccess()` - Records successful API call, may close circuit
  - `recordRateLimitFailure()` - Records 429 errors, may open circuit
  - `recordGeneralFailure()` - Records general failures with higher threshold
  - `getCircuitStatus()` - Status reporting for monitoring
  - `reset()` - Manual circuit reset for admin purposes
- **Configuration:** 3 failure threshold, 60min open duration, 5min half-open timeout
- **Features:** State machine pattern, time-based recovery, different failure types
- **Patterns:** Circuit breaker pattern, state machine, thread-safe operations
- **Duplication:** Time-based logic, atomic operations patterns

### TieredBookSearchService
**Path:** `src/main/java/.../service/TieredBookSearchService.java`
- **Purpose:** Extracted tiered search orchestration handling DB-first search with multiple fallbacks
- **Dependencies:** BookSearchService, GoogleApiFetcher, OpenLibraryBookDataService, PostgresBookRepository
- **Conditional:** @ConditionalOnBean({BookSearchService.class, PostgresBookRepository.class})
- **Source of Truth:** Single pipeline for Postgres-first queries with Google/OpenLibrary fallbacks.
- **Followers:** `BookDataOrchestrator.searchBooksTiered`, `RecommendationService`, `BookApiProxy`, controllers.
- **Key Methods:**
  - `searchBooks()` - Main tiered search orchestration (Postgres → Google → OpenLibrary)
  - `searchAuthors()` - Author search with result limiting
- **Private Methods:**
  - `searchPostgresFirst()` - Database-first search with result resolution
  - `executePagedSearch()` - Google API paged search execution
- **Features:** Multi-tier fallback strategy, qualifier injection, result aggregation
- **Search Strategy:** Postgres first, then authenticated Google, then unauthenticated Google, then OpenLibrary
- **Patterns:** Strategy pattern, reactive chaining, fallback chains
- **Duplication:** Similar tiered patterns to BookDataOrchestrator, search result processing

### BookRecommendationPersistenceService
**Path:** `src/main/java/.../service/BookRecommendationPersistenceService.java`
- **Purpose:** Persists recommendation relationships to Postgres for downstream similarity analysis
- **Dependencies:** JdbcTemplate, BookDataOrchestrator
- **Record Classes:** `RecommendationRecord`, `PersistableRecommendation`
- **Key Methods:**
  - `persistPipelineRecommendations()` - Main reactive persistence orchestration
- **Private Methods:**
  - `deleteExistingPipelineRows()` - Cleanup before upsert
  - `upsertRecommendation()` - Individual recommendation persistence with scoring
  - `resolveCanonicalUuid()` - UUID resolution with tiered fallback
  - `formatReasons()` - Reason formatting with deduplication
- **Features:** Reactive processing, score normalization (0-1 range), conflict resolution, canonical UUID resolution
- **Patterns:** Reactive programming, batch processing, upsert pattern
- **Duplication:** Similar UUID resolution patterns, reactive error handling

### BookSupplementalPersistenceService
**Path:** `src/main/java/.../service/BookSupplementalPersistenceService.java`
- **Purpose:** Handles supplemental book data persistence (authors, categories, tags, qualifiers)
- **Dependencies:** JdbcTemplate, ObjectMapper, BookCollectionPersistenceService
- **Key Methods:**
  - `persistAuthors()` - Author persistence with position ordering
  - `persistCategories()` - Category persistence via collection service
  - `assignQualifierTags()` - Qualifier tag assignment with metadata
  - `assignTag()` - Generic tag assignment with confidence scoring
- **Private Methods:**
  - `assignTagInternal()` - Core tag assignment logic
  - `upsertAuthor()` - Author upsert with name normalization
  - `upsertTag()` - Tag upsert with type handling
  - `serializeQualifierMetadata()`, `serializeMetadata()` - JSON serialization
  - `assignTagWithSerializedMetadata()` - Tag assignment with JSON metadata
- **Features:** Position-based author ordering, metadata serialization, confidence scoring, normalization
- **Patterns:** Upsert patterns, JSON serialization, data normalization
- **Duplication:** Upsert patterns repeated, metadata serialization logic

### BookCollectionPersistenceService
**Path:** `src/main/java/.../service/BookCollectionPersistenceService.java`
- **Purpose:** Handles book collection and list persistence (categories, bestseller lists)
- **Dependencies:** JdbcTemplate
- **Key Methods:**
  - `upsertCategory()` - Category collection upsert with normalization
  - `addBookToCategory()` - Book-category relationship management
  - `upsertBestsellerCollection()` - NYT bestseller list persistence
  - `upsertBestsellerMembership()` - Bestseller list item persistence with rankings
  - `upsertList()` - Generic list persistence with deterministic UUIDs
  - `upsertListMembership()` - Generic list membership persistence
- **Features:** Name normalization, deterministic UUID generation, comprehensive ranking data
- **Patterns:** Upsert patterns, deterministic ID generation, relationship management
- **Duplication:** Similar upsert patterns, name normalization logic

### BookCoverManagementService
**Path:** `src/main/java/.../service/image/BookCoverManagementService.java`
- **Purpose:** Orchestrates book cover retrieval, caching, and background processing
- **Dependencies:** CoverCacheManager, CoverSourceFetchingService, S3BookCoverService, LocalDiskCoverCacheService, ApplicationEventPublisher, EnvironmentService
- **Source of Truth:** Central pipeline for preparing covers (`prepareBook(s)ForDisplay`, async refresh) including provenance + placeholder decisions.
- **Followers:** Controllers, `ExternalCoverFetchHelper`, and downstream cover services reuse this orchestration instead of duplicating normalization.
- **Key Methods:**
  - `getInitialCoverUrlAndTriggerBackgroundUpdate()` - Main orchestration method with reactive pipeline
  - `processCoverInBackground()` - @Async background processing for optimal cover selection
- **Private Methods:**
  - `createPlaceholderCoverImages()` - Placeholder generation
  - `checkMemoryCachesAndDefaults()` - Memory cache checking pipeline
  - `inferSourceFromUrl()` - Source inference from URL patterns
  - `determineFallbackUrl()` - Fallback URL selection logic
- **Features:** Multi-tier caching (S3 → Memory → Provisional), background optimization, event publishing, comprehensive source detection
- **Cache Strategy:** S3 first, then memory caches, then provisional/defaults with background optimization
- **Patterns:** Reactive programming, async processing, multi-tier caching, event-driven architecture
- **Duplication:** URL pattern matching, cache management patterns, async processing patterns

### S3RetryService
**Path:** `src/main/java/.../service/S3RetryService.java`
- **Purpose:** Service for handling S3 operations with retry capabilities and exponential backoff
- **Dependencies:** S3StorageService, ApiRequestMonitor
- **Conditional:** @Conditional(S3EnvironmentCondition.class)
- **Configuration:** Configurable max retries (default 3), initial backoff (200ms), backoff multiplier (2.0)
- **Key Methods:**
  - `fetchJsonWithRetry(volumeId)` - Fetch JSON from S3 with retry logic for transient errors
  - `uploadJsonWithRetry(volumeId, jsonContent)` - Upload JSON to S3 with retry logic
  - `updateBookJsonWithRetry(book)` - Update existing book JSON with new data, merges qualifiers
- **Private Methods:**
  - `fetchJsonWithRetryInternal()` - Internal retry implementation with exponential backoff
  - `uploadJsonWithRetryInternal()` - Internal upload retry with backoff
- **Features:** Exponential backoff, retry metrics tracking, qualifier merging logic, graceful degradation
- **Retry Logic:** Distinguishes between retriable service errors and permanent failures (404, not found)
- **Patterns:** Retry with exponential backoff, CompletableFuture composition, error categorization
- **Duplication:** Similar retry patterns across fetch/upload methods, exponential backoff calculation

### EnvironmentService
**Path:** `src/main/java/.../service/EnvironmentService.java`
- **Purpose:** Service for environment detection and configuration management
- **Dependencies:** Spring Environment
- **Key Methods:**
  - `isDevelopmentMode()` - Checks if 'dev' profile is active
  - `isProductionMode()` - Checks if 'prod' profile is active
  - `getCurrentEnvironmentMode()` - Gets string representation of environment mode
  - `isBookCoverDebugMode()` - Checks if book cover debug mode is enabled
- **Features:** Spring profile detection, property-based configuration, environment-specific behavior control
- **Patterns:** Service facade for environment detection, boolean flag checking
- **Duplication:** Simple property checking patterns

### DuplicateBookService
**Path:** `src/main/java/.../service/DuplicateBookService.java`
- **Purpose:** Service for managing duplicate book detection and edition consolidation (DISABLED after Redis removal)
- **Status:** All functionality disabled after Redis removal
- **Key Methods:**
  - `findPotentialDuplicates()` - Returns empty list (disabled)
  - `populateDuplicateEditions()` - No-op implementation (disabled)
  - `findPrimaryCanonicalBook()` - Returns empty optional (disabled)
  - `mergeDataIfBetter()` - Returns false (disabled)
- **Features:** Graceful degradation, logging of disabled state, minimal memory footprint
- **Legacy Purpose:** Previously detected duplicates by title/author matching, edition consolidation, data merging
- **Patterns:** Disabled service pattern, graceful degradation, no-op implementations
- **Duplication:** None (service is disabled)

### BookDataAggregatorService
**Path:** `src/main/java/.../service/BookDataAggregatorService.java`
- **Purpose:** Service for merging book data from multiple sources with intelligent conflict resolution
- **Dependencies:** ObjectMapper
- **Key Methods:**
  - `aggregateBookDataSources()` - Main aggregation method with configurable source precedence
- **Private Methods:**
  - `getStringValueFromSources()` - Extract string values with precedence
  - `getIntValueFromSources()` - Extract integer values with validation and parsing
  - `getNestedValue()` - Navigate nested JSON structures
  - `determineSourceType()` - Source type detection based on JSON structure
- **Features:** Multi-source data merging, conflict resolution, source attribution, comprehensive field mapping
- **Aggregation Logic:**
  - Title: First non-empty title with primaryId fallback
  - Authors: Union of unique authors across sources
  - Description: Longest non-empty description
  - ISBNs: Collection of all unique ISBN-10 and ISBN-13 identifiers
  - Categories: Union of unique categories/subjects
  - Source tracking: Records contributing sources and primary source attribution
- **Source Detection:** GoogleBooks (volumeInfo), OpenLibrary (key patterns), NewYorkTimes (rank + nyt_buy_links)
- **Patterns:** Field-specific aggregation strategies, source precedence handling, comprehensive metadata preservation
- **Duplication:** Similar field extraction patterns, source detection logic

### S3BookMigrationService
**Path:** `src/main/java/.../service/S3BookMigrationService.java`
- **Purpose:** Handles heavy S3 migration logic extracted from BookDataOrchestrator
- **Dependencies:** S3StorageService, ObjectMapper, BookCollectionPersistenceService
- **Key Methods:**
  - `migrateBooksFromS3()` - Migrate book records from S3 to database
  - `migrateListsFromS3()` - Migrate list/collection data from S3 to database
- **Private Methods:**
  - `prepareJsonKeysForMigration()` - Filter and prepare S3 keys for processing
  - `parseBookJsonPayload()` - Parse and sanitize JSON payloads with error recovery
  - `processS3JsonKeys()` - Process S3 keys with progress tracking
  - `findJsonStartIndex()` - Locate JSON start in corrupted payloads
  - `splitConcatenatedJson()` - Split concatenated JSON objects
  - `extractFromPreProcessed()` - Extract inner JSON from wrapped structures
  - `deduplicateBookNodes()` - Remove duplicate JSON nodes
  - `computeDedupKey()` - Generate deduplication keys (ISBN → ID → title+author)
- **Features:** Robust JSON parsing, corruption recovery, progress tracking, deduplication, batch processing
- **Error Recovery:** Control character removal, concatenated JSON splitting, JSON start detection, malformed payload handling
- **Constants:** Control character patterns, concatenated object patterns, JSON start hints
- **Patterns:** Batch processing, error recovery, progress tracking, JSON sanitization
- **Duplication:** JSON parsing patterns, error recovery strategies, progress tracking logic

---

## Schedulers

### NewYorkTimesBestsellerScheduler
**Path:** `src/main/java/.../scheduler/NewYorkTimesBestsellerScheduler.java`
- **Purpose:** Scheduled ingestion of New York Times bestseller data directly into Postgres
- **Dependencies:** NewYorkTimesService, BookDataOrchestrator, BookLookupService, ObjectMapper, JdbcTemplate, BookCollectionPersistenceService, BookSupplementalPersistenceService
- **Schedule:** @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}") - Sundays at 4 AM
- **Key Methods:**
  - `processNewYorkTimesBestsellers()` - Main scheduled entry point
- **Private Methods:**
  - `persistList()` - Processes individual bestseller lists
  - `persistListEntry()` - Processes individual book entries with ISBN resolution
  - `hydrateAndResolve()` - Book hydration via tiered lookup
  - `resolveCanonicalBookId()` - Canonical ID resolution
  - `assignCoreTags()` - NYT-specific tag assignment
  - `parseDate()` - ISO date parsing with error handling
- **Features:** List metadata persistence, book membership tracking, canonical ID resolution, tag assignment
- **Configuration:** Configurable via app.nyt.scheduler.enabled and app.nyt.scheduler.cron
- **Patterns:** Scheduled task pattern, batch processing, tiered data resolution
- **Duplication:** Similar ID resolution patterns, tag assignment patterns

### BookCacheWarmingScheduler
**Path:** `src/main/java/.../scheduler/BookCacheWarmingScheduler.java`
- **Purpose:** Proactive cache warming for popular books during off-peak hours (cache warming disabled)
- **Dependencies:** GoogleBooksService, BookDataOrchestrator, RecentlyViewedService, ApplicationContext
- **Schedule:** @Scheduled(cron = "${app.cache.warming.cron:0 0 3 * * ?}") - Daily at 3 AM
- **Key Methods:**
  - `warmPopularBookCaches()` - Main cache warming orchestration
- **Private Methods:**
  - `fetchBookForWarming()` - Tiered book fetching for warming
  - `getBookIdsToWarm()` - Candidate book selection based on recent views
- **Features:** Rate limiting, API usage monitoring, recently viewed prioritization, duplicate tracking
- **Configuration:** Highly configurable via app.cache.warming.* properties
- **State Management:** ConcurrentHashMap for tracking recently warmed books
- **Patterns:** Rate-limited processing, scheduled execution, tiered fallback
- **Duplication:** Similar tiered lookup patterns, rate limiting patterns

### SitemapRefreshScheduler
**Path:** `src/main/java/.../scheduler/SitemapRefreshScheduler.java`
- **Purpose:** Consolidated sitemap refresh job for warming queries, S3 artifacts, and external data hydration
- **Dependencies:** SitemapProperties, BookSitemapService, SitemapService, ObjectProvider<S3BookCoverService>
- **Schedule:** @Scheduled(cron = "${sitemap.scheduler-cron:0 15 * * * *}") - Every hour at 15 minutes past
- **Key Methods:**
  - `refreshSitemapArtifacts()` - Main scheduled orchestration
- **Private Methods:**
  - `warmCoverAssets()` - Cover asset warming with sample limiting
- **Features:** Postgres query warming, S3 snapshot synchronization, external hydration, cover warming
- **Configuration:** Configurable via sitemap.scheduler-enabled and sitemap.scheduler-cron
- **Patterns:** Multi-phase processing, comprehensive error handling, configurable limits
- **Duplication:** Similar warming patterns to BookCacheWarmingScheduler

---

## Utilities

### ApplicationConstants
**Path:** `src/main/java/.../util/ApplicationConstants.java`
- **Purpose:** Central repository for shared literal values to reduce duplication
- **Categories:**
  - **Cover:** Placeholder image paths
  - **Provider:** API provider constants (GOOGLE_BOOKS, OPEN_LIBRARY)
  - **Paging:** Search and result limits, defaults
  - **Search:** Default fallback queries
  - **Tag:** Tag type constants
  - **Urls:** Base URLs, social images, sitemap paths
  - **Database.Queries:** All SQL query constants (book lookups, existence checks, edition queries, etc.)
  - **ExternalServices:** API base URLs and link templates
- **Features:** Centralized constants, nested classes for organization, comprehensive query library
- **Patterns:** Static final constants, nested utility classes
- **Duplication:** Eliminates duplicate string literals across codebase

### JdbcUtils
**Path:** `src/main/java/.../util/JdbcUtils.java`
- **Purpose:** Shared JDBC helper methods for retrieving optional values without boilerplate
- **Key Methods:**
  - `optionalString()` - Query for optional string with error callback support
  - `queryForOptional()` - Generic optional query method
  - `queryForUuid()`, `queryForInt()`, `queryForLong()` - Type-specific queries
  - `exists()` - Existence checking
  - `queryForOptionalObject()` - Object query with RowMapper
  - `executeUpdate()` - Update execution with result checking
- **Features:** Null-safe operations, error handling, type-safe queries
- **Patterns:** Optional pattern, error callback pattern, utility static methods
- **Duplication:** Eliminates repetitive JDBC boilerplate across services

### ValidationUtils
**Path:** `src/main/java/.../util/ValidationUtils.java`
- **Purpose:** Utility helpers for common null/blank/empty validation checks
- **Key Methods:**
  - `isNullOrBlank()`, `hasText()`, `isNullOrEmpty()` - String validation
  - `isNullOrEmpty()` for Collections and Maps
  - `anyNull()`, `allNotNull()` - Multi-value validation
  - `nullIfBlank()` - Null conversion
- **Inner Class:** `BookValidator` - Book-specific validation utilities
  - `getIdentifier()` - Non-null identifier extraction
  - `hasValidIsbn()` - ISBN validation
  - `hasRequiredFields()` - Required field checking
  - `getPreferredIsbn()` - ISBN preference logic
  - `hasCoverImages()`, `getCoverUrl()` - Cover validation
- **Patterns:** Utility static methods, inner class organization, null-safe operations
- **Duplication:** Centralizes common validation patterns

### IsbnUtils
**Path:** `src/main/java/.../util/IsbnUtils.java`
- **Purpose:** Shared helpers for normalizing ISBN input before lookups or persistence
- **Key Methods:**
  - `sanitize()` - ISBN normalization (removes non-numeric except X, uppercases)
  - `isValidIsbn13()` - ISBN-13 format validation
  - `isValidIsbn10()` - ISBN-10 format validation
- **Source of Truth:** ISBN sanitisation + validation across the codebase.
- **Followers:** `BookLookupService`, controllers, schedulers, and migration utilities call these helpers instead of regex copies.
- **Features:** Pattern-based cleaning, format validation, case normalization
- **Patterns:** Input sanitization, validation pattern
- **Duplication:** Eliminates duplicate ISBN handling logic

### BookJsonParser
**Path:** `src/main/java/.../util/BookJsonParser.java`
- **Purpose:** Utility for parsing Google Books API JSON responses into Book objects
- **Key Methods:**
  - `convertJsonToBook()` - Main JSON to Book conversion
  - `extractQualifiersFromSearchQuery()` - Search query qualifier extraction
  - `isValidIsbn()` - Basic ISBN structure validation
- **Source of Truth:** JSON → `Book` transformations, qualifier extraction, multi-format date parsing.
- **Followers:** `GoogleBooksService`, `TieredBookSearchService`, `BookDataOrchestrator`, and migration utilities reuse this parser.
- **Private Methods:**
  - `extractBookBaseInfo()` - Core book data extraction
  - `getAuthorsFromVolumeInfo()` - Author extraction with array/string handling
  - `getGoogleCoverImageFromVolumeInfo()` - Best quality cover URL selection
  - `enhanceGoogleCoverUrl()` - URL enhancement for quality/protocol
  - `parsePublishedDate()` - Multi-format date parsing
  - `deriveEditionNumber()` - Edition number extraction from multiple sources
  - `buildEditionGroupKey()` - Edition grouping key generation
  - `normalizeKeyComponent()` - Text normalization for keys
- **Features:** Comprehensive JSON parsing, multi-format support, robust error handling, qualifier extraction
- **Patterns:** Builder pattern, multi-format parsing, normalization
- **Duplication:** Complex JSON parsing logic, date format handling

### PagingUtils
**Path:** `src/main/java/.../util/PagingUtils.java`
- **Purpose:** Lightweight helpers for common paging math with shared clamping semantics
- **Key Methods:**
  - `clamp()` - Value clamping to range
  - `atLeast()` - Minimum value enforcement
  - `safeLimit()` - Page size clamping with defaults
  - `window()` - Comprehensive paging window creation
- **Record Class:** `Window` - Immutable paging descriptor
- **Features:** Range validation, default handling, comprehensive window management
- **Patterns:** Utility static methods, record classes, builder pattern
- **Duplication:** Eliminates repeated paging logic across controllers/services

### IdGenerator
**Path:** `src/main/java/.../util/IdGenerator.java`
- **Purpose:** Minimal ID utilities for NanoId and UUID v7 generation
- **Key Methods:**
  - `generate()` - Default 10-char NanoId
  - `generateShort()` - 8-char NanoId for low-volume tables
  - `generateLong()` - 12-char NanoId for high-volume tables
  - `generate(int size)` - Custom size NanoId
  - `generate(int size, char[] alphabet)` - Custom alphabet NanoId
  - `uuidV7()` - Time-ordered epoch UUID v7
- **Features:** Thread-safe SecureRandom, configurable sizes, URL-safe Base62 alphabet, time-ordered UUIDs
- **Patterns:** Static utility methods, secure random generation, multiple output formats
- **Duplication:** Centralizes ID generation patterns

### SlugGenerator
**Path:** `src/main/java/.../util/SlugGenerator.java`
- **Purpose:** Utility for generating SEO-friendly URL slugs from book titles and authors
- **Key Methods:**
  - `generateBookSlug()` - Main book slug generation (multiple overloads)
  - `slugify()` - General string to slug conversion
  - `makeSlugUnique()` - Unique slug generation with counter
  - `isValidSlug()` - Slug validation
- **Private Methods:**
  - `truncateAtWordBoundary()` - Smart truncation at word boundaries
- **Features:** Unicode normalization, length limits, word boundary truncation, author inclusion
- **Configuration:** Configurable max lengths (100 total, 60 title, 30 author)
- **Patterns:** Unicode normalization, regex-based cleaning, multiple format support
- **Duplication:** Centralized slug generation logic

### CompressionUtils
**Path:** `src/main/java/.../util/CompressionUtils.java`
- **Purpose:** Utility helpers for decoding byte arrays that may be gzip-compressed JSON
- **Key Methods:**
  - `decodeUtf8WithOptionalGzip(byte[])` - Attempts gzip decompression with UTF-8 fallback
  - `decodeUtf8ExpectingGzip(byte[])` - Decodes gzip-compressed UTF-8, throws on failure
- **Private Methods:**
  - `decompressGzip(byte[])` - Core gzip decompression implementation
- **Features:** Graceful fallback handling, gzip detection, UTF-8 encoding, null safety
- **Error Handling:** IOException for decompression failures, ZipException wrapping
- **Patterns:** Try-with-resources, fallback strategies, static utility methods
- **Duplication:** Standard compression/decompression patterns

### SearchQueryUtils
**Path:** `src/main/java/.../util/SearchQueryUtils.java`
- **Purpose:** Utility methods for working with search queries to centralize normalization behavior
- **Constants:** DEFAULT_QUERY ("*"), CACHE_KEY_SANITIZER pattern
- **Key Methods:**
  - `normalize(String)` - Normalizes queries for user-facing search APIs (trims, defaults to "*")
  - `canonicalize(String)` - Case-insensitive representation for map keys and cache lookups
  - `cacheKey(String)` - Filesystem/cache-safe key without language qualifier
  - `cacheKey(String, String)` - Cache-safe key with language code scope
- **Features:** Consistent normalization, cache key generation, language scoping, filesystem safety
- **Patterns:** Static utility methods, regex-based sanitization, consistent defaults
- **Duplication:** Centralized normalization eliminates duplicate query handling

### S3Paths
**Path:** `src/main/java/.../util/S3Paths.java`
- **Purpose:** Centralized S3 path helpers for consistent cache prefixes across services
- **Constants:** GOOGLE_BOOK_CACHE_PREFIX ("books/v1/")
- **Key Methods:**
  - `ensureTrailingSlash(String)` - Ensures prefix ends with single trailing slash
  - `ensureTrailingSlash(String, String)` - With default value fallback
- **Features:** Path normalization, consistent S3 prefixes, safe concatenation
- **Patterns:** Static utility methods, path manipulation, null safety
- **Duplication:** Centralizes S3 path handling patterns

### EnumParsingUtils
**Path:** `src/main/java/.../util/EnumParsingUtils.java`
- **Purpose:** Shared helper for safely parsing user-provided strings into enum values
- **Key Methods:**
  - `parseOrDefault(String, Class<E>, E)` - Parse enum with default fallback
  - `parseOrDefault(String, Class<E>, E, Consumer<String>)` - With invalid value callback
- **Features:** Generic enum parsing, graceful error handling, uppercase normalization, callback support
- **Patterns:** Generic utility methods, functional interface callbacks, safe parsing
- **Duplication:** Centralizes enum parsing patterns

### LoggingUtils
**Path:** `src/main/java/.../util/LoggingUtils.java`
- **Purpose:** Lightweight helpers for consistent logging of warnings and errors with optional causes
- **Inner Enum:** LogLevel (ERROR, WARN)
- **Key Methods:**
  - `error(Logger, Throwable, String, Object...)` - Error logging with throwable and arguments
  - `warn(Logger, Throwable, String, Object...)` - Warning logging with throwable and arguments
- **Private Methods:**
  - `log(Logger, LogLevel, Throwable, String, Object...)` - Core logging implementation
- **Features:** Consistent exception logging, argument array handling, null safety
- **Patterns:** Static utility methods, varargs handling, array manipulation
- **Duplication:** Centralizes logging patterns with exception handling

### ReactiveErrorUtils
**Path:** `src/main/java/.../util/ReactiveErrorUtils.java`
- **Purpose:** Reactive programming error handling utilities for consistent error responses
- **Key Methods:**
  - `logAndReturnEmpty(String)` - Function that logs error and returns empty Mono
  - `logAndReturnEmptyList(String)` - Function that logs error and returns empty list Mono
  - `logAndReturnEmptyFlux(String)` - Function that logs error and returns empty Flux
  - `logAndReturnDefault(String, T)` - Function that logs error and returns default value
  - `logAndReturnEmptyString(String)`, `logAndReturnFalse(String)` - Specific type defaults
  - `collectSafely(Flux)` - Collect Flux to list with error handling
  - `limitAndCollect(Flux, int)` - Take limited items and collect
  - `withDefault(Mono, T)` - Convert empty Mono to default value
  - `withDefaultOnError(Mono, T, String)` - Convert error Mono to default value
- **Features:** Reactive error handling, consistent fallback strategies, logging integration, type-specific helpers
- **Patterns:** Functional interface factories, reactive composition, error transformation
- **Duplication:** Centralizes reactive error handling patterns across services

---

## Models & DTOs

### Book (Main Entity Model)
**Path:** `src/main/java/.../model/Book.java`
- **Purpose:** Core book entity containing all book metadata, cover image information, and edition data
- **Features:** Comprehensive book representation from external sources (Google Books API), bibliographic data storage, cover image metadata tracking, edition relationships
- **Key Fields:**
  - **Identity:** id, slug, title, authors, description
  - **Publishing:** isbn10, isbn13, publishedDate, publisher, language, pageCount
  - **Media:** categories, averageRating, ratingsCount, listPrice, currencyCode
  - **Digital:** infoLink, previewLink, purchaseLink, webReaderLink, pdfAvailable, epubAvailable
  - **Images:** s3ImagePath, externalImageUrl, coverImageWidth, coverImageHeight, isCoverHighResolution, coverImages
  - **Physical:** heightCm, widthCm, thicknessCm, weightGrams
  - **Relationships:** collections, otherEditions, cachedRecommendationIds, qualifiers
- **Key Methods:**
  - `setAuthors()` - Null-safe author list with filtering and trimming
  - `getCoverImageUrl()` - S3 path preference over external URL
  - `setCoverImageUrl()` - URL detection for S3 vs external assignment
  - `addCollection()`, `addQualifier()`, `addRecommendationIds()` - Safe collection management
- **Inner Classes:**
  - `CollectionAssignment` - Collection membership with rank and source
  - `EditionInfo` - Related edition details with ISBNs and cover images
- **Patterns:** Defensive collection handling, automatic list initialization, URL protocol detection
- **Duplication:** Collection management patterns repeated across similar entities

### Image Models

#### CoverImages
**Path:** `src/main/java/.../model/image/CoverImages.java`
- **Purpose:** Immutable container for book cover image URLs with source tracking
- **Features:** Preferred/fallback URL structure, source attribution, graceful fallback handling
- **Fields:** preferredUrl, fallbackUrl, source (CoverImageSource)
- **Constructors:** Default (UNDEFINED source), URLs-only, full with source
- **Patterns:** Immutable data container, multiple constructor overloads
- **Duplication:** Similar container patterns across image models

#### CoverImageSource (Enum)
**Path:** `src/main/java/.../model/image/CoverImageSource.java`
- **Purpose:** Enum for available external cover image sources and cache locations
- **Values:** ANY, GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD, S3_CACHE, LOCAL_CACHE, NONE, MOCK, UNDEFINED
- **Features:** Display names for UI, comprehensive source tracking, test support (MOCK)
- **Methods:** `getDisplayName()` - Human-readable labels
- **Patterns:** Enum with display names, comprehensive source enumeration
- **Duplication:** Standard enum pattern

#### ImageResolutionPreference (Enum)
**Path:** `src/main/java/.../model/image/ImageResolutionPreference.java`
- **Purpose:** Enum for image resolution filtering and prioritization options
- **Values:** ANY, HIGH_ONLY, HIGH_FIRST, LARGE, MEDIUM, SMALL, ORIGINAL, UNKNOWN
- **Features:** Quality-based filtering, sorting preferences, bandwidth optimization
- **Methods:** `getDisplayName()` - UI-friendly labels
- **Patterns:** Enum with display names, preference-based filtering
- **Duplication:** Similar pattern to CoverImageSource

#### ProcessedImage (Record)
**Path:** `src/main/java/.../model/image/ProcessedImage.java`
- **Purpose:** Immutable record for processed image data with metadata and success/failure status
- **Parameters:** processedBytes, newFileExtension, newMimeType, width, height, processingSuccessful, processingError
- **Features:** Defensive byte array copying, static factory methods, success/failure modeling
- **Static Methods:**
  - `success()` - Factory for successful processing results
  - `failure()` - Factory for failed processing with error message
- **Patterns:** Record with factory methods, defensive copying, success/failure modeling
- **Duplication:** Factory method pattern, defensive copying patterns

#### ImageDetails
**Path:** `src/main/java/.../model/image/ImageDetails.java`
- **Purpose:** Book cover image metadata including source and dimension information
- **Features:** Source attribution, dimension tracking, resolution preference, mutable properties
- **Fields:** urlOrPath, sourceName, sourceSystemId, coverImageSource, resolutionPreference, width, height
- **Constructors:** Minimal (no dimensions), full (with dimensions)
- **Methods:** Complete getter/setter suite, toString() implementation
- **Patterns:** Mutable data class, multiple constructor overloads, comprehensive metadata tracking
- **Duplication:** Similar metadata tracking patterns

### Controller DTOs

#### BookDto (Record)
**Path:** `src/main/java/.../controller/dto/BookDto.java`
- **Purpose:** Canonical API representation of a book assembled from Postgres-backed data
- **Parameters:** id, slug, title, description, publication, authors, categories, collections, tags, cover, editions, recommendationIds, extras
- **Features:** Comprehensive book representation, nested DTO structure, immutable record
- **Patterns:** Record-based DTO, nested composition
- **Duplication:** Standard DTO record pattern

#### BookDtoMapper
**Path:** `src/main/java/.../controller/dto/BookDtoMapper.java`
- **Purpose:** Transforms domain Book objects into API-facing DTOs with centralized mapping logic
- **Features:** Postgres-first data transformation, null-safe mapping, defensive copying
- **Key Methods:**
  - `toDto(Book)` - Main transformation method with comprehensive mapping
  - `resolveSlug()` - Slug resolution with fallback generation
  - `buildCover()` - Cover DTO construction from CoverImages
  - `mapAuthors()`, `mapTags()`, `mapCollections()`, `mapEditions()` - Relationship mapping
  - `safeCopy(Date)` - Defensive date copying
- **Features:** Centralized mapping logic, null-safe operations, immutable result construction
- **Patterns:** Static utility mapper, defensive copying, null-safe transformations
- **Duplication:** Similar mapping patterns could be abstracted

#### CoverDto (Record)
**Path:** `src/main/java/.../controller/dto/CoverDto.java`
- **Purpose:** DTO capturing cover metadata for API clients
- **Parameters:** s3ImagePath, externalImageUrl, width, height, highResolution, preferredUrl, fallbackUrl, source
- **Features:** Complete cover image representation, multiple URL sources
- **Patterns:** Record-based DTO
- **Duplication:** Standard DTO pattern

#### AuthorDto (Record)
**Path:** `src/main/java/.../controller/dto/AuthorDto.java`
- **Purpose:** DTO representing an author in API responses
- **Parameters:** id, name
- **Features:** Simple author representation
- **Patterns:** Minimal record DTO
- **Duplication:** Standard simple DTO pattern

#### CollectionDto (Record)
**Path:** `src/main/java/.../controller/dto/CollectionDto.java`
- **Purpose:** DTO representing a normalized collection/list assignment
- **Parameters:** id, name, type, rank, source
- **Features:** Collection membership with ranking
- **Patterns:** Record-based DTO with ranking support
- **Duplication:** Standard DTO pattern

#### PublicationDto (Record)
**Path:** `src/main/java/.../controller/dto/PublicationDto.java`
- **Purpose:** DTO containing publication metadata for a book
- **Parameters:** publishedDate, language, pageCount, publisher
- **Features:** Publication-specific information grouping
- **Patterns:** Record-based DTO for metadata grouping
- **Duplication:** Standard DTO pattern

#### TagDto (Record)
**Path:** `src/main/java/.../controller/dto/TagDto.java`
- **Purpose:** DTO representing a qualifier/tag assignment
- **Parameters:** key, attributes (Map<String, Object>)
- **Features:** Flexible tag representation with key-value attributes
- **Patterns:** Record-based DTO with dynamic attributes
- **Duplication:** Standard DTO pattern

#### EditionDto (Record)
**Path:** `src/main/java/.../controller/dto/EditionDto.java`
- **Purpose:** DTO describing a related edition of a book
- **Parameters:** googleBooksId, type, identifier, isbn10, isbn13, publishedDate, coverImageUrl
- **Features:** Complete edition representation with multiple identifiers
- **Patterns:** Record-based DTO with comprehensive edition data
- **Duplication:** Standard DTO pattern

### Additional Image Models

#### ImageSourceName (Enum)
**Path:** `src/main/java/.../model/image/ImageSourceName.java`
- **Purpose:** Enum representing various sources of book cover images
- **Values:** GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD, LOCAL_CACHE, S3_CACHE, INTERNAL_PROCESSING, UNKNOWN
- **Features:** Image source attribution, display names for UI, comprehensive source tracking
- **Methods:** `getDisplayName()` - Human-readable source names
- **Patterns:** Enum with display names, source enumeration
- **Duplication:** Similar pattern to CoverImageSource (potential consolidation opportunity)

#### ImageAttemptStatus (Enum)
**Path:** `src/main/java/.../model/image/ImageAttemptStatus.java`
- **Purpose:** Status codes for image fetch and processing attempts
- **Values:** SUCCESS, FAILURE_404, FAILURE_NOT_FOUND, FAILURE_TIMEOUT, FAILURE_GENERIC, SKIPPED, SKIPPED_BAD_URL, FAILURE_PROCESSING, FAILURE_EMPTY_CONTENT, FAILURE_PLACEHOLDER_DETECTED, FAILURE_IO, FAILURE_GENERIC_DOWNLOAD, SUCCESS_NO_METADATA, PENDING, FAILURE_INVALID_DETAILS, FAILURE_NO_URL_IN_RESPONSE, FAILURE_CONTENT_REJECTED
- **Features:** Comprehensive error categorization, retry strategy support, detailed failure tracking
- **Patterns:** Enum for status tracking, comprehensive error categorization
- **Duplication:** Status enum pattern (could be generalized)

#### ImageProvenanceData
**Path:** `src/main/java/.../model/image/ImageProvenanceData.java`
- **Purpose:** Data class for tracking provenance of book cover images with attempt history
- **Features:** Records attempted sources and results, tracks selected image attributes, preserves API responses for debugging, timestamp auditing
- **Fields:** bookId, googleBooksApiResponse, attemptedImageSources, selectedImageInfo, timestamp
- **Inner Classes:**
  - `AttemptedSourceInfo` - Details about image fetch attempts (source, URL, status, failure reason, metadata, dimensions)
  - `SelectedImageInfo` - Final selected image details (source, URL, resolution, dimensions, selection reason, storage location, S3 key)
- **Methods:** Complete getter/setter suite for all fields and nested classes
- **Patterns:** Complex data class with nested classes, comprehensive auditing, metadata preservation
- **Duplication:** Nested class patterns, comprehensive metadata tracking

---

## Repositories

### SitemapRepository
**Path:** `src/main/java/.../repository/SitemapRepository.java`
- **Purpose:** Repository for Postgres-backed sitemap queries without S3 dependence
- **Features:** Letter bucket counts, paginated slices for books and authors, HTML and XML sitemap payload support
- **Key Methods:**
  - `countAllBooks()` - Total count of books with slugs
  - `countBooksByBucket()` - Book counts grouped by first letter (A-Z, 0-9)
  - `countBooksForBucket(bucket)` - Count books for specific letter bucket
  - `fetchBooksForBucket(bucket, limit, offset)` - Paginated books for letter bucket
  - `fetchBooksForXml(limit, offset)` - Books for XML sitemap ordered by updated_at DESC
  - `countAuthorsByBucket()` - Author counts grouped by first letter
  - `countAuthorsForBucket(bucket)` - Count authors for specific letter bucket
  - `fetchAuthorsForBucket(bucket, limit, offset)` - Paginated authors for letter bucket
  - `fetchBooksForAuthors(authorIds)` - Books grouped by author IDs with joins
- **Record Classes:**
  - `BookRow` - Minimal book representation (bookId, slug, title, updatedAt)
  - `AuthorRow` - Minimal author representation (id, name, updatedAt)
- **Features:** Letter bucket normalization, graceful degradation when JDBC unavailable, SQL injection protection with parameterized queries
- **Constants:** `LETTER_BUCKET_EXPRESSION` - SQL expression for A-Z vs 0-9 bucketing
- **Patterns:** Repository pattern, record-based DTOs, bucket-based pagination, graceful degradation
- **Duplication:** Similar bucket counting patterns, paginated query patterns

---

## Filters & Support

### RequestLoggingFilter
**Path:** `src/main/java/.../RequestLoggingFilter.java`
- **Purpose:** Request logging and timing filter for HTTP requests with selective filtering
- **Features:** Request/response logging, timing measurement, static resource filtering, API endpoint prioritization
- **Key Methods:**
  - `doFilter()` - Main filter method with request logging, timing, and selective filtering
- **Filtering Logic:**
  - Always logs API requests (`/api` prefix)
  - Skips logging for non-API requests with non-whitelisted extensions
  - Whitelisted extensions: png, jpg, jpeg, svg, css, js, ico, html
  - Logs request method, URI, source IP, status code, and duration
- **Features:** Extension-based filtering, request timing, status code logging, remote address tracking
- **Patterns:** Servlet filter pattern, selective logging, performance measurement
- **Duplication:** Standard filter implementation

### Event Classes

#### BookCoverUpdatedEvent
**Path:** `src/main/java/.../service/event/BookCoverUpdatedEvent.java`
- **Purpose:** Event fired when book cover image is updated for cross-service notifications
- **Features:** Cover image update notifications, cache invalidation metadata, WebSocket UI notifications, image source tracking
- **Fields:** identifierKey (ISBN/Google Book ID), newCoverUrl, googleBookId, source (CoverImageSource)
- **Constructors:** Basic (with undefined source), full (with source specification)
- **Methods:** Complete getter suite, comprehensive toString() for debugging
- **Features:** Multiple identifier support, source attribution, cache invalidation triggers
- **Patterns:** Event-driven architecture, immutable event data, comprehensive toString()
- **Duplication:** Standard event class pattern

#### SearchProgressEvent
**Path:** `src/main/java/.../service/event/SearchProgressEvent.java`
- **Purpose:** Event for notifying clients about search progress and status updates
- **Features:** Real-time search status, loading indicators support, multi-source tracking, WebSocket integration
- **Inner Enum:** `SearchStatus` - STARTING, SEARCHING_CACHE, SEARCHING_GOOGLE, SEARCHING_OPENLIBRARY, RATE_LIMITED, DEDUPLICATING, COMPLETE, ERROR
- **Fields:** searchQuery, status, message, queryHash (WebSocket routing), source (API source)
- **Constructors:** Full constructor, convenience constructor without source
- **Methods:** Complete getter suite
- **Features:** Status progression tracking, WebSocket topic routing, human-readable messages, source attribution
- **Patterns:** Event-driven architecture, enum for status, WebSocket routing support
- **Duplication:** Similar event structure to other event classes

#### SearchResultsUpdatedEvent
**Path:** `src/main/java/.../service/event/SearchResultsUpdatedEvent.java`
- **Purpose:** Event fired when new search results are available from background API calls
- **Features:** Incremental search results, progressive result loading, WebSocket notifications, source attribution
- **Fields:** searchQuery, newResults (List<Book>), source, totalResultsNow, queryHash, isComplete
- **Constructor:** Single comprehensive constructor with all fields
- **Methods:** Complete getter suite including `isComplete()` for completion status
- **Features:** Progressive loading support, result source tracking, completion status, WebSocket routing
- **Patterns:** Event-driven architecture, incremental data delivery, completion tracking
- **Duplication:** Consistent event class structure and patterns

---

## Comprehensive Duplication Analysis

### Source of Truth Declarations (as of Sep 21, 2025)

For each identified duplication area, the single source of truth (SSOT) is declared below. All follower usages have been updated where practical in this pass; any remaining edge sites are listed for follow-up.

1) Database Query Patterns
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/JdbcUtils.java
- Status: CanonicalBookPersistenceService / PostgresBookRepository / BookSupplementalPersistenceService continue to depend on JdbcTemplate directly for complex SQL; JdbcUtils is the shared helper for optionals, existence, typed queries. Further refactors can adopt JdbcUtils where trivial.

2) ID Resolution and Lookup Patterns
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/service/BookLookupService.java
- Status: In use across PostgresBookRepository, CanonicalBookPersistenceService, NewYorkTimesBestsellerScheduler.

3) Tiered Data Access Patterns (DB → S3 → APIs)
- SSOT (fetch-by-id/slug): src/main/java/com/williamcallahan/book_recommendation_engine/service/BookDataOrchestrator.java
- SSOT (search): src/main/java/com/williamcallahan/book_recommendation_engine/service/TieredBookSearchService.java
- Status: Controllers and proxies route through these. BookController now delegates canonical resolution to BookDataOrchestrator.fetchCanonicalBookReactive().

4) Reactive Programming Patterns (error handling for controllers/services)
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/ReactiveErrorUtils.java and ReactiveControllerUtils.java
- Status: BookController converted. Additional services continue to use onErrorResume/LoggingUtils; can be migrated incrementally.

5) URL Pattern Matching and Source Detection
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/UrlPatternMatcher.java
- Status: BookCoverManagementService updated to use UrlPatternMatcher for source inference. Other sites (if any) should use the same.

6) Caching and Warming Patterns
- SSOT: Cover caching managed by BookCoverManagementService; query warming handled within SitemapRefreshScheduler and BookCacheWarmingScheduler.
- Status: No base class introduced; existing responsibilities retained. Consider WarmingUtils in future if patterns expand.

7) Configuration and Environment Detection
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/ApplicationConstants.java and src/main/java/com/williamcallahan/book_recommendation_engine/service/EnvironmentService.java

8) JSON Serialization and Parsing
- SSOT (Google Books → Book): src/main/java/com/williamcallahan/book_recommendation_engine/util/BookJsonParser.java
- SSOT (general date parsing shared): src/main/java/com/williamcallahan/book_recommendation_engine/util/DateParsingUtils.java
- Status: BookJsonParser updated to use DateParsingUtils.parseFlexibleDate.

9) Input Validation and Sanitization
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/ValidationUtils.java and IsbnUtils.java

10) Pagination and Result Limiting
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/PagingUtils.java
- Status: BookSearchService, controllers, and services use PagingUtils; normalization wrappers removed.

11) Upsert and Persistence Patterns
- SSOT helpers: JdbcUtils (query helpers), IdGenerator (ID creation)

12) Circuit Breaker and Resilience Patterns
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/service/ApiCircuitBreakerService.java (+ Resilience4j annotations)

13) Affiliate Link Generation
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/service/AffiliateLinkService.java

14) Book Cover Processing
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/service/image/BookCoverManagementService.java

15) SEO and Keywords Generation
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/SeoUtils.java

16) Slug Generation
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/SlugGenerator.java

17) Compression Utilities
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/CompressionUtils.java

18) Date Parsing
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/DateParsingUtils.java
- Status: Adopted in BookJsonParser, OpenLibraryBookDataService, and NYT scheduler.

19) Retry Logic
- SSOT: S3-specific: src/main/java/com/williamcallahan/book_recommendation_engine/service/S3RetryService.java
- General retry utils: To be introduced if non-S3 sites proliferate.

20) Search Query Normalization
- SSOT: src/main/java/com/williamcallahan/book_recommendation_engine/util/SearchQueryUtils.java
- Status: BookSearchService now calls SearchQueryUtils directly (wrappers removed).

Based on the complete class and method inventory, here are the key areas of duplication and opportunities for centralization:

### 1. Database Query Patterns ✅ **CENTRALIZED**

**Central Authority:** `JdbcUtils`
- **Location:** `src/main/java/.../util/JdbcUtils.java`
- **Provides:** `optionalString()`, `queryForUuid()`, `queryForInt()`, `exists()`, etc.
- **Classes That Should Use It:**
  - CanonicalBookPersistenceService - Still has inline JDBC queries
  - PostgresBookRepository - Some queries could use JdbcUtils methods
  - BookSupplementalPersistenceService - Has repetitive query patterns
- **Action:** Refactor remaining inline JDBC to use JdbcUtils methods

### 2. ID Resolution and Lookup Patterns ✅ **CENTRALIZED**

**Central Authority:** `BookLookupService`
- **Location:** `src/main/java/.../service/BookLookupService.java`
- **Provides:** `findBookIdByIsbn()`, `findBookIdByIsbn13()`, `findBookIdByIsbn10()`, `findBookIdByExternalIdentifier()`, `resolveCanonicalBookId()`
- **Already Using It:**
  - PostgresBookRepository (all ISBN/external hydration now funnels through `BookLookupService`)
  - CanonicalBookPersistenceService & BookDataOrchestrator (canonical lookup helpers)
  - NewYorkTimesBestsellerScheduler (tiered ISBN resolution)
- **Action:** _Completed_ – remaining direct SQL lookups consolidated into `BookLookupService`

### 3. Tiered Data Access Patterns ✅ **CENTRALIZED**

**Central Authority:** BookDataOrchestrator (by-id/slug) & TieredBookSearchService (search)
- **Status:** Centralized. Controllers and proxies route through these services. BookController now delegates canonical resolution to `BookDataOrchestrator.fetchCanonicalBookReactive()`.
- **Optional Future:** Consider an abstract `TieredAccessPattern<T>` only if new tiers proliferate.

### 4. Reactive Programming Patterns ✅ **CENTRALIZED**

**Central Authority:** `ReactiveErrorUtils` & `ReactiveControllerUtils`
- **Source of Truth:** `ReactiveErrorUtils` (`src/main/java/.../util/ReactiveErrorUtils.java`) for service-level flows and `ReactiveControllerUtils` (`src/main/java/.../util/ReactiveControllerUtils.java`) for controller endpoints.
- **Followers:** `GoogleBooksService`, `RecommendationService`, `BookDataOrchestrator`, `TieredBookSearchService`, `BookCoverManagementService`, any controller returning `Mono`/`Flux` responses.
- **Next Adoption Batch:** Tighten `BookCoverManagementService` and `BookApiProxy` to delegate their `onErrorResume` blocks to the shared helpers so every tier emits consistent diagnostics.
- **Location:** `src/main/java/.../util/ReactiveErrorUtils.java` & `ReactiveControllerUtils.java`
- **ReactiveErrorUtils Provides:** Error handling patterns, logging utilities
- **ReactiveControllerUtils Provides:** `withErrorHandling()` for controllers
- **Classes That Should Use It:**
  - GoogleBooksService - Custom `.onErrorResume()` patterns
  - RecommendationService - Duplicate error handling
  - BookDataOrchestrator - Custom reactive error chains
  - TieredBookSearchService - Own error handling patterns
  - BookCoverManagementService - Custom fallback chains
- **Action:** Replace custom reactive patterns with utility methods

### 5. URL Pattern Matching and Source Detection ✅ **CENTRALIZED**

**Central Authority:** `UrlPatternMatcher`
- **Status:** Centralized. BookCoverManagementService now uses `UrlPatternMatcher.identifySource()` for source inference. Other sites should use `UrlPatternMatcher` where needed.

### 6. Caching and Warming Patterns ✅ **PARTIALLY CENTRALIZED**

**Central Authorities:**
- Cover caching: `BookCoverManagementService`
- Warming jobs: `SitemapRefreshScheduler` and `BookCacheWarmingScheduler`
- **Status:** Responsibilities are clearly owned. A shared `WarmingScheduler` base class can be introduced later if patterns expand, but is not required for compliance.

### 7. Configuration and Environment Detection ✅ **CENTRALIZED**

**Central Authority:** `ApplicationConstants` & `EnvironmentService`
- **ApplicationConstants Location:** `src/main/java/.../util/ApplicationConstants.java`
- **EnvironmentService Location:** `src/main/java/.../service/EnvironmentService.java`
- **ApplicationConstants Provides:** All constants, URLs, SQL queries
- **EnvironmentService Provides:** `isDevelopmentMode()`, `getCurrentEnvironmentMode()`
- **Classes That Should Use It:**
  - Multiple config classes with hardcoded values
  - Services with inline string literals
- **Action:** Replace all hardcoded values with ApplicationConstants entries

### 8. JSON Serialization and Parsing ✅ **PARTIALLY CENTRALIZED**

**Central Authority:** `BookJsonParser` & `BookJsonWriter`
- **Source of Truth:** `BookJsonParser` (`src/main/java/.../util/BookJsonParser.java`) for inbound JSON → `Book` hydration, qualifier extraction, and date parsing; `BookJsonWriter` (`src/main/java/.../util/BookJsonWriter.java`) for serialising books and merging qualifier updates before persistence.
- **Followers:** `S3BookMigrationService`, `S3StorageService`, `NewYorkTimesBestsellerScheduler`, `BookCollectionPersistenceService`, `S3RetryService`, `BookApiProxy` caching.
- **Next Adoption Batch:** Replace remaining `ObjectMapper` book writes in `BookApiProxy`/`S3BookMigrationService` with `BookJsonWriter`; migrate any lingering manual parsing to `BookJsonParser` helpers.

### 9. Input Validation and Sanitization ✅ **CENTRALIZED**

**Central Authority:** `ValidationUtils` & `IsbnUtils`
- **Source of Truth:** `ValidationUtils` (`src/main/java/.../util/ValidationUtils.java`) for generic null/text checks and book validators; `IsbnUtils` (`src/main/java/.../util/IsbnUtils.java`) for ISBN cleaning/validation.
- **Followers:** Controllers, schedulers, `BookSearchService`, persistence services, migration jobs.
- **ValidationUtils Location:** `src/main/java/.../util/ValidationUtils.java`
- **IsbnUtils Location:** `src/main/java/.../util/IsbnUtils.java`
- **ValidationUtils Provides:** `isNullOrBlank()`, `hasText()`, `BookValidator` inner class
- **IsbnUtils Provides:** `sanitize()`, `isValidIsbn13()`, `isValidIsbn10()`
- **Classes That Should Use It:**
  - Controllers with custom validation
  - Services with inline null checks
  - BookSearchService - Has own normalization
- **Action:** Use ValidationUtils.BookValidator for book-specific validation

### 10. Pagination and Result Limiting ✅ **CENTRALIZED**

**Central Authority:** `PagingUtils`
- **Source of Truth:** `PagingUtils` (`src/main/java/.../util/PagingUtils.java`) for clamping, windowing, and result slicing.
- **Followers:** Controllers, `BookSearchService`, `SitemapService`, `RecommendationService`, any repo/service imposing limits.
- **Location:** `src/main/java/.../util/PagingUtils.java`
- **Provides:** `clamp()`, `safeLimit()`, `window()`, `Window` record class
- **Classes That Should Use It:**
  - All controllers with pagination
  - SitemapService - Custom pagination logic
  - BookSearchService - Has own `safeLimit()` method
- **Action:** Replace all custom pagination with PagingUtils methods

### 11. Upsert and Persistence Patterns ✅ **PARTIALLY CENTRALIZED**

**Central Authority:** `JdbcUtils` & `IdGenerator`
- **JdbcUtils Location:** `src/main/java/.../util/JdbcUtils.java`
- **IdGenerator Location:** `src/main/java/.../util/IdGenerator.java`
- **JdbcUtils Provides:** Query helpers, could be extended with upsert templates
- **IdGenerator Provides:** `generate()`, `generateShort()`, `generateLong()`, `uuidV7()`
- **Classes That Should Use It:**
  - BookSupplementalPersistenceService - Custom upsert patterns
  - BookCollectionPersistenceService - Duplicate COALESCE patterns
  - CanonicalBookPersistenceService - Complex upsert logic
- **Action:** Add upsert template methods to JdbcUtils

### 12. Circuit Breaker and Resilience Patterns ✅ **PARTIALLY CENTRALIZED**

**Central Authority:** `ApiCircuitBreakerService`
- **Location:** `src/main/java/.../service/ApiCircuitBreakerService.java`
- **Provides:** `isApiCallAllowed()`, `recordSuccess()`, `recordRateLimitFailure()`
- **Classes That Should Use It:**
  - GoogleBooksService - Has own @CircuitBreaker annotations
  - OpenLibraryBookDataService - Duplicate resilience patterns
  - GoogleApiFetcher - Already uses ApiCircuitBreakerService
- **Action:** Standardize resilience configuration, possibly create base class

### 13. Affiliate Link Generation ✅ **CENTRALIZED**

**Central Authority:** `AffiliateLinkService`
- **Location:** `src/main/java/.../service/AffiliateLinkService.java`
- **Provides:** `generateLinks(Book)` - Creates all affiliate links
- **Already Using It:**
  - HomeController - Using AffiliateLinkService.generateLinks()
- **Action:** None needed - fully centralized

### 14. Book Cover Processing ✅ **CENTRALIZED**

**Central Authority:** `BookCoverManagementService`
- **Location:** `src/main/java/.../service/image/BookCoverManagementService.java`
- **Provides:** `prepareBooksForDisplay()`, `prepareBookForDisplay()`, `getInitialCoverUrlAndTriggerBackgroundUpdate()`
- **Shared Helpers:** `ExternalCoverFetchHelper` consolidates external source downloads/provenance for Google/OpenLibrary/Longitood services
- **Source of Truth:** `ExternalCoverFetchHelper` (service/image) handles validation + provenance tagging for every external cover retrieval.
- **Followers:** `GoogleBooksService`, `BookCoverManagementService`, and cover schedulers rely on it rather than bespoke fetch logic.
- **Already Using It:**
  - HomeController - Using prepareBooksForDisplay()
- **Action:** Ensure all controllers use these methods

### 15. SEO and Keywords Generation ✅ **CENTRALIZED**

**Central Authority:** `SeoUtils`
- **Location:** `src/main/java/.../util/SeoUtils.java`
- **Provides:** `truncateDescription()`, `generateKeywords()`
- **Classes That Should Use It:**
  - HomeController - Already using SeoUtils methods
  - Other controllers with SEO logic
- **Action:** Use SeoUtils for all SEO-related operations

### 16. Slug Generation ✅ **CENTRALIZED**

**Central Authority:** `SlugGenerator`
- **Location:** `src/main/java/.../util/SlugGenerator.java`
- **Provides:** `generateBookSlug()`, `slugify()`, `makeSlugUnique()`
- **Classes That Should Use It:**
  - CanonicalBookPersistenceService - Has slug generation logic
  - Any service creating slugs
- **Action:** Use SlugGenerator for all slug creation

### 17. Compression Utilities ✅ **CENTRALIZED**

**Central Authority:** `CompressionUtils`
- **Location:** `src/main/java/.../util/CompressionUtils.java`
- **Provides:** GZIP compression/decompression methods
- **Classes That Should Use It:**
  - S3StorageService - Already using it
- **Action:** None needed - properly centralized

### 18. Date Parsing ✅ **CENTRALIZED**

**Central Authority:** `DateParsingUtils`
- **Status:** Centralized. Adopted in BookJsonParser, OpenLibraryBookDataService, and NewYorkTimesBestsellerScheduler.

### 19. Retry Logic ✅ **PARTIALLY CENTRALIZED**

**Central Authority:** `S3RetryService` (S3-specific)
- **Location:** `src/main/java/.../service/S3RetryService.java`
- **Provides:** S3-specific retry with exponential backoff
- **Classes Needing General Retry:**
  - GoogleApiFetcher - Custom retry logic
  - BookSitemapService - Timeout and retry patterns
- **Action:** Create general `RetryUtils` or generalize S3RetryService

### 20. Search Query Normalization ✅ **CENTRALIZED**

**Central Authority:** `SearchQueryUtils`
- **Source of Truth:** `SearchQueryUtils` (`src/main/java/.../util/SearchQueryUtils.java`) for normalization, cache key sanitization, and qualifier extraction.
- **Followers:** `BookSearchService`, controllers, `BookApiProxy`, schedulers using search queries.
- **Location:** `src/main/java/.../util/SearchQueryUtils.java`
- **Provides:** `normalize()`, `sanitizeForCacheKey()`, `extractQualifiers()`
- **Classes That Should Use It:**
  - BookSearchService - Has own `normaliseQuery()` method
  - Controllers with query normalization
- **Action:** Replace custom normalization with SearchQueryUtils

### Updated Priority Recommendations for DRY Improvements

#### ✅ **Already Centralized (Use Existing):**
1. BookLookupService - ID resolution
2. ReactiveErrorUtils/ReactiveControllerUtils - Reactive patterns
3. AffiliateLinkService - Affiliate links
4. ValidationUtils/IsbnUtils - Validation
5. PagingUtils - Pagination
6. BookCoverManagementService - Cover processing
7. SeoUtils - SEO operations
8. SlugGenerator - Slug generation
9. CompressionUtils - Compression
10. SearchQueryUtils - Query normalization
11. ApplicationConstants - Constants
12. JdbcUtils - Database queries

#### 🔧 **Needs Better Adoption:**
1. Ensure all ID resolution uses BookLookupService
2. Replace custom reactive patterns with utility methods
3. Use ValidationUtils.BookValidator consistently
4. Replace custom pagination with PagingUtils
5. Use SearchQueryUtils for all query normalization

#### ❌ **Actually Needs Creation (optional, future):**
1. **TieredAccessPattern<T>** - Template for DB → S3 → API (only if new tiers proliferate)
2. **WarmingScheduler** - Base class for cache warming (patterns are already owned; optional)
3. **RetryUtils** - General retry patterns (S3-specific retry already centralized)

### Identified Anti-Patterns

1. **String Duplication:** Good use of ApplicationConstants eliminates most string literals
2. **Logic Duplication:** Some complex business logic is appropriately duplicated for domain separation
3. **Configuration Duplication:** Similar @ConditionalOnProperty patterns could be abstracted
4. **Error Handling Duplication:** Reactive error handling patterns could be standardized

This analysis provides a roadmap for reducing duplication while maintaining appropriate separation of concerns and domain boundaries.

## Additional Duplication Patterns Identified

### 13. Enum Display Name Patterns

**Problem:** Similar enum patterns with display names
- **Affected Classes:** CoverImageSource, ImageResolutionPreference, ImageSourceName, ImageAttemptStatus, SearchProgressEvent.SearchStatus
- **Duplication:**
  - Enum constructors with displayName parameter
  - `getDisplayName()` method implementation
  - Human-readable descriptions for UI
- **Solution Status:** Consistent pattern across enums
- **Recommendation:** Consider abstract base class or interface for enums with display names

### 14. Event Class Patterns

**Problem:** Similar event class structures and patterns
- **Affected Classes:** BookCoverUpdatedEvent, SearchProgressEvent, SearchResultsUpdatedEvent
- **Duplication:**
  - Constructor overloads with default values
  - Complete getter suites
  - Field validation and null handling
  - toString() implementations for debugging
- **Recommendation:** Create abstract base `Event` class or `EventSupport` utility

### 15. Retry and Backoff Patterns

**Problem:** Exponential backoff and retry logic duplication
- **Affected Classes:** S3RetryService, various resilience configurations
- **Duplication:**
  - Exponential backoff calculation
  - Retry attempt counting
  - Error categorization (retriable vs permanent)
  - Metric tracking for retry attempts
- **Recommendation:** Create `RetryUtils` class with configurable backoff strategies

### 16. JSON Processing and Error Recovery

**Problem:** JSON parsing with error recovery patterns
- **Affected Classes:** S3BookMigrationService, BookDataAggregatorService, various services with ObjectMapper
- **Duplication:**
  - JSON sanitization (control character removal)
  - Nested JSON navigation patterns
  - Error-tolerant parsing with fallbacks
  - Null-safe JSON field extraction
- **Recommendation:** Create `JsonProcessingUtils` with common patterns

### 17. Reactive Error Handling Standardization

**Problem:** Reactive error handling patterns across services
- **Affected Classes:** Multiple services using Mono/Flux
- **Duplication:**
  - `.onErrorResume()` with logging patterns
  - `.defaultIfEmpty()` fallback patterns
  - Error-to-empty transformations
  - Timeout handling patterns
- **Solution Status:** ReactiveErrorUtils addresses many patterns
- **Recommendation:** Expand ReactiveErrorUtils usage across all reactive services

### 18. Model/DTO Mapping Patterns

**Problem:** Similar model-to-DTO mapping logic
- **Affected Classes:** BookDtoMapper, various service classes
- **Duplication:**
  - Null-safe field copying
  - Collection transformation patterns
  - Date copying with null checks
  - Nested object mapping
- **Recommendation:** Create generic `MappingUtils` or expand mapper framework usage

### 19. S3 and Storage Patterns

**Problem:** S3 operation patterns and path handling
- **Affected Classes:** S3StorageService, S3RetryService, S3BookMigrationService
- **Duplication:**
  - S3 error handling and status checking
  - Path normalization and key generation
  - Compression handling (gzip)
  - Batch processing with progress tracking
- **Solution Status:** S3Paths centralizes some patterns
- **Recommendation:** Create `S3OperationsUtils` for common patterns

### 20. Database Repository Patterns

**Problem:** Similar database query and mapping patterns
- **Affected Classes:** SitemapRepository, PostgresBookRepository, various persistence services
- **Duplication:**
  - Null-safe query execution
  - Result mapping with error handling
  - Paginated query patterns
  - Optional result handling
- **Solution Status:** JdbcUtils addresses some patterns
- **Recommendation:** Expand JdbcUtils with more repository utilities

### Priority Recommendations for New Patterns

1. **High Priority:**
   - Standardize reactive error handling with ReactiveErrorUtils
   - Create RetryUtils for backoff strategies
   - Expand JsonProcessingUtils for parsing patterns

2. **Medium Priority:**
   - Create abstract Event base class
   - Extend S3OperationsUtils for storage patterns
   - Develop MappingUtils for DTO transformations

3. **Low Priority:**
   - Enum display name interface/base class
   - Expand JdbcUtils for repository patterns
   - Template-based SQL pattern utilities

This comprehensive analysis reveals the codebase has good separation of concerns while maintaining opportunities for further DRY improvements through utility abstraction.
