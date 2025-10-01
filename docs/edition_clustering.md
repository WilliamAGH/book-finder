# Edition Clustering System

## Overview

The book database uses a **work_clusters** system to group different editions of the same book (hardcover, paperback, audio, etc.) for display in the frontend "Other Editions" dropdown.

## Architecture

### Tables

- `work_clusters` - Groups of books representing the same work
- `work_cluster_members` - Join table linking books to their work cluster

### Clustering Methods

Books are grouped when they share:

1. **ISBN Prefix** (first 9-11 digits) - same publisher/work identifier
2. **Google Books Canonical ID** - Google's canonical volume link
3. **OCLC Work ID** - WorldCat work identifier
4. **OpenLibrary Work ID** - OpenLibrary `/works/` identifier
5. **Goodreads Work ID** - Goodreads work grouping

## How It Works

### Automatic Clustering (Scheduled)

`WorkClusterScheduler` runs every 6 hours to cluster newly added books:

- Calls `cluster_books_by_isbn()` - groups books with matching ISBN prefixes
- Calls `cluster_books_by_google_canonical()` - groups books with same Google canonical volume

### Manual Clustering

After bulk imports or migrations, run:

```bash
make cluster-books
```

Or directly via SQL:

```sql
SELECT * FROM cluster_books_by_isbn();
SELECT * FROM cluster_books_by_google_canonical();
SELECT * FROM get_clustering_stats();
```

### Current Status (as of NYT migration)

- **372 clusters** created
- **773 books** grouped into edition families  
- **47,361 unclustered books** (unique works with single editions)
- **Average: 2.08 books per cluster**

## Frontend Integration

The `PostgresBookRepository.hydrateEditions()` method queries `work_cluster_members` to populate the `Book.editions` field, which the frontend uses to display the "Other Editions" dropdown.

**Frontend query:**

```sql
SELECT b.id, b.slug, b.title, b.isbn13, b.publisher, b.published_date
FROM work_cluster_members wcm1
JOIN work_cluster_members wcm ON wcm.cluster_id = wcm1.cluster_id
JOIN books b ON b.id = wcm.book_id
WHERE wcm1.book_id = ? AND wcm.book_id <> ?
ORDER BY wcm.is_primary DESC, b.published_date DESC
```

## Why Some Books Aren't Clustered

Books like different editions of "The Body Keeps the Score" may not cluster together if:

- Different publishers (different ISBN prefixes)
- No shared external work IDs (OCLC, OpenLibrary, Goodreads, Google)
- This is **intentional** - the system is conservative to avoid false groupings

## Improving Clustering

To cluster more books, we could:

1. Add title+author matching (fuzzy matching)
2. Fetch external work IDs from APIs (OpenLibrary, Goodreads)
3. Use ML/embeddings for similarity detection
4. Manual curation of work clusters

## Maintenance

- **Scheduled**: Runs automatically every 6 hours via `WorkClusterScheduler`
- **After migrations**: Run `make cluster-books`
- **On-demand**: Via admin endpoint or Makefile

## Related Files

- `src/main/resources/schema.sql` - Table definitions and clustering functions
- `src/main/java/.../scheduler/WorkClusterScheduler.java` - Scheduled clustering
- `src/main/java/.../service/PostgresBookRepository.java` - Edition hydration
- `Makefile` - Manual clustering command
