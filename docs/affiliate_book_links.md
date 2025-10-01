# Affiliate Link Configuration Guide

## Overview

The book recommendation engine supports affiliate links for Amazon, Barnes & Noble, Bookshop.org, and Audible. These links are only shown when:

1. The affiliate program is configured with proper credentials
2. The book has ISBN data to generate the links

## Default Configuration

The following defaults are configured in `application.yml`:

- **Amazon Associate Tag**: `williamagh-20` ✅ Active
- **Bookshop Affiliate ID**: `113888` ✅ Active
- **Barnes & Noble**: Not configured (requires both Publisher ID and Website ID)

These defaults work out of the box. You can override them with environment variables if needed.

## Environment Variables (Optional)

Override the default affiliate IDs by setting these environment variables:

### Amazon Associates

```bash
export AMAZON_ASSOCIATE_TAG="your-amazon-tag-20"
```

### Barnes & Noble (via Commission Junction)

```bash
export BARNES_NOBLE_PUBLISHER_ID="your-publisher-id"
export BARNES_NOBLE_WEBSITE_ID="your-website-id"
```

### Bookshop.org

```bash
export BOOKSHOP_AFFILIATE_ID="your-bookshop-id"
```

## How It Works

1. **Service Layer** (`AffiliateLinkService.java`)
   - Generates links using book ISBN data
   - Returns a HashMap with keys: `amazon`, `barnesAndNoble`, `bookshop`, `audible`
   - **Resilient behavior**: Links ALWAYS work, even without affiliate configuration
     - **With affiliate IDs**: Generates tracked affiliate links for revenue
     - **Without affiliate IDs**: Falls back to direct retailer links
   - Only requires ISBN data (ISBN-13 or ISBN-10)

2. **Template** (`affiliate-dropdown.html`)
   - Displays a dropdown button labeled "Buy Now"
   - Only shows if `affiliateLinks` map is not empty
   - Each retailer link only appears if that specific link exists in the map

3. **Current Issues Fixed**
   - ✅ Changed from property-style (`affiliateLinks.amazon`) to bracket notation (`affiliateLinks['amazon']`) to prevent SpEL errors
   - ✅ Added condition to hide dropdown when empty
   - ✅ Changed button label from "Amazon.com" to "Buy Now" since it contains multiple retailers

## Testing

To test affiliate links:

1. **Set at least one affiliate environment variable**
2. **Find a book with ISBN data** (most books have ISBN-13 or ISBN-10)
3. **Visit the book detail page**
4. **Look for the "Buy Now" dropdown** in the "View On" section

Example books that should have ISBNs:

- Search for popular fiction titles
- Recently published books
- Books from major publishers

## Troubleshooting

**Dropdown doesn't appear:**

- Check if book has ISBN data (visible in the Details tab)
- Verify environment variables are set
- Check browser console for JavaScript errors

**Dropdown appears but doesn't expand:**

- This was the bug - empty dropdowns don't expand
- Fixed by hiding dropdown when no links exist
- Ensure Bootstrap JS is loading (check browser console)

**Links don't work:**

- Verify affiliate IDs are correct
- Check that `target="_blank"` and `rel="noopener sponsored"` are present
- Test the generated URL directly

## Link Fallback Behavior

When affiliate IDs are NOT configured, the service generates direct retailer links:

| Retailer | With Affiliate ID | Without Affiliate ID |
|----------|------------------|---------------------|
| Amazon | `amazon.com/dp/{isbn}?tag=yourTag-20` | `amazon.com/dp/{isbn}` |
| Barnes & Noble | CJ tracking link | `barnesandnoble.com/w/?ean={isbn13}` |
| Bookshop | `bookshop.org/a/{affiliateId}/{isbn13}` | `bookshop.org/books?keywords={isbn13}` |
| Audible | Amazon search with affiliate tag | `audible.com/search?keywords={title}` |

This ensures users can **always** purchase books, while you earn revenue when affiliate IDs are configured.

## Revenue Tracking

All affiliate links include:

- `target="_blank"` - Opens in new tab
- `rel="noopener sponsored"` - Proper link attributes for SEO and security
- `clicky_log_outbound` class - For analytics tracking via Clicky
