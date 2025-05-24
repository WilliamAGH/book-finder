/**
 * Utility for flattening book data from various API formats into unified document structure
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.util;

import java.util.*;

@SuppressWarnings("unchecked")
public class BookDataFlattener {
    
    /**
     * Add flattened fields from Google Books volumeInfo, saleInfo, accessInfo
     * for easier Redis queries while preserving the original nested structure
     */
    public static void addFlattenedGoogleBooksFields(Map<String, Object> unifiedDoc, Map<String, Object> googleBooksData) {
        // Extract from volumeInfo
        if (googleBooksData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) googleBooksData.get("volumeInfo");
            
            // Core fields
            if (volumeInfo.containsKey("title")) {
                unifiedDoc.put("title", volumeInfo.get("title"));
            }
            if (volumeInfo.containsKey("subtitle")) {
                unifiedDoc.put("subtitle", volumeInfo.get("subtitle"));
            }
            if (volumeInfo.containsKey("authors")) {
                unifiedDoc.put("authors", volumeInfo.get("authors"));
            }
            if (volumeInfo.containsKey("description")) {
                unifiedDoc.put("description", volumeInfo.get("description"));
            }
            if (volumeInfo.containsKey("categories")) {
                unifiedDoc.put("categories", volumeInfo.get("categories"));
            }
            if (volumeInfo.containsKey("publisher")) {
                unifiedDoc.put("publisher", volumeInfo.get("publisher"));
            }
            if (volumeInfo.containsKey("publishedDate")) {
                unifiedDoc.put("publishedDate", volumeInfo.get("publishedDate"));
            }
            if (volumeInfo.containsKey("language")) {
                unifiedDoc.put("language", volumeInfo.get("language"));
            }
            if (volumeInfo.containsKey("pageCount")) {
                unifiedDoc.put("pageCount", volumeInfo.get("pageCount"));
            }
            if (volumeInfo.containsKey("averageRating")) {
                unifiedDoc.put("averageRating", volumeInfo.get("averageRating"));
            }
            if (volumeInfo.containsKey("ratingsCount")) {
                unifiedDoc.put("ratingsCount", volumeInfo.get("ratingsCount"));
            }
            if (volumeInfo.containsKey("maturityRating")) {
                unifiedDoc.put("maturityRating", volumeInfo.get("maturityRating"));
            }
            
            // Handle imageLinks
            if (volumeInfo.containsKey("imageLinks")) {
                Map<String, Object> imageLinks = (Map<String, Object>) volumeInfo.get("imageLinks");
                if (imageLinks.containsKey("thumbnail")) {
                    unifiedDoc.put("coverImageUrl", imageLinks.get("thumbnail"));
                }
                if (imageLinks.containsKey("small")) {
                    unifiedDoc.put("coverImageSmall", imageLinks.get("small"));
                }
                if (imageLinks.containsKey("medium")) {
                    unifiedDoc.put("coverImageMedium", imageLinks.get("medium"));
                }
                if (imageLinks.containsKey("large")) {
                    unifiedDoc.put("coverImageLarge", imageLinks.get("large"));
                }
            }
            
            // Handle industry identifiers (ISBN)
            if (volumeInfo.containsKey("industryIdentifiers")) {
                List<Map<String, Object>> identifiers = (List<Map<String, Object>>) volumeInfo.get("industryIdentifiers");
                for (Map<String, Object> identifier : identifiers) {
                    String type = (String) identifier.get("type");
                    String value = (String) identifier.get("identifier");
                    if ("ISBN_10".equals(type)) {
                        unifiedDoc.put("isbn10", value);
                    } else if ("ISBN_13".equals(type)) {
                        unifiedDoc.put("isbn13", value);
                    }
                }
            }
        }
        
        // Extract from saleInfo
        if (googleBooksData.containsKey("saleInfo")) {
            Map<String, Object> saleInfo = (Map<String, Object>) googleBooksData.get("saleInfo");
            if (saleInfo.containsKey("saleability")) {
                unifiedDoc.put("saleability", saleInfo.get("saleability"));
            }
            if (saleInfo.containsKey("isEbook")) {
                unifiedDoc.put("isEbook", saleInfo.get("isEbook"));
            }
            if (saleInfo.containsKey("listPrice") && saleInfo.get("listPrice") instanceof Map) {
                Map<String, Object> listPrice = (Map<String, Object>) saleInfo.get("listPrice");
                if (listPrice.containsKey("amount")) {
                    unifiedDoc.put("listPrice", listPrice.get("amount"));
                }
            }
        }
        
        // Extract from accessInfo
        if (googleBooksData.containsKey("accessInfo")) {
            Map<String, Object> accessInfo = (Map<String, Object>) googleBooksData.get("accessInfo");
            if (accessInfo.containsKey("publicDomain")) {
                unifiedDoc.put("publicDomain", accessInfo.get("publicDomain"));
            }
            if (accessInfo.containsKey("textToSpeechPermission")) {
                unifiedDoc.put("textToSpeechPermission", accessInfo.get("textToSpeechPermission"));
            }
            if (accessInfo.containsKey("webReaderLink")) {
                unifiedDoc.put("webReaderLink", accessInfo.get("webReaderLink"));
            }
        }
        
        // Use Google Books ID as googleBooksId
        if (googleBooksData.containsKey("id")) {
            unifiedDoc.put("googleBooksId", googleBooksData.get("id"));
        }
        
        // Extract preview and info links from volumeInfo
        if (googleBooksData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) googleBooksData.get("volumeInfo");
            if (volumeInfo.containsKey("previewLink")) {
                unifiedDoc.put("previewLink", volumeInfo.get("previewLink"));
            }
            if (volumeInfo.containsKey("infoLink")) {
                unifiedDoc.put("infoLink", volumeInfo.get("infoLink"));
            }
        }
    }
    
    /**
     * Add flattened fields from OpenLibrary data
     */
    public static void addFlattenedOpenLibraryFields(Map<String, Object> unifiedDoc, Map<String, Object> openLibraryData) {
        // Extract title
        if (openLibraryData.containsKey("title")) {
            unifiedDoc.put("title", openLibraryData.get("title"));
        }
        
        // Extract authors (can be array of strings or objects)
        if (openLibraryData.containsKey("authors")) {
            Object authorsObj = openLibraryData.get("authors");
            if (authorsObj instanceof List) {
                List<?> authorsList = (List<?>) authorsObj;
                List<String> authorNames = new ArrayList<>();
                for (Object author : authorsList) {
                    if (author instanceof Map) {
                        Map<String, Object> authorMap = (Map<String, Object>) author;
                        if (authorMap.containsKey("name")) {
                            authorNames.add((String) authorMap.get("name"));
                        }
                    } else if (author instanceof String) {
                        authorNames.add((String) author);
                    }
                }
                unifiedDoc.put("authors", authorNames);
            }
        }
        
        // Extract description
        if (openLibraryData.containsKey("description")) {
            Object desc = openLibraryData.get("description");
            if (desc instanceof String) {
                unifiedDoc.put("description", desc);
            } else if (desc instanceof Map) {
                Map<String, Object> descMap = (Map<String, Object>) desc;
                if (descMap.containsKey("value")) {
                    unifiedDoc.put("description", descMap.get("value"));
                }
            }
        }
        
        // Extract ISBNs
        if (openLibraryData.containsKey("isbn_13")) {
            List<?> isbn13List = (List<?>) openLibraryData.get("isbn_13");
            if (isbn13List != null && !isbn13List.isEmpty()) {
                unifiedDoc.put("isbn13", isbn13List.get(0).toString());
            }
        }
        if (openLibraryData.containsKey("isbn_10")) {
            List<?> isbn10List = (List<?>) openLibraryData.get("isbn_10");
            if (isbn10List != null && !isbn10List.isEmpty()) {
                unifiedDoc.put("isbn10", isbn10List.get(0).toString());
            }
        }
        
        // Extract covers
        if (openLibraryData.containsKey("covers")) {
            List<?> covers = (List<?>) openLibraryData.get("covers");
            if (covers != null && !covers.isEmpty()) {
                String coverId = covers.get(0).toString();
                unifiedDoc.put("openLibraryCoverUrl", "https://covers.openlibrary.org/b/id/" + coverId + "-L.jpg");
                unifiedDoc.put("coverImageUrl", "https://covers.openlibrary.org/b/id/" + coverId + "-L.jpg");
            }
        }
        
        // Extract publication date
        if (openLibraryData.containsKey("publish_date")) {
            unifiedDoc.put("publishedDate", openLibraryData.get("publish_date"));
        }
        
        // Extract publisher
        if (openLibraryData.containsKey("publishers")) {
            List<?> publishers = (List<?>) openLibraryData.get("publishers");
            if (publishers != null && !publishers.isEmpty()) {
                unifiedDoc.put("publisher", publishers.get(0).toString());
            }
        }
        
        // Extract page count
        if (openLibraryData.containsKey("number_of_pages")) {
            unifiedDoc.put("pageCount", openLibraryData.get("number_of_pages"));
        }
        
        // Extract subjects as categories
        if (openLibraryData.containsKey("subjects")) {
            unifiedDoc.put("categories", openLibraryData.get("subjects"));
        }
        
        // Store OpenLibrary ID
        if (openLibraryData.containsKey("key")) {
            String key = (String) openLibraryData.get("key");
            unifiedDoc.put("openLibraryId", key);
        }
    }
    
    /**
     * Add flattened fields from NYT bestseller data
     */
    public static void addFlattenedNYTFields(Map<String, Object> unifiedDoc, Map<String, Object> nytData) {
        // Extract title
        if (nytData.containsKey("title")) {
            unifiedDoc.put("title", nytData.get("title"));
        }
        
        // Extract author
        if (nytData.containsKey("author")) {
            String author = (String) nytData.get("author");
            unifiedDoc.put("authors", List.of(author));
        }
        
        // Extract description
        if (nytData.containsKey("description")) {
            unifiedDoc.put("description", nytData.get("description"));
        }
        
        // Extract ISBNs
        if (nytData.containsKey("primary_isbn13")) {
            unifiedDoc.put("isbn13", nytData.get("primary_isbn13"));
        }
        if (nytData.containsKey("primary_isbn10")) {
            unifiedDoc.put("isbn10", nytData.get("primary_isbn10"));
        }
        
        // Extract publisher
        if (nytData.containsKey("publisher")) {
            unifiedDoc.put("publisher", nytData.get("publisher"));
        }
        
        // Store NYT bestseller info in structured format
        Map<String, Object> nytInfo = new HashMap<>();
        if (nytData.containsKey("rank")) {
            nytInfo.put("rank", nytData.get("rank"));
        }
        if (nytData.containsKey("weeks_on_list")) {
            nytInfo.put("weeks_on_list", nytData.get("weeks_on_list"));
        }
        if (nytData.containsKey("list_name")) {
            nytInfo.put("list_name", nytData.get("list_name"));
        }
        if (nytData.containsKey("bestsellers_date")) {
            nytInfo.put("bestseller_date", nytData.get("bestsellers_date"));
        }
        
        if (!nytInfo.isEmpty()) {
            unifiedDoc.put("nyt_bestseller_info", nytInfo);
        }
        
        // Extract cover image
        if (nytData.containsKey("book_image")) {
            unifiedDoc.put("coverImageUrl", nytData.get("book_image"));
        }
        
        // Extract links
        if (nytData.containsKey("amazon_product_url")) {
            unifiedDoc.put("purchaseLink", nytData.get("amazon_product_url"));
        }
        if (nytData.containsKey("book_review_link")) {
            unifiedDoc.put("reviewLink", nytData.get("book_review_link"));
        }
    }
}