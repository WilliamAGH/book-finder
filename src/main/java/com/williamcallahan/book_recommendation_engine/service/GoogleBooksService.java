package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.io.IOException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class GoogleBooksService {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksService.class);

    private final WebClient webClient;

    @Value("${googlebooks.api.url}")
    private String googleBooksApiUrl;

    @Value("${googlebooks.api.key}")
    private String googleBooksApiKey;

    public GoogleBooksService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    public Mono<JsonNode> searchBooks(String query, int startIndex, String orderBy, String langCode) {
        String url = String.format("%s/volumes?q=%s&startIndex=%d&maxResults=40&key=%s",
                googleBooksApiUrl, query, startIndex, googleBooksApiKey);
        if (orderBy != null && !orderBy.isEmpty()) {
            url += "&orderBy=" + orderBy;
        }
        if (langCode != null && !langCode.isEmpty()) {
            url += "&langRestrict=" + langCode;
            logger.debug("Google Books API call with langRestrict: {}", langCode);
        }
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> logger.warn("Retrying API call for query '{}', startIndex {}. Attempt #{}. Error: {}", query, startIndex, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for query '{}', startIndex {}. Final error: {}", query, startIndex, retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .onErrorResume(e -> {
                    // This now catches errors after retries are exhausted or if the error wasn't retryable
                    logger.error("Error fetching page for query '{}' at startIndex {} after retries or due to non-retryable error: {}", query, startIndex, e.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode) {
        final int maxResultsPerPage = 40;
        final int maxTotalResults = 200;

        return Flux.range(0, (maxTotalResults + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> 
                searchBooks(query, startIndex, "newest", langCode)
                    .flatMapMany(response -> {
                        if (response != null && response.has("items") && response.get("items").isArray()) {
                            List<JsonNode> items = new ArrayList<>();
                            response.get("items").forEach(items::add);
                            return Flux.fromIterable(items);
                        }
                        return Flux.empty();
                    })
            )
            .takeUntil(jsonNode -> !jsonNode.has("kind") )
            .map(this::convertGroupToBook)
            .filter(Objects::nonNull)
            .collectList()
            .map(books -> {
                if (books.size() > maxTotalResults) {
                    return books.subList(0, maxTotalResults);
                }
                return books;
            });
    }

    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        return searchBooksAsyncReactive(query, null);
    }

    public Mono<List<Book>> searchBooksByTitle(String title, String langCode) {
        return searchBooksAsyncReactive("intitle:" + title, langCode);
    }

    public Mono<List<Book>> searchBooksByTitle(String title) {
        return searchBooksByTitle(title, null);
    }

    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode) {
        return searchBooksAsyncReactive("inauthor:" + author, langCode);
    }

    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksByAuthor(author, null);
    }

    public Mono<List<Book>> searchBooksByISBN(String isbn, String langCode) {
        return searchBooksAsyncReactive("isbn:" + isbn, langCode);
    }

    public Mono<List<Book>> searchBooksByISBN(String isbn) {
        return searchBooksByISBN(isbn, null);
    }

    public Mono<Book> getBookById(String bookId) {
        String url = String.format("%s/volumes/%s?key=%s", googleBooksApiUrl, bookId, googleBooksApiKey);
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> logger.warn("Retrying API call for book ID {}. Attempt #{}. Error: {}", bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for book ID {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .map(item -> {
                    if (item != null) {
                        return convertSingleItemToBook(item);
                    }
                    return null;
                })
                .onErrorResume(e -> {
                    logger.error("Error fetching book by ID {} after retries or due to non-retryable error: {}", bookId, e.getMessage());
                    return Mono.empty(); // Return an empty Mono on error
                });
    }

    private Book convertGroupToBook(JsonNode item) {
        if (item == null) {
            return null;
        }

        Book book = convertSingleItemToBook(item);
        
        return book;
    }

    private Book convertSingleItemToBook(JsonNode item) {
        Book book = new Book();
        if (item != null) {
            book.setRawJsonResponse(item.toString());
        }
        extractBookBaseInfo(item, book);
        setAdditionalFields(item, book);
        setLinks(item, book);

        return book;
    }
    
    private void extractBookBaseInfo(JsonNode item, Book book) {
        if (item == null || !item.has("volumeInfo")) {
            return;
        }

        JsonNode volumeInfo = item.get("volumeInfo");

        book.setId(item.has("id") ? item.get("id").asText() : null);
        book.setTitle(volumeInfo.has("title") ? volumeInfo.get("title").asText() : null);
        book.setAuthors(getAuthorsFromVolumeInfo(volumeInfo));
        // Sanitize publisher: strip surrounding quotes if present
        String rawPublisher = volumeInfo.has("publisher") ? volumeInfo.get("publisher").asText() : null;
        if (rawPublisher != null) {
            // Remove any leading/trailing double quotes
            rawPublisher = rawPublisher.replaceAll("^\"|\"$", "");
        }
        book.setPublisher(rawPublisher);
        book.setPublishedDate(parsePublishedDate(volumeInfo));
        book.setDescription(volumeInfo.has("description") ? volumeInfo.get("description").asText() : null);
        book.setCoverImageUrl(getGoogleCoverImageFromVolumeInfo(volumeInfo));
        book.setLanguage(volumeInfo.has("language") ? volumeInfo.get("language").asText() : null);

        if (volumeInfo.has("industryIdentifiers")) {
            List<Book.EditionInfo> otherEditions = new ArrayList<>();
            for (JsonNode identifier : volumeInfo.get("industryIdentifiers")) {
                extractEditionInfoFromItem(identifier, otherEditions);
            }
            book.setOtherEditions(otherEditions);
        }
    }

    private List<String> getAuthorsFromVolumeInfo(JsonNode volumeInfo) {
        List<String> authors = new ArrayList<>();
        if (volumeInfo.has("authors")) {
            volumeInfo.get("authors").forEach(authorNode -> authors.add(authorNode.asText()));
        }
        return authors;
    }

    /**
     * Get the best available cover image URL from the volume info
     * This method still returns a single URL for backward compatibility
     */
    private String getGoogleCoverImageFromVolumeInfo(JsonNode volumeInfo) {
        if (volumeInfo.has("imageLinks")) {
            JsonNode imageLinks = volumeInfo.get("imageLinks");
            String coverUrl = null;
            
            // Try to get the best available image, prioritizing larger ones
            if (imageLinks.has("extraLarge")) {
                coverUrl = imageLinks.get("extraLarge").asText();
            } else if (imageLinks.has("large")) {
                coverUrl = imageLinks.get("large").asText();
            } else if (imageLinks.has("medium")) {
                coverUrl = imageLinks.get("medium").asText();
            } else if (imageLinks.has("thumbnail")) {
                coverUrl = imageLinks.get("thumbnail").asText();
            } else if (imageLinks.has("smallThumbnail")) {
                coverUrl = imageLinks.get("smallThumbnail").asText();
            }
            
            if (coverUrl != null) {
                // Enhance the URL to get a higher resolution image
                coverUrl = enhanceGoogleCoverUrl(coverUrl, "high");
                return coverUrl;
            }
        }
        return null;
    }
    
    /**
     * Enhance a Google Books cover URL to get the best quality possible
     * @param url The original URL
     * @param quality The desired quality ("high", "medium", or "low")
     * @return The enhanced URL
     */
    private String enhanceGoogleCoverUrl(String url, String quality) {
        if (url == null) return null;
        
        // Make a copy of the URL to avoid modifying the original
        String enhancedUrl = url;
        
        // Remove http protocol to use https
        if (enhancedUrl.startsWith("http://")) {
            enhancedUrl = "https://" + enhancedUrl.substring(7);
        }
        
        // Enhance based on requested quality
        switch (quality) {
            case "high":
                // Prefer less aggressive upscaling. Try to get the best available quality
                // by removing common resizing parameters or setting them to fetch original/larger sizes
                // For "high" quality:
                // 1. Remove 'fife' parameter to avoid forced width
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                    // Clean up if '?' is now trailing
                    if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                    }
                }
                // Remove trailing '&' if fife was the last parameter and removed
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }

                // 2. Adjust 'zoom' parameter conservatively
                //    If zoom is high (e.g., > 2), reduce it to 2, otherwise, keep existing zoom (0, 1, or 2)
                //    If no zoom, don't add one
                if (enhancedUrl.contains("zoom=")) {
                    try {
                        String zoomValueStr = enhancedUrl.substring(enhancedUrl.indexOf("zoom=") + 5);
                        if (zoomValueStr.contains("&")) {
                            zoomValueStr = zoomValueStr.substring(0, zoomValueStr.indexOf("&"));
                        }
                        int currentZoom = Integer.parseInt(zoomValueStr);
                        if (currentZoom > 2) {
                            enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=2");
                            logger.debug("Adjusted high zoom to zoom=2 for URL: {}", enhancedUrl);
                        }
                        // Keep zoom if it's 0, 1, or 2
                    } catch (NumberFormatException e) {
                        logger.warn("Could not parse zoom value in URL: {}. Leaving zoom as is.", enhancedUrl);
                    }
                }
                break;
                
            case "medium":
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                     enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                     if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() -1);
                     }
                }
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }
                if (enhancedUrl.contains("zoom=")) {
                    enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=1"); // Use zoom=1 for medium
                }
                break;
                
            case "low":
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                     enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                     if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() -1);
                     }
                }
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }
                if (enhancedUrl.contains("zoom=")) {
                    enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=1"); // zoom=1 for low (thumbnail)
                }
                break;
        }
        
        return enhancedUrl;
    }
    
    private void setAdditionalFields(JsonNode item, Book book) {
        if (item.has("saleInfo")) {
            JsonNode saleInfo = item.get("saleInfo");
            if (saleInfo.has("listPrice")) {
                JsonNode listPrice = saleInfo.get("listPrice");
                if (listPrice.has("amount")) {
                    book.setListPrice(listPrice.get("amount").asDouble());
                }
                if (listPrice.has("currencyCode")) {
                    book.setCurrencyCode(listPrice.get("currencyCode").asText());
                }
            }
        }
    }

    private void setLinks(JsonNode item, Book book) {
        if (item.has("accessInfo")) {
            JsonNode accessInfo = item.get("accessInfo");
            if (accessInfo.has("webReaderLink")) {
                book.setWebReaderLink(accessInfo.get("webReaderLink").asText());
            }
        }
    }

    private Date parsePublishedDate(JsonNode volumeInfo) {
        if (volumeInfo.has("publishedDate")) {
            String dateString = volumeInfo.get("publishedDate").asText();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return format.parse(dateString);
            } catch (ParseException e) {
                format = new SimpleDateFormat("yyyy");
                try {
                    return format.parse(dateString);
                } catch (ParseException ex) {
                    logger.error("Failed to parse published date: {}", dateString);
                    return null;
                }
            }
        }
        return null;
    }

    private void extractEditionInfoFromItem(JsonNode identifier, List<Book.EditionInfo> otherEditions) {
        if (identifier.has("type") && identifier.has("identifier")) {
            String type = identifier.get("type").asText();
            String ident = identifier.get("identifier").asText();
            Book.EditionInfo editionInfo = new Book.EditionInfo();
            editionInfo.setType(type);
            editionInfo.setIdentifier(ident);
            otherEditions.add(editionInfo);
        }
    }

    public Mono<List<Book>> getSimilarBooks(Book book) {
        if (book == null || book.getAuthors() == null || book.getAuthors().isEmpty() || book.getTitle() == null) {
            return Mono.just(Collections.emptyList());
        }
        
        String authorQuery = book.getAuthors().stream()
                .findFirst()
                .orElse("")
                .replace(" ", "+");

        String titleQuery = book.getTitle().replace(" ", "+");
        if (authorQuery.isEmpty() && titleQuery.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }

        String query = String.format("inauthor:%s intitle:%s", authorQuery, titleQuery);
        
        return searchBooksAsyncReactive(query)
            .map(similarBooksList -> similarBooksList.stream()
                .filter(similarBook -> !similarBook.getId().equals(book.getId()))
                .limit(5)
                .collect(Collectors.toList())
            );
    }
}
