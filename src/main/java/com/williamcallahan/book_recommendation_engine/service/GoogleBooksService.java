package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    public Mono<JsonNode> searchBooks(String query, int startIndex, String orderBy) {
        String url = String.format("%s/volumes?q=%s&startIndex=%d&maxResults=40&key=%s",
                googleBooksApiUrl, query, startIndex, googleBooksApiKey);
        if (orderBy != null && !orderBy.isEmpty()) {
            url += "&orderBy=" + orderBy;
        }
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> {
                    logger.warn("Error fetching page for query '{}' at startIndex {}: {}", query, startIndex, e.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        final int maxResultsPerPage = 40;
        final int maxTotalResults = 200;

        return Flux.range(0, (maxTotalResults + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> 
                searchBooks(query, startIndex, "newest")
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

    public Mono<List<Book>> searchBooksByTitle(String title) {
        return searchBooksAsyncReactive("intitle:" + title);
    }

    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksAsyncReactive("inauthor:" + author);
    }

    public Mono<List<Book>> searchBooksByISBN(String isbn) {
        return searchBooksAsyncReactive("isbn:" + isbn);
    }

    public Mono<Book> getBookById(String bookId) {
        String url = String.format("%s/volumes/%s?key=%s", googleBooksApiUrl, bookId, googleBooksApiKey);
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(item -> {
                    if (item != null) {
                        return convertSingleItemToBook(item);
                    }
                    return null; // Or handle error/empty case differently, e.g., Mono.empty()
                })
                .onErrorResume(e -> {
                    logger.error("Error fetching book by ID {}: {}", bookId, e.getMessage());
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
        book.setPublisher(volumeInfo.has("publisher") ? volumeInfo.get("publisher").asText() : null);
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
                if (enhancedUrl.contains("zoom=1")) {
                    enhancedUrl = enhancedUrl.replace("zoom=1", "zoom=5");
                }
                if (!enhancedUrl.contains("fife=")) {
                    enhancedUrl = enhancedUrl + (enhancedUrl.contains("?") ? "&" : "?") + "fife=w800";
                }
                break;
                
            case "medium":
                if (enhancedUrl.contains("zoom=1")) {
                    enhancedUrl = enhancedUrl.replace("zoom=1", "zoom=3");
                }
                if (!enhancedUrl.contains("fife=")) {
                    enhancedUrl = enhancedUrl + (enhancedUrl.contains("?") ? "&" : "?") + "fife=w400";
                }
                break;
                
            case "low":
                // Use zoom=1 for low resolution (default)
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
