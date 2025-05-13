package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.Map;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverCandidate;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
public class LongitoodServiceImpl implements LongitoodService {

    private static final Logger logger = LoggerFactory.getLogger(LongitoodServiceImpl.class);

    private final WebClient webClient;

    @Autowired
    public LongitoodServiceImpl(WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public CompletableFuture<List<CoverCandidate>> fetchCovers(Book book) {
        return fetchCovers(book, CoverImageSource.ANY);
    }
    
    @Override
    public CompletableFuture<List<CoverCandidate>> fetchCovers(Book book, CoverImageSource preferredSource) {
        // If a specific source is requested that isn't Longitood, return empty list
        if (preferredSource != CoverImageSource.ANY && preferredSource != CoverImageSource.LONGITOOD) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        if (isbn == null) {
            logger.warn("No ISBN found for book {}, cannot fetch cover from Longitood", book.getId());
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        String apiUrl = "https://bookcover.longitood.com/bookcover/" + isbn;
        return tryLongitoodApi(apiUrl, book.getId())
            .thenApply(coverUrl -> {
                List<CoverCandidate> candidates = new ArrayList<>();
                if (coverUrl != null) {
                    candidates.add(new CoverCandidate(
                        coverUrl,
                        "Longitood",
                        60,
                        null,
                        "Longitood-" + isbn
                    ));
                }
                return candidates;
            });
    }

    private CompletableFuture<String> tryLongitoodApi(String apiUrl, String bookIdForLog) {
        if (apiUrl == null || apiUrl.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Calling bookcover.longitood.com API: {} for book ID {}", apiUrl, bookIdForLog);
        return webClient.get()
            .uri(apiUrl)
            .retrieve()
            .bodyToMono(new org.springframework.core.ParameterizedTypeReference<Map<String, String>>() {})
            .map(response -> {
                if (response != null && response.containsKey("url")) {
                    String coverUrl = response.get("url");
                    if (coverUrl != null && !coverUrl.isEmpty()) {
                        logger.debug("Found cover URL from bookcover.longitood.com API for book {}: {}", bookIdForLog, coverUrl);
                        return coverUrl;
                    }
                }
                logger.debug("No valid cover URL found from bookcover.longitood.com API for book {}", bookIdForLog);
                return null;
            })
            .onErrorResume(WebClientResponseException.class, e -> {
                logger.warn("Longitood API error for book {}: {}", bookIdForLog, e.getMessage());
                return Mono.just(null);
            })
            .toFuture();
    }
}
