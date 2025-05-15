package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService; 
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
public class LongitoodServiceImpl implements LongitoodService { 

    private static final Logger logger = LoggerFactory.getLogger(LongitoodServiceImpl.class);
    private static final String LONGITOOD_SOURCE_NAME = "Longitood";

    private final WebClient webClient;

    @Autowired
    public LongitoodServiceImpl(WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public Mono<ImageDetails> fetchCover(Book book) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN found for book ID: {}, cannot fetch cover from Longitood", book.getId());
            return Mono.empty();
        }

        String apiUrl = "https://bookcover.longitood.com/bookcover/" + isbn;
        final String finalIsbn = isbn; 

        return webClient.get()
            .uri(apiUrl)
            .retrieve()
            .bodyToMono(new org.springframework.core.ParameterizedTypeReference<Map<String, String>>() {})
            .flatMap(response -> {
                if (response != null && response.containsKey("url")) {
                    String coverUrl = response.get("url");
                    if (coverUrl != null && !coverUrl.isEmpty()) {
                        logger.debug("Found cover URL from Longitood API for book {}: {}", book.getId(), coverUrl);
                        String sourceSystemId = String.format("%s-%s", LONGITOOD_SOURCE_NAME, finalIsbn);
                        ImageDetails imageDetails = new ImageDetails(
                            coverUrl,
                            LONGITOOD_SOURCE_NAME,
                            sourceSystemId,
                            CoverImageSource.LONGITOOD,
                            ImageResolutionPreference.ORIGINAL 
                        );
                        return Mono.just(imageDetails);
                    }
                }
                logger.debug("No valid cover URL found from Longitood API for book {}", book.getId());
                return Mono.empty();
            })
            .onErrorResume(WebClientResponseException.class, e -> {
                logger.warn("Longitood API error for book {}: {}", book.getId(), e.getMessage());
                return Mono.empty(); 
            })
            .onErrorResume(Exception.class, e -> {
                 logger.error("Unexpected error during Longitood API call for book {}: {}", book.getId(), e.getMessage(), e);
                 return Mono.empty(); 
            });
    }

}
