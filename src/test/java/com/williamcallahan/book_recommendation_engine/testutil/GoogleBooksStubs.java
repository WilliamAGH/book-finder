package com.williamcallahan.book_recommendation_engine.testutil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.service.GoogleApiFetcher;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/** Common stubs for GoogleBooksService tests interacting with GoogleApiFetcher. */
public final class GoogleBooksStubs {
    private GoogleBooksStubs() {}

    public static ObjectNode volume(ObjectMapper om, String id, String title, String author) {
        ObjectNode vol = om.createObjectNode();
        vol.put("id", id);
        ObjectNode info = om.createObjectNode();
        info.put("title", title);
        ArrayNode authors = om.createArrayNode();
        authors.add(author);
        info.set("authors", authors);
        ObjectNode imageLinks = om.createObjectNode();
        imageLinks.put("thumbnail", "http://example.com/thumbnail.jpg");
        info.set("imageLinks", imageLinks);
        vol.set("volumeInfo", info);
        return vol;
    }

    public static ObjectNode responseWithItems(ObjectMapper om, JsonNode... items) {
        ObjectNode resp = om.createObjectNode();
        ArrayNode arr = om.createArrayNode();
        for (JsonNode n : items) arr.add(n);
        resp.set("items", arr);
        return resp;
    }

    public static ObjectNode responseWithEmptyItems(ObjectMapper om) {
        ObjectNode resp = om.createObjectNode();
        resp.set("items", om.createArrayNode());
        return resp;
    }

    public static void stubSearchReturns(GoogleApiFetcher fetcher, String query, String orderBy, String lang, ObjectNode response) {
        when(fetcher.searchVolumesAuthenticated(eq(query), anyInt(), eq(orderBy), eq(lang)))
                .thenReturn(Mono.just(response));
    }

    public static void stubSearchError(GoogleApiFetcher fetcher, String query, String orderBy, String lang, Throwable error) {
        when(fetcher.searchVolumesAuthenticated(eq(query), anyInt(), eq(orderBy), eq(lang)))
                .thenReturn(Mono.error(error));
    }

    public static void stubFetchVolumeReturns(GoogleApiFetcher fetcher, String bookId, JsonNode volume) {
        when(fetcher.fetchVolumeByIdAuthenticated(eq(bookId))).thenReturn(Mono.just(volume));
    }

    public static void stubFetchVolumeError(GoogleApiFetcher fetcher, String bookId, Throwable error) {
        when(fetcher.fetchVolumeByIdAuthenticated(eq(bookId))).thenReturn(Mono.error(error));
    }
}