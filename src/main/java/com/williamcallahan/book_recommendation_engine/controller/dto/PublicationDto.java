package com.williamcallahan.book_recommendation_engine.controller.dto;

import java.util.Date;

/** DTO containing publication metadata for a book. */
public record PublicationDto(Date publishedDate,
                             String language,
                             Integer pageCount,
                             String publisher) {
}
