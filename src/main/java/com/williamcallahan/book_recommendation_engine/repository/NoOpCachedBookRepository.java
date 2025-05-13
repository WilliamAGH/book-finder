package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A no-op implementation of CachedBookRepository that is used when no database is available.
 * This allows the application to function without database access.
 */
@Repository
@Primary
@ConditionalOnExpression("'${spring.datasource.url:}'.length() == 0")
public class NoOpCachedBookRepository implements CachedBookRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(NoOpCachedBookRepository.class);
    
    public NoOpCachedBookRepository() {
        logger.info("No database URL provided. Using no-op cache repository implementation.");
    }

    @Override
    public Optional<CachedBook> findByGoogleBooksId(String googleBooksId) {
        return Optional.empty();
    }

    @Override
    public Optional<CachedBook> findByIsbn10(String isbn10) {
        return Optional.empty();
    }

    @Override
    public Optional<CachedBook> findByIsbn13(String isbn13) {
        return Optional.empty();
    }

    @Override
    public List<CachedBook> findSimilarBooksById(String id, int limit) {
        return Collections.emptyList();
    }

    @Override
    public <S extends CachedBook> S save(S entity) {
        return entity;
    }

    @Override
    public <S extends CachedBook> Iterable<S> saveAll(Iterable<S> entities) {
        return entities;
    }

    @Override
    public Optional<CachedBook> findById(String s) {
        return Optional.empty();
    }

    @Override
    public boolean existsById(String s) {
        return false;
    }

    @Override
    public Iterable<CachedBook> findAll() {
        return Collections.emptyList();
    }

    @Override
    public Iterable<CachedBook> findAllById(Iterable<String> strings) {
        return Collections.emptyList();
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void deleteById(String s) {
    }

    @Override
    public void delete(CachedBook entity) {
    }

    @Override
    public void deleteAllById(Iterable<? extends String> strings) {
    }

    @Override
    public void deleteAll(Iterable<? extends CachedBook> entities) {
    }

    @Override
    public void deleteAll() {
    }
}
