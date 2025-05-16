package com.williamcallahan.book_recommendation_engine.types;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JPA converter for PostgreSQL vector type
 *
 * @author William Callahan
 *
 * Handles conversion between PgVector entity attribute and database column string representation
 * Used for storing and retrieving vector embeddings in PostgreSQL
 */
@Converter(autoApply = false)
public class PgVectorConverter implements AttributeConverter<PgVector, String> {

    private static final Logger logger = LoggerFactory.getLogger(PgVectorConverter.class);

    /**
     * Converts PgVector entity attribute to database column representation
     *
     * @param attribute PgVector to be converted
     * @return String representation of the vector for database storage
     */
    @Override
    public String convertToDatabaseColumn(PgVector attribute) {
        if (attribute == null) {
            return null;
        }
        return attribute.toString();
    }

    /**
     * Converts database column representation to PgVector entity attribute
     *
     * @param dbData String vector representation from database
     * @return PgVector instance created from the string representation
     */
    @Override
    public PgVector convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isEmpty()) {
            return null;
        }
        
        try {
            return new PgVector(dbData);
        } catch (IllegalArgumentException e) {
            logger.error("Error converting database column to PgVector: {} - {}", dbData, e.getMessage(), e);
            return null;
        }
    }
}
