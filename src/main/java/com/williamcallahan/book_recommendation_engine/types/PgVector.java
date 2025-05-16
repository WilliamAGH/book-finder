package com.williamcallahan.book_recommendation_engine.types;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents a vector for use with PostgreSQL's pgvector extension
 * 
 * @author William Callahan
 *
 * Handles conversion to and from the string representation
 * required by pgvector (e.g., "[1.0,2.5,3.0]")
 * Provides immutable vector storage with proper serialization support
 */
public class PgVector implements Serializable {

    private static final long serialVersionUID = 1L;

    private final float[] vector;

    /**
     * Constructs a PgVector from a float array
     * 
     * @param vector Float array containing the vector values
     * @throws IllegalArgumentException if vector is null
     */
    public PgVector(float[] vector) {
        if (vector == null) {
            throw new IllegalArgumentException("Vector array cannot be null");
        }
        this.vector = Arrays.copyOf(vector, vector.length);
    }

    /**
     * Constructs a PgVector from a string representation
     * 
     * @param vectorString String representation of vector in format "[val1,val2,...]"
     * @throws IllegalArgumentException if string is invalid format
     */
    public PgVector(String vectorString) {
        if (vectorString == null || vectorString.isEmpty()) {
            throw new IllegalArgumentException("Vector string cannot be null or empty");
        }
        if (!vectorString.startsWith("[") || !vectorString.endsWith("]")) {
            throw new IllegalArgumentException("Vector string must start with '[' and end with ']'. Found: " + vectorString);
        }
        String[] items = vectorString.substring(1, vectorString.length() - 1).split(",");
        this.vector = new float[items.length];
        for (int i = 0; i < items.length; i++) {
            try {
                this.vector[i] = Float.parseFloat(items[i].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number format in vector string: " + items[i], e);
            }
        }
    }

    /**
     * Returns a copy of the vector
     * 
     * @return Copy of the internal vector array
     */
    public float[] getVector() {
        return Arrays.copyOf(vector, vector.length);
    }

    @Override
    public String toString() {
        if (vector == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            sb.append(vector[i]);
            if (i < vector.length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PgVector pgVector = (PgVector) o;
        return Arrays.equals(vector, pgVector.vector);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vector);
    }
}
