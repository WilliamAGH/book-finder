/**
 * Simple vector implementation for Redis storage
 *
 * @author William Callahan
 *
 * Features:
 * - Wraps float arrays for embedding vectors
 * - Serializable to JSON for Redis storage
 * - Compatible with Jackson serialization
 * - Used for semantic similarity searches in Redis
 */
package com.williamcallahan.book_recommendation_engine.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;

@Data
@NoArgsConstructor
public class RedisVector {

    private float[] values;

    @JsonCreator
    public RedisVector(@JsonProperty("values") float[] values) {
        this.values = values != null ? Arrays.copyOf(values, values.length) : new float[0];
    }

    /**
     * Creates a RedisVector from a float array
     * 
     * @param values The float array containing vector values
     * @return New RedisVector instance
     */
    public static RedisVector of(float[] values) {
        return new RedisVector(values);
    }

    /**
     * Creates a RedisVector from a string representation
     * This is useful for parsing stored vectors from Redis
     * 
     * @param vectorString Comma-separated float values
     * @return New RedisVector instance
     */
    public static RedisVector fromString(String vectorString) {
        if (vectorString == null || vectorString.trim().isEmpty()) {
            return new RedisVector(new float[0]);
        }
        
        String[] parts = vectorString.split(",");
        float[] values = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            values[i] = Float.parseFloat(parts[i].trim());
        }
        return new RedisVector(values);
    }

    /**
     * Converts the vector to a string representation
     * 
     * @return Comma-separated float values
     */
    public String toString() {
        if (values == null || values.length == 0) {
            return "";
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(values[i]);
        }
        return sb.toString();
    }

    /**
     * Gets the dimension of this vector
     * 
     * @return Number of dimensions in the vector
     */
    public int getDimension() {
        return values != null ? values.length : 0;
    }

    /**
     * Calculates cosine similarity with another vector
     * 
     * @param other The other vector to compare with
     * @return Cosine similarity value between -1 and 1
     */
    public double cosineSimilarity(RedisVector other) {
        if (this.values == null || other.values == null || 
            this.values.length != other.values.length) {
            return 0.0;
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < this.values.length; i++) {
            dotProduct += this.values[i] * other.values[i];
            normA += this.values[i] * this.values[i];
            normB += other.values[i] * other.values[i];
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Creates a copy of this vector
     * 
     * @return New RedisVector with copied values
     */
    public RedisVector copy() {
        return new RedisVector(this.values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RedisVector other = (RedisVector) obj;
        return Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
