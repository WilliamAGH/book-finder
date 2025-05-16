package com.williamcallahan.book_recommendation_engine.types;

import java.util.Arrays; // For Arrays.copyOf

/**
 * Record representing a processed image with its metadata
 *
 * @author William Callahan
 *
 * Features:
 * - Immutable container for image data and metadata
 * - Supports both successful and failed processing results
 * - Tracks image dimensions and format information
 * - Provides static factory methods for easy instantiation
 * - Implements defensive copy for mutable byte arrays
 * 
 * @param processedBytes The processed image data as a byte array
 * @param newFileExtension The file extension for the processed image (e.g., ".jpg")
 * @param newMimeType The MIME type of the processed image (e.g., "image/jpeg")
 * @param width The width of the processed image in pixels
 * @param height The height of the processed image in pixels
 * @param processingSuccessful Whether the image processing was successful
 * @param processingError Error message if processing failed
 */
public record ProcessedImage(
        byte[] processedBytes,
        String newFileExtension,
        String newMimeType,
        int width,
        int height,
        boolean processingSuccessful,
        String processingError) {

    // Compact canonical constructor for defensive copy
    public ProcessedImage {
        // Defensive copy for mutable byte array
        if (processedBytes != null) {
            processedBytes = Arrays.copyOf(processedBytes, processedBytes.length);
        }
    }

    /**
     * Static factory method for creating a successfully processed image.
     *
     * @param processedBytes The processed image data
     * @param newFileExtension The file extension for the processed image
     * @param newMimeType The MIME type of the processed image
     * @param width The width of the processed image in pixels
     * @param height The height of the processed image in pixels
     * @return A new ProcessedImage instance representing a successful operation
     */
    public static ProcessedImage success(byte[] processedBytes, String newFileExtension, String newMimeType, int width, int height) {
        return new ProcessedImage(processedBytes, newFileExtension, newMimeType, width, height, true, null);
    }

    /**
     * Static factory method for creating a failed processing result.
     *
     * @param processingError The error message describing why processing failed
     * @return A new ProcessedImage instance representing a failed operation
     */
    public static ProcessedImage failure(String processingError) {
        return new ProcessedImage(null, null, null, 0, 0, false, processingError);
    }

    // Getters might be useful for other services, though direct field access is fine for now in a simple DTO.
    public byte[] getProcessedBytes() {
        return processedBytes;
    }

    public String getNewFileExtension() {
        return newFileExtension;
    }

    public String getNewMimeType() {
        return newMimeType;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public boolean isProcessingSuccessful() {
        return processingSuccessful;
    }

    public String getProcessingError() {
        return processingError;
    }
} 