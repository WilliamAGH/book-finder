package com.williamcallahan.book_recommendation_engine.types;

public class ProcessedImage {
    public final byte[] processedBytes;
    public final String newFileExtension;
    public final String newMimeType;
    public final int width;
    public final int height;
    public final boolean processingSuccessful;
    public final String processingError; // Null if successful

    // Constructor for a successfully processed image
    public ProcessedImage(byte[] processedBytes, String newFileExtension, String newMimeType, int width, int height) {
        this.processedBytes = processedBytes;
        this.newFileExtension = newFileExtension;
        this.newMimeType = newMimeType;
        this.width = width;
        this.height = height;
        this.processingSuccessful = true;
        this.processingError = null;
    }

    // Constructor for a failed processing attempt
    public ProcessedImage(String processingError) {
        this.processedBytes = null;
        this.newFileExtension = null;
        this.newMimeType = null;
        this.width = 0;
        this.height = 0;
        this.processingSuccessful = false;
        this.processingError = processingError;
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