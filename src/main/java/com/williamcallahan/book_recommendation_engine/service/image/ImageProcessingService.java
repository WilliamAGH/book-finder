/**
 * Service for processing and optimizing book cover images
 *
 * @author William Callahan
 *
 * Features:
 * - Resizes large images to standardized dimensions
 * - Preserves aspect ratio during resizing operations
 * - Converts images to consistent color format (RGB)
 * - Compresses to JPEG format with configurable quality
 * - Prevents upscaling of small images to maintain quality
 * - Provides detailed processing logs for troubleshooting
 * - Returns standardized result object with success/failure info
 */

package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.types.ProcessedImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.awt.Graphics2D;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;


@Service
public class ImageProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ImageProcessingService.class);
    private static final int TARGET_WIDTH = 800; // Target width for resizing
    private static final float JPEG_QUALITY = 0.85f; // Standard JPEG quality
    private static final int MIN_ACCEPTABLE_DIMENSION = 50; // Reject if smaller than this
    private static final int NO_UPSCALE_THRESHOLD_WIDTH = 300; // Don't upscale if original is smaller than this

    // Constants for dominant color check
    private static final int DOMINANT_COLOR_SAMPLE_STEP = 5; // Sample every 5th pixel
    private static final double DOMINANT_COLOR_THRESHOLD_PERCENTAGE = 0.80; // Adjusted from 0.90 to 0.80 (80%)
    private static final int WHITE_THRESHOLD_RGB = 240; // RGB components > 240 are considered "white"

    /**
     * Processes an image for S3 storage, optimizing size and quality
     * 
     * @param rawImageBytes The raw image bytes to process
     * @param bookIdForLog Book identifier for logging purposes
     * @return ProcessedImage containing the processed bytes or failure details
     * 
     * @implNote Processing workflow:
     * 1. Validates input bytes are valid image data
     * 2. Converts to standardized RGB color space
     * 3. Checks if image meets minimum size requirements
     * 4. Determines if resizing is needed based on configurable thresholds
     * 5. Resizes if necessary while maintaining aspect ratio
     * 6. Compresses to JPEG with optimized quality settings
     * 7. Returns complete result object with metadata or error details
     */
    @Async("imageProcessingExecutor") // Offload CPU-intensive work to dedicated executor
    public CompletableFuture<ProcessedImage> processImageForS3(byte[] rawImageBytes, String bookIdForLog) {
        if (rawImageBytes == null || rawImageBytes.length == 0) {
            logger.warn("Book ID {}: Raw image bytes are null or empty. Cannot process.", bookIdForLog);
            return CompletableFuture.completedFuture(ProcessedImage.failure("Raw image bytes null or empty"));
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(rawImageBytes)) {
            BufferedImage rawOriginalImage = ImageIO.read(bais);
            if (rawOriginalImage == null) {
                logger.warn("Book ID {}: Could not read raw bytes into a BufferedImage. Image format might be unsupported or corrupt.", bookIdForLog);
                return CompletableFuture.completedFuture(ProcessedImage.failure("Unsupported or corrupt image format"));
            }

            // Convert to a standard RGB colorspace to avoid issues with JPEG writer and for consistent analysis
            BufferedImage originalImage = new BufferedImage(rawOriginalImage.getWidth(), rawOriginalImage.getHeight(), BufferedImage.TYPE_INT_RGB);
            Graphics2D g = originalImage.createGraphics();
            g.drawImage(rawOriginalImage, 0, 0, null);
            g.dispose();

            int originalWidth = originalImage.getWidth();
            int originalHeight = originalImage.getHeight();

            // Perform dominant color check
            if (isDominantlyWhite(originalImage, bookIdForLog)) {
                logger.warn("Book ID {}: Image is predominantly white. Flagged as likely not a cover.", bookIdForLog);
                return CompletableFuture.completedFuture(ProcessedImage.failure("LikelyNotACover_DominantColor"));
            }

            if (originalWidth < MIN_ACCEPTABLE_DIMENSION || originalHeight < MIN_ACCEPTABLE_DIMENSION) {
                logger.warn("Book ID {}: Original image dimensions ({}x{}) are below the minimum acceptable ({}x{}). Processing as is, but quality will be low.", 
                    bookIdForLog, originalWidth, originalHeight, MIN_ACCEPTABLE_DIMENSION, MIN_ACCEPTABLE_DIMENSION);
                // Still attempt to compress it, but don't resize.
                // Note: originalImage is already in TYPE_INT_RGB here
                return CompletableFuture.completedFuture(compressOriginal(originalImage, bookIdForLog, originalWidth, originalHeight));
            }

            int newWidth;
            int newHeight;

            if (originalWidth <= NO_UPSCALE_THRESHOLD_WIDTH) {
                // If image is already small, don't upscale. Use original dimensions.
                newWidth = originalWidth;
                newHeight = originalHeight;
                logger.debug("Book ID {}: Image width ({}) is below no-upscale threshold ({}). Using original dimensions for processing.", 
                    bookIdForLog, originalWidth, NO_UPSCALE_THRESHOLD_WIDTH);
            } else if (originalWidth > TARGET_WIDTH) {
                // Resize to TARGET_WIDTH if wider, maintaining aspect ratio
                newWidth = TARGET_WIDTH;
                newHeight = (int) Math.round(((double) originalHeight / originalWidth) * newWidth);
                logger.debug("Book ID {}: Resizing image from {}x{} to {}x{}.", 
                    bookIdForLog, originalWidth, originalHeight, newWidth, newHeight);
            } else {
                // Image is between NO_UPSCALE_THRESHOLD_WIDTH and TARGET_WIDTH, or exactly TARGET_WIDTH. Use original dimensions.
                newWidth = originalWidth;
                newHeight = originalHeight;
                logger.debug("Book ID {}: Image width ({}) is acceptable. Using original dimensions {}x{} for processing.", 
                    bookIdForLog, originalWidth, newWidth, newHeight);
            }
            
            BufferedImage outputImage = originalImage;
            if (newWidth != originalWidth || newHeight != originalHeight) { // Only resize if dimensions changed
                 outputImage = new BufferedImage(newWidth, newHeight, BufferedImage.TYPE_INT_RGB); // For JPEG, ensure no alpha
                 Graphics2D g2d = outputImage.createGraphics();
                 g2d.drawImage(originalImage, 0, 0, newWidth, newHeight, null);
                 g2d.dispose();
            }

            return CompletableFuture.completedFuture(compressImageToJpeg(outputImage, bookIdForLog, newWidth, newHeight));

        } catch (IOException e) {
            logger.error("Book ID {}: IOException during image processing: {}", bookIdForLog, e.getMessage(), e);
            return CompletableFuture.completedFuture(ProcessedImage.failure("IOException during image processing: " + e.getMessage()));
        } catch (Exception e) {
            logger.error("Book ID {}: Unexpected exception during image processing: {}", bookIdForLog, e.getMessage(), e);
            return CompletableFuture.completedFuture(ProcessedImage.failure("Unexpected error during image processing: " + e.getMessage()));
        }
    }

    /**
     * Compresses an image without resizing when dimensions are below thresholds.
     *
     * @param imageToCompress The image to compress
     * @param bookIdForLog Book identifier for logging purposes
     * @param width Original width of the image
     * @param height Original height of the image
     * @return ProcessedImage containing the compressed image data
     * @throws IOException If compression fails
     */
    private ProcessedImage compressOriginal(BufferedImage imageToCompress, String bookIdForLog, int width, int height) throws IOException {
        logger.debug("Book ID {}: Compressing original small image ({}x{}) as JPEG.", bookIdForLog, width, height);
        return compressImageToJpeg(imageToCompress, bookIdForLog, width, height);
    }

    /**
     * Compresses a BufferedImage to JPEG format with the configured quality settings.
     *
     * @param imageToCompress The image to compress to JPEG
     * @param bookIdForLog Book identifier for logging purposes
     * @param finalWidth Final width of the image
     * @param finalHeight Final height of the image
     * @return ProcessedImage containing the compressed JPEG data
     * @throws IOException If compression fails
     */
    private ProcessedImage compressImageToJpeg(BufferedImage imageToCompress, String bookIdForLog, int finalWidth, int finalHeight) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
            if (!writers.hasNext()) {
                logger.error("Book ID {}: No JPEG ImageWriters found. Cannot compress image.", bookIdForLog);
                return ProcessedImage.failure("No JPEG ImageWriters available.");
            }
            ImageWriter writer = writers.next();
            ImageWriteParam jpegParams = writer.getDefaultWriteParam();
            jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            jpegParams.setCompressionQuality(JPEG_QUALITY);

            try (ImageOutputStream ios = ImageIO.createImageOutputStream(baos)) {
                writer.setOutput(ios);
                writer.write(null, new javax.imageio.IIOImage(imageToCompress, null, null), jpegParams);
                writer.dispose();
            }

            byte[] processedBytes = baos.toByteArray();
            logger.info("Book ID {}: Successfully processed image to JPEG. Original size (approx if read): N/A, Processed size: {} bytes, Dimensions: {}x{}", 
                bookIdForLog, processedBytes.length, finalWidth, finalHeight);
            return ProcessedImage.success(processedBytes, ".jpg", "image/jpeg", finalWidth, finalHeight);
        }
    }

    /**
     * Checks if the given image is predominantly white.
     *
     * @param image The image to check
     * @param bookIdForLog Book identifier for logging
     * @return True if the image is predominantly white, false otherwise
     */
    private boolean isDominantlyWhite(BufferedImage image, String bookIdForLog) {
        if (image == null) {
            return false;
        }

        int width = image.getWidth();
        int height = image.getHeight();
        long whitePixelCount = 0;
        long sampledPixelCount = 0;

        for (int y = 0; y < height; y += DOMINANT_COLOR_SAMPLE_STEP) {
            for (int x = 0; x < width; x += DOMINANT_COLOR_SAMPLE_STEP) {
                int rgb = image.getRGB(x, y);
                int r = (rgb >> 16) & 0xFF;
                int g = (rgb >> 8) & 0xFF;
                int b = rgb & 0xFF;

                if (r >= WHITE_THRESHOLD_RGB && g >= WHITE_THRESHOLD_RGB && b >= WHITE_THRESHOLD_RGB) {
                    whitePixelCount++;
                }
                sampledPixelCount++;
            }
        }

        if (sampledPixelCount == 0) {
            logger.debug("Book ID {}: No pixels sampled for dominant white check (image might be too small for step).", bookIdForLog);
            return false; // Avoid division by zero for very small images or step > dimensions
        }

        double whitePercentage = (double) whitePixelCount / sampledPixelCount;
        boolean isDominant = whitePercentage >= DOMINANT_COLOR_THRESHOLD_PERCENTAGE;

        if (isDominant) {
            logger.info("Book ID {}: Dominant white check: {}% white pixels ({} / {} sampled). Threshold: {}%. Result: LIKELY NOT A COVER.",
                bookIdForLog, String.format("%.2f", whitePercentage * 100), whitePixelCount, sampledPixelCount, DOMINANT_COLOR_THRESHOLD_PERCENTAGE * 100);
        } else {
            logger.debug("Book ID {}: Dominant white check: {}% white pixels ({} / {} sampled). Threshold: {}%. Result: LIKELY A COVER.",
                bookIdForLog, String.format("%.2f", whitePercentage * 100), whitePixelCount, sampledPixelCount, DOMINANT_COLOR_THRESHOLD_PERCENTAGE * 100);
        }
        return isDominant;
    }

    /**
     * Asynchronously checks if an image from raw bytes is predominantly white.
     * This is a convenience method for services that only need this check without full processing.
     *
     * @param rawImageBytes The raw image bytes to check
     * @param imageIdForLog Identifier for logging (e.g., S3 key or book ID)
     * @return CompletableFuture<Boolean> true if the image is predominantly white, false otherwise. Returns false if image can't be read or in case of error.
     */
    @Async("imageProcessingExecutor")
    public CompletableFuture<Boolean> isDominantlyWhiteAsync(byte[] rawImageBytes, String imageIdForLog) {
        return CompletableFuture.supplyAsync(() -> {
            if (rawImageBytes == null || rawImageBytes.length == 0) {
                logger.warn("Image ID {}: Raw image bytes are null or empty for dominant white check.", imageIdForLog);
                return false;
            }

            try (ByteArrayInputStream bais = new ByteArrayInputStream(rawImageBytes)) {
                BufferedImage rawOriginalImage = ImageIO.read(bais);
                if (rawOriginalImage == null) {
                    logger.warn("Image ID {}: Could not read raw bytes into a BufferedImage for dominant white check. Image format might be unsupported or corrupt.", imageIdForLog);
                    return false;
                }

                // Convert to a standard RGB colorspace for consistent analysis
                BufferedImage imageInRGB = new BufferedImage(rawOriginalImage.getWidth(), rawOriginalImage.getHeight(), BufferedImage.TYPE_INT_RGB);
                Graphics2D g = imageInRGB.createGraphics();
                g.drawImage(rawOriginalImage, 0, 0, null);
                g.dispose();

                return isDominantlyWhite(imageInRGB, imageIdForLog);

            } catch (IOException e) {
                logger.error("Image ID {}: IOException during dominant white check from bytes: {}", imageIdForLog, e.getMessage(), e);
                return false;
            } catch (Exception e) {
                logger.error("Image ID {}: Unexpected exception during dominant white check from bytes: {}", imageIdForLog, e.getMessage(), e);
                return false;
            }
        });
    }

    /**
     * @deprecated Use {@link #isDominantlyWhiteAsync(byte[], String)} instead.
     */
    @Deprecated
    public boolean isDominantlyWhite(byte[] rawImageBytes, String imageIdForLog) {
        try {
            return isDominantlyWhiteAsync(rawImageBytes, imageIdForLog).join();
        } catch (Exception e) {
            logger.error("Error in synchronous isDominantlyWhite for imageId {}: {}", imageIdForLog, e.getMessage(), e);
            return false;
        }
    }
}
