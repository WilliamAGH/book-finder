package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.types.ProcessedImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Service
public class ImageProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ImageProcessingService.class);
    private static final int TARGET_WIDTH = 800; // Target width for resizing
    private static final float JPEG_QUALITY = 0.85f; // Standard JPEG quality
    private static final int MIN_ACCEPTABLE_DIMENSION = 50; // Reject if smaller than this
    private static final int NO_UPSCALE_THRESHOLD_WIDTH = 300; // Don't upscale if original is smaller than this

    public ProcessedImage processImageForS3(byte[] rawImageBytes, String bookIdForLog) {
        if (rawImageBytes == null || rawImageBytes.length == 0) {
            logger.warn("Book ID {}: Raw image bytes are null or empty. Cannot process.", bookIdForLog);
            return new ProcessedImage("Raw image bytes are null or empty.");
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(rawImageBytes)) {
            BufferedImage originalImage = ImageIO.read(bais);
            if (originalImage == null) {
                logger.warn("Book ID {}: Could not read raw bytes into a BufferedImage. Image format might be unsupported or corrupt.", bookIdForLog);
                return new ProcessedImage("Unsupported or corrupt image format.");
            }

            int originalWidth = originalImage.getWidth();
            int originalHeight = originalImage.getHeight();

            if (originalWidth < MIN_ACCEPTABLE_DIMENSION || originalHeight < MIN_ACCEPTABLE_DIMENSION) {
                logger.warn("Book ID {}: Original image dimensions ({}x{}) are below the minimum acceptable ({}x{}). Processing as is, but quality will be low.", 
                    bookIdForLog, originalWidth, originalHeight, MIN_ACCEPTABLE_DIMENSION, MIN_ACCEPTABLE_DIMENSION);
                // Still attempt to compress it, but don't resize.
                return compressOriginal(originalImage, bookIdForLog, originalWidth, originalHeight);
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

            return compressImageToJpeg(outputImage, bookIdForLog, newWidth, newHeight);

        } catch (IOException e) {
            logger.error("Book ID {}: IOException during image processing: {}", bookIdForLog, e.getMessage(), e);
            return new ProcessedImage("IOException during image processing: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Book ID {}: Unexpected exception during image processing: {}", bookIdForLog, e.getMessage(), e);
            return new ProcessedImage("Unexpected error during image processing: " + e.getMessage());
        }
    }

    private ProcessedImage compressOriginal(BufferedImage imageToCompress, String bookIdForLog, int width, int height) throws IOException {
        logger.debug("Book ID {}: Compressing original small image ({}x{}) as JPEG.", bookIdForLog, width, height);
        return compressImageToJpeg(imageToCompress, bookIdForLog, width, height);
    }

    private ProcessedImage compressImageToJpeg(BufferedImage imageToCompress, String bookIdForLog, int finalWidth, int finalHeight) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
            if (!writers.hasNext()) {
                logger.error("Book ID {}: No JPEG ImageWriters found. Cannot compress image.", bookIdForLog);
                return new ProcessedImage("No JPEG ImageWriters available.");
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
            return new ProcessedImage(processedBytes, ".jpg", "image/jpeg", finalWidth, finalHeight);
        }
    }
} 