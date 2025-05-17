/**
 * Main application class for Book Finder
 *
 * @author William Callahan
 *
 * Features:
 * - Excludes default database auto-configurations to allow conditional DB setup (Note: Some exclusions removed to fix bean issue)
 * - Enables caching for improved performance
 * - Supports asynchronous operations for non-blocking API calls
 * - Entry point for Spring Boot application
 */
package com.williamcallahan.book_recommendation_engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioSpeechAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioTranscriptionAutoConfiguration;
@SpringBootApplication(exclude = {
    OpenAiAudioSpeechAutoConfiguration.class, 
    OpenAiAudioTranscriptionAutoConfiguration.class 
})
@EnableCaching
@EnableAsync
@EnableScheduling
public class BookRecommendationEngineApplication {

    /**
     * Main method that starts the Spring Boot application
     *
     * @param args Command line arguments passed to the application
     */
	public static void main(String[] args) {
		SpringApplication.run(BookRecommendationEngineApplication.class, args);
	}
}
