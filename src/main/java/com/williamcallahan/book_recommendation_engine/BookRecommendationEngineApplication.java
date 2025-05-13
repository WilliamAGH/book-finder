package com.williamcallahan.book_recommendation_engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioSpeechAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioTranscriptionAutoConfiguration;

// Simple application entry point - specialized configurations handle database presence/absence
@SpringBootApplication(exclude = {
    DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class,
    HibernateJpaAutoConfiguration.class,
    JpaRepositoriesAutoConfiguration.class,
    OpenAiAudioSpeechAutoConfiguration.class, 
    OpenAiAudioTranscriptionAutoConfiguration.class 
})
@EnableCaching
@EnableAsync // Enable asynchronous operations
public class BookRecommendationEngineApplication {

	public static void main(String[] args) {
		SpringApplication.run(BookRecommendationEngineApplication.class, args);
	}

    // Removed conditional DB classes to prevent manual DataSource imports and auto-config conflicts
}
