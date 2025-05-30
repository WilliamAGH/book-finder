/**
 * Configuration for WebClient
 * - Defines beans for creating WebClient instances
 * - Sets up default timeouts and connection settings
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configures the application's WebClient instances
 * - Provides a pre-configured WebClient Builder
 * - Ensures consistent HTTP client behavior
 */
@Configuration
public class WebClientConfig {

    /**
     * Creates a pre-configured WebClient Builder bean
     * - Sets connection timeout to 5000ms
     * - Sets read and write timeouts to 5 seconds
     * - Sets response timeout to 5 seconds
     *
     * @return A WebClient Builder instance
     */
    @Bean
    public WebClient.Builder webClientBuilder() {
        HttpClient httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .doOnConnected(conn -> conn
                .addHandlerLast(new ReadTimeoutHandler(5, TimeUnit.SECONDS))
                .addHandlerLast(new WriteTimeoutHandler(5, TimeUnit.SECONDS))
            )
            .responseTimeout(Duration.ofSeconds(5));

        ExchangeStrategies exchangeStrategies = ExchangeStrategies.builder()
            .codecs(configurer -> configurer
                .defaultCodecs()
                .maxInMemorySize(10 * 1024 * 1024)) // 10MB
            .build();

        return WebClient.builder()
            .exchangeStrategies(exchangeStrategies)
            .clientConnector(new ReactorClientHttpConnector(httpClient));
    }
}
