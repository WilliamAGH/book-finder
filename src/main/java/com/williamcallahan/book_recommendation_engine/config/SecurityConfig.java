package com.williamcallahan.book_recommendation_engine.config;

/**
 * Configuration class for Spring Security settings in the Book Recommendation Engine
 *
 * @author William Callahan
 *
 * Key Features:
 * - Enables Web Security and Method Security for @PreAuthorize annotations
 * - Configures role-based access control for different URL patterns
 * - Sets up HTTP Basic Authentication and Form Login
 * - Uses custom AuthenticationEntryPoint for admin paths
 * - Defines in-memory user details for admin and user roles
 * - Implements Content Security Policy and Referrer-Policy headers
 * - Manages CSRF protection
 */

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;

import static org.springframework.security.config.Customizer.withDefaults;

import java.util.Arrays;
import java.util.stream.Collectors;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
public class SecurityConfig {

    private final AuthenticationEntryPoint customBasicAuthenticationEntryPoint;

    @Value("${app.security.headers.content-security-policy.enabled:true}")
    private boolean cspEnabled;

    @Value("${app.security.headers.referrer-policy:ORIGIN_WHEN_CROSS_ORIGIN}")
    private String referrerPolicy;

    @Value("${app.clicky.enabled:true}")
    private boolean clickyEnabled;

    @Value("${app.clicky.site-id:101484793}")
    private String clickySiteId;
    
    @Value("${app.book.covers.cdn-domain:https://book-finder.sfo3.digitaloceanspaces.com}")
    private String bookCoversCdnDomain;
    
    @Value("${app.book.covers.additional-domains:}")
    private String bookCoversAdditionalDomains;

    public SecurityConfig(CustomBasicAuthenticationEntryPoint customBasicAuthenticationEntryPoint) {
        this.customBasicAuthenticationEntryPoint = customBasicAuthenticationEntryPoint;
    }

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            // Specify a security matcher pattern to avoid conflicts with test configurations
            .securityMatcher("/**")
            .authorizeHttpRequests(authorizeRequests ->
                authorizeRequests
                    .requestMatchers("/admin/**").hasRole("ADMIN")
                    .requestMatchers("/robots.txt").permitAll() // Explicitly permit robots.txt
                    .anyRequest().permitAll() // Default to permit all for non-admin routes
            )
            .formLogin(withDefaults()) // Enable form-based login
            .httpBasic(httpBasic -> httpBasic
                .authenticationEntryPoint(customBasicAuthenticationEntryPoint) // Use custom entry point for admin paths
            )
            .csrf(withDefaults()); // Enable CSRF protection with defaults

        // Configure headers if CSP is enabled
        if (cspEnabled) {
            configureSecurity(http);
        }
            
        return http.build();
    }

    private void configureSecurity(HttpSecurity http) throws Exception {
        http.headers(headers -> {
            // Set Referrer-Policy based on configuration
            ReferrerPolicyHeaderWriter.ReferrerPolicy policy = ReferrerPolicyHeaderWriter.ReferrerPolicy.valueOf(referrerPolicy);
            headers.referrerPolicy(referrer -> referrer.policy(policy));

            if (cspEnabled) { // Check if CSP is enabled first
                StringBuilder imgSrcDirective = new StringBuilder("'self' data: ");
                StringBuilder scriptSrcDirective = new StringBuilder("'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com 'unsafe-inline'");
                StringBuilder connectSrcDirective = new StringBuilder("'self'");

                if (clickyEnabled) {
                    // Add Clicky Analytics domains for img-src, script-src, and connect-src
                    imgSrcDirective.append("https://static.getclicky.com https://in.getclicky.com https://clicky.com ");
                    scriptSrcDirective.append(" https://static.getclicky.com https://clicky.com");
                    connectSrcDirective.append(" https://static.getclicky.com https://in.getclicky.com https://clicky.com");
                }

                // Add book covers CDN domain
                if (bookCoversCdnDomain != null && !bookCoversCdnDomain.isEmpty()) {
                    imgSrcDirective.append(bookCoversCdnDomain).append(" ");
                }

                // Add additional domains if specified
                if (bookCoversAdditionalDomains != null && !bookCoversAdditionalDomains.isEmpty()) {
                    String formattedDomains = Arrays.stream(bookCoversAdditionalDomains.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.joining(" "));
                    if (!formattedDomains.isEmpty()) {
                        imgSrcDirective.append(formattedDomains);
                    }
                }

                // Add Content Security Policy header with dynamic directives
                headers.addHeaderWriter(new StaticHeadersWriter("Content-Security-Policy",
                    "default-src 'self'; " +
                    "script-src " + scriptSrcDirective.toString() + "; " +
                    "style-src 'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://fonts.googleapis.com 'unsafe-inline'; " +
                    "img-src " + imgSrcDirective.toString().trim() + "; " + // trim to remove trailing space if no additional domains
                    "font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; " +
                    "connect-src " + connectSrcDirective.toString() + "; " +
                    "frame-src 'self'; " +
                    "object-src 'none'"
                ));
            }
        });
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
        UserDetails admin = User.builder()
            .username("admin")
            .password(passwordEncoder.encode("${app.security.admin.password}"))
            .roles("ADMIN", "USER")
            .build();
        UserDetails regularUser = User.builder()
            .username("user")
            .password(passwordEncoder.encode("${app.security.user.password}"))
            .roles("USER")
            .build();
        return new InMemoryUserDetailsManager(admin, regularUser);
    }
}
