/**
 * Configuration class for Spring Security settings in the Book Recommendation Engine
 *
 * @author William Callahan
 *
 * Features:
 * - Enables Web Security and Method Security for @PreAuthorize annotations
 * - Configures role-based access control for different URL patterns
 * - Sets up HTTP Basic Authentication and Form Login
 * - Uses custom AuthenticationEntryPoint for admin paths
 * - Defines in-memory user details for admin and user roles
 * - Implements Content Security Policy and Referrer-Policy headers
 * - Manages CSRF protection
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
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
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.util.StringUtils;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    private final AuthenticationEntryPoint customBasicAuthenticationEntryPoint;
    private final Environment environment;

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

    public SecurityConfig(CustomBasicAuthenticationEntryPoint customBasicAuthenticationEntryPoint,
                          Environment environment) {
        this.customBasicAuthenticationEntryPoint = customBasicAuthenticationEntryPoint;
        this.environment = environment;
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
            .csrf(csrf -> csrf
                .ignoringRequestMatchers(
                    new AntPathRequestMatcher("/admin/s3-cleanup/dry-run", "GET"),
                    new AntPathRequestMatcher("/admin/api-metrics/**", "GET")
                )
            );

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
                // Allow HTTP and HTTPS images from any source - book covers come from many external sources
                // (Google Books, Open Library, Goodreads, Amazon, etc.)
                // Note: HTTP allowed because some providers (e.g., Google Books API) return HTTP URLs
                StringBuilder imgSrcDirective = new StringBuilder("'self' data: blob: https: http: ");
                StringBuilder scriptSrcDirective = new StringBuilder("'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com 'unsafe-inline' blob:");
                StringBuilder connectSrcDirective = new StringBuilder("'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com");

                if (clickyEnabled) {
                    // Add Clicky Analytics domains for script-src, connect-src, and img-src
                    // Include both HTTP and HTTPS as Clicky may use either depending on page protocol
                    scriptSrcDirective.append(" https://static.getclicky.com http://static.getclicky.com https://in.getclicky.com http://in.getclicky.com https://clicky.com http://clicky.com");
                    connectSrcDirective.append(" https://static.getclicky.com http://static.getclicky.com https://in.getclicky.com http://in.getclicky.com https://clicky.com http://clicky.com");
                    imgSrcDirective.append(" https://in.getclicky.com http://in.getclicky.com");
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
        String adminUsername = resolveUsername("app.security.admin.username", "admin");
        String userUsername = resolveUsername("app.security.user.username", "user");
        String adminPassword = resolvePassword("app.security.admin.password");
        String userPassword = resolvePassword("app.security.user.password");

        InMemoryUserDetailsManager userDetailsManager = new InMemoryUserDetailsManager();
        boolean adminRegistered = false;
        boolean userRegistered = false;

        if (adminPassword != null) {
            UserDetails admin = User.builder()
                .username(adminUsername)
                .password(passwordEncoder.encode(adminPassword))
                .roles("ADMIN", "USER")
                .build();
            userDetailsManager.createUser(admin);
            adminRegistered = true;
        } else {
            log.error("Admin endpoints disabled: missing app.security.admin.password. Set the secret to re-enable /admin/**.");
        }

        if (userPassword != null) {
            UserDetails regularUser = User.builder()
                .username(userUsername)
                .password(passwordEncoder.encode(userPassword))
                .roles("USER")
                .build();
            userDetailsManager.createUser(regularUser);
            userRegistered = true;
        } else {
            log.warn("Basic auth user account disabled: missing app.security.user.password. Non-admin authenticated endpoints will reject credentials until configured.");
        }

        if (!adminRegistered && !userRegistered) {
            log.error("No basic-auth credentials configured. Protected endpoints will respond with 401 until credentials are provided via environment variables.");
        }

        return userDetailsManager;
    }

    private String resolvePassword(String propertyKey) {
        String value = environment.getProperty(propertyKey);
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return value.trim();
    }

    private String resolveUsername(String propertyKey, String defaultValue) {
        String value = environment.getProperty(propertyKey);
        if (!StringUtils.hasText(value)) {
            return defaultValue;
        }
        return value.trim();
    }
}
