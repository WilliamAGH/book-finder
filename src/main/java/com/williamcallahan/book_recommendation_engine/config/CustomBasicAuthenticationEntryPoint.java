/**
 * Custom implementation of Spring Security's BasicAuthenticationEntryPoint
 * This class is responsible for commencing the HTTP Basic authentication scheme
 * When an unauthenticated user attempts to access a protected resource requiring Basic Auth,
 * this entry point is invoked to send a 401 Unauthorized response, prompting the client
 * for credentials
 *
 * Key Features:
 * - Returns a custom JSON error message on authentication failure for admin endpoints
 * - Specifies the WWW-Authenticate header with a custom realm name
 * - Provides clear instructions to the user on how to authenticate, including the expected header format
 *   and the environment variable for the admin password
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Customizes the response for unauthorized access to endpoints protected by HTTP Basic authentication
 * Instead of a generic 401 page, it returns a JSON response with guidance on how to authenticate,
 * specifically tailored for the admin API, including the expected username and the environment
 * variable for the password
 */
@Component
public class CustomBasicAuthenticationEntryPoint extends BasicAuthenticationEntryPoint {

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authEx) throws IOException {
        response.addHeader("WWW-Authenticate", "Basic realm=\"" + getRealmName() + "\"");
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.println("{\n" +
                "  \"error\": \"Unauthorized\",\n" +
                "  \"message\": \"HTTP Basic Authentication required for admin endpoints.\",\n" +
                "  \"details\": {\n" +
                "    \"expectedHeader\": \"Authorization: Basic <base64_encoded_username:password>\",\n" +
                "    \"username\": \"admin\",\n" +
                "    \"passwordEnvironmentVariable\": \"APP_SECURITY_ADMIN_PASSWORD (or app.security.admin.password property)\"\n" +
                "  }\n" +
                "}");
    }

    @Override
    public void afterPropertiesSet() {
        setRealmName("BookRecommendationAdmin");
        super.afterPropertiesSet();
    }
}
