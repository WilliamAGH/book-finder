/**
 * Main application configuration properties
 * Centralizes all app.* configuration properties for better type safety and IDE support
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app")
public class AppConfigurationProperties {

    @NestedConfigurationProperty
    private Security security = new Security();
    
    @NestedConfigurationProperty
    private Cache cache = new Cache();
    
    @NestedConfigurationProperty
    private CoverCache coverCache = new CoverCache();
    
    @NestedConfigurationProperty
    private Book book = new Book();
    
    @NestedConfigurationProperty
    private Clicky clicky = new Clicky();
    
    @NestedConfigurationProperty
    private Embedding embedding = new Embedding();
    
    @NestedConfigurationProperty
    private Retry retry = new Retry();
    
    @NestedConfigurationProperty
    private Timeout timeout = new Timeout();
    
    @NestedConfigurationProperty
    private Environment environment = new Environment();

    // Getters and setters
    public Security getSecurity() { return security; }
    public void setSecurity(Security security) { this.security = security; }
    
    public Cache getCache() { return cache; }
    public void setCache(Cache cache) { this.cache = cache; }
    
    public CoverCache getCoverCache() { return coverCache; }
    public void setCoverCache(CoverCache coverCache) { this.coverCache = coverCache; }
    
    public Book getBook() { return book; }
    public void setBook(Book book) { this.book = book; }
    
    public Clicky getClicky() { return clicky; }
    public void setClicky(Clicky clicky) { this.clicky = clicky; }
    
    public Embedding getEmbedding() { return embedding; }
    public void setEmbedding(Embedding embedding) { this.embedding = embedding; }
    
    public Retry getRetry() { return retry; }
    public void setRetry(Retry retry) { this.retry = retry; }
    
    public Timeout getTimeout() { return timeout; }
    public void setTimeout(Timeout timeout) { this.timeout = timeout; }
    
    public Environment getEnvironment() { return environment; }
    public void setEnvironment(Environment environment) { this.environment = environment; }

    // Nested configuration classes
    public static class Security {
        @NestedConfigurationProperty
        private Admin admin = new Admin();
        
        @NestedConfigurationProperty
        private User user = new User();
        
        @NestedConfigurationProperty
        private Headers headers = new Headers();
        
        public Admin getAdmin() { return admin; }
        public void setAdmin(Admin admin) { this.admin = admin; }
        
        public User getUser() { return user; }
        public void setUser(User user) { this.user = user; }
        
        public Headers getHeaders() { return headers; }
        public void setHeaders(Headers headers) { this.headers = headers; }
        
        public static class Admin {
            private String password;
            public String getPassword() { return password; }
            public void setPassword(String password) { this.password = password; }
        }
        
        public static class User {
            private String password;
            public String getPassword() { return password; }
            public void setPassword(String password) { this.password = password; }
        }
        
        public static class Headers {
            @NestedConfigurationProperty
            private ContentSecurityPolicy contentSecurityPolicy = new ContentSecurityPolicy();
            
            public ContentSecurityPolicy getContentSecurityPolicy() { return contentSecurityPolicy; }
            public void setContentSecurityPolicy(ContentSecurityPolicy contentSecurityPolicy) { this.contentSecurityPolicy = contentSecurityPolicy; }
            
            public static class ContentSecurityPolicy {
                private boolean enabled = true;
                public boolean isEnabled() { return enabled; }
                public void setEnabled(boolean enabled) { this.enabled = enabled; }
            }
        }
    }
    
    public static class Cache {
        private boolean enabled = true;
        
        @NestedConfigurationProperty
        private BookCache book = new BookCache();
        
        @NestedConfigurationProperty
        private Warming warming = new Warming();
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        
        public BookCache getBook() { return book; }
        public void setBook(BookCache book) { this.book = book; }
        
        public Warming getWarming() { return warming; }
        public void setWarming(Warming warming) { this.warming = warming; }
        
        public static class BookCache {
            private String ttl = "24h";
            public String getTtl() { return ttl; }
            public void setTtl(String ttl) { this.ttl = ttl; }
        }
        
        public static class Warming {
            private boolean enabled = false;
            private String cron = "0 0 3 * * ?";
            private int rateLimitPerMinute = 3;
            private int maxBooksPerRun = 10;
            private int recentlyViewedDays = 7;
            
            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            
            public String getCron() { return cron; }
            public void setCron(String cron) { this.cron = cron; }
            
            public int getRateLimitPerMinute() { return rateLimitPerMinute; }
            public void setRateLimitPerMinute(int rateLimitPerMinute) { this.rateLimitPerMinute = rateLimitPerMinute; }
            
            public int getMaxBooksPerRun() { return maxBooksPerRun; }
            public void setMaxBooksPerRun(int maxBooksPerRun) { this.maxBooksPerRun = maxBooksPerRun; }
            
            public int getRecentlyViewedDays() { return recentlyViewedDays; }
            public void setRecentlyViewedDays(int recentlyViewedDays) { this.recentlyViewedDays = recentlyViewedDays; }
        }
    }
    
    public static class CoverCache {
        private String dir = "book-covers";
        public String getDir() { return dir; }
        public void setDir(String dir) { this.dir = dir; }
    }
    
    public static class Book {
        @NestedConfigurationProperty
        private Covers covers = new Covers();
        
        public Covers getCovers() { return covers; }
        public void setCovers(Covers covers) { this.covers = covers; }
        
        public static class Covers {
            private String cdnDomain;
            private String additionalDomains;
            
            public String getCdnDomain() { return cdnDomain; }
            public void setCdnDomain(String cdnDomain) { this.cdnDomain = cdnDomain; }
            
            public String getAdditionalDomains() { return additionalDomains; }
            public void setAdditionalDomains(String additionalDomains) { this.additionalDomains = additionalDomains; }
        }
    }
    
    public static class Clicky {
        private boolean enabled = true;
        private String siteId;
        private int cacheDurationMinutes = 60;
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        
        public String getSiteId() { return siteId; }
        public void setSiteId(String siteId) { this.siteId = siteId; }
        
        public int getCacheDurationMinutes() { return cacheDurationMinutes; }
        public void setCacheDurationMinutes(int cacheDurationMinutes) { this.cacheDurationMinutes = cacheDurationMinutes; }
    }
    
    public static class Embedding {
        @NestedConfigurationProperty
        private Service service = new Service();
        private String provider = "local";
        
        public Service getService() { return service; }
        public void setService(Service service) { this.service = service; }
        
        public String getProvider() { return provider; }
        public void setProvider(String provider) { this.provider = provider; }
        
        public static class Service {
            private boolean enabled = false;
            private String url = "http://localhost:11434";
            
            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            
            public String getUrl() { return url; }
            public void setUrl(String url) { this.url = url; }
        }
    }
    
    public static class Retry {
        @NestedConfigurationProperty
        private RetryDefault defaultProperty = new RetryDefault();
        
        @NestedConfigurationProperty
        private RetryS3 s3 = new RetryS3();
        
        @NestedConfigurationProperty
        private RetryRedis redis = new RetryRedis();
        
        @NestedConfigurationProperty
        private RetryGoogleApi googleApi = new RetryGoogleApi();
        
        public RetryDefault getDefault() { return defaultProperty; }
        public void setDefault(RetryDefault defaultProperty) { this.defaultProperty = defaultProperty; }
        
        public RetryS3 getS3() { return s3; }
        public void setS3(RetryS3 s3) { this.s3 = s3; }
        
        public RetryRedis getRedis() { return redis; }
        public void setRedis(RetryRedis redis) { this.redis = redis; }
        
        public RetryGoogleApi getGoogleApi() { return googleApi; }
        public void setGoogleApi(RetryGoogleApi googleApi) { this.googleApi = googleApi; }
        
        public static class RetryDefault {
            private int maxAttempts = 3;
            private long initialBackoffMs = 1000;
            private long maxBackoffMs = 30000;
            private double backoffMultiplier = 2.0;
            private double jitterFactor = 0.5;
            
            // Getters and setters
            public int getMaxAttempts() { return maxAttempts; }
            public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
            
            public long getInitialBackoffMs() { return initialBackoffMs; }
            public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
            
            public long getMaxBackoffMs() { return maxBackoffMs; }
            public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
            
            public double getBackoffMultiplier() { return backoffMultiplier; }
            public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
            
            public double getJitterFactor() { return jitterFactor; }
            public void setJitterFactor(double jitterFactor) { this.jitterFactor = jitterFactor; }
        }
        
        public static class RetryS3 {
            private int maxAttempts = 3;
            private long initialBackoffMs = 200;
            private double backoffMultiplier = 2.0;
            
            public int getMaxAttempts() { return maxAttempts; }
            public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
            
            public long getInitialBackoffMs() { return initialBackoffMs; }
            public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
            
            public double getBackoffMultiplier() { return backoffMultiplier; }
            public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        }
        
        public static class RetryRedis {
            private int maxAttempts = 2;
            private long initialBackoffMs = 500;
            private double backoffMultiplier = 1.5;
            
            public int getMaxAttempts() { return maxAttempts; }
            public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
            
            public long getInitialBackoffMs() { return initialBackoffMs; }
            public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
            
            public double getBackoffMultiplier() { return backoffMultiplier; }
            public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        }
        
        public static class RetryGoogleApi {
            private int maxAttempts = 3;
            private long initialBackoffMs = 2000;
            private double backoffMultiplier = 2.0;
            
            public int getMaxAttempts() { return maxAttempts; }
            public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
            
            public long getInitialBackoffMs() { return initialBackoffMs; }
            public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
            
            public double getBackoffMultiplier() { return backoffMultiplier; }
            public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
        }
    }
    
    public static class Timeout {
        @NestedConfigurationProperty
        private TimeoutDefault defaultProperty = new TimeoutDefault();
        
        @NestedConfigurationProperty
        private TimeoutRedis redis = new TimeoutRedis();
        
        @NestedConfigurationProperty
        private TimeoutS3 s3 = new TimeoutS3();
        
        @NestedConfigurationProperty
        private TimeoutGoogleApi googleApi = new TimeoutGoogleApi();
        
        @NestedConfigurationProperty
        private TimeoutWebclient webclient = new TimeoutWebclient();
        
        public TimeoutDefault getDefault() { return defaultProperty; }
        public void setDefault(TimeoutDefault defaultProperty) { this.defaultProperty = defaultProperty; }
        
        public TimeoutRedis getRedis() { return redis; }
        public void setRedis(TimeoutRedis redis) { this.redis = redis; }
        
        public TimeoutS3 getS3() { return s3; }
        public void setS3(TimeoutS3 s3) { this.s3 = s3; }
        
        public TimeoutGoogleApi getGoogleApi() { return googleApi; }
        public void setGoogleApi(TimeoutGoogleApi googleApi) { this.googleApi = googleApi; }
        
        public TimeoutWebclient getWebclient() { return webclient; }
        public void setWebclient(TimeoutWebclient webclient) { this.webclient = webclient; }
        
        public static class TimeoutDefault {
            private long connectionMs = 5000;
            private long readMs = 10000;
            private long writeMs = 10000;
            
            public long getConnectionMs() { return connectionMs; }
            public void setConnectionMs(long connectionMs) { this.connectionMs = connectionMs; }
            
            public long getReadMs() { return readMs; }
            public void setReadMs(long readMs) { this.readMs = readMs; }
            
            public long getWriteMs() { return writeMs; }
            public void setWriteMs(long writeMs) { this.writeMs = writeMs; }
        }
        
        public static class TimeoutRedis {
            private long pingMs = 1000;
            private long operationMs = 5000;
            
            public long getPingMs() { return pingMs; }
            public void setPingMs(long pingMs) { this.pingMs = pingMs; }
            
            public long getOperationMs() { return operationMs; }
            public void setOperationMs(long operationMs) { this.operationMs = operationMs; }
        }
        
        public static class TimeoutS3 {
            private long operationMs = 30000;
            
            public long getOperationMs() { return operationMs; }
            public void setOperationMs(long operationMs) { this.operationMs = operationMs; }
        }
        
        public static class TimeoutGoogleApi {
            private long operationMs = 60000;
            
            public long getOperationMs() { return operationMs; }
            public void setOperationMs(long operationMs) { this.operationMs = operationMs; }
        }
        
        public static class TimeoutWebclient {
            private long defaultMs = 30000;
            
            public long getDefaultMs() { return defaultMs; }
            public void setDefaultMs(long defaultMs) { this.defaultMs = defaultMs; }
        }
    }
    
    public static class Environment {
        private String mode;
        
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
    }
} 