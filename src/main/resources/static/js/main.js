/**
 * Book Finder - Main JavaScript
 * Contains common functionality used across the application
 */

/**
 * Applies consistent dimensions to book cover images
 * This ensures consistent sizing regardless of the original image dimensions
 * @param {HTMLImageElement} imgElement - The image element to normalize
 * @param {number} naturalWidth - Natural width of the image
 * @param {number} naturalHeight - Natural height of the image
 */
function applyConsistentDimensions(imgElement, naturalWidth, naturalHeight) {
    // Store original dimensions for debugging
    imgElement.setAttribute('data-natural-width', naturalWidth);
    imgElement.setAttribute('data-natural-height', naturalHeight);
    
    // Calculate aspect ratio
    const aspectRatio = naturalWidth / naturalHeight;
    
    // The image will now rely on its container and its own CSS rules
    // (e.g., object-fit: contain and max-width/max-height: 100%)
    // to size itself appropriately
}

/**
 * Updates the server-side theme preference via API
 * @param {string|null} theme - The theme preference ('light', 'dark', or null for system)
 * @param {boolean} useSystem - Whether to use system preference
 */
function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
}

function updateServerThemePreference(theme, useSystem) {
    // Create request data
    const data = {
        theme: theme,
        useSystem: useSystem
    };
    
    // Send POST request to theme API endpoint
    fetch('/api/theme', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            ...(getCookie('XSRF-TOKEN') ? {'X-XSRF-TOKEN': getCookie('XSRF-TOKEN')} : {}) // Add CSRF token header if available
        },
        body: JSON.stringify(data)
    })
    .then(response => {
        if (!response.ok) {
            console.error('Theme preference update failed:', response.statusText);
        }
        return response.json();
    })
    .then(data => {
        console.log('Theme preference updated successfully:', data);
    })
    .catch(error => {
        console.error('Error updating theme preference:', error);
    });
}

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips if Bootstrap is available
    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(function (tooltipTriggerEl) {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
    }

    // Handle search form submissions
    const searchForms = document.querySelectorAll('form.search-form');
    searchForms.forEach(form => {
        form.addEventListener('submit', function(e) {
            const searchInput = this.querySelector('input[name="query"]');
            if (!searchInput || !searchInput.value.trim()) {
                e.preventDefault();
                searchInput.focus();
            }
        });
    });

    // Handle book card interactions
    const bookCards = document.querySelectorAll('.card');
    bookCards.forEach(card => {
        // Add hover effects
        card.addEventListener('mouseenter', function() {
            this.classList.add('shadow-lg');
        });
        
        card.addEventListener('mouseleave', function() {
            this.classList.remove('shadow-lg');
        });
    });

    // Track recent books viewed
    const bookLinks = document.querySelectorAll('a[href^="/book/"]');
    bookLinks.forEach(link => {
        link.addEventListener('click', function() {
            const bookId = this.getAttribute('href').split('/book/')[1];
            trackRecentBook(bookId);
        });
    });

    // Helper for adding books to localStorage for recent books
    function trackRecentBook(bookId) {
        if (!bookId) return;
        
        try {
            // Get existing recent books from localStorage
            const recentBooks = JSON.parse(localStorage.getItem('recentBooks')) || [];
            
            // Remove this book if it already exists
            const filteredBooks = recentBooks.filter(id => id !== bookId);
            
            // Add book to the beginning of the array
            filteredBooks.unshift(bookId);
            
            // Keep only the 10 most recent books
            const trimmedBooks = filteredBooks.slice(0, 10);
            
            // Save back to localStorage
            localStorage.setItem('recentBooks', JSON.stringify(trimmedBooks));
        } catch (e) {
            console.error('Error tracking recent book:', e);
        }
    }

    // Add a global function to help with debugging cover issues
    window.debugBookCovers = function() {
        const covers = document.querySelectorAll('img.book-cover');
        console.log('Found ' + covers.length + ' book covers on page');
        covers.forEach((img, index) => {
            console.log('Cover ' + (index + 1) + ':', {
                alt: img.alt || 'No alt text',
                originalSrc: img.getAttribute('data-original-src'),
                currentSrc: img.src,
                naturalWidth: img.naturalWidth,
                naturalHeight: img.naturalHeight,
                loadState: img.getAttribute('data-load-state') || 'unknown',
                retryCount: parseInt(img.getAttribute('data-retry-count') || '0'),
                usingPlaceholder: img.src.includes('/images/placeholder-book-cover.svg')
            });
        });
        return 'Logged ' + covers.length + ' book covers to console';
    };
    
    // Add a global function to retry loading all book covers
    window.retryAllBookCovers = function() {
        const covers = document.querySelectorAll('img.book-cover[data-original-src]');
        console.log('Attempting to reload ' + covers.length + ' book covers');
        covers.forEach(img => {
            const originalSrc = img.getAttribute('data-original-src');
            if (originalSrc && originalSrc !== img.src) {
                console.log('Retrying cover: ' + (img.alt || 'Unknown book'));
                img.src = originalSrc;
            }
        });
        return 'Attempted to reload ' + covers.length + ' book covers';
    };
    
    // Add a global function to test and debug theme switching
    window.testThemeDetection = function() {
        const results = {
            documentTheme: document.documentElement.getAttribute('data-theme'),
            localStorageTheme: localStorage.getItem('theme'),
            browserPrefersDark: window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches,
            systemPreferenceActive: !localStorage.getItem('theme'),
            navbarClasses: document.getElementById('main-navbar')?.className || 'navbar not found',
            iconClasses: Array.from(document.querySelectorAll('.theme-icon')).map(icon => icon.className).join(', ')
        };
        
        console.table(results);
        
        // Return a formatted string with the results
        return 'Theme Detection Status:\n' +
               '- Current theme: ' + results.documentTheme + '\n' +
               '- User preference in localStorage: ' + (results.localStorageTheme || 'Not set (using system)') + '\n' +
               '- System prefers dark mode: ' + results.browserPrefersDark + '\n' +
               '- Using system preference: ' + results.systemPreferenceActive + '\n' +
               '- Navbar classes: ' + results.navbarClasses + '\n' +
               '- Theme icons: ' + results.iconClasses;
    };

    // Dynamic text truncation for long descriptions
    document.querySelectorAll('.description-text').forEach(desc => {
        if (desc.textContent.length > 300 && !desc.classList.contains('expanded')) {
            const originalText = desc.innerHTML;
            const truncatedText = desc.textContent.substring(0, 300) + '...';
            
            desc.innerHTML = truncatedText;
            
            const readMoreLink = document.createElement('a');
            readMoreLink.href = '#';
            readMoreLink.className = 'read-more-link d-block mt-2';
            readMoreLink.textContent = 'Read More';
            
            readMoreLink.addEventListener('click', function(e) {
                e.preventDefault();
                if (desc.classList.contains('expanded')) {
                    desc.innerHTML = truncatedText;
                    this.textContent = 'Read More';
                    desc.classList.remove('expanded');
                } else {
                    desc.innerHTML = originalText;
                    this.textContent = 'Read Less';
                    desc.classList.add('expanded');
                }
                desc.appendChild(this);
            });
            
            desc.appendChild(readMoreLink);
        }
    });

    // Handle book cover image loading
    // Remove any CORS attribute on DO Spaces images to prevent NS_BINDING_ABORTED errors until CORS propagates
    // document.querySelectorAll('img.book-cover').forEach(img => {
    //     if (img.src.includes('digitaloceanspaces.com')) {
    //         img.removeAttribute('crossorigin');
    //     }
    // });
    initializeBookCovers();

    // Subscribe to real-time cover updates for all book covers
    (function() {
        var socket = new SockJS('/ws');
        var stompClient = Stomp.over(socket);
        stompClient.connect({}, function() {
            document.querySelectorAll('img.book-cover[data-book-id]').forEach(function(img) {
                var id = img.getAttribute('data-book-id');
                stompClient.subscribe('/topic/book/' + id + '/coverUpdate', function(message) {
                    var payload = JSON.parse(message.body);
                    if (payload.newCoverUrl) {
                        // Preload image to get dimensions
                        var tempImg = new Image();
                        tempImg.onload = function() {
                            // Apply normalized dimensions through the shared function
                            applyConsistentDimensions(img, tempImg.naturalWidth, tempImg.naturalHeight);
                            
                            // Set the source last, after all dimension adjustments are applied
                            img.src = payload.newCoverUrl;
                            console.log('WebSocket updated cover for book ' + id + ' with normalized dimensions (' + tempImg.naturalWidth + 'x' + tempImg.naturalHeight + ')');
                        };
                        tempImg.onerror = function() {
                            console.warn('Failed to preload WebSocket updated cover for book ' + id);
                            // Still update the src on error, but without dimension normalization
                            img.src = payload.newCoverUrl;
                        };
                        tempImg.src = payload.newCoverUrl;
                    }
                });
            });
        });
    })();

    // Theme toggler implementation
    const themeToggleBtns = document.querySelectorAll('.theme-toggle');
    const themeIcons = document.querySelectorAll('.theme-icon');
    
    // Check for OS/browser theme preference
    function getSystemThemePreference() {
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        } else {
            return 'light';
        }
    }
    
    // NEW INITIALIZATION LOGIC
    let currentTheme;
    const storedUserPreference = localStorage.getItem('theme');
    const serverSentTheme = document.documentElement.getAttribute('data-theme'); // Theme from cookie via server

    if (storedUserPreference) {
        // 1. User has an explicit preference in localStorage. This wins.
        currentTheme = storedUserPreference;
    } else if (serverSentTheme && serverSentTheme !== '_auto_') {
        // 2. No localStorage preference, but server (via cookie) provided one. Use it.
        currentTheme = serverSentTheme;
        // Sync this to localStorage so it becomes the user's preference now.
        localStorage.setItem('theme', currentTheme);
    } else {
        // 3. No localStorage, and server sent _auto_ (or nothing specific). Use system preference.
        currentTheme = getSystemThemePreference();
        // We DO NOT set localStorage here if using system preference initially,
        // to allow system theme changes to be reflected automatically if user hasn't made a choice.
    }

    document.documentElement.setAttribute('data-theme', currentTheme);
    // END NEW INITIALIZATION LOGIC
    
    // Listen for system theme changes
    if (window.matchMedia) {
        const colorSchemeQuery = window.matchMedia('(prefers-color-scheme: dark)');
        
        // Define the handler function
        const handleSystemColorSchemeChange = (e) => { // Renamed for clarity
            // Only update if user hasn't set a preference in localStorage
            if (!localStorage.getItem('theme')) {
                const newSystemTheme = e.matches ? 'dark' : 'light';
                if (currentTheme !== newSystemTheme) { // Update only if it actually changed
                    currentTheme = newSystemTheme;
                    document.documentElement.setAttribute('data-theme', currentTheme);
                    updateAndReinitializeTooltip(); // This ensures icons and navbar update
                    
                    // If we're using system preference, update server
                    updateServerThemePreference(null, true);
                }
            }
        };
        
        // Modern event listener
        try {
            colorSchemeQuery.addEventListener('change', handleSystemColorSchemeChange);
        } catch (error) {
            // Fallback for older browsers
            try {
                // Safari 13.1 and older support
                colorSchemeQuery.addListener(handleSystemColorSchemeChange);
                console.log('Using legacy matchMedia.addListener for system theme detection');
            } catch (fallbackError) {
                console.error('Browser does not support system theme change detection:', fallbackError);
            }
        }
    }

    function setThemeDisplay(themeToDisplay, isHover = false) {
        if (!themeIcons.length || !themeToggleBtns.length) return;
        
        // Update all icons
        themeIcons.forEach(icon => {
            // First, remove any existing theme-related classes
            icon.classList.remove('fa-moon', 'fa-sun', 'fa-circle-half-stroke');
            
            // For hover state, just show the opposite icon
            if (isHover) {
                icon.classList.add(themeToDisplay === 'light' ? 'fa-sun' : 'fa-moon');
                return;
            }
            
            // For non-hover state, I can indicate system theme with a special icon
            const isSystemPreference = !localStorage.getItem('theme');
            
            if (isSystemPreference) {
                // Using system preference, show half-filled circle
                icon.classList.add('fa-circle-half-stroke');
                // Add title attribute for clarity
                const closestLink = icon.closest('a');
                if (closestLink) {
                    closestLink.setAttribute('title', 'Using system preference (Click to toggle, right-click to reset)');
                }
            } else {
                // User-selected theme
                icon.classList.add(themeToDisplay === 'light' ? 'fa-sun' : 'fa-moon');
            }
        });
        
        // Update the navbar classes to ensure proper theming
        const navbar = document.getElementById('main-navbar');
        if (navbar) {
            if (themeToDisplay === 'dark') {
                navbar.classList.remove('navbar-light');
                navbar.classList.add('navbar-dark');
            } else {
                navbar.classList.remove('navbar-dark');
                navbar.classList.add('navbar-light');
            }
        }
    }

    function updateAndReinitializeTooltip() {
        // Update display on all toggles
        setThemeDisplay(currentTheme, false);
        if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
            // Recreate tooltips for all toggle buttons
            themeToggleBtns.forEach(btn => {
                if (btn.tooltipInstance) {
                    btn.tooltipInstance.dispose();
                }
                
                // Set title to include right-click info
                const isSystemDefault = !localStorage.getItem('theme');
                const themeText = currentTheme === 'light' ? 'Dark' : 'Light';
                let tooltipText = `Switch to ${themeText} Mode`;
                
                if (isSystemDefault) {
                    tooltipText += ' (Using system preference)';
                } else {
                    tooltipText += ' (Right-click to use system preference)';
                }
                
                btn.setAttribute('title', tooltipText);
                btn.tooltipInstance = new bootstrap.Tooltip(btn);
            });
        }

        // Update navbar theme classes for toggler icon color
        const navbar = document.getElementById('main-navbar');
        if (navbar) {
            if (currentTheme === 'dark') {
                navbar.classList.remove('navbar-light');
                navbar.classList.add('navbar-dark');
            } else {
                navbar.classList.remove('navbar-dark');
                navbar.classList.add('navbar-light');
            }
        }
    }

    // Attach event listeners to all toggle buttons
    if (themeToggleBtns.length) {
        updateAndReinitializeTooltip(); // Call this to set initial state of icons/tooltips
        themeToggleBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.preventDefault(); // Prevent default anchor behavior if it's a link
                // Normal click toggles between light/dark
                currentTheme = (currentTheme === 'light') ? 'dark' : 'light';
                document.documentElement.setAttribute('data-theme', currentTheme);
                localStorage.setItem('theme', currentTheme); // Explicit user choice
                updateAndReinitializeTooltip();
                
                // Send theme preference to server
                updateServerThemePreference(currentTheme, false);
            });
            // Add right-click handler to reset to system preference
            btn.addEventListener('contextmenu', (e) => {
                e.preventDefault(); // Prevent default context menu
                // Reset to system preference on right-click
                localStorage.removeItem('theme'); // User wants system preference
                currentTheme = getSystemThemePreference();
                document.documentElement.setAttribute('data-theme', currentTheme);
                updateAndReinitializeTooltip();
                
                // Send system preference to server
                updateServerThemePreference(null, true);
                
                return false; // Also helps prevent context menu in some browsers
            });
            btn.addEventListener('mouseenter', () => {
                const hoverTheme = (currentTheme === 'light') ? 'dark' : 'light';
                setThemeDisplay(hoverTheme, true);
            });
            btn.addEventListener('mouseleave', () => {
                setThemeDisplay(currentTheme, false);
            });
        });
    }
});

/**
 * Initialize book cover images with loading and placeholder functionality
 * Includes advanced error handling, retry mechanism, and preload validation
 */
function initializeBookCovers() {
    const covers = document.querySelectorAll('img.book-cover');
    if (covers.length === 0) {
        console.log("No book covers found to initialize.");
        return;
    }
    console.log('Initializing ' + covers.length + ' book covers.');

    const LOCAL_PLACEHOLDER = '/images/placeholder-book-cover.svg';
    const MAX_RETRIES = 1;
    
    covers.forEach((cover, index) => {
        if (cover.getAttribute('data-cover-initialized') === 'true') {
            return;
        }

        const preferredUrl = cover.dataset.preferredUrl;
        const fallbackUrl = cover.dataset.fallbackUrl;
        const ultimateFallback = cover.dataset.ultimateFallback || LOCAL_PLACEHOLDER;

        // Clean up old listeners if any (though data-cover-initialized should prevent this)
        cover.removeEventListener('load', handleImageSuccess);
        cover.removeEventListener('error', handleImageFailure);

        cover.onload = handleImageSuccess;
        cover.onerror = handleImageFailure;

        // Store these on the element for the handlers to access
        cover.setAttribute('data-preferred-url-internal', preferredUrl || '');
        cover.setAttribute('data-fallback-url-internal', fallbackUrl || '');
        cover.setAttribute('data-ultimate-fallback-internal', ultimateFallback);

        // Add loading indicators and placeholder div
        const parent = cover.parentNode;
        let placeholderDiv = parent.querySelector('.cover-placeholder-overlay');
        if (!placeholderDiv) {
            placeholderDiv = document.createElement('div');
            placeholderDiv.className = 'cover-placeholder-overlay';
            placeholderDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"><span class="visually-hidden">Loading...</span></div>';
            
            // Insert placeholder before the image if it's a direct child of book-cover-container/wrapper
            if (parent.classList.contains('book-cover-container') || parent.classList.contains('book-cover-wrapper')) {
                 parent.insertBefore(placeholderDiv, cover);
            } else if (parent.tagName === 'A' && (parent.parentNode.classList.contains('book-cover-container') || parent.parentNode.classList.contains('book-cover-wrapper'))){
                // if image is wrapped in <a>, insert placeholder before the <a>
                parent.parentNode.insertBefore(placeholderDiv, parent);
            }
        }
        placeholderDiv.style.display = 'flex';
        cover.classList.add('loading');
        cover.style.opacity = '0.5';


        if (preferredUrl && preferredUrl !== "null" && preferredUrl.trim() !== "") {
            console.log('[Cover ' + index + '] Attempting preferred URL: ' + preferredUrl);
            cover.src = preferredUrl;
        } else if (fallbackUrl && fallbackUrl !== "null" && fallbackUrl.trim() !== "") {
            console.log('[Cover ' + index + '] No preferred URL, attempting fallback URL: ' + fallbackUrl);
            cover.src = fallbackUrl;
        } else {
            console.log('[Cover ' + index + '] No preferred or fallback URL, using ultimate fallback: ' + ultimateFallback);
            cover.src = ultimateFallback;
            // If it's already the placeholder, the load event might not fire consistently if src doesn't change
            // Manually trigger if it's already the placeholder and not loading
            if (cover.complete && ultimateFallback.includes(LOCAL_PLACEHOLDER)) {
                 setTimeout(() => handleImageSuccess.call(cover), 0);
            }
        }
        cover.setAttribute('data-cover-initialized', 'true');
    });
}

function handleImageSuccess() {
    // 'this' is the image element
    const cover = this;
    console.log('[Cover Success] Loaded: ' + cover.src);
    
    const container = cover.closest('.book-cover-container, .book-cover-wrapper');
    const placeholderDiv = container?.querySelector('.cover-placeholder-overlay');

    if (placeholderDiv) {
        placeholderDiv.style.display = 'none';
    }
    cover.classList.remove('loading');
    cover.classList.add('loaded');
    cover.style.opacity = '1';

    if (cover.naturalWidth < 20 && cover.naturalHeight < 20 && !cover.src.includes(LOCAL_PLACEHOLDER)) {
        console.warn('[Cover Warning] Loaded image is tiny (' + cover.naturalWidth + 'x' + cover.naturalHeight + '), treating as failure for: ' + cover.src);
        handleImageFailure.call(cover); // Treat as error
    } else {
        // Apply consistent dimensions to normalize the appearance
        applyConsistentDimensions(cover, cover.naturalWidth, cover.naturalHeight);
        console.log('[Cover Success] Applied normalized dimensions to: ' + cover.src + ' (' + cover.naturalWidth + 'x' + cover.naturalHeight + ')');
        
        // Successfully loaded a real image or the intended local placeholder
        cover.onerror = null; // Prevent future errors on this now successfully loaded image (e.g. if removed from DOM then re-added by mistake)
    }
}

function handleImageFailure() {
    // 'this' is the image element
    const cover = this;
    const currentSrc = cover.src;
    console.warn('[Cover Failure] Failed to load: ' + currentSrc);

    const preferred = cover.getAttribute('data-preferred-url-internal');
    const fallback = cover.getAttribute('data-fallback-url-internal');
    const ultimate = cover.getAttribute('data-ultimate-fallback-internal');

    let nextSrc = null;

    // Check if currentSrc matches preferred (even if currentSrc has cache-busting params)
    if (currentSrc.startsWith(preferred) && preferred !== "") { 
        if (fallback && fallback !== "" && fallback !== currentSrc) {
            console.log('[Cover Retry] Preferred failed, trying fallback: ' + fallback);
            nextSrc = fallback;
        } else if (ultimate && ultimate !== "" && ultimate !== currentSrc) {
            console.log('[Cover Retry] Preferred failed, no fallback or fallback is same, trying ultimate: ' + ultimate);
            nextSrc = ultimate;
        }
    } 
    // Check if currentSrc matches fallback
    else if (currentSrc.startsWith(fallback) && fallback !== "") {
        if (ultimate && ultimate !== "" && ultimate !== currentSrc) {
            console.log('[Cover Retry] Fallback failed, trying ultimate: ' + ultimate);
            nextSrc = ultimate;
        }
    }
    // If it was some other URL (or already the ultimate fallback and it somehow errored)
    else if (ultimate && ultimate !== "" && currentSrc !== ultimate) {
        console.log('[Cover Retry] Current URL is not recognized or ultimate fallback itself failed previously, ensuring ultimate: ' + ultimate);
        nextSrc = ultimate;
    }


    if (nextSrc) {
        cover.src = nextSrc;
        if (nextSrc === ultimate) {
            // If we're falling back to the ultimate (local) placeholder,
            // it should ideally not error. If it does, stop trying.
            cover.onerror = function() {
                console.error('[Cover Final Failure] Ultimate fallback itself failed: ' + this.src);
                const container = this.closest('.book-cover-container, .book-cover-wrapper');
                const placeholderDiv = container?.querySelector('.cover-placeholder-overlay');
                if (placeholderDiv) placeholderDiv.style.display = 'none';
                this.style.opacity = '1';
                this.classList.remove('loading');
                this.classList.add('failed');
            };
        }
    } else {
        console.error('[Cover Final Failure] All fallbacks exhausted for initial src: ' + cover.getAttribute('data-preferred-url-internal'));
        const container = cover.closest('.book-cover-container, .book-cover-wrapper');
        const placeholderDiv = container?.querySelector('.cover-placeholder-overlay');
        if (placeholderDiv) placeholderDiv.style.display = 'none';
        cover.style.opacity = '1';
        cover.src = ultimate;
        cover.onerror = null;
        cover.classList.remove('loading');
        cover.classList.add('failed');
    }
}

/**
 * Reset search form
 */
function resetSearchForm() {
    document.getElementById('searchForm')?.reset();
}

// Format book data for consistent display
const BookFormatter = {
    /**
     * Format authors list into a readable string
     * @param {Array} authors - Array of author names
     * @returns {string} Formatted authors string
     */
    formatAuthors: function(authors) {
        if (!authors || authors.length === 0) {
            return 'Unknown Author';
        }
        
        if (authors.length === 1) {
            return authors[0];
        }
        
        if (authors.length === 2) {
            return `${authors[0]} and ${authors[1]}`;
        }
        
        return `${authors[0]} et al.`;
    },
    
    /**
     * Truncate text to specified length
     * @param {string} text - Text to truncate
     * @param {number} maxLength - Maximum length before truncation
     * @returns {string} Truncated text with ellipsis if needed
     */
    truncateText: function(text, maxLength) {
        if (!text) return '';
        
        if (text.length <= maxLength) {
            return text;
        }
        
        return text.substring(0, maxLength) + '...';
    },
    
    /**
     * Format the published date into a readable format
     * @param {string} dateString - Date string from API
     * @returns {string} Formatted date string
     */
    formatPublishedDate: function(dateString) {
        if (!dateString) return 'Unknown';
        
        try {
            const date = new Date(dateString);
            return date.toLocaleDateString(undefined, { 
                year: 'numeric', 
                month: 'long', 
                day: 'numeric' 
            });
        } catch (e) {
            return dateString;
        }
    }
};

// Make initializeBookCovers globally accessible if search.js needs to call it
window.initializeBookCovers = initializeBookCovers;

/**
 * Affiliate link click tracking
 * Tracks outbound clicks on affiliate links for analytics purposes
 */
(function() {
    // Set up delegated event listener for affiliate link clicks
    document.addEventListener('click', function(e) {
        const link = e.target.closest('a.clicky_log_outbound');
        if (!link) return;

        // Log the click for analytics
        const href = link.href;
        const bookId = link.getAttribute('data-book-id') || 'unknown';
        const affiliate = link.getAttribute('data-affiliate') || 'unknown';

        // Send analytics event (customize based on your analytics provider)
        if (typeof gtag !== 'undefined') {
            // Google Analytics
            gtag('event', 'click', {
                'event_category': 'affiliate',
                'event_label': affiliate,
                'value': bookId,
                'transport_type': 'beacon'
            });
        }

        // Console log for debugging
        console.log('Affiliate link clicked:', {
            affiliate: affiliate,
            bookId: bookId,
            url: href
        });
    }, { capture: true });
})();
