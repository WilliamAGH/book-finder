/**
 * Book Recommendation Engine - Main JavaScript
 * Contains common functionality used across the application
 */

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
        console.log(`Found ${covers.length} book covers on page`);
        covers.forEach((img, index) => {
            console.log(`Cover ${index + 1}:`, {
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
        return `Logged ${covers.length} book covers to console`;
    };
    
    // Add a global function to retry loading all book covers
    window.retryAllBookCovers = function() {
        const covers = document.querySelectorAll('img.book-cover[data-original-src]');
        console.log(`Attempting to reload ${covers.length} book covers`);
        covers.forEach(img => {
            const originalSrc = img.getAttribute('data-original-src');
            if (originalSrc && originalSrc !== img.src) {
                console.log(`Retrying cover: ${img.alt || 'Unknown book'}`);
                img.src = originalSrc;
            }
        });
        return `Attempted to reload ${covers.length} book covers`;
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
                        img.src = payload.newCoverUrl;
                    }
                });
            });
        });
    })();

    // Theme toggler implementation
    const themeToggleBtns = document.querySelectorAll('.theme-toggle');
    const themeIcons = document.querySelectorAll('.theme-icon');
    let currentTheme = localStorage.getItem('theme') || 'light';
    document.documentElement.setAttribute('data-theme', currentTheme);

    function setThemeDisplay(themeToDisplay, isHover = false) {
        if (!themeIcons.length || !themeToggleBtns.length) return;
        // Update all icons
        themeIcons.forEach(icon => {
            if (themeToDisplay === 'light') {
                icon.classList.remove('fa-moon');
                icon.classList.add('fa-sun');
            } else {
                icon.classList.remove('fa-sun');
                icon.classList.add('fa-moon');
            }
        });
        // Update titles on toggle buttons when not hovering
        if (!isHover) {
            themeToggleBtns.forEach(btn => {
                btn.setAttribute('title', themeToDisplay === 'light' ? 'Switch to Dark Mode' : 'Switch to Light Mode');
            });
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
        updateAndReinitializeTooltip();
        themeToggleBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                currentTheme = (currentTheme === 'light') ? 'dark' : 'light';
                document.documentElement.setAttribute('data-theme', currentTheme);
                localStorage.setItem('theme', currentTheme);
                updateAndReinitializeTooltip();
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
    console.log(`Initializing ${covers.length} book covers.`);

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
            console.log(`[Cover ${index}] Attempting preferred URL: ${preferredUrl}`);
            cover.src = preferredUrl;
        } else if (fallbackUrl && fallbackUrl !== "null" && fallbackUrl.trim() !== "") {
            console.log(`[Cover ${index}] No preferred URL, attempting fallback URL: ${fallbackUrl}`);
            cover.src = fallbackUrl;
        } else {
            console.log(`[Cover ${index}] No preferred or fallback URL, using ultimate fallback: ${ultimateFallback}`);
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
    console.log(`[Cover Success] Loaded: ${cover.src}`);
    
    const placeholderDiv = (cover.parentNode.classList.contains('book-cover-container') || cover.parentNode.classList.contains('book-cover-wrapper')) 
        ? cover.parentNode.querySelector('.cover-placeholder-overlay')
        : cover.parentNode.parentNode.querySelector('.cover-placeholder-overlay');

    if (placeholderDiv) {
        placeholderDiv.style.display = 'none';
    }
    cover.classList.remove('loading');
    cover.classList.add('loaded');
    cover.style.opacity = '1';

    if (cover.naturalWidth < 20 && cover.naturalHeight < 20 && !cover.src.includes(LOCAL_PLACEHOLDER)) {
        console.warn(`[Cover Warning] Loaded image is tiny (${cover.naturalWidth}x${cover.naturalHeight}), treating as failure for: ${cover.src}`);
        handleImageFailure.call(cover); // Treat as error
    } else {
        // Successfully loaded a real image or the intended local placeholder
        cover.onerror = null; // Prevent future errors on this now successfully loaded image (e.g. if removed from DOM then re-added by mistake)
    }
}

function handleImageFailure() {
    // 'this' is the image element
    const cover = this;
    const currentSrc = cover.src;
    console.warn(`[Cover Failure] Failed to load: ${currentSrc}`);

    const preferred = cover.getAttribute('data-preferred-url-internal');
    const fallback = cover.getAttribute('data-fallback-url-internal');
    const ultimate = cover.getAttribute('data-ultimate-fallback-internal');

    let nextSrc = null;

    // Check if currentSrc matches preferred (even if currentSrc has cache-busting params)
    if (currentSrc.startsWith(preferred) && preferred !== "") { 
        if (fallback && fallback !== "" && fallback !== currentSrc) {
            console.log(`[Cover Retry] Preferred failed, trying fallback: ${fallback}`);
            nextSrc = fallback;
        } else if (ultimate && ultimate !== "" && ultimate !== currentSrc) {
            console.log(`[Cover Retry] Preferred failed, no fallback or fallback is same, trying ultimate: ${ultimate}`);
            nextSrc = ultimate;
        }
    } 
    // Check if currentSrc matches fallback
    else if (currentSrc.startsWith(fallback) && fallback !== "") {
        if (ultimate && ultimate !== "" && ultimate !== currentSrc) {
            console.log(`[Cover Retry] Fallback failed, trying ultimate: ${ultimate}`);
            nextSrc = ultimate;
        }
    }
    // If it was some other URL (or already the ultimate fallback and it somehow errored)
    else if (ultimate && ultimate !== "" && currentSrc !== ultimate) {
        console.log(`[Cover Retry] Current URL is not recognized or ultimate fallback itself failed previously, ensuring ultimate: ${ultimate}`);
        nextSrc = ultimate;
    }


    if (nextSrc) {
        cover.src = nextSrc;
        if (nextSrc === ultimate) {
            // If we're falling back to the ultimate (local) placeholder,
            // it should ideally not error. If it does, stop trying.
            cover.onerror = function() {
                console.error(`[Cover Final Failure] Ultimate fallback itself failed: ${this.src}`);
                const placeholderDiv = (this.parentNode.classList.contains('book-cover-container') || this.parentNode.classList.contains('book-cover-wrapper')) 
                    ? this.parentNode.querySelector('.cover-placeholder-overlay')
                    : this.parentNode.parentNode.querySelector('.cover-placeholder-overlay');
                if (placeholderDiv) placeholderDiv.style.display = 'none';
                this.style.opacity = '1';
                this.classList.remove('loading');
                this.classList.add('failed');
            };
        }
    } else {
        console.error(`[Cover Final Failure] All fallbacks exhausted for initial src: ${cover.getAttribute('data-preferred-url-internal')}`);
        const placeholderDiv = (cover.parentNode.classList.contains('book-cover-container') || cover.parentNode.classList.contains('book-cover-wrapper')) 
            ? cover.parentNode.querySelector('.cover-placeholder-overlay')
            : cover.parentNode.parentNode.querySelector('.cover-placeholder-overlay');
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