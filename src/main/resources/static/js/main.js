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
    document.querySelectorAll('img.book-cover').forEach(img => {
        if (img.src.includes('digitaloceanspaces.com')) {
            img.removeAttribute('crossorigin');
        }
    });
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
    // Get all book cover images
    const bookCovers = document.querySelectorAll('.book-cover');
    const LOCAL_PLACEHOLDER = '/images/placeholder-book-cover.svg';
    const MAX_RETRIES = 1; // Maximum number of retry attempts
    
    bookCovers.forEach(cover => {
        // Set a data attribute to track if this is the original source
        if (!cover.hasAttribute('data-original-src')) {
            cover.setAttribute('data-original-src', cover.src);
        }
        
        // Initialize retry count
        cover.setAttribute('data-retry-count', '0');
        cover.setAttribute('data-load-state', 'initializing');
        
        // Add loading class
        cover.classList.add('loading');
        
        // Create placeholder container if not already present
        const container = cover.closest('.book-cover-container');
        if (container && !container.querySelector('.book-cover-placeholder')) {
            const placeholder = document.createElement('div');
            placeholder.className = 'book-cover-placeholder';
            placeholder.textContent = 'Loading...';
            container.appendChild(placeholder);
        }
        
        // Function to show placeholder with custom message
        const showPlaceholder = (message = 'Cover not available') => {
            const placeholder = cover.closest('.book-cover-container')?.querySelector('.book-cover-placeholder');
            if (placeholder) {
                placeholder.textContent = message;
                placeholder.style.display = 'flex';
            }
            // Make sure the image is hidden
            cover.style.opacity = '0';
        };
        
        // Function to hide placeholder and show image
        const showImage = () => {
            cover.style.opacity = '1';
            const placeholder = cover.closest('.book-cover-container')?.querySelector('.book-cover-placeholder');
            if (placeholder) {
                placeholder.style.display = 'none';
            }
        };
        
        // Function to handle image load failure
        const handleImageFailure = () => {
            const retryCount = parseInt(cover.getAttribute('data-retry-count') || '0');
            const originalSrc = cover.getAttribute('data-original-src');
            
            cover.setAttribute('data-load-state', 'failed');
            
            // Log the failure for debugging
            console.log(`Image failed to load: ${cover.alt || 'Unknown book'}`, {
                src: cover.src,
                originalSrc: originalSrc,
                retryCount: retryCount
            });
            
            if (retryCount < MAX_RETRIES && originalSrc && !originalSrc.includes(LOCAL_PLACEHOLDER)) {
                // Increment retry count
                cover.setAttribute('data-retry-count', (retryCount + 1).toString());
                cover.setAttribute('data-load-state', 'retrying');
                
                // Show placeholder during retry
                showPlaceholder('Retrying...');
                
                // Try again with the original source after a short delay
                setTimeout(() => {
                    console.log(`Retrying image (${retryCount + 1}/${MAX_RETRIES}): ${cover.alt || 'Unknown book'}`);
                    cover.src = originalSrc;
                }, 1000);
            } else {
                // We've exhausted retries or this is already the placeholder
                cover.setAttribute('data-load-state', 'using-placeholder');
                
                // Only switch to placeholder if not already using it
                if (!cover.src.includes(LOCAL_PLACEHOLDER)) {
                    console.log(`Using placeholder for: ${cover.alt || 'Unknown book'}`);
                    cover.src = LOCAL_PLACEHOLDER;
                    cover.onerror = null; // Prevent infinite loop
                    showPlaceholder();
                } else {
                    // Even the placeholder failed, completely hide the image
                    cover.style.display = 'none';
                    showPlaceholder('Cover unavailable');
                }
            }
        };
        
        // Handle successful load
        cover.addEventListener('load', function() {
            // Check if the image has valid dimensions (to detect 1x1 placeholders)
            if (this.naturalWidth < 20 || this.naturalHeight < 20) {
                console.log(`Detected tiny image (likely placeholder): ${this.naturalWidth}x${this.naturalHeight} for ${cover.alt || 'Unknown book'}`);
                handleImageFailure();
                return;
            }
            
            // Image loaded successfully
            cover.classList.remove('loading');
            cover.classList.add('loaded');
            cover.setAttribute('data-load-state', 'loaded');
            
            showImage();
            
            // Log success for debugging
            console.log(`Image loaded successfully: ${cover.alt || 'Unknown book'}`, {
                src: cover.src,
                dimensions: `${this.naturalWidth}x${this.naturalHeight}`
            });
        });
        
        // Handle load error
        cover.addEventListener('error', handleImageFailure);
        
        // If image already loaded before listener was attached, manually trigger load
        if (cover.complete) {
            cover.dispatchEvent(new Event('load'));
        }
    });
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