/**
 * Cover Source Handler
 *
 * This script handles the loading of book covers from different sources
 * without blocking the initial display of search results.
 */

/**
 * Converts HTTP Google Books URLs to HTTPS to comply with CSP
 * @param {string} url - The image URL to fix
 * @returns {string} - The fixed HTTPS URL
 */
function ensureHttpsForGoogleBooks(url) {
    if (!url) return url;
    // Convert HTTP Google Books URLs to HTTPS for CSP compliance
    if (url.startsWith('http://books.google.com/') || url.startsWith('http://books.googleapis.com/')) {
        console.log('[CSP Fix] Converting HTTP to HTTPS for Google Books URL:', url);
        return url.replace('http://', 'https://');
    }
    return url;
}

document.addEventListener('DOMContentLoaded', function() {
    // Initialize cover source preference
    let currentCoverSource = 'ANY';
    
    // Set up event listeners for cover source dropdown
    document.querySelectorAll('.cover-source-option').forEach(option => {
        option.addEventListener('click', function(e) {
            e.preventDefault();
            const coverSource = this.getAttribute('data-source');
            
            // Update active state
            document.querySelectorAll('.cover-source-option').forEach(opt => opt.classList.remove('active'));
            this.classList.add('active');
            
            // Update dropdown button text
            const dropdownButton = document.getElementById('coverSourceDropdown');
            if (dropdownButton) {
                dropdownButton.innerHTML = `<i class="fas fa-image me-1"></i> Cover: ${coverSource.replace('_', ' ')}`;
            }
            
            currentCoverSource = coverSource;
            
            // Apply to existing book covers
            applyPreferredCoverSource();
        });
    });
    
    /**
     * Apply the preferred cover source to all book covers on the page
     */
    function applyPreferredCoverSource() {
        // Skip if no preference is set
        if (currentCoverSource === 'ANY') return;
        
        // Find all book covers on the page
        const bookCovers = document.querySelectorAll('.book-cover');
        
        bookCovers.forEach(img => {
            // Get the book ID from the parent element's data attribute or URL
            const bookLink = img.closest('a') || img.parentElement.querySelector('a');
            if (!bookLink) return;
            
            const bookUrl = bookLink.href;
            const bookIdMatch = bookUrl.match(/\/book\/([^/?]+)/);
            if (!bookIdMatch) return;
            
            const bookId = bookIdMatch[1];
            const originalSrc = img.getAttribute('data-original-src') || img.src;
            
            // Store the original source if not already stored
            if (!img.hasAttribute('data-original-src')) {
                img.setAttribute('data-original-src', originalSrc);
            }
            
            // Only fetch if the image isn't already loading or loaded from preferred source
            if (!img.hasAttribute('data-loading-source') || img.getAttribute('data-loading-source') !== currentCoverSource) {
                // Mark as loading from this source
                img.setAttribute('data-loading-source', currentCoverSource);
                
                // Fetch the preferred cover URL
                fetch(`/api/covers/${bookId}?source=${currentCoverSource}`)
                    .then(response => {
                        if (!response.ok) throw new Error('Failed to fetch cover');
                        return response.json();
                    })
                    .then(data => {
                        if (data && data.coverUrl) {
                            // Create a new image to preload
                            const newImg = new Image();
                            newImg.onload = function() {
                                // Only update if the source preference hasn't changed
                                if (img.getAttribute('data-loading-source') === currentCoverSource) {
                                    // Apply consistent dimensions using global function if available
                                    if (typeof applyConsistentDimensions === 'function') {
                                        applyConsistentDimensions(img, newImg.naturalWidth, newImg.naturalHeight);
                                    } else {
                                        // Add normalized-cover class directly if the function isn't available
                                        img.classList.add('normalized-cover');
                                    }
                                    
                                    // Apply source after dimensions are normalized
                                    img.src = ensureHttpsForGoogleBooks(data.coverUrl);
                                    console.log(`Cover source updated for book ${bookId}`);
                                }
                            };
                            newImg.onerror = function() {
                                console.warn(`Failed to load preferred cover for book ${bookId}`);
                                // Revert to original source on error
                                if (img.getAttribute('data-loading-source') === currentCoverSource) {
                                    img.src = ensureHttpsForGoogleBooks(originalSrc);
                                }
                            };
                            newImg.src = ensureHttpsForGoogleBooks(data.coverUrl);
                        }
                    })
                    .catch(error => {
                        console.error(`Error fetching preferred cover for book ${bookId}:`, error);
                        // Clear loading state on error
                        if (img.getAttribute('data-loading-source') === currentCoverSource) {
                            img.removeAttribute('data-loading-source');
                        }
                    });
            }
        });
    }
    
    // Initialize from URL parameter if present
    const urlParams = new URLSearchParams(window.location.search);
    const urlCoverSource = urlParams.get('coverSource');
    if (urlCoverSource) {
        currentCoverSource = urlCoverSource;
        
        // Update dropdown UI
        document.querySelectorAll('.cover-source-option').forEach(opt => {
            if (opt.getAttribute('data-source') === urlCoverSource) {
                opt.classList.add('active');
                const dropdownButton = document.getElementById('coverSourceDropdown');
                if (dropdownButton) {
                    dropdownButton.innerHTML = `<i class="fas fa-image me-1"></i> Cover: ${urlCoverSource.replace('_', ' ')}`;
                }
            } else {
                opt.classList.remove('active');
            }
        });
        
        // Apply the preference after a short delay to ensure DOM is ready
        setTimeout(applyPreferredCoverSource, 500);
    }
});
