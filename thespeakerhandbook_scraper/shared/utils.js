/**
 * Delay execution for specified milliseconds
 * @param {number} ms - Milliseconds to delay
 * @returns {Promise}
 */
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Retry a function with exponential backoff
 * @param {Function} fn - Function to retry
 * @param {number} maxAttempts - Maximum number of attempts
 * @param {number} initialDelay - Initial delay in milliseconds
 * @returns {Promise}
 */
async function retryWithBackoff(fn, maxAttempts = 3, initialDelay = 1000) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await fn();
        } catch (error) {
            lastError = error;
            
            if (attempt === maxAttempts) {
                throw error;
            }
            
            const delayMs = initialDelay * Math.pow(2, attempt - 1);
            console.log(`Attempt ${attempt} failed. Retrying in ${delayMs}ms...`);
            await delay(delayMs);
        }
    }
    
    throw lastError;
}

/**
 * Process array in batches
 * @param {Array} array - Array to process
 * @param {number} batchSize - Size of each batch
 * @param {Function} processor - Function to process each batch
 * @returns {Promise<Array>} - Combined results from all batches
 */
async function processBatches(array, batchSize, processor) {
    const results = [];
    
    for (let i = 0; i < array.length; i += batchSize) {
        const batch = array.slice(i, i + batchSize);
        const batchResults = await processor(batch);
        results.push(...batchResults);
    }
    
    return results;
}

/**
 * Clean and normalize text
 * @param {string} text - Text to clean
 * @returns {string} - Cleaned text
 */
function cleanText(text) {
    if (!text) return '';
    if (typeof text !== 'string') return String(text);
    
    return text
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#039;/g, "'")
        .replace(/\\r\\n/g, '\n')
        .replace(/\\n/g, '\n')
        .trim();
}

/**
 * Extract speaker ID from URL
 * @param {string} url - Speaker profile URL
 * @returns {string|null} - Speaker ID or null
 */
function extractSpeakerIdFromUrl(url) {
    if (!url) return null;
    
    const match = url.match(/\/speaker\/([^\/]+)\/?$/);
    return match ? match[1] : null;
}

module.exports = {
    delay,
    retryWithBackoff,
    processBatches,
    cleanText,
    extractSpeakerIdFromUrl
};