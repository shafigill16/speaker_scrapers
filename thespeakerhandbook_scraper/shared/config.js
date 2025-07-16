require('dotenv').config();

module.exports = {
    // MongoDB configuration
    MONGODB: {
        URI: process.env.MONGODB_URI || 'mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin',
        DATABASE: process.env.MONGODB_DATABASE || 'thespeakerhandbook_scraper'
    },
    
    // Typesense API configuration
    TYPESENSE: {
        API_URL: 'https://3m56bo9q2xlszhuip-1.a1.typesense.net/multi_search/',
        API_KEY: 'S1wzZZbfXaDH81o5CFmqVKoflxy6J5D0',
        COLLECTION: 'tsh_speakers'
    },
    
    // Request configuration
    REQUEST: {
        TIMEOUT: parseInt(process.env.REQUEST_TIMEOUT_MS) || 30000, // 30 seconds
        RETRY_ATTEMPTS: parseInt(process.env.MAX_RETRIES) || 3,
        RETRY_DELAY: parseInt(process.env.SCRAPE_DELAY_MS) || 1000 // 1 second
    },
    
    // Scraping configuration
    SCRAPING: {
        BATCH_SIZE: 100, // Number of speakers to fetch per request
        DELAY_BETWEEN_REQUESTS: parseInt(process.env.SCRAPE_DELAY_MS) || 500, // Milliseconds
        MAX_CONCURRENT_PROFILE_REQUESTS: 5
    },
    
    // Base URL for speaker profiles
    BASE_URL: 'https://thespeakerhandbook.com'
};