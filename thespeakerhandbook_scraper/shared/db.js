const { MongoClient } = require('mongodb');

const MONGODB_URI = 'mongodb://admin:dev2018@5.161.225.172:27017/?authSource=admin';
const DB_NAME = 'thespeakerhandbook_scraper';

let client = null;
let db = null;

/**
 * Connect to MongoDB
 * @returns {Promise<Object>} Database instance
 */
async function connect() {
    if (db) {
        return db;
    }

    try {
        client = new MongoClient(MONGODB_URI, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });

        await client.connect();
        console.log('Connected to MongoDB successfully');
        
        db = client.db(DB_NAME);
        return db;
    } catch (error) {
        console.error('MongoDB connection error:', error);
        throw error;
    }
}

/**
 * Get MongoDB database instance
 * @returns {Object} Database instance
 */
function getDB() {
    if (!db) {
        throw new Error('Database not connected. Call connect() first.');
    }
    return db;
}

/**
 * Close MongoDB connection
 */
async function close() {
    if (client) {
        await client.close();
        client = null;
        db = null;
        console.log('MongoDB connection closed');
    }
}

module.exports = {
    connect,
    getDB,
    close,
    COLLECTIONS: {
        SPEAKERS: 'speakers',
        SPEAKER_PROFILES: 'speaker_profiles'
    }
};