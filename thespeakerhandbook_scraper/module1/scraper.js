const https = require('https');
const { connect, getDB, COLLECTIONS } = require('../shared/db');
const config = require('../shared/config');
const { delay, retryWithBackoff, cleanText } = require('../shared/utils');

/**
 * Fetch speakers from Typesense API
 * @param {number} page - Page number to fetch
 * @param {number} perPage - Number of results per page
 * @returns {Promise<Object>} - API response
 */
async function fetchSpeakersFromAPI(page = 1, perPage = 100) {
    const searchQuery = {
        searches: [{
            collection: config.TYPESENSE.COLLECTION,
            q: '*',
            query_by: 'display_name',
            per_page: perPage,
            page: page,
            include_fields: [
                'id',
                'display_name',
                'first_name',
                'last_name',
                'url',
                'img_url',
                'strapline',
                'topics',
                'home_country',
                'languages',
                'gender',
                'membership',
                'notability',
                'engagement_types',
                'event_type'
            ].join(',')
        }]
    };

    return new Promise((resolve, reject) => {
        const options = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-TYPESENSE-API-KEY': config.TYPESENSE.API_KEY
            },
            timeout: config.REQUEST.TIMEOUT
        };

        const req = https.request(config.TYPESENSE.API_URL, options, (res) => {
            let data = '';

            res.on('data', (chunk) => {
                data += chunk;
            });

            res.on('end', () => {
                try {
                    const jsonData = JSON.parse(data);
                    resolve(jsonData);
                } catch (error) {
                    reject(new Error(`Failed to parse response: ${error.message}`));
                }
            });
        });

        req.on('error', reject);
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('Request timeout'));
        });

        req.write(JSON.stringify(searchQuery));
        req.end();
    });
}

/**
 * Process and transform speaker data
 * @param {Object} speakerData - Raw speaker data from API
 * @returns {Object} - Transformed speaker data
 */
function transformSpeakerData(speakerData) {
    return {
        speaker_id: speakerData.id,
        display_name: cleanText(speakerData.display_name),
        first_name: cleanText(speakerData.first_name),
        last_name: cleanText(speakerData.last_name),
        profile_url: speakerData.url,
        image_url: speakerData.img_url,
        strapline: cleanText(speakerData.strapline),
        topics: speakerData.topics || [],
        home_country: speakerData.home_country,
        languages: speakerData.languages || [],
        gender: speakerData.gender,
        membership: speakerData.membership,
        notability: speakerData.notability || [],
        engagement_types: speakerData.engagement_types || [],
        event_type: speakerData.event_type || [],
        scraped_at: new Date(),
        profile_scraped: false
    };
}

/**
 * Save speakers to MongoDB
 * @param {Array} speakers - Array of speaker objects
 * @returns {Promise<Object>} - Insert result
 */
async function saveSpeakers(speakers) {
    const db = getDB();
    const collection = db.collection(COLLECTIONS.SPEAKERS);
    
    // Use bulkWrite for better performance with upserts
    const operations = speakers.map(speaker => ({
        updateOne: {
            filter: { speaker_id: speaker.speaker_id },
            update: { $set: speaker },
            upsert: true
        }
    }));
    
    return await collection.bulkWrite(operations);
}

/**
 * Main scraping function
 */
async function scrapeAllSpeakers() {
    try {
        // Connect to MongoDB
        await connect();
        
        console.log('Starting speaker directory scraping...');
        
        let page = 1;
        let totalSpeakers = 0;
        let hasMore = true;
        
        while (hasMore) {
            console.log(`\nFetching page ${page}...`);
            
            // Fetch speakers with retry logic
            const response = await retryWithBackoff(
                () => fetchSpeakersFromAPI(page, config.SCRAPING.BATCH_SIZE),
                config.REQUEST.RETRY_ATTEMPTS,
                config.REQUEST.RETRY_DELAY
            );
            
            // Check if we have results
            if (!response.results || !response.results[0] || !response.results[0].hits) {
                console.error('Invalid response structure');
                break;
            }
            
            const result = response.results[0];
            const speakers = result.hits.map(hit => transformSpeakerData(hit.document));
            
            if (speakers.length === 0) {
                hasMore = false;
                break;
            }
            
            // Save speakers to MongoDB
            const saveResult = await saveSpeakers(speakers);
            console.log(`Saved ${speakers.length} speakers to database`);
            console.log(`Modified: ${saveResult.modifiedCount}, Upserted: ${saveResult.upsertedCount}`);
            
            totalSpeakers += speakers.length;
            
            // Check if we have more pages
            const totalFound = result.found;
            const fetchedSoFar = page * config.SCRAPING.BATCH_SIZE;
            
            if (fetchedSoFar >= totalFound) {
                hasMore = false;
            } else {
                page++;
                // Add delay between requests to be respectful
                await delay(config.SCRAPING.DELAY_BETWEEN_REQUESTS);
            }
        }
        
        console.log(`\nScraping completed! Total speakers scraped: ${totalSpeakers}`);
        
        // Get final count from database
        const db = getDB();
        const collection = db.collection(COLLECTIONS.SPEAKERS);
        const dbCount = await collection.countDocuments();
        console.log(`Total speakers in database: ${dbCount}`);
        
    } catch (error) {
        console.error('Scraping error:', error);
        throw error;
    }
}

// Export functions
module.exports = {
    scrapeAllSpeakers,
    fetchSpeakersFromAPI,
    transformSpeakerData
};

// Run if called directly
if (require.main === module) {
    scrapeAllSpeakers()
        .then(() => {
            console.log('Script completed successfully');
            process.exit(0);
        })
        .catch(error => {
            console.error('Script failed:', error);
            process.exit(1);
        });
}