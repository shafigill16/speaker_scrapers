const https = require('https');
const cheerio = require('cheerio');
const { connect, getDB, COLLECTIONS, close } = require('../shared/db');
const config = require('../shared/config');
const { delay, retryWithBackoff, cleanText, processBatches } = require('../shared/utils');

/**
 * Fetch HTML content from URL
 * @param {string} url - URL to fetch
 * @returns {Promise<string>} - HTML content
 */
async function fetchHTML(url) {
    return new Promise((resolve, reject) => {
        https.get(url, { timeout: config.REQUEST.TIMEOUT }, (res) => {
            let data = '';
            
            if (res.statusCode !== 200) {
                reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
                return;
            }
            
            res.on('data', (chunk) => {
                data += chunk;
            });
            
            res.on('end', () => {
                resolve(data);
            });
        }).on('error', reject)
          .on('timeout', () => {
            reject(new Error('Request timeout'));
        });
    });
}

/**
 * Extract structured data from speaker profile HTML
 * @param {string} html - HTML content
 * @param {Object} speaker - Speaker object from database
 * @returns {Object} - Extracted profile data
 */
function extractProfileData(html, speaker) {
    const $ = cheerio.load(html);
    const profileData = {
        speaker_id: speaker.speaker_id,
        display_name: speaker.display_name,
        profile_url: speaker.profile_url,
        scraped_at: new Date()
    };
    
    // Extract title/meta description
    profileData.page_title = $('title').text().trim();
    profileData.meta_description = $('meta[name="description"]').attr('content') || '';
    
    // Extract structured data from JSON-LD
    const jsonLdScript = $('script[type="application/ld+json"]').filter((i, el) => {
        const text = $(el).text();
        return text.includes('@type":"Person"') || text.includes('"Person"');
    }).first();
    
    if (jsonLdScript.length) {
        try {
            const jsonData = JSON.parse(jsonLdScript.text());
            const personData = jsonData['@graph'] ? 
                jsonData['@graph'].find(item => item['@type'] === 'Person') : 
                jsonData;
            
            if (personData) {
                profileData.biography = cleanText(personData.description || '');
                profileData.job_title = cleanText(personData.jobTitle || '');
                profileData.knows_about = personData.knowsAbout || '';
                profileData.nationality = personData.nationality?.name || '';
                
                // Extract performance/talk information
                if (personData.performerIn) {
                    profileData.talks = [{
                        title: cleanText(personData.performerIn.name || ''),
                        description: cleanText(personData.performerIn.about || '')
                    }];
                }
            }
        } catch (error) {
            console.error('Error parsing JSON-LD:', error);
        }
    }
    
    // Extract main content area (fallback if JSON-LD is incomplete)
    const mainContent = $('.elementor-widget-container').filter((i, el) => {
        const text = $(el).text();
        return text.length > 500 && !text.includes('menu') && !text.includes('footer');
    });
    
    if (mainContent.length && !profileData.biography) {
        profileData.biography = cleanText(mainContent.first().text());
    }
    
    // Extract social links
    const socialLinks = [];
    $('a[href*="linkedin.com"], a[href*="twitter.com"], a[href*="facebook.com"], a[href*="instagram.com"]').each((i, el) => {
        const href = $(el).attr('href');
        if (href && !socialLinks.includes(href)) {
            socialLinks.push(href);
        }
    });
    profileData.social_links = socialLinks;
    
    // Extract any video URLs
    const videoUrls = [];
    $('iframe[src*="youtube.com"], iframe[src*="vimeo.com"]').each((i, el) => {
        const src = $(el).attr('src');
        if (src) videoUrls.push(src);
    });
    profileData.video_urls = videoUrls;
    
    // Extract image URLs (beyond the main profile image)
    const imageUrls = [];
    $('img').each((i, el) => {
        const src = $(el).attr('src');
        if (src && src.includes('speaker') && !src.includes('logo')) {
            imageUrls.push(src);
        }
    });
    profileData.image_urls = imageUrls.slice(0, 10); // Limit to 10 images
    
    return profileData;
}

/**
 * Scrape a single speaker profile
 * @param {Object} speaker - Speaker object from database
 * @returns {Promise<Object>} - Scraped profile data
 */
async function scrapeSpeakerProfile(speaker) {
    try {
        console.log(`Scraping profile for: ${speaker.display_name}`);
        
        const html = await retryWithBackoff(
            () => fetchHTML(speaker.profile_url),
            config.REQUEST.RETRY_ATTEMPTS,
            config.REQUEST.RETRY_DELAY
        );
        
        const profileData = extractProfileData(html, speaker);
        profileData.scrape_status = 'success';
        
        return profileData;
    } catch (error) {
        console.error(`Error scraping ${speaker.display_name}:`, error.message);
        
        return {
            speaker_id: speaker.speaker_id,
            display_name: speaker.display_name,
            profile_url: speaker.profile_url,
            scrape_status: 'error',
            error_message: error.message,
            scraped_at: new Date()
        };
    }
}

/**
 * Process a batch of speakers concurrently
 * @param {Array} speakers - Array of speaker objects
 * @returns {Promise<Array>} - Array of scraped profile data
 */
async function processSpeakerBatch(speakers) {
    const promises = speakers.map(speaker => 
        scrapeSpeakerProfile(speaker).then(async (profileData) => {
            // Add delay between requests
            await delay(config.SCRAPING.DELAY_BETWEEN_REQUESTS);
            return profileData;
        })
    );
    
    return Promise.all(promises);
}

/**
 * Save speaker profiles to MongoDB
 * @param {Array} profiles - Array of profile objects
 * @returns {Promise<Object>} - Insert result
 */
async function saveSpeakerProfiles(profiles) {
    const db = getDB();
    const collection = db.collection(COLLECTIONS.SPEAKER_PROFILES);
    
    const operations = profiles.map(profile => ({
        updateOne: {
            filter: { speaker_id: profile.speaker_id },
            update: { $set: profile },
            upsert: true
        }
    }));
    
    return await collection.bulkWrite(operations);
}

/**
 * Update speaker's profile_scraped status
 * @param {Array} speakerIds - Array of speaker IDs
 * @returns {Promise<Object>} - Update result
 */
async function updateSpeakerStatus(speakerIds) {
    const db = getDB();
    const collection = db.collection(COLLECTIONS.SPEAKERS);
    
    return await collection.updateMany(
        { speaker_id: { $in: speakerIds } },
        { $set: { profile_scraped: true } }
    );
}

/**
 * Main function to scrape all speaker profiles
 */
async function scrapeAllProfiles() {
    try {
        await connect();
        console.log('Starting speaker profile scraping...');
        
        const db = getDB();
        const speakersCollection = db.collection(COLLECTIONS.SPEAKERS);
        
        // Get count of unscraped profiles
        const unscrappedCount = await speakersCollection.countDocuments({ 
            profile_scraped: { $ne: true } 
        });
        
        console.log(`Found ${unscrappedCount} profiles to scrape`);
        
        let processedCount = 0;
        const batchSize = config.SCRAPING.MAX_CONCURRENT_PROFILE_REQUESTS;
        
        while (true) {
            // Get batch of unscraped speakers
            const speakers = await speakersCollection
                .find({ profile_scraped: { $ne: true } })
                .limit(batchSize)
                .toArray();
            
            if (speakers.length === 0) {
                break;
            }
            
            console.log(`\nProcessing batch of ${speakers.length} speakers...`);
            
            // Scrape profiles in batch
            const profiles = await processSpeakerBatch(speakers);
            
            // Save profiles to database
            const saveResult = await saveSpeakerProfiles(profiles);
            console.log(`Saved ${profiles.length} profiles`);
            
            // Update speaker status
            const speakerIds = speakers.map(s => s.speaker_id);
            await updateSpeakerStatus(speakerIds);
            
            processedCount += speakers.length;
            console.log(`Progress: ${processedCount}/${unscrappedCount} profiles scraped`);
            
            // Add delay between batches
            await delay(1000);
        }
        
        console.log(`\nProfile scraping completed! Total profiles scraped: ${processedCount}`);
        
        // Get final statistics
        const profilesCollection = db.collection(COLLECTIONS.SPEAKER_PROFILES);
        const successCount = await profilesCollection.countDocuments({ scrape_status: 'success' });
        const errorCount = await profilesCollection.countDocuments({ scrape_status: 'error' });
        
        console.log(`Success: ${successCount}, Errors: ${errorCount}`);
        
    } catch (error) {
        console.error('Profile scraping error:', error);
        throw error;
    } finally {
        await close();
    }
}

// Export functions
module.exports = {
    scrapeAllProfiles,
    scrapeSpeakerProfile,
    extractProfileData
};

// Run if called directly
if (require.main === module) {
    scrapeAllProfiles()
        .then(() => {
            console.log('Script completed successfully');
            process.exit(0);
        })
        .catch(error => {
            console.error('Script failed:', error);
            process.exit(1);
        });
}