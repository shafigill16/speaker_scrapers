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
 * Extract testimonials from repeater sections
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of testimonial objects
 */
function extractTestimonials($) {
    const testimonials = [];
    
    // Look for testimonials in ACF repeater sections
    $('.tsh-sp-test .dce-acf-repeater-item, .testimonials .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        const testimonial = {
            content: cleanText($item.find('.test-text, .testimonial-content, p').first().text()),
            person: cleanText($item.find('.test-person, .testimonial-author').text()),
            position: cleanText($item.find('.test-position, .testimonial-position').text()),
            organization: cleanText($item.find('.test-organization, .testimonial-org').text())
        };
        
        if (testimonial.content) {
            testimonials.push(testimonial);
        }
    });
    
    return testimonials;
}

/**
 * Extract books from the profile
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of book objects
 */
function extractBooks($) {
    const books = [];
    
    // Look for books in ACF repeater sections
    $('.tsh-sp-books .dce-acf-repeater-item, .books .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        const book = {
            title: cleanText($item.find('.book-title, h3, h4').first().text()),
            summary: cleanText($item.find('.book-summary, .book-description, p').text()),
            categories: []
        };
        
        // Extract categories if available
        $item.find('.book-category, .category').each((j, cat) => {
            book.categories.push(cleanText($(cat).text()));
        });
        
        if (book.title) {
            books.push(book);
        }
    });
    
    return books;
}

/**
 * Extract talks/presentations from the profile
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of talk objects
 */
function extractTalks($) {
    const talks = [];
    
    // Look for talks in various sections
    $('.popular-talks .dce-acf-repeater-item, .talks .dce-acf-repeater-item, [id*="talks"] .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        const talk = {
            title: cleanText($item.find('h3, h4, .talk-title').first().text()),
            description: cleanText($item.find('p, .talk-description').text())
        };
        
        if (talk.title) {
            talks.push(talk);
        }
    });
    
    return talks;
}

/**
 * Extract videos from the profile
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of video objects
 */
function extractVideos($) {
    const videos = [];
    
    // Extract YouTube videos
    $('iframe[src*="youtube.com"], iframe[src*="vimeo.com"]').each((i, el) => {
        const src = $(el).attr('src');
        const title = $(el).attr('title') || '';
        
        if (src) {
            videos.push({
                url: src,
                title: cleanText(title),
                platform: src.includes('youtube') ? 'youtube' : 'vimeo'
            });
        }
    });
    
    // Look for video categories
    const videoCategories = {};
    $('.tsh-sp-videos-category').each((i, cat) => {
        const categoryName = cleanText($(cat).find('.tsh-sp-videos-category-name').text());
        const categoryVideos = [];
        
        $(cat).find('iframe').each((j, iframe) => {
            const src = $(iframe).attr('src');
            const title = $(iframe).attr('title') || '';
            
            if (src) {
                categoryVideos.push({
                    url: src,
                    title: cleanText(title)
                });
            }
        });
        
        if (categoryName && categoryVideos.length > 0) {
            videoCategories[categoryName] = categoryVideos;
        }
    });
    
    return { videos, videoCategories };
}

/**
 * Extract contact information
 * @param {Object} $ - Cheerio instance
 * @returns {Object} - Contact information
 */
function extractContactInfo($) {
    const contact = {};
    
    // Look for contact form hidden fields
    const speakerEmail = $('#field_speaker-email2').attr('value');
    if (speakerEmail) {
        contact.email = speakerEmail;
    }
    
    // Look for phone numbers
    $('a[href^="tel:"]').each((i, el) => {
        const phone = $(el).attr('href').replace('tel:', '');
        if (phone && !contact.phone) {
            contact.phone = phone;
        }
    });
    
    // Look for website
    $('a[href*="website"], a[href*="site"]').each((i, el) => {
        const href = $(el).attr('href');
        if (href && !href.includes('thespeakerhandbook') && !contact.website) {
            contact.website = href;
        }
    });
    
    return contact;
}

/**
 * Extract social links (filtering out TheSpeakerHandbook's own links)
 * @param {Object} $ - Cheerio instance
 * @param {string} speakerName - Speaker's name to help filter
 * @returns {Array} - Array of social links
 */
function extractSocialLinks($, speakerName) {
    const socialLinks = [];
    const seenUrls = new Set();
    
    // Common social platforms
    const socialPatterns = {
        linkedin: /linkedin\.com\/in\/[^\/]+/,
        twitter: /twitter\.com\/[^\/]+/,
        facebook: /facebook\.com\/[^\/]+/,
        instagram: /instagram\.com\/[^\/]+/,
        youtube: /youtube\.com\/(channel|user|c)\/[^\/]+/
    };
    
    $('a[href*="linkedin.com"], a[href*="twitter.com"], a[href*="facebook.com"], a[href*="instagram.com"], a[href*="youtube.com"]').each((i, el) => {
        const href = $(el).attr('href');
        if (!href || seenUrls.has(href)) return;
        
        // Skip if it's TheSpeakerHandbook's own social links
        if (href.includes('thespeakerhandbook') || 
            href.includes('speakerhandbook') ||
            href.includes('/company/') && !href.includes(speakerName.toLowerCase())) {
            return;
        }
        
        // Check if it matches expected patterns
        for (const [platform, pattern] of Object.entries(socialPatterns)) {
            if (pattern.test(href)) {
                socialLinks.push(href);
                seenUrls.add(href);
                break;
            }
        }
    });
    
    return socialLinks;
}

/**
 * Extract all fees/pricing information
 * @param {Object} $ - Cheerio instance
 * @returns {Object} - Fees information
 */
function extractFees($) {
    const fees = {};
    
    // Look for fee sections
    $('.fees-container .fees-css, .speaker-fees .fee-item').each((i, item) => {
        const $item = $(item);
        const feeType = cleanText($item.find('.fee-type, h4').text());
        const feeAmount = cleanText($item.find('.fee-amount, .price').text());
        
        if (feeType && feeAmount) {
            fees[feeType] = feeAmount;
        }
    });
    
    return fees;
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
                profileData.biography_full = cleanText(personData.description || '');
                profileData.job_title = cleanText(personData.jobTitle || '');
                profileData.knows_about = cleanText(personData.knowsAbout || '');
                profileData.nationality = personData.nationality?.name || '';
                profileData.gender = personData.gender || speaker.gender;
                profileData.image_url_hd = personData.image || speaker.image_url;
                
                // Extract performance/talk information from JSON-LD
                if (personData.performerIn) {
                    const perfData = Array.isArray(personData.performerIn) ? personData.performerIn : [personData.performerIn];
                    profileData.json_ld_talks = perfData.map(perf => ({
                        title: cleanText(perf.name || ''),
                        description: cleanText(perf.about || '')
                    }));
                }
            }
        } catch (error) {
            console.error('Error parsing JSON-LD:', error.message);
        }
    }
    
    // Extract biography from main content
    const biographySection = $('#biography, .biography').first();
    if (biographySection.length) {
        profileData.biography = cleanText(biographySection.find('.elementor-widget-dyncontel-acf, .elementor-text-editor').text());
    }
    
    // Extract biography highlights
    const highlights = [];
    $('.prof-bhigh li, .biography-highlights li').each((i, el) => {
        const highlight = cleanText($(el).text());
        if (highlight) highlights.push(highlight);
    });
    profileData.biography_highlights = highlights;
    
    // Extract testimonials
    profileData.testimonials = extractTestimonials($);
    
    // Extract books
    profileData.books = extractBooks($);
    
    // Extract talks
    profileData.talks = extractTalks($);
    
    // Extract videos
    const videoData = extractVideos($);
    profileData.videos = videoData.videos;
    profileData.video_categories = videoData.videoCategories;
    
    // Extract contact information
    profileData.contact = extractContactInfo($);
    
    // Extract social links (filtered)
    profileData.social_links = extractSocialLinks($, speaker.display_name);
    
    // Extract fees
    profileData.fees = extractFees($);
    
    // Extract speaker topics/expertise
    const topics = [];
    $('.speaker-topics li, .expertise li, .topics li').each((i, el) => {
        const topic = cleanText($(el).text());
        if (topic) topics.push(topic);
    });
    if (topics.length > 0) {
        profileData.detailed_topics = topics;
    }
    
    // Extract languages spoken
    const languages = [];
    $('.languages li, .speaker-languages li').each((i, el) => {
        const lang = cleanText($(el).text());
        if (lang) languages.push(lang);
    });
    if (languages.length > 0) {
        profileData.languages_detailed = languages;
    }
    
    // Extract awards/achievements
    const awards = [];
    $('.awards li, .achievements li').each((i, el) => {
        const award = cleanText($(el).text());
        if (award) awards.push(award);
    });
    if (awards.length > 0) {
        profileData.awards = awards;
    }
    
    // Extract all images (gallery)
    const imageGallery = [];
    $('.speaker-gallery img, .gallery img, .elementor-gallery img').each((i, el) => {
        const src = $(el).attr('src');
        const alt = $(el).attr('alt') || '';
        if (src && !src.includes('logo')) {
            imageGallery.push({
                url: src,
                alt: cleanText(alt)
            });
        }
    });
    profileData.image_gallery = imageGallery.slice(0, 20); // Limit to 20 images
    
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
        console.log('Starting enhanced speaker profile scraping...');
        
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