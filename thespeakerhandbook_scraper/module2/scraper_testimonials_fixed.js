const https = require('https');
const cheerio = require('cheerio');
const { connect, getDB, COLLECTIONS, close } = require('../shared/db');
const config = require('../shared/config');
const { delay, retryWithBackoff, cleanText, processBatches } = require('../shared/utils');

/**
 * Extract testimonials with correct structure based on actual HTML
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of testimonial objects
 */
function extractTestimonialsFixed($) {
    const testimonials = [];
    
    // Look for testimonials in the specific structure found in the HTML
    $('.tsh-sp-test .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        const testimonial = {
            content: '',
            person: '',
            position: '',
            organization: ''
        };
        
        // Extract testimonial content - look for rp_t field
        const contentElement = $item.find('[data-settings*="rp_t"]').first();
        if (contentElement.length) {
            testimonial.content = cleanText(contentElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Extract organization - look for rp_o field
        const orgElement = $item.find('[data-settings*="rp_o"]').first();
        if (orgElement.length) {
            testimonial.organization = cleanText(orgElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Try to find person name - might be in rp_p or similar field
        const personElement = $item.find('[data-settings*="rp_p"], [data-settings*="rp_n"]').first();
        if (personElement.length) {
            testimonial.person = cleanText(personElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Try to find position - might be in rp_pos or similar field
        const positionElement = $item.find('[data-settings*="rp_pos"], [data-settings*="rp_position"]').first();
        if (positionElement.length) {
            testimonial.position = cleanText(positionElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // If no person found but organization exists, check if organization contains person info
        if (!testimonial.person && testimonial.organization) {
            // Sometimes the organization field contains "Person Name, Organization"
            const parts = testimonial.organization.split(',');
            if (parts.length > 1) {
                // Check if first part looks like a person name (has space, starts with capital)
                const firstPart = parts[0].trim();
                if (firstPart.includes(' ') && /^[A-Z]/.test(firstPart)) {
                    testimonial.person = firstPart;
                    testimonial.organization = parts.slice(1).join(',').trim();
                }
            }
        }
        
        // Only add if we have content
        if (testimonial.content) {
            testimonials.push(testimonial);
        }
    });
    
    // Also look for testimonials in alternative structures
    $('.testimonials .dce-acf-repeater-item, .dce-acf-repeater-testimonials .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        
        // Skip if already processed
        if ($item.closest('.tsh-sp-test').length) return;
        
        const testimonial = {
            content: '',
            person: '',
            position: '',
            organization: ''
        };
        
        // Look for content in various ways
        const contentSelectors = [
            '[data-settings*="testimonial"]',
            '[data-settings*="quote"]',
            '[data-settings*="text"]',
            '.testimonial-content',
            '.quote-text',
            'blockquote',
            'p'
        ];
        
        for (const selector of contentSelectors) {
            const element = $item.find(selector).first();
            if (element.length) {
                const text = cleanText(element.find('.dynamic-content-for-elementor-acf').text() || element.text());
                if (text && text.length > 20) {
                    testimonial.content = text;
                    break;
                }
            }
        }
        
        // Look for attribution
        const attributionSelectors = [
            '[data-settings*="author"]',
            '[data-settings*="name"]',
            '[data-settings*="attribution"]',
            '.testimonial-author',
            '.author-name',
            'cite'
        ];
        
        for (const selector of attributionSelectors) {
            const element = $item.find(selector).first();
            if (element.length) {
                const text = cleanText(element.find('.dynamic-content-for-elementor-acf').text() || element.text());
                if (text) {
                    // Parse attribution
                    const parts = text.split(/[,\-–—]/);
                    testimonial.person = parts[0].trim();
                    if (parts.length > 1) {
                        // Second part might be position or organization
                        const secondPart = parts[1].trim();
                        if (parts.length > 2) {
                            testimonial.position = secondPart;
                            testimonial.organization = parts[2].trim();
                        } else {
                            // Guess if it's position or organization
                            if (/^(CEO|CTO|CFO|Director|Manager|President|VP|Vice|Chief|Head|Lead)/i.test(secondPart)) {
                                testimonial.position = secondPart;
                            } else {
                                testimonial.organization = secondPart;
                            }
                        }
                    }
                    break;
                }
            }
        }
        
        // Only add if we have content and it's not a duplicate
        if (testimonial.content && !testimonials.some(t => t.content === testimonial.content)) {
            testimonials.push(testimonial);
        }
    });
    
    return testimonials;
}

/**
 * Test the testimonial extraction on local HTML file
 */
async function testTestimonialExtraction() {
    try {
        const fs = require('fs').promises;
        const htmlPath = '/home/mudassir/work/shafi/thespeakerhandbook_scraper/speaker_profile.html';
        
        console.log('Reading HTML file...');
        const html = await fs.readFile(htmlPath, 'utf-8');
        
        console.log('Extracting testimonials...');
        const $ = cheerio.load(html);
        const testimonials = extractTestimonialsFixed($);
        
        console.log(`\nFound ${testimonials.length} testimonials:\n`);
        
        testimonials.forEach((testimonial, index) => {
            console.log(`Testimonial ${index + 1}:`);
            console.log(`Content: ${testimonial.content}`);
            console.log(`Person: ${testimonial.person || 'Not found'}`);
            console.log(`Position: ${testimonial.position || 'Not found'}`);
            console.log(`Organization: ${testimonial.organization || 'Not found'}`);
            console.log('-'.repeat(80));
        });
        
        // Also look for any ACF fields with rp_ prefix to understand the structure
        console.log('\nLooking for all ACF fields with rp_ prefix:');
        $('[data-settings*="rp_"]').each((i, el) => {
            const settings = $(el).attr('data-settings');
            const match = settings.match(/"acf_field_list":"(rp_[^"]+)"/);
            if (match) {
                const fieldName = match[1];
                const content = cleanText($(el).find('.dynamic-content-for-elementor-acf').text());
                if (content) {
                    console.log(`Field: ${fieldName}`);
                    console.log(`Content: ${content.substring(0, 100)}${content.length > 100 ? '...' : ''}`);
                    console.log('-'.repeat(40));
                }
            }
        });
        
    } catch (error) {
        console.error('Error:', error);
    }
}

// Export the fixed function
module.exports = {
    extractTestimonialsFixed
};

// Run test if called directly
if (require.main === module) {
    testTestimonialExtraction()
        .then(() => console.log('\nTest completed'))
        .catch(error => console.error('Test failed:', error));
}