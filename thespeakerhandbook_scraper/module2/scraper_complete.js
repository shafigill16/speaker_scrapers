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
 * Extract speaker metadata (gender, nationality, etc) separately
 * @param {Object} $ - Cheerio instance
 * @returns {Object} - Speaker metadata
 */
function extractSpeakerMetadata($) {
    const metadata = {};
    
    // Extract Gender
    const genderElement = $('.dynamic-content-for-elementor-acf').filter((i, el) => {
        return $(el).find('.tx-before').text().includes('Gender:');
    }).first();
    if (genderElement.length) {
        metadata.gender = cleanText(genderElement.text().replace('Gender:', ''));
    }
    
    // Extract Nationality
    const nationalityElement = $('.dynamic-content-for-elementor-acf').filter((i, el) => {
        return $(el).find('.tx-before').text().includes('Nationality:');
    }).first();
    if (nationalityElement.length) {
        metadata.nationality = cleanText(nationalityElement.text().replace('Nationality:', ''));
    }
    
    // Extract Languages
    const languagesElement = $('.dynamic-content-for-elementor-acf').filter((i, el) => {
        return $(el).find('.tx-before').text().includes('Languages:');
    }).first();
    if (languagesElement.length) {
        const langText = cleanText(languagesElement.text().replace('Languages:', ''));
        metadata.languages = langText.split(',').map(lang => lang.trim()).filter(Boolean);
    }
    
    // Extract Travels From
    const travelsFromElement = $('.dynamic-content-for-elementor-acf').filter((i, el) => {
        return $(el).find('.tx-before').text().includes('Travels from:');
    }).first();
    if (travelsFromElement.length) {
        metadata.travels_from = cleanText(travelsFromElement.text().replace('Travels from:', ''));
    }
    
    return metadata;
}

/**
 * Extract actual biography text
 * @param {Object} $ - Cheerio instance
 * @returns {string} - Biography text
 */
function extractBiography($) {
    // Look for the biography section specifically
    const biographySection = $('#biography, .biography').first();
    
    if (biographySection.length) {
        // Find the actual biography text (not the metadata)
        const bioText = biographySection.find('.elementor-widget-dyncontel-acf').filter((i, el) => {
            const text = $(el).text();
            // Skip if it contains metadata keywords
            return !text.includes('Gender:') && 
                   !text.includes('Nationality:') && 
                   !text.includes('Languages:') &&
                   !text.includes('Travels from:');
        });
        
        if (bioText.length) {
            return cleanText(bioText.text());
        }
    }
    
    // Fallback: look for long text content
    const longTextElements = $('.elementor-text-editor, .elementor-widget-text-editor').filter((i, el) => {
        const text = $(el).text();
        return text.length > 200 && !text.includes('Gender:');
    });
    
    if (longTextElements.length) {
        return cleanText(longTextElements.first().text());
    }
    
    return '';
}

/**
 * Extract testimonials with complete content
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of testimonial objects
 */
function extractTestimonials($) {
    const testimonials = [];
    
    // Look for testimonials in the tsh-sp-test structure
    $('.tsh-sp-test .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        const testimonial = {
            content: '',
            person: '',
            position: '',
            organization: ''
        };
        
        // Extract testimonial content from ACF field rp_t
        const contentElement = $item.find('[data-settings*="rp_t"]').first();
        if (contentElement.length) {
            testimonial.content = cleanText(contentElement.find('.dynamic-content-for-elementor-acf').text());
        } else {
            // Fallback: look for any substantial text content
            const textElements = $item.find('.dynamic-content-for-elementor-acf, .elementor-text-editor, p');
            textElements.each((j, el) => {
                const text = cleanText($(el).text());
                if (text.length > 50 && !testimonial.content) {
                    testimonial.content = text;
                }
            });
        }
        
        // Extract organization from ACF field rp_o
        const orgElement = $item.find('[data-settings*="rp_o"]').first();
        if (orgElement.length) {
            const orgText = cleanText(orgElement.find('.dynamic-content-for-elementor-acf').text());
            
            // Check if organization contains person info (e.g., "John Doe, Company Name")
            if (orgText.includes(',')) {
                const parts = orgText.split(',').map(p => p.trim());
                // If first part looks like a name (has space), split it
                if (parts[0].includes(' ') && /^[A-Z]/.test(parts[0])) {
                    testimonial.person = parts[0];
                    testimonial.organization = parts.slice(1).join(', ');
                } else {
                    testimonial.organization = orgText;
                }
            } else {
                testimonial.organization = orgText;
            }
        }
        
        // Try to find person name from ACF fields
        const personElement = $item.find('[data-settings*="rp_p"], [data-settings*="rp_n"], [data-settings*="rp_name"]').first();
        if (personElement.length) {
            testimonial.person = cleanText(personElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Try to find position from ACF fields
        const positionElement = $item.find('[data-settings*="rp_pos"], [data-settings*="rp_position"], [data-settings*="rp_title"]').first();
        if (positionElement.length) {
            testimonial.position = cleanText(positionElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Only add if we have content
        if (testimonial.content) {
            testimonials.push(testimonial);
        }
    });
    
    return testimonials;
}

/**
 * Extract all social and web links with proper YouTube/Facebook detection
 * @param {Object} $ - Cheerio instance
 * @param {string} speakerName - Speaker's name
 * @returns {Object} - Social links and website
 */
function extractAllLinks($, speakerName) {
    const result = {
        social_links: [],
        website: null,
        all_links: []
    };
    
    const seenUrls = new Set();
    const speakerNameLower = speakerName.toLowerCase().replace(/\s+/g, '');
    
    // First, specifically look for social media links in the dedicated container
    $('.tsh-sp-sm-links a, .social-links a').each((i, el) => {
        const href = $(el).attr('href');
        if (!href || seenUrls.has(href)) return;
        
        // Don't skip based on title attribute - check the actual URL
        if (!href.includes('thespeakerhandbook')) {
            seenUrls.add(href);
            
            // Add all social media links from this container
            if (href.includes('linkedin.com') || 
                href.includes('twitter.com') || 
                href.includes('x.com') ||
                href.includes('instagram.com') || 
                href.includes('facebook.com') || 
                href.includes('fb.com') ||
                href.includes('youtube.com') || 
                href.includes('youtu.be') ||
                href.includes('tiktok.com')) {
                result.social_links.push(href);
            }
        }
    });
    
    // Extract from dedicated website section
    $('.tsh-sp-websites a, .speaker-website a, a:contains("Website")').each((i, el) => {
        const href = $(el).attr('href');
        const text = $(el).text().toLowerCase();
        
        if (href && !href.includes('thespeakerhandbook') && text.includes('website')) {
            result.website = href;
        }
    });
    
    // Look for all other external links
    $('a[href^="http"]').each((i, el) => {
        const href = $(el).attr('href');
        if (!href || seenUrls.has(href)) return;
        
        // Skip TheSpeakerHandbook links
        if (href.includes('thespeakerhandbook.com') || 
            href.includes('speakerhandbook')) {
            return;
        }
        
        seenUrls.add(href);
        
        // Categorize links if not already found
        if (href.includes('linkedin.com') && !result.social_links.includes(href)) {
            if (href.includes('/in/') || href.includes(speakerNameLower)) {
                result.social_links.push(href);
            }
        } else if ((href.includes('twitter.com') || href.includes('x.com')) && !result.social_links.includes(href)) {
            result.social_links.push(href);
        } else if (href.includes('instagram.com') && !result.social_links.includes(href)) {
            result.social_links.push(href);
        } else if ((href.includes('facebook.com') || href.includes('fb.com')) && !result.social_links.includes(href)) {
            if (!href.includes('/sharer')) {
                result.social_links.push(href);
            }
        } else if ((href.includes('youtube.com') || href.includes('youtu.be')) && !result.social_links.includes(href)) {
            // Accept various YouTube URL formats
            if (href.includes('/@') || 
                href.includes('/channel/') || 
                href.includes('/user/') || 
                href.includes('/c/') ||
                href.includes('youtu.be/')) {
                result.social_links.push(href);
            }
        } else if (href.includes('tiktok.com') && !result.social_links.includes(href)) {
            result.social_links.push(href);
        } else {
            // Potential personal website
            if (!result.website && !href.includes('youtube.com/embed') && !href.includes('vimeo.com')) {
                try {
                    const domain = new URL(href).hostname;
                    if (domain.includes(speakerNameLower) || 
                        (!domain.includes('amazon') && !domain.includes('google'))) {
                        result.website = href;
                    }
                } catch (e) {
                    // Invalid URL, skip
                }
            }
        }
        
        result.all_links.push(href);
    });
    
    return result;
}

/**
 * Extract download profile link
 * @param {Object} $ - Cheerio instance
 * @returns {string|null} - Download profile URL
 */
function extractDownloadProfileLink($) {
    // Look for the specific download profile button
    const downloadButton = $('.tsh-sp-pdf-cta a, .elementor-button:contains("DOWNLOAD PROFILE")').first();
    if (downloadButton.length) {
        const href = downloadButton.attr('href');
        if (href) {
            // The link format is usually /speaker/{name}/pdf
            return href.startsWith('http') ? href : `https://thespeakerhandbook.com${href}`;
        }
    }
    
    // Fallback: look for other download/PDF links
    const downloadSelectors = [
        'a[href*="/pdf"]',
        'a[href*=".pdf"]',
        'a:contains("Download Profile")',
        'a:contains("DOWNLOAD PROFILE")',
        '.download-profile a',
        '.speaker-download a',
        'a[href$="/pdf"]'
    ];
    
    for (const selector of downloadSelectors) {
        const link = $(selector).first();
        if (link.length) {
            const href = link.attr('href');
            if (href && (href.includes('.pdf') || href.includes('/pdf'))) {
                return href.startsWith('http') ? href : `https://thespeakerhandbook.com${href}`;
            }
        }
    }
    
    return null;
}

/**
 * Extract topics/expertise
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of topics
 */
function extractTopics($) {
    const topics = [];
    const seenTopics = new Set();
    
    // Look for topics in the dedicated topics section
    $('h3:contains("Topics")').parent().parent().find('ul li a').each((i, el) => {
        const topic = cleanText($(el).text());
        const normalizedTopic = topic.toLowerCase();
        if (topic && !seenTopics.has(normalizedTopic)) {
            topics.push(topic);
            seenTopics.add(normalizedTopic);
        }
    });
    
    // Also check for topics in href links
    $('a[href*="/speaker-directory?topic="]').each((i, el) => {
        const topic = cleanText($(el).text());
        const normalizedTopic = topic.toLowerCase();
        if (topic && !seenTopics.has(normalizedTopic)) {
            topics.push(topic);
            seenTopics.add(normalizedTopic);
        }
    });
    
    // Extract from theme classes (thm-*)
    const bodyClasses = $('body').attr('class') || '';
    const divClasses = $('div[data-elementor-type="single-post"]').attr('class') || '';
    const allClasses = `${bodyClasses} ${divClasses}`;
    
    const themeMatches = allClasses.match(/thm-[a-z-]+/g) || [];
    themeMatches.forEach(thm => {
        const topic = thm.replace('thm-', '').replace(/-/g, ' ');
        const formattedTopic = topic.split(' ').map(word => 
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
        
        const normalizedTopic = formattedTopic.toLowerCase();
        if (formattedTopic && !seenTopics.has(normalizedTopic)) {
            topics.push(formattedTopic);
            seenTopics.add(normalizedTopic);
        }
    });
    
    return topics;
}

/**
 * Extract engagement types
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of engagement types
 */
function extractEngagementTypes($) {
    const types = [];
    const seenTypes = new Set();
    
    // Extract from enga-type classes
    const bodyClasses = $('body').attr('class') || '';
    const divClasses = $('div[data-elementor-type="single-post"]').attr('class') || '';
    const allClasses = `${bodyClasses} ${divClasses}`;
    
    const engaMatches = allClasses.match(/enga-type-[a-z-]+/g) || [];
    engaMatches.forEach(enga => {
        const type = enga.replace('enga-type-', '').replace(/-/g, ' ');
        const formattedType = type.split(' ').map(word => 
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
        
        if (formattedType && !seenTypes.has(formattedType)) {
            types.push(formattedType);
            seenTypes.add(formattedType);
        }
    });
    
    // Look for engagement types in the dedicated section
    $('h3:contains("Engagement Types")').parent().parent().find('ul li').each((i, el) => {
        const type = cleanText($(el).text());
        if (type && !seenTypes.has(type)) {
            types.push(type);
            seenTypes.add(type);
        }
    });
    
    // Also check shortcode sections
    $('.elementor-widget-shortcode').filter((i, el) => {
        const prevHeading = $(el).prev('.elementor-widget-heading').find('h3').text();
        return prevHeading && prevHeading.includes('Engagement Types');
    }).find('li').each((i, el) => {
        const type = cleanText($(el).text());
        if (type && !seenTypes.has(type)) {
            types.push(type);
            seenTypes.add(type);
        }
    });
    
    return types;
}

/**
 * Extract talks from accordion structure
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of talk objects
 */
function extractTalks($) {
    const talks = [];
    const seenTitles = new Set();
    
    // Look for talks in accordion items - excluding Table of Contents
    $('.e-n-accordion-item').each((i, item) => {
        const $item = $(item);
        const titleText = cleanText($item.find('.e-n-accordion-item-title-text').text());
        
        // Skip if it's Table of Contents
        if (titleText.toLowerCase().includes('table of contents')) {
            return;
        }
        
        // Get the description from the content area
        const $contentArea = $item.find('[role="region"]').first();
        let description = '';
        
        if ($contentArea.length) {
            // Look for ACF field content first
            const acfContent = $contentArea.find('.dynamic-content-for-elementor-acf').first();
            if (acfContent.length) {
                description = cleanText(acfContent.text());
            } else {
                // Fallback to any paragraph content
                description = cleanText($contentArea.find('p').first().text());
            }
        }
        
        if (titleText && !seenTitles.has(titleText)) {
            talks.push({
                title: titleText,
                description: description
            });
            seenTitles.add(titleText);
        }
    });
    
    // Look for talks in the "Popular Talks" section specifically
    $('h2:contains("Popular Talks")').parent().parent().find('.dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        
        // Find the accordion within this item
        const $accordion = $item.find('.e-n-accordion-item').first();
        if ($accordion.length) {
            const title = cleanText($accordion.find('.e-n-accordion-item-title-text').text());
            const $contentArea = $accordion.find('[role="region"]').first();
            let description = '';
            
            if ($contentArea.length) {
                const acfContent = $contentArea.find('.dynamic-content-for-elementor-acf').first();
                if (acfContent.length) {
                    description = cleanText(acfContent.text());
                }
            }
            
            if (title && !seenTitles.has(title)) {
                talks.push({
                    title: title,
                    description: description
                });
                seenTitles.add(title);
            }
        }
    });
    
    return talks;
}

/**
 * Extract books with complete structure
 * @param {Object} $ - Cheerio instance
 * @returns {Array} - Array of book objects
 */
function extractBooks($) {
    const books = [];
    
    // Look for books in the tsh-sp-books section
    $('.tsh-sp-books .dce-acf-repeater-item').each((i, item) => {
        const $item = $(item);
        
        const book = {
            title: '',
            summary: '',
            image_url: '',
            purchase_link: '',
            publisher: '',
            categories: []
        };
        
        // Extract book title - look for h2 first, then other headings
        const titleElement = $item.find('h2.elementor-heading-title, h3.elementor-heading-title, h4.elementor-heading-title').first();
        if (titleElement.length) {
            book.title = cleanText(titleElement.text());
        }
        
        // Extract book cover image
        const bookImage = $item.find('img').first();
        if (bookImage.length) {
            book.image_url = bookImage.attr('src') || '';
        }
        
        // Extract book summary - look for text that's not the title or button
        const allText = [];
        $item.find('.elementor-widget-container').each((j, container) => {
            const $container = $(container);
            // Skip if it contains the title element or button
            if ($container.find('h2, h3, h4, a.elementor-button').length === 0) {
                const text = cleanText($container.text());
                if (text && text !== book.title && !text.includes('Buy the book')) {
                    allText.push(text);
                }
            }
        });
        
        // The longest text is likely the summary
        if (allText.length > 0) {
            book.summary = allText.sort((a, b) => b.length - a.length)[0];
        }
        
        // Extract purchase link
        const purchaseLink = $item.find('a').filter((j, el) => {
            const text = $(el).text();
            const href = $(el).attr('href') || '';
            return text.includes('Buy') || href.includes('amazon') || href.includes('buy');
        }).first();
        
        if (purchaseLink.length) {
            book.purchase_link = purchaseLink.attr('href') || '';
        }
        
        // Try to extract publisher from ACF field if available
        const publisherElement = $item.find('[data-settings*="rp_bp"]').first();
        if (publisherElement.length) {
            book.publisher = cleanText(publisherElement.find('.dynamic-content-for-elementor-acf').text());
        }
        
        // Try to extract category from ACF field if available
        const categoryElement = $item.find('[data-settings*="rp_bc"]').first();
        if (categoryElement.length) {
            const category = cleanText(categoryElement.find('.dynamic-content-for-elementor-acf').text());
            if (category) {
                book.categories.push(category);
            }
        }
        
        // Only add if we have a title
        if (book.title) {
            books.push(book);
        }
    });
    
    return books;
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
    
    // Extract speaker metadata separately
    const metadata = extractSpeakerMetadata($);
    profileData.gender = metadata.gender || speaker.gender;
    profileData.nationality = metadata.nationality || '';
    profileData.languages = metadata.languages || speaker.languages || [];
    profileData.travels_from = metadata.travels_from || '';
    
    // Extract actual biography (not metadata)
    profileData.biography = extractBiography($);
    
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
                // Use JSON-LD biography if our extraction didn't find one
                if (!profileData.biography && personData.description) {
                    profileData.biography = cleanText(personData.description);
                }
                
                profileData.job_title = cleanText(personData.jobTitle || '');
                profileData.knows_about = cleanText(personData.knowsAbout || '');
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
    
    // Extract biography highlights
    const highlights = [];
    $('.prof-bhigh li, .biography-highlights li, .bio-highlights li').each((i, el) => {
        const highlight = cleanText($(el).text());
        if (highlight) highlights.push(highlight);
    });
    profileData.biography_highlights = highlights;
    
    // Extract all links (social, website, etc) - with YouTube/Facebook fix
    const linkData = extractAllLinks($, speaker.display_name);
    profileData.social_links = linkData.social_links;
    profileData.website = linkData.website;
    
    // Extract download profile link
    profileData.download_profile_link = extractDownloadProfileLink($);
    
    // Extract topics/expertise
    profileData.topics = extractTopics($);
    
    // Extract engagement types
    profileData.engagement_types = extractEngagementTypes($);
    
    // Extract testimonials with complete content
    profileData.testimonials = extractTestimonials($);
    
    // Extract talks from accordion
    profileData.talks = extractTalks($);
    
    // Extract books with complete data
    profileData.books = extractBooks($);
    
    // Extract videos
    const videos = [];
    const videoCategories = {};
    
    // Extract videos by category
    $('.tsh-sp-videos-category').each((i, cat) => {
        const categoryName = cleanText($(cat).find('.tsh-sp-videos-category-name').text());
        const categoryVideos = [];
        
        $(cat).find('iframe').each((j, iframe) => {
            const src = $(iframe).attr('src');
            const title = $(iframe).attr('title') || '';
            
            if (src) {
                const video = {
                    url: src,
                    title: cleanText(title),
                    platform: src.includes('youtube') ? 'youtube' : 'vimeo'
                };
                videos.push(video);
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
    
    profileData.videos = videos;
    profileData.video_categories = videoCategories;
    
    // Extract contact information
    const contact = {};
    
    // Look for contact form hidden fields
    const speakerEmail = $('#field_speaker-email2, input[name*="speaker-email"]').attr('value');
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
    
    profileData.contact = contact;
    
    // Extract fees
    const fees = {};
    $('.fees-container .fees-css, .speaker-fees .fee-item, .pricing-item').each((i, item) => {
        const $item = $(item);
        const feeType = cleanText($item.find('.fee-type, h4, .pricing-title').text());
        const feeAmount = cleanText($item.find('.fee-amount, .price, .pricing-amount').text());
        
        if (feeType && feeAmount) {
            fees[feeType] = feeAmount;
        }
    });
    profileData.fees = fees;
    
    // Extract awards/achievements
    const awards = [];
    $('.awards li, .achievements li, .accolades li').each((i, el) => {
        const award = cleanText($(el).text());
        if (award) awards.push(award);
    });
    profileData.awards = awards;
    
    // Extract image gallery
    const imageGallery = [];
    $('.speaker-gallery img, .gallery img, .photos img').each((i, el) => {
        const src = $(el).attr('src');
        const alt = $(el).attr('alt') || '';
        if (src && !src.includes('logo') && !src.includes('TheSpeakerHandbook')) {
            imageGallery.push({
                url: src,
                alt: cleanText(alt)
            });
        }
    });
    profileData.image_gallery = imageGallery.slice(0, 20);
    
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
        console.log('Starting comprehensive speaker profile scraping...');
        
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