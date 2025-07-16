const { connect, getDB, COLLECTIONS, close } = require('./shared/db');
const { extractProfileData } = require('./module2/scraper_complete');
const fs = require('fs');

async function testSingleProfile() {
    try {
        await connect();
        const db = getDB();
        
        // Get the speaker "Vivian Tu" for testing
        const speaker = await db.collection(COLLECTIONS.SPEAKERS)
            .findOne({ display_name: "Vivian Tu" });
        
        if (!speaker) {
            console.log("Speaker 'Vivian Tu' not found in database");
            return;
        }
        
        // Read the saved HTML file
        const html = fs.readFileSync('./speaker_profile.html', 'utf8');
        
        // Extract profile data
        const profileData = extractProfileData(html, speaker);
        
        // Display results
        console.log("\n=== PROFILE EXTRACTION RESULTS ===\n");
        
        console.log("BOOKS:", profileData.books.length);
        profileData.books.forEach((book, i) => {
            console.log(`\nBook ${i + 1}:`);
            console.log("  Title:", book.title);
            console.log("  Summary:", book.summary.substring(0, 100) + "...");
            console.log("  Purchase Link:", book.purchase_link);
            console.log("  Image URL:", book.image_url);
        });
        
        console.log("\n\nSOCIAL LINKS:", profileData.social_links.length);
        profileData.social_links.forEach(link => {
            if (link.includes('facebook')) console.log("  Facebook:", link);
            if (link.includes('youtube')) console.log("  YouTube:", link);
            if (link.includes('linkedin')) console.log("  LinkedIn:", link);
            if (link.includes('twitter') || link.includes('x.com')) console.log("  Twitter/X:", link);
            if (link.includes('instagram')) console.log("  Instagram:", link);
        });
        
        console.log("\n\nTESTIMONIALS:", profileData.testimonials.length);
        profileData.testimonials.forEach((test, i) => {
            console.log(`\nTestimonial ${i + 1}:`);
            console.log("  Content:", test.content);
            console.log("  Person:", test.person);
            console.log("  Organization:", test.organization);
        });
        
    } catch (error) {
        console.error('Test error:', error);
    } finally {
        await close();
    }
}

testSingleProfile();