# Source Database Field Analysis Report

Generated: 2025-07-16 14:36:50 UTC

MongoDB URI: 5.161.225.172:27017/?authSource=admin

## Executive Summary

- **Total Databases Analyzed**: 9
- **Total Documents**: 53,592
- **Total Unique Fields**: 463
- **Average Fields per Source**: 51

### Database Overview

| Database | Collection | Documents | Fields | Avg Doc Size |
|----------|------------|-----------|--------|--------------|
| speakerhub_scraper | speaker_details | 19,876 | 61 | 4,869 bytes |
| sessionize_scraper | speaker_profiles | 12,827 | 117 | 10,846 bytes |
| allamericanspeakers | speakers | 6,957 | 27 | 3,497 bytes |
| a_speakers | speakers | 3,592 | 29 | 8,055 bytes |
| thespeakerhandbook_scraper | speaker_profiles | 3,510 | 56 | 10,057 bytes |
| eventraptor | speakers | 2,986 | 21 | 1,670 bytes |
| bigspeak_scraper | speaker_profiles | 2,178 | 72 | 14,035 bytes |
| leading_authorities | speakers_final_details | 1,230 | 38 | 8,338 bytes |
| freespeakerbureau_scraper | speakers_profiles | 436 | 42 | 3,221 bytes |

## Detailed Field Analysis by Database

### a_speakers

**Collection**: `speakers`
**Documents**: 3,592
**Total Fields**: 29

#### Field Categories

**Professional** (5 fields):
- `job_title`: As a bestselling author and innovation expert, Alf Rehn help...
- `keynotes[0].title`: Moving minds with words | Keynote topics
- `reviews[0].author_organization`: CEO, Providence St. Joseph Health | U2
- `reviews[0].author_title`: Michael Bernard Beckwith, Founder & CEO | Jeremy Zoch
- `videos[0].title`: A taste of leadership: Alf Rehn at TEDxUmeå | Interview to A...

**Media** (6 fields):
- `image_url`: https://www.a-speakers.com/media/pojozb5o/alexis-zahner-hero...
- `videos`: [8 items] | [2 items]
- `videos[0].description`: Watch Alfie Joey in action | Watch Alexis Zahner in action
- `videos[0].thumbnail`: https://img.youtube.com/vi/tOCX-hoOpc0/sddefault.jpg | https...
- `videos[0].url`: https://www.youtube-nocookie.com/embed/ZxlK_6fGxgQ?controls=...
- `videos[0].video_id`: player-07fc8e81-0083-47b3-a6d7-60c107a57beb | player-132dc64...

**Metadata** (3 fields):
- `_id`: No samples
- `keynotes[0].id`: speaking-topics | moving-minds-with-words
- `scraped_at`: No samples

**Location** (1 fields):
- `location`: UK | Australia

**Content** (2 fields):
- `keynotes`: [5 items] | [1 items]
- `keynotes[0].description`: Topics:
Journalism
Media & policy advisor
German affairs
Int...

#### Top-Level Fields (17)
```
_id (ObjectId)
average_rating (float)
description (str)
fee_range (str)
full_bio (str)
image_url (str)
job_title (str)
keynotes (list)
location (str)
name (str)
reviews (list)
scraped_at (datetime)
topics (list)
total_reviews (int)
url (str)
videos (list)
why_book_points (list)
```

#### Nested Field Structures

**keynotes[0]** (3 nested fields):
- keynotes[0].description
- keynotes[0].id
- keynotes[0].title

**reviews[0]** (4 nested fields):
- reviews[0].author_organization
- reviews[0].author_title
- reviews[0].rating
- reviews[0].review_text

**videos[0]** (5 nested fields):
- videos[0].description
- videos[0].thumbnail
- videos[0].title
- videos[0].url
- videos[0].video_id

### allamericanspeakers

**Collection**: `speakers`
**Documents**: 6,957
**Total Fields**: 27

#### Field Categories

**Professional** (3 fields):
- `job_title`: Grammy Award-Winning Rapper, Songwriter & Entrepreneur; Foun...
- `speaking_topics[0].title`: Productivity | The Consumer is BossLeadership at Procter and...
- `videos[0].title`: 5 & A Dime-Pretty Lights Get Me High (Mashup) - YouTube | Am...

**Media** (4 fields):
- `videos`: [1 items] | [2 items]
- `videos[0].description`: The official video for A Boogie Wit Da Hoodie's "My Shit" fr...
- `videos[0].type`: youtube
- `videos[0].url`: https://www.youtube.com/watch?v=7jvqA8jkVbc | https://www.yo...

**Metadata** (3 fields):
- `_id`: No samples
- `scraped_at`: No samples
- `speaker_id`: 389198 | 399474

**Location** (1 fields):
- `location`: an undisclosed location | Boulder, CO, USA

#### Top-Level Fields (14)
```
_id (ObjectId)
biography (str)
categories (list)
fee_range (NoneType, dict)
job_title (str)
location (str)
name (str)
rating (dict)
reviews (list)
scraped_at (datetime)
speaker_id (str)
speaking_topics (list)
url (str)
videos (list)
```

#### Nested Field Structures

**fee_range** (2 nested fields):
- fee_range.live_event
- fee_range.virtual_event

**rating** (2 nested fields):
- rating.average_rating
- rating.review_count

**reviews[0]** (3 nested fields):
- reviews[0].author
- reviews[0].rating
- reviews[0].text

**speaking_topics[0]** (2 nested fields):
- speaking_topics[0].description
- speaking_topics[0].title

**videos[0]** (4 nested fields):
- videos[0].description
- videos[0].title
- videos[0].type
- videos[0].url

### bigspeak_scraper

**Collection**: `speaker_profiles`
**Documents**: 2,178
**Total Fields**: 72

#### Field Categories

**Social Media** (4 fields):
- `social_media`: No samples
- `social_media.facebook`: https://l.facebook.com/l.php?u=http%3A%2F%2F21cgirls.com%2F&...
- `social_media.instagram`: https://www.instagram.com/bigspeak_speakers_bureau/
- `social_media.youtube`: https://www.youtube.com/user/TheBigSpeak

**Contact** (2 fields):
- `structured_data.email`: info@bigspeak.com
- `structured_data.telephone`: 805.965.1400

**Professional** (6 fields):
- `books[0].title`: America's Possibility Coach, | Learn More
- `speaking_programs[0].title`: A Nonpartisan Overview of Politics and Economic Outlook in t...
- `structured_data.job_title`: Energizing, Engaging, Entertaining Keynote Speaker and Emcee...
- `suggested_programs[0].title`: A Nonpartisan Overview of Politics and Economic Outlook in t...
- `testimonials[0].company`: Jorge Soto | Dan Maddux
- `videos[0].title`: Jeff Bush - Full Presentation, October 2018 USbank | Joel Ko...

**Media** (14 fields):
- `additional_info.downloads`: [1 items] | [2 items]
- `additional_info.downloads[0].text`: BigSpeak Podcast Bob Sutton | Speaking Trends
- `additional_info.downloads[0].url`: https://www.bigspeak.com/wp-content/uploads/2018/03/BigSpeak...
- `basic_info.image_url`: https://www.bigspeak.com/wp-content/uploads/2017/08/cD03MGVk...
- `images`: [5 items] | [1 items]
- `images[0].source`: og:image
- `images[0].type`: primary | lazy-loaded
- `images[0].url`: https://www.bigspeak.com/wp-content/uploads/2015/01/headshot...
- `videos`: [5 items] | [1 items]
- `videos[0].embed_url`: https://www.youtube.com/embed/tVy0TyNkok4 | https://www.yout...
- ... and 4 more

**Metadata** (7 fields):
- `_id`: No samples
- `additional_info.meta_description`: Contact BigSpeak Speakers Bureau for the best keynote speake...
- `additional_info.post_id`: 105540 | 23280
- `first_scraped_at`: No samples
- `scraped_at`: No samples
- `source`: profile_page_final
- `speaker_id`: Joel-Kotkin | Jeff-Bush

**Location** (4 fields):
- `location`: No samples
- `location.travels_from`: Santa Barbara, CA, USA | California, USA
- `structured_data.address.addressCountry`: USA
- `structured_data.address.addressRegion`: CA

**Content** (9 fields):
- `keynote_topics`: [6 items] | [5 items]
- `speaking_programs`: [5 items] | [1 items]
- `speaking_programs[0].full_description`: We are all improvising. We face challenges, disruption, chan...
- `speaking_programs[0].key_takeaways`: [7 items] | [5 items]
- `speaking_programs[0].short_description`: We are all improvising. We face challenges, disruption, chan...
- `suggested_programs`: [5 items] | [1 items]
- `suggested_programs[0].audience_takeaways`: [4 items]
- `suggested_programs[0].full_description`: In a world characterized by rapid change and uncertainty, th...
- `suggested_programs[0].short_description`: We are all improvising. We face challenges, disruption, chan...

**Credentials** (1 fields):
- `awards`: No samples

#### Top-Level Fields (23)
```
_id (ObjectId)
additional_info (dict)
awards (list)
basic_info (dict)
biography (str)
books (list)
first_scraped_at (datetime)
images (list)
keynote_topics (list)
languages (list)
location (dict)
name (str)
profile_url (str)
scraped_at (datetime)
social_media (dict)
source (str)
speaker_id (str)
speaking_programs (list)
structured_data (dict)
suggested_programs (list)
testimonials (list)
videos (list)
why_choose (str)
```

#### Nested Field Structures

**additional_info** (6 nested fields):
- additional_info.downloads
- additional_info.downloads[0].text
- additional_info.downloads[0].url
- additional_info.meta_description
- additional_info.post_id
- ... and 1 more

**basic_info** (6 nested fields):
- basic_info.description
- basic_info.fee_range
- basic_info.image_url
- basic_info.topics
- basic_info.topics[0].name
- ... and 1 more

**books[0]** (3 nested fields):
- books[0].bestseller
- books[0].purchase_link
- books[0].title

**images[0]** (3 nested fields):
- images[0].source
- images[0].type
- images[0].url

**location** (1 nested fields):
- location.travels_from

**social_media** (3 nested fields):
- social_media.facebook
- social_media.instagram
- social_media.youtube

**speaking_programs[0]** (4 nested fields):
- speaking_programs[0].full_description
- speaking_programs[0].key_takeaways
- speaking_programs[0].short_description
- speaking_programs[0].title

**structured_data** (11 nested fields):
- structured_data.address
- structured_data.address.@type
- structured_data.address.addressCountry
- structured_data.address.addressLocality
- structured_data.address.addressRegion
- ... and 6 more

**suggested_programs[0]** (4 nested fields):
- suggested_programs[0].audience_takeaways
- suggested_programs[0].full_description
- suggested_programs[0].short_description
- suggested_programs[0].title

**testimonials[0]** (2 nested fields):
- testimonials[0].company
- testimonials[0].quote

**videos[0]** (6 nested fields):
- videos[0].embed_url
- videos[0].platform
- videos[0].thumbnail
- videos[0].title
- videos[0].video_id
- ... and 1 more

### eventraptor

**Collection**: `speakers`
**Documents**: 2,986
**Total Fields**: 21

#### Field Categories

**Social Media** (6 fields):
- `social_media`: No samples
- `social_media.facebook`: https://facebook.com/dannellaburnett | https://facebook.com/...
- `social_media.instagram`: https://instagram.com/janelle.anderson3 | https://instagram....
- `social_media.linkedin`: https://linkedin.com/in/steveeriksen | https://linkedin.com/...
- `social_media.twitter`: https://x.com/sheyennek | https://x.com/IntuitiveCheryl
- `social_media.youtube`: https://www.youtube.com/user/DianeRolstonCoaching | https://...

**Contact** (1 fields):
- `email`: Marc@MarcHaine.com | Success@TheSecretProfits.com

**Media** (1 fields):
- `profile_image`: https://app-eventraptor.b-cdn.net/storage/media/00/0001/0001...

**Metadata** (4 fields):
- `_id`: No samples
- `events[0].event_id`: 6 | 14
- `scraped_at`: No samples
- `speaker_id`: susan-jarema | dannella-burnett

**Credentials** (1 fields):
- `credentials`: Certified Virtual Expert™, Master's Degree in Organizational...

#### Top-Level Fields (13)
```
_id (ObjectId)
biography (str)
business_areas (list)
credentials (str)
email (str)
events (list)
name (str)
profile_image (str)
scraped_at (datetime)
social_media (dict)
speaker_id (str)
tagline (str)
url (str)
```

#### Nested Field Structures

**events[0]** (3 nested fields):
- events[0].event_id
- events[0].name
- events[0].url

**social_media** (5 nested fields):
- social_media.facebook
- social_media.instagram
- social_media.linkedin
- social_media.twitter
- social_media.youtube

### freespeakerbureau_scraper

**Collection**: `speakers_profiles`
**Documents**: 436
**Total Fields**: 42

#### Field Categories

**Social Media** (8 fields):
- `social_media`: No samples
- `social_media.facebook`: https://www.facebook.com/profile.php?id=61573023334183 | htt...
- `social_media.instagram`: https://www.instagram.com/jasonparmstrong/ | https://www.lin...
- `social_media.linkedin`: https://www.linkedin.com/in/tylercerny/ | https://www.linked...
- `social_media.tiktok`: https://www.tiktok.com/@williamcdavis64 | https://www.tiktok...
- `social_media.twitter`: https://x.com/ChiefJPStrong | https://x.com/WCD1964
- `social_media.whatsapp`: https://wa.me/12247151888 | https://whatsapp.com/channel/002...
- `social_media.youtube`: https://www.youtube.com/@WilliamDavis-v1g/videos | https://w...

**Contact** (10 fields):
- `contact_info`: No samples
- `contact_info.booking_url`: https://calendly.com/etccpa | https://nxtlevltrng.com/
- `contact_info.email`: info@lauracbulluck.com | contact.suited@gmail.com
- `contact_info.phone`: 9728162708 | 4048618740
- `contact_info.scheduling_url`: https://calendly.com/etccpa | https://calendly.com/tylercern...
- `contact_info.whatsapp`: https://wa.me/12247151888 | https://whatsapp.com/channel/002...
- `email_source`: mailto | text
- `has_phone_section`: True
- `phone_source`: general | href
- `website`: https://jasonparmstrong.com | https://www.tylercerny.com/

**Professional** (2 fields):
- `company`: 22nd Century Management | Luxlead
- `role`: Speaker / Presenter

**Media** (1 fields):
- `image_url`: https://www.freespeakerbureau.com/pictures/profile/pimage-56...

**Metadata** (5 fields):
- `_id`: No samples
- `created_at`: No samples
- `last_updated`: No samples
- `meta_description`: Connect with Maria Papacosta, Speaker / Presenter in Athens,...
- `scraped_at`: No samples

**Location** (4 fields):
- `city`: Durham | Troy
- `country`: Sweden | Australia
- `location`: Brookings, South Dakota | Reno, Nevada
- `state`: New York | South Dakota

**Credentials** (2 fields):
- `awards`: Maria’s personal branding workshop has been recognized as #2...
- `credentials`: [5 items] | [1 items]

#### Top-Level Fields (30)
```
_id (ObjectId)
areas_of_expertise (list)
awards (str)
biography (str)
city (str)
company (str)
contact_info (dict)
country (str)
created_at (datetime)
credentials (list)
email_source (str)
has_phone_section (bool)
image_url (str)
last_updated (datetime)
location (str)
member_level (str)
meta_description (str)
name (str)
phone_source (str)
previous_engagements (str)
profile_url (str)
role (str)
scraped_at (datetime)
social_media (dict)
speaker_onesheet_url (str)
speaker_since (int)
speaking_topics (list)
specialties (list)
state (str)
website (str)
```

#### Nested Field Structures

**contact_info** (5 nested fields):
- contact_info.booking_url
- contact_info.email
- contact_info.phone
- contact_info.scheduling_url
- contact_info.whatsapp

**social_media** (7 nested fields):
- social_media.facebook
- social_media.instagram
- social_media.linkedin
- social_media.tiktok
- social_media.twitter
- ... and 2 more

### leading_authorities

**Collection**: `speakers_final_details`
**Documents**: 1,230
**Total Fields**: 38

#### Field Categories

**Social Media** (2 fields):
- `social_media`: No samples
- `social_media.twitter`: https://twitter.com/GinnyClarke | https://twitter.com/DexBar...

**Contact** (1 fields):
- `speaker_website`: https://dexhuntertorricke.com/ | http://www.nicollewallace.c...

**Professional** (5 fields):
- `books_and_publications[0].title`: Madam President: A Novel | Extreme You: Step Up. Stand Out. ...
- `job_title`: N/A | Political Analyst, New York Times Best-Selling Author,...
- `recent_news[0].title`: Gen. Stanley McChrystal discusses leadership with President ...
- `topics[0].title`: The World in Sight: A Guide to the Key Technological Trends ...
- `videos[0].title`: Ginny Clarke: Leading Workplace Transformation | Sarah Robb ...

**Media** (8 fields):
- `books_and_publications[0].image_url`: https://www.leadingauthorities.com/sites/default/files/image...
- `download_profile_link`: https://www.leadingauthorities.com/print/view/pdf/speaker/bi...
- `download_topics_link`: https://www.leadingauthorities.com/print/view/pdf/speaker/to...
- `speaker_image_url`: https://www.leadingauthorities.com/sites/default/files/style...
- `videos`: [6 items] | [7 items]
- `videos[0].thumbnail_url`: https://play.vidyard.com/j1EAgGsudKQdzFZyPmC71v.jpg | https:...
- `videos[0].video_id`: 6B994S4fWP25HkuBYuoYJV | 3JbjS1GQk2MoHU1DLhG16o
- `videos[0].video_page_url`: https://www.leadingauthorities.com/speakers/video/stanley-mc...

**Metadata** (1 fields):
- `_id`: No samples

#### Top-Level Fields (17)
```
_id (ObjectId)
books_and_publications (list)
client_testimonials (list)
description (str)
download_profile_link (str)
download_topics_link (str)
job_title (str)
name (str)
recent_news (list)
social_media (dict)
speaker_fees (dict)
speaker_image_url (str)
speaker_page_url (str)
speaker_website (NoneType, str)
topics (list)
topics_and_types (list)
videos (list)
```

#### Nested Field Structures

**books_and_publications[0]** (3 nested fields):
- books_and_publications[0].image_url
- books_and_publications[0].title
- books_and_publications[0].url

**client_testimonials[0]** (2 nested fields):
- client_testimonials[0].author
- client_testimonials[0].quote

**recent_news[0]** (2 nested fields):
- recent_news[0].title
- recent_news[0].url

**social_media** (1 nested fields):
- social_media.twitter

**speaker_fees** (5 nested fields):
- speaker_fees.Asia
- speaker_fees.Europe
- speaker_fees.Local
- speaker_fees.US East
- speaker_fees.US West

**topics[0]** (2 nested fields):
- topics[0].description
- topics[0].title

**topics_and_types[0]** (2 nested fields):
- topics_and_types[0].name
- topics_and_types[0].url

**videos[0]** (4 nested fields):
- videos[0].thumbnail_url
- videos[0].title
- videos[0].video_id
- videos[0].video_page_url

### sessionize_scraper

**Collection**: `speaker_profiles`
**Documents**: 12,827
**Total Fields**: 117

#### Field Categories

**Social Media** (88 fields):
- `professional_info.social_links`: No samples
- `professional_info.social_links.academia`: No samples
- `professional_info.social_links.academia.profile`: Academia
- `professional_info.social_links.academia.url`: https://karachi.academia.edu/AbrarHussain1
- `professional_info.social_links.amazon_author`: No samples
- `professional_info.social_links.amazon_author.books`: Blog
- `professional_info.social_links.amazon_author.url`: https://www.amazon.com/Supply-Chain-Software-Security-Applic...
- `professional_info.social_links.behance`: No samples
- `professional_info.social_links.behance.portfolio`: Behance
- `professional_info.social_links.behance.url`: https://www.behance.net/zachary-zbranak | https://www.behanc...
- ... and 78 more

**Professional** (4 fields):
- `professional_info`: No samples
- `professional_info.expertise_areas`: [5 items] | [1 items]
- `professional_info.topics`: [5 items] | [4 items]
- `speaking_history.sessions[0].title`: Custom-Built CI/CD with Pragmatic Approach for Cloud Era | U...

**Metadata** (5 fields):
- `_id`: No samples
- `metadata`: No samples
- `metadata.run_id`: 20250711_140333
- `metadata.scraped_at`: 2025-07-11T14:03:39.171859 | 2025-07-11T14:03:42.843727
- `metadata.source_categories`: [1 items] | [2 items]

**Location** (2 fields):
- `basic_info.location`: Ibadan, Nigeria | Raleigh, North Carolina, United States
- `speaking_history.events[0].location`: Chicago, Illinois, United States | Malmö, Sweden

#### Top-Level Fields (6)
```
_id (ObjectId)
basic_info (dict)
metadata (dict)
professional_info (dict)
speaking_history (dict)
username (str)
```

#### Nested Field Structures

**basic_info** (7 nested fields):
- basic_info.bio
- basic_info.location
- basic_info.name
- basic_info.profile_picture
- basic_info.tagline
- ... and 2 more

**metadata** (3 nested fields):
- metadata.run_id
- metadata.scraped_at
- metadata.source_categories

**professional_info** (90 nested fields):
- professional_info.expertise_areas
- professional_info.social_links
- professional_info.social_links.academia
- professional_info.social_links.academia.profile
- professional_info.social_links.academia.url
- ... and 85 more

**speaking_history** (11 nested fields):
- speaking_history.events
- speaking_history.events[0].date
- speaking_history.events[0].is_sessionize_event
- speaking_history.events[0].location
- speaking_history.events[0].name
- ... and 6 more

### speakerhub_scraper

**Collection**: `speaker_details`
**Documents**: 19,876
**Total Fields**: 61

#### Field Categories

**Professional** (7 fields):
- `company`: Angie Wisdom Coaching & Consulting | The Standards Guy
- `job_title`: Certified Life & Business Coach | Founder
- `past_talks[0].title`: Communication Faux Pas that Kill Efficiency | Legal Terms, t...
- `presentations[0].title`: CEO / Founder of Speak Up Business English Solutions | Deleg...
- `professional_title`: FIML | J.D.
- `publications[0].title`: Are We From Different Planets? | Head + Heart + Hand: The Fo...
- `workshops[0].title`: Broadening Your Cultural Lens: Social Determinants and Healt...

**Media** (1 fields):
- `videos`: No samples

**Metadata** (4 fields):
- `_id`: No samples
- `last_updated`: No samples
- `scraped_at`: No samples
- `uid`: 4095 | 83869

**Location** (5 fields):
- `city`: Tarnaveni | Newport Beach
- `country`: United States | United Kingdom
- `past_talks[0].location`: Virtual | Sussex, Surrey, Kent
- `state_province`: California | New York
- `timezone`: Africa/Algiers | Asia/Brunei

**Content** (10 fields):
- `past_talks`: [7 items] | [1 items]
- `past_talks[0].date`: April 14, 2024 | September 19, 2018
- `past_talks[0].description`: No samples
- `past_talks[0].event_name`: Emily Reed Wedding Singer | Divorce Super Summit
- `presentations`: [7 items] | [5 items]
- `presentations[0].description`: Be Yourself: No One Does it Better Than You
Having a unique ...
- `total_talks`: 4 | 2
- `workshops`: [1 items] | [2 items]
- `workshops[0].description`: Have a special retreat idea? We can make it happen, simply w...
- `workshops[0].duration`: Full day(View workshop agenda) | 2 hours(View workshop agend...

**Credentials** (6 fields):
- `awards`: No samples
- `certifications`: [1 items]
- `education`: [1 items]
- `education[0].degree`: Masters Degree - Counselling Psychology | Banking and Financ...
- `education[0].institution`: IUP Banque Finance Assurance France | UNBC
- `education[0].year`: 2010-2012 | 1998-2022

#### Top-Level Fields (40)
```
_id (ObjectId)
affiliations (list)
awards (list)
bio_summary (str)
certifications (list)
city (str)
company (str)
competencies (dict)
country (str)
education (list)
event_types (list)
first_name (str)
full_bio (str)
job_title (str)
languages (list)
last_name (str)
last_updated (datetime)
name (str)
past_talks (list)
presentations (list)
press_kit_url (str)
professional_title (str)
profile_picture_url (str)
profile_url (str)
pronouns (str)
publications (list)
recommendations_count (int)
scraped_at (datetime)
scraping_status (str)
speaker_fees (list)
state_province (str)
testimonials (list)
timezone (str)
topic_categories (list)
topics (list)
total_talks (int)
uid (str)
videos (list)
why_choose_me (str)
workshops (list)
```

#### Nested Field Structures

**education[0]** (3 nested fields):
- education[0].degree
- education[0].institution
- education[0].year

**past_talks[0]** (5 nested fields):
- past_talks[0].date
- past_talks[0].description
- past_talks[0].event_name
- past_talks[0].location
- past_talks[0].title

**presentations[0]** (2 nested fields):
- presentations[0].description
- presentations[0].title

**publications[0]** (5 nested fields):
- publications[0].date
- publications[0].publication
- publications[0].title
- publications[0].type
- publications[0].url

**speaker_fees[0]** (3 nested fields):
- speaker_fees[0].event_description
- speaker_fees[0].event_type
- speaker_fees[0].fee

**workshops[0]** (3 nested fields):
- workshops[0].description
- workshops[0].duration
- workshops[0].title

### thespeakerhandbook_scraper

**Collection**: `speaker_profiles`
**Documents**: 3,510
**Total Fields**: 56

#### Field Categories

**Social Media** (1 fields):
- `social_links`: [5 items] | [1 items]

**Contact** (3 fields):
- `contact`: No samples
- `contact.email`: patrick@universalspeakergroup.com
- `website`: https://www.jacovangass.com/ | http://www.sonnykalar.com/

**Professional** (10 fields):
- `job_title`: Business Leader | Social Impact Leader
- `json_ld_talks[0].title`: Lead Like a Bonobo, not a Baboon: Smarter leadership lessons...
- `page_title`: Karen Eber - Official Speaker Bio | Sonny Kalar - Official S...
- `talks[0].title`: Telling Stories that Inform, Influence, and Inspire | Women’...
- `testimonials[0].organization`: Network Rail | Coca-Cola
- `video_categories.Media, podcast appearances and interviews[0].title`: The Plight Of Poor Planning with Greg Rutherford MBE & Andre...
- `video_categories.Showreels[0].title`: Anna Gumbau - SHOWREEL - Multilingual event moderator & Mast...
- `video_categories.Speaking videos[0].title`: "Leading With Influence" full keynote | How to Build Confide...
- `video_categories.Testimonial videos[0].title`: Meet Simon T Bailey (Bureau-Friendly) | Jon Macks - Speaker ...
- `videos[0].title`: Karen Eber Speaker Reel | Jaco van Gass on finding hope in t...

**Media** (15 fields):
- `download_profile_link`: https://thespeakerhandbook.com/speaker/anna-gumbau/pdf | htt...
- `image_gallery`: No samples
- `image_url_hd`: https://thespeakerhandbook.com/wp-content/uploads/prf_pho/Gr...
- `video_categories`: No samples
- `video_categories.Media, podcast appearances and interviews`: [7 items] | [5 items]
- `video_categories.Media, podcast appearances and interviews[0].url`: https://www.youtube.com/embed/qO6BtwQR334?feature=oembed | h...
- `video_categories.Showreels`: [1 items] | [2 items]
- `video_categories.Showreels[0].url`: https://www.youtube.com/embed/h4USJ_2e2hc?feature=oembed | h...
- `video_categories.Speaking videos`: [5 items] | [1 items]
- `video_categories.Speaking videos[0].url`: https://www.youtube.com/embed/p616q1ePJaU?feature=oembed | h...
- ... and 5 more

**Metadata** (4 fields):
- `_id`: No samples
- `meta_description`: View the official keynote speaker bio of Greg Rutherford, Ol...
- `scraped_at`: No samples
- `speaker_id`: 40979 | 41011

**Content** (4 fields):
- `json_ld_talks`: [7 items] | [10 items]
- `json_ld_talks[0].description`: When the tides of change rise like a storm, most leaders rea...
- `talks`: [7 items] | [10 items]
- `talks[0].description`: When the tides of change rise like a storm, most leaders rea...

**Credentials** (1 fields):
- `awards`: No samples

#### Top-Level Fields (32)
```
_id (ObjectId)
awards (list)
biography (str)
biography_highlights (list)
books (list)
contact (dict)
display_name (str)
download_profile_link (NoneType, str)
engagement_types (list)
fees (dict)
gender (str)
image_gallery (list)
image_url_hd (str)
job_title (str)
json_ld_talks (list)
knows_about (str)
languages (list)
meta_description (str)
nationality (str)
page_title (str)
profile_url (str)
scrape_status (str)
scraped_at (datetime)
social_links (list)
speaker_id (str)
talks (list)
testimonials (list)
topics (list)
travels_from (str)
video_categories (dict)
videos (list)
website (NoneType, str)
```

#### Nested Field Structures

**contact** (1 nested fields):
- contact.email

**json_ld_talks[0]** (2 nested fields):
- json_ld_talks[0].description
- json_ld_talks[0].title

**talks[0]** (2 nested fields):
- talks[0].description
- talks[0].title

**testimonials[0]** (4 nested fields):
- testimonials[0].content
- testimonials[0].organization
- testimonials[0].person
- testimonials[0].position

**video_categories** (12 nested fields):
- video_categories.Media, podcast appearances and interviews
- video_categories.Media, podcast appearances and interviews[0].title
- video_categories.Media, podcast appearances and interviews[0].url
- video_categories.Showreels
- video_categories.Showreels[0].title
- ... and 7 more

**videos[0]** (3 nested fields):
- videos[0].platform
- videos[0].title
- videos[0].url

## Cross-Database Field Analysis

### Common Fields (present in all 9 databases)
- _id

### Unique Fields by Database

**a_speakers** (10 unique fields):
- average_rating
- keynotes
- keynotes[0].description
- keynotes[0].id
- keynotes[0].title
- reviews[0].author_organization
- reviews[0].author_title
- reviews[0].review_text
- total_reviews
- why_book_points

**allamericanspeakers** (11 unique fields):
- categories
- fee_range.live_event
- fee_range.virtual_event
- rating
- rating.average_rating
- rating.review_count
- reviews[0].author
- reviews[0].text
- speaking_topics[0].description
- speaking_topics[0].title
- ... and 1 more

**bigspeak_scraper** (51 unique fields):
- additional_info
- additional_info.downloads
- additional_info.downloads[0].text
- additional_info.downloads[0].url
- additional_info.meta_description
- additional_info.post_id
- additional_info.virtual_capable
- basic_info.description
- basic_info.fee_range
- basic_info.image_url
- ... and 41 more

**eventraptor** (8 unique fields):
- business_areas
- email
- events
- events[0].event_id
- events[0].name
- events[0].url
- profile_image
- tagline

**freespeakerbureau_scraper** (20 unique fields):
- areas_of_expertise
- contact_info
- contact_info.booking_url
- contact_info.email
- contact_info.phone
- contact_info.scheduling_url
- contact_info.whatsapp
- created_at
- email_source
- has_phone_section
- ... and 10 more

**leading_authorities** (26 unique fields):
- books_and_publications
- books_and_publications[0].image_url
- books_and_publications[0].title
- books_and_publications[0].url
- client_testimonials
- client_testimonials[0].author
- client_testimonials[0].quote
- download_topics_link
- recent_news
- recent_news[0].title
- ... and 16 more

**sessionize_scraper** (115 unique fields):
- basic_info.bio
- basic_info.location
- basic_info.name
- basic_info.profile_picture
- basic_info.tagline
- basic_info.url
- basic_info.username
- metadata
- metadata.run_id
- metadata.scraped_at
- ... and 105 more

**speakerhub_scraper** (45 unique fields):
- affiliations
- bio_summary
- certifications
- competencies
- education
- education[0].degree
- education[0].institution
- education[0].year
- event_types
- first_name
- ... and 35 more

**thespeakerhandbook_scraper** (38 unique fields):
- biography_highlights
- contact
- contact.email
- display_name
- engagement_types
- fees
- gender
- image_gallery
- image_url_hd
- json_ld_talks
- ... and 28 more

## Key Findings

### Social Media Field Coverage
- **sessionize_scraper**: 88 social media fields
- **freespeakerbureau_scraper**: 8 social media fields
- **eventraptor**: 6 social media fields
- **bigspeak_scraper**: 4 social media fields
- **leading_authorities**: 2 social media fields
- **thespeakerhandbook_scraper**: 1 social media fields

### Contact Information Coverage
- **freespeakerbureau_scraper**: 10 contact fields
- **thespeakerhandbook_scraper**: 3 contact fields
- **bigspeak_scraper**: 2 contact fields
- **eventraptor**: 1 contact fields
- **leading_authorities**: 1 contact fields

## Recommendations for Standardization

1. **Priority Fields for Mapping**:
   - Social media links (varies significantly across sources)
   - Contact information (email, phone, website)
   - Professional credentials (awards, certifications)
   - Media assets (images, videos, PDFs)

2. **Data Quality Considerations**:
   - Standardize date formats across sources
   - Normalize location data (city, state, country)
   - Deduplicate social media URLs
   - Handle missing fields gracefully

3. **Schema Design Suggestions**:
   - Create unified social_media object
   - Standardize contact information structure
   - Preserve source-specific metadata
   - Implement field-level data quality scores