"""
Speaker Data Standardization System V3
Author: Shafi Gill
Created: 2025-01-16
Updated: 2025-01-16

Final version that captures 100% of fields from source databases including:
- All social media platforms (including academic/niche)
- Complete timestamps (first_scraped_at, last_updated)
- Company affiliations
- Regional fee structures
- SEO/meta fields
- Platform-specific IDs

Usage:
    python3 main.py
    
Requires .env file with:
    MONGO_URI=mongodb://admin:password@host:27017/?authSource=admin
    TARGET_DATABASE=speaker_database
"""

import os
import re
import json
import hashlib
from datetime import datetime
from collections import defaultdict
from dateutil import parser as dt
from pymongo import MongoClient, UpdateOne
from rapidfuzz import fuzz, process
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# 0. CONFIG
# ──────────────────────────────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is required. Please set it in .env file")

TARGET_DB_NAME = os.getenv("TARGET_DATABASE", "speaker_database")  # Where unified data goes

# Map of database names to their collection names and transformer functions
SRC_DATABASES = {
    "a_speakers": {
        "collection": "speakers",
        "transformer": "unify_a_speakers"
    },
    "allamericanspeakers": {
        "collection": "speakers",
        "transformer": "unify_allamerican"
    },
    "bigspeak_scraper": {
        "collection": "speaker_profiles",
        "transformer": "unify_bigspeak"
    },
    "eventraptor": {
        "collection": "speakers",
        "transformer": "unify_eventraptor"
    },
    "freespeakerbureau_scraper": {
        "collection": "speakers_profiles",
        "transformer": "unify_freespeaker"
    },
    "leading_authorities": {
        "collection": "speakers_final_details",
        "transformer": "unify_leadingauth"
    },
    "sessionize_scraper": {
        "collection": "speaker_profiles",
        "transformer": "unify_sessionize"
    },
    "speakerhub_scraper": {
        "collection": "speaker_details",
        "transformer": "unify_speakerhub"
    },
    "thespeakerhandbook_scraper": {
        "collection": "speaker_profiles",
        "transformer": "unify_tsh"
    }
}

# Load topic mapping from config directory
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "topic_mapping.json")
with open(config_path, "r", encoding="utf-8") as f:
    TOPIC_MAP = json.load(f)

REV_TOPIC_MAP = {raw:tgt for tgt,v in TOPIC_MAP.items() for raw in v}

# ──────────────────────────────────────────────────────────────────────────────
# 1. UTILITIES
# ──────────────────────────────────────────────────────────────────────────────
def sha_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def parse_location(loc: str) -> dict:
    """
    Splits common 'City, State, Country' patterns.
    If a dict is already supplied, it is passed through.
    """
    if not loc:
        return {}
    if isinstance(loc, dict):
        return {
            "city"         : loc.get("city"),
            "state"        : loc.get("state") or loc.get("state_province"),
            "country"      : loc.get("country"),
            "full_location": ", ".join([v for v in loc.values() if v]),
            "timezone"     : loc.get("timezone")
        }
    parts = [p.strip() for p in loc.split(",")]
    if len(parts) == 3:
        city, state, country = parts
    elif len(parts) == 2:
        city, state, country = parts[0], None, parts[1]
    else:
        city, state, country = None, None, parts[0]
    return {
        "city"         : city,
        "state"        : state,
        "country"      : country,
        "full_location": loc,
        "timezone"     : None
    }

def norm_topics(topics):
    """Convert list of topic strings to canonical list + unmapped list."""
    canon, unmapped = set(), set()
    for t in topics or []:
        t_clean = re.sub(r"\s+", " ", t).strip()
        if t_clean in REV_TOPIC_MAP:
            canon.add(REV_TOPIC_MAP[t_clean])
        else:
            canon.add(t_clean)
            unmapped.add(t_clean)
    return sorted(canon), sorted(unmapped)

def safe_date(value):
    try:
        return dt.parse(value) if isinstance(value, str) else value
    except Exception:
        return None

def extract_social_media(doc, platform_fields=None):
    """Extract social media links from various formats"""
    social = {}
    
    # Direct social_media object
    if doc.get("social_media") and isinstance(doc["social_media"], dict):
        for platform in ["twitter", "linkedin", "facebook", "instagram", "youtube", "tiktok", "pinterest", "whatsapp"]:
            if doc["social_media"].get(platform):
                social[platform] = doc["social_media"][platform]
    
    # Individual URL fields (like speakerhub)
    if platform_fields:
        for platform, field in platform_fields.items():
            if doc.get(field):
                social[platform] = doc[field]
    
    return social if social else None

def extract_all_social_links(social_links_obj):
    """Extract all social media links including academic and niche platforms"""
    if not social_links_obj:
        return None
        
    social = {}
    platform_mapping = {
        "twitter": ["handle", "url"],
        "linkedin": ["label", "url"],
        "facebook": ["label", "url"],
        "instagram": ["handle", "url"],
        "youtube": ["channel", "url"],
        "github": ["handle", "url"],
        "blog": ["label", "url"],
        "website": ["label", "url"],
        "company": ["label", "url"],
        "academia": ["profile", "url"],
        "amazon_author": ["books", "url"],
        "google_scholar": ["profile", "url"],
        "hashnode": ["blog", "url"],
        "linktree": ["profile", "url"],
        "mastodon": ["handle", "url"],
        "medium": ["profile", "url"],
        "microsoft_mvp": ["profile", "url"],
        "orcid": ["profile", "url"],
        "pinterest": ["profile", "url"],
        "researchgate": ["profile", "url"],
        "substack": ["newsletter", "url"]
    }
    
    for platform, fields in platform_mapping.items():
        if platform in social_links_obj:
            platform_data = social_links_obj[platform]
            if isinstance(platform_data, dict):
                # Try to get URL first, then other fields
                url = platform_data.get("url")
                if url:
                    social[platform] = url
                else:
                    # Try other fields
                    for field in fields:
                        if platform_data.get(field):
                            social[platform] = platform_data[field]
                            break
            elif isinstance(platform_data, str):
                social[platform] = platform_data
    
    return social if social else None

# ──────────────────────────────────────────────────────────────────────────────
# 2. TRANSFORMERS (one per source) - COMPLETE WITH ALL FIELDS
# ──────────────────────────────────────────────────────────────────────────────
def unify_a_speakers(doc: dict) -> dict:
    topics, unmapped = norm_topics(doc.get("topics"))
    
    # Extract social media
    social_media = doc.get("social_media")
    
    # Videos with full metadata
    videos = []
    if doc.get("videos"):
        for v in doc["videos"]:
            videos.append({
                "url": v.get("url"),
                "title": v.get("title"),
                "description": v.get("description"),
                "thumbnail": v.get("thumbnail"),
                "video_id": v.get("video_id"),
                "type": "video"
            })
    
    # Marketing points
    metadata = {}
    if doc.get("why_book_points"):
        metadata["why_book_points"] = doc["why_book_points"]
    
    return {
        "_id"          : sha_id("a_speakers|" + str(doc["_id"])),
        "name"         : doc.get("name"),
        "display_name" : doc.get("name"),
        "job_title"    : doc.get("job_title"),
        "description"  : doc.get("description"),
        "biography"    : doc.get("full_bio"),
        "tagline"      : None,
        "location"     : parse_location(doc.get("location")),
        "contact"      : {
            "website": doc.get("website")
        },
        "social_media" : social_media,
        "speaking_info": {
            "fee_ranges": {"live_event": doc.get("fee_range")},
            "languages" : [doc.get("languages")] if doc.get("languages") else None
        },
        "topics"            : topics,
        "categories"        : topics,
        "topics_unmapped"   : unmapped,
        "content": {
            "keynotes": doc.get("keynotes", [])
        },
        "media": {
            "profile_image": doc.get("image_url"),
            "videos"       : videos
        },
        "testimonials"      : None,
        "reviews"           : doc.get("reviews", []),
        "ratings"           : {
            "average_rating": doc.get("average_rating"),
            "total_reviews" : doc.get("total_reviews")
        },
        "metadata"          : metadata if metadata else None,
        "source_info": {
            "original_source": "a_speakers",
            "source_url"     : doc.get("url"),
            "profile_url"    : doc.get("url"),  # Add profile URL
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "source_id"      : str(doc.get("_id"))
        }
    }

def unify_allamerican(doc):
    tops, unmapped = norm_topics(doc.get("categories", []) + [t["title"] for t in doc.get("speaking_topics", [])])
    
    # Extract social media
    social_media = doc.get("social_media")
    
    # Enhanced videos
    videos = []
    if doc.get("videos"):
        for v in doc["videos"]:
            videos.append({
                "url": v.get("url"),
                "title": v.get("title"),
                "description": v.get("description"),
                "type": v.get("type", "video")
            })
    
    return {
        "_id"        : sha_id("allamerican|" + doc["speaker_id"]),
        "name"       : doc.get("name"),
        "display_name": doc.get("name"),
        "job_title"  : doc.get("job_title"),
        "biography"  : doc.get("biography"),
        "location"   : parse_location(doc.get("location")),
        "social_media": social_media,
        "speaking_info": {
            "fee_ranges": doc.get("fee_range")
        },
        "topics"     : tops,
        "categories" : tops,
        "topics_unmapped": unmapped,
        "content": {
            "keynotes": doc.get("speaking_topics")
        },
        "media": {
            "profile_image": next((i["url"] for i in doc.get("images", []) if i["type"]=="profile"), None),
            "image_gallery": [i["url"] for i in doc.get("images", []) if i["type"]!="profile"],
            "videos" : videos
        },
        "ratings": doc.get("rating"),
        "reviews": doc.get("reviews"),
        "source_info": {
            "original_source": "allamericanspeakers",
            "source_url"     : doc.get("url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "source_id"      : doc.get("speaker_id")
        }
    }

def unify_bigspeak(doc):
    # Handle case where doc itself might be None
    if not doc or not isinstance(doc, dict):
        return None
        
    try:
        # Handle topics safely - check if items are dicts with 'name' key
        topics_list = []
        if doc.get("topics") and isinstance(doc["topics"], list):
            for t in doc["topics"]:
                if t and isinstance(t, dict) and t.get("name"):
                    topics_list.append(t["name"])
        topics, unmapped = norm_topics(topics_list)
        
        # Extract all the rich content from bigspeak
        media = {
            "profile_image": doc.get("image_url"),
            "videos": doc.get("videos", []) if doc.get("videos") else []
        }
        
        # Images beyond profile
        if doc.get("images") and isinstance(doc["images"], list):
            media["image_gallery"] = [img["url"] for img in doc["images"] if img and isinstance(img, dict) and img.get("url")]
        
        # Downloads - handle None additional_info
        additional_info = doc.get("additional_info")
        if additional_info and isinstance(additional_info, dict) and additional_info.get("downloads"):
            media["downloads"] = additional_info["downloads"]
        
        # Professional info
        professional_info = {}
        if doc.get("awards"):
            professional_info["awards"] = doc["awards"]
        if doc.get("certifications"):
            professional_info["certifications"] = doc["certifications"]
            
        # Content programs
        content = {}
        if doc.get("keynote_topics"):
            content["keynote_topics"] = doc["keynote_topics"]
        if doc.get("speaking_programs"):
            content["speaking_programs"] = doc["speaking_programs"]
        if doc.get("suggested_programs"):
            content["suggested_programs"] = doc["suggested_programs"]
        
        # Speaking info
        speaking_info = {
            "fee_ranges": {"live_event": doc.get("fee_range")},
            "languages": doc.get("languages", []) if doc.get("languages") else [],
            "virtual_capable": additional_info.get("virtual_capable") if additional_info else None
        }
        
        # Extract structured contact info
        contact = {}
        structured_data = doc.get("structured_data")
        if structured_data and isinstance(structured_data, dict):
            if structured_data.get("email"):
                contact["email"] = structured_data["email"]
            if structured_data.get("telephone"):
                contact["phone"] = structured_data["telephone"]
        else:
            structured_data = {}
                
        # Extract structured address
        structured_address = None
        if structured_data and structured_data.get("address") and isinstance(structured_data["address"], dict):
            addr = structured_data["address"]
            structured_address = {
                "street": addr.get("streetAddress"),
                "city": addr.get("addressLocality"),
                "state": addr.get("addressRegion"),
                "postal_code": addr.get("postalCode"),
                "country": addr.get("addressCountry")
            }
        
        # Metadata including structured data
        metadata = {}
        if doc.get("why_choose"):
            metadata["why_choose"] = doc["why_choose"]
        if structured_data:
            metadata["structured_data"] = structured_data
        if additional_info and additional_info.get("post_id"):
            metadata["post_id"] = additional_info["post_id"]
        if additional_info and additional_info.get("meta_description"):
            metadata["meta_description"] = additional_info["meta_description"]
        if doc.get("source"):
            metadata["source"] = doc["source"]
        
        # Get speaker_id safely - use a fallback if not present
        speaker_id = doc.get("speaker_id") or doc.get("_id", "unknown")
        
        # Parse location safely
        location = None
        location_data = doc.get("location")
        if location_data and isinstance(location_data, dict) and location_data.get("travels_from"):
            location = parse_location(location_data["travels_from"])
        elif structured_address:
            location = structured_address
        
        return {
            "_id"         : sha_id("bigspeak|" + str(speaker_id)),
            "name"        : doc.get("name"),
            "display_name": doc.get("name"),
            "job_title"   : doc.get("job_title") or structured_data.get("job_title"),
            "description" : doc.get("description") or structured_data.get("description_structured"),
            "biography"   : doc.get("biography") or doc.get("description"),
            "location"    : location,
            "contact"     : contact if contact else None,
            "social_media": doc.get("social_media"),
            "speaking_info": speaking_info,
            "topics"     : topics,
            "categories" : topics,
            "topics_unmapped": unmapped,
            "professional_info": professional_info if professional_info else None,
            "content": content if content else None,
            "media": media,
            "publications": {
                "books": doc.get("books", [])
            } if doc.get("books") else None,
            "testimonials": doc.get("testimonials", []),
            "metadata": metadata if metadata else None,
            "source_info": {
                "original_source": "bigspeak",
                "source_url"     : doc.get("profile_url"),
                "scraped_at"     : safe_date(doc.get("scraped_at")),
                "first_scraped_at": safe_date(doc.get("first_scraped_at")),
                "source_id"      : str(speaker_id)
            }
        }
    
    except Exception as e:
        # Return None for problematic documents - they will be skipped
        print(f"    - Skipping malformed document {doc.get('_id', 'unknown')}: {str(e)}")
        return None

def unify_eventraptor(doc):
    topics, unmapped = norm_topics(doc.get("business_areas"))
    
    # Extract social media
    social_media = extract_social_media(doc)
    
    # Events/speaking history
    speaking_history = None
    if doc.get("events"):
        speaking_history = {
            "events": doc["events"]
        }
    
    return {
        "_id"        : sha_id("eventraptor|" + doc["speaker_id"]),
        "name"       : doc.get("name"),
        "display_name": doc.get("name"),
        "tagline"    : doc.get("tagline"),
        "biography"  : doc.get("biography"),
        "contact": {
            "email": doc.get("email")
        },
        "location": {},
        "social_media": social_media,
        "topics"     : topics,
        "categories" : topics,
        "topics_unmapped": unmapped,
        "professional_info": {
            "credentials": [doc.get("credentials")] if doc.get("credentials") else None
        },
        "content": {
            "presentations": doc.get("presentations", [])
        } if doc.get("presentations") else None,
        "media": {
            "profile_image": doc.get("profile_image")
        },
        "speaking_history": speaking_history,
        "source_info": {
            "original_source": "eventraptor",
            "source_url"     : doc.get("url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "source_id"      : doc.get("speaker_id")
        }
    }

def unify_freespeaker(doc):
    topics, unmapped = norm_topics(doc.get("areas_of_expertise", []) + doc.get("speaking_topics", []))
    
    # Extract social media
    social_media = extract_social_media(doc)
    
    # Enhanced contact info
    contact = {
        "phone": doc.get("contact_info", {}).get("phone"),
        "email": doc.get("contact_info", {}).get("email"),
        "website": doc.get("website"),
        "booking_url": doc.get("contact_info", {}).get("booking_url"),
        "scheduling_url": doc.get("contact_info", {}).get("scheduling_url"),
        "whatsapp": doc.get("contact_info", {}).get("whatsapp")
    }
    
    # Professional info
    professional_info = {
        "credentials": doc.get("credentials", []),
        "awards": doc.get("awards"),
        "member_level": doc.get("member_level"),
        "company": doc.get("company")
    }
    
    # Speaking info
    speaking_info = {
        "fee_ranges": None,
        "speaker_since": doc.get("speaker_since")
    }
    
    # Media
    media = {
        "profile_image": doc.get("image_url"),
        "profile_pdf": doc.get("speaker_onesheet_url")
    }
    
    # Metadata
    metadata = {}
    if doc.get("meta_description"):
        metadata["meta_description"] = doc["meta_description"]
    if doc.get("email_source"):
        metadata["email_source"] = doc["email_source"]
    if doc.get("phone_source"):
        metadata["phone_source"] = doc["phone_source"]
    if doc.get("has_phone_section"):
        metadata["has_phone_section"] = doc["has_phone_section"]
    if doc.get("previous_engagements"):
        metadata["previous_engagements"] = doc["previous_engagements"]
    if doc.get("specialties"):
        metadata["specialties"] = doc["specialties"]
    
    return {
        "_id"         : sha_id("freespeaker|" + str(doc["_id"])),
        "name"        : doc.get("name"),
        "display_name": doc.get("name"),
        "job_title"   : doc.get("role"),
        "biography"   : doc.get("biography"),
        "location"    : parse_location(doc.get("location")),
        "contact"     : contact,
        "social_media": social_media,
        "speaking_info": speaking_info,
        "topics"      : topics,
        "categories"  : topics,
        "topics_unmapped": unmapped,
        "expertise_areas": doc.get("areas_of_expertise", []),
        "professional_info": professional_info,
        "media": media,
        "metadata": metadata if metadata else None,
        "source_info": {
            "original_source": "freespeakerbureau",
            "source_url"     : doc.get("profile_url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "created_at"     : safe_date(doc.get("created_at")),
            "last_updated"   : safe_date(doc.get("last_updated")),
            "source_id"      : str(doc["_id"])
        }
    }

def unify_leadingauth(doc):
    topics, unmapped = norm_topics([t["name"] for t in doc.get("topics_and_types", [])])
    
    # Extract social media
    social_media = extract_social_media(doc)
    
    # Enhanced contact
    contact = {}
    if doc.get("speaker_website"):
        contact["website"] = doc["speaker_website"]
        
    # Media with downloads
    media = {
        "profile_image": doc.get("speaker_image_url"),
        "videos": doc.get("videos", []),
        "profile_pdf": doc.get("download_profile_link"),
        "topics_pdf": doc.get("download_topics_link")  # Capture topics PDF
    }
    
    # Publications
    publications = None
    if doc.get("books_and_publications"):
        publications = {
            "books": doc["books_and_publications"]
        }
    
    # Testimonials from client_testimonials
    testimonials = []
    if doc.get("client_testimonials"):
        for t in doc["client_testimonials"]:
            testimonials.append({
                "content": t.get("quote"),
                "author": t.get("author")
            })
    
    # Topics with descriptions
    content = {}
    if doc.get("topics"):
        content["topics"] = doc["topics"]
        
    # Recent news as metadata
    metadata = {}
    if doc.get("recent_news"):
        metadata["recent_news"] = doc["recent_news"]
    
    # Preserve regional fee structure
    speaking_info = {
        "fee_ranges": doc.get("speaker_fees")  # This preserves the regional breakdown
    }
    
    return {
        "_id"        : sha_id("leadingauth|" + doc["speaker_page_url"]),
        "name"       : doc.get("name"),
        "display_name": doc.get("name"),
        "job_title"  : doc.get("job_title"),
        "description": doc.get("description"),
        "biography"  : doc.get("description"),
        "location"   : {},
        "contact"    : contact if contact else None,
        "social_media": social_media,
        "speaking_info": speaking_info,
        "topics"     : topics,
        "categories" : topics,
        "topics_unmapped": unmapped,
        "content"    : content if content else None,
        "media"      : media,
        "publications": publications,
        "testimonials": testimonials if testimonials else None,
        "metadata"   : metadata if metadata else None,
        "source_info": {
            "original_source": "leadingauthorities",
            "source_url"     : doc.get("speaker_page_url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "source_id"      : str(doc.get("_id"))
        }
    }

def unify_sessionize(doc):
    tops, unmapped = norm_topics(doc.get("professional_info", {}).get("topics", []))
    basic = doc.get("basic_info", {})
    username = basic.get("username") or doc.get("username") or str(doc.get("_id", ""))
    
    # Complex social media extraction - ALL platforms
    social_media = extract_all_social_links(doc.get("professional_info", {}).get("social_links"))
    
    # Speaking history with full details
    speaking_history = None
    if doc.get("speaking_history"):
        history = doc["speaking_history"]
        speaking_history = {}
        
        # Events with all details
        if history.get("events"):
            events = []
            for e in history["events"]:
                events.append({
                    "name": e.get("name"),
                    "url": e.get("url"),
                    "date": e.get("date"),
                    "location": e.get("location"),
                    "is_sessionize_event": e.get("is_sessionize_event"),
                    "sessions": e.get("sessions", [])
                })
            speaking_history["events"] = events
            
        # Sessions
        if history.get("sessions"):
            speaking_history["sessions"] = history["sessions"]
        
    # Professional info
    professional_info = None
    if doc.get("professional_info", {}).get("expertise_areas"):
        professional_info = {
            "expertise_areas": doc["professional_info"]["expertise_areas"]
        }
    
    # Platform specific fields
    platform_fields = {}
    if username:
        platform_fields["username"] = username
    
    return {
        "_id"         : sha_id("sessionize|" + username),
        "name"        : basic.get("name") or doc.get("name"),
        "display_name": basic.get("name") or doc.get("name"),
        "tagline"     : basic.get("tagline"),
        "biography"   : basic.get("bio"),
        "location"    : parse_location(basic.get("location")),
        "social_media": social_media,
        "topics"      : tops,
        "categories"  : tops,
        "topics_unmapped": unmapped,
        "expertise_areas": doc.get("professional_info", {}).get("expertise_areas", []),
        "professional_info": professional_info,
        "media": {
            "profile_image": basic.get("profile_picture")
        },
        "speaking_history": speaking_history,
        "metadata": doc.get("metadata"),
        "platform_fields": platform_fields,
        "source_info": {
            "original_source": "sessionize",
            "source_url"     : basic.get("url"),
            "scraped_at"     : safe_date(doc.get("metadata", {}).get("scraped_at")),
            "source_id"      : username
        }
    }

def unify_speakerhub(doc):
    tops, unmapped = norm_topics(doc.get("topic_categories", []) + doc.get("topics", []))
    
    # Build location with timezone
    location_parts = []
    if doc.get("city"):
        location_parts.append(doc.get("city"))
    if doc.get("state_province") or doc.get("state"):
        location_parts.append(doc.get("state_province") or doc.get("state"))
    if doc.get("country"):
        location_parts.append(doc.get("country"))
    location_str = ", ".join(location_parts) if location_parts else ""
    
    location = parse_location(location_str)
    if doc.get("timezone"):
        location["timezone"] = doc["timezone"]
    
    # Extract social media from individual fields
    social_media = extract_social_media(doc, {
        "linkedin": "linkedin_url",
        "twitter": "twitter_url",
        "facebook": "facebook_url",
        "instagram": "instagram_url",
        "youtube": "youtube_url"
    })
    
    # Contact info
    contact = {}
    if doc.get("website"):
        contact["website"] = doc["website"]
        
    # Rich professional info
    professional_info = {
        "pronouns": doc.get("pronouns"),
        "certifications": doc.get("certifications", []),
        "awards": doc.get("awards", []),
        "education": doc.get("education", []),
        "affiliations": doc.get("affiliations", []),
        "company": doc.get("company"),
        "professional_title": doc.get("professional_title")
    }
    
    # Speaking info with ALL fields
    speaking_info = {
        "fee_ranges": doc.get("speaker_fees") or doc.get("fee_range"),
        "languages": doc.get("languages", []),
        "available_regions": doc.get("available_regions", []),
        "years_experience": doc.get("years_experience"),
        "total_talks": doc.get("total_talks"),
        "event_types": doc.get("event_types", [])
    }
    
    # Content
    content = {}
    if doc.get("presentations"):
        content["presentations"] = doc["presentations"]
    if doc.get("workshops"):
        content["workshops"] = doc["workshops"]
        
    # Media
    media = {
        "profile_image": doc.get("profile_picture_url") or doc.get("profile_picture"),
        "banner_image": doc.get("banner_image_url"),
        "videos": doc.get("videos", []),
        "profile_pdf": doc.get("press_kit_url")
    }
    
    # Publications
    publications = None
    if doc.get("publications"):
        publications = {
            "articles": doc["publications"]
        }
        
    # Speaking history
    speaking_history = None
    if doc.get("past_talks"):
        speaking_history = {
            "past_talks": doc["past_talks"]
        }
        
    # Metadata
    metadata = {
        "why_choose": doc.get("why_choose_me"),
        "competencies": doc.get("competencies"),
        "first_name": doc.get("first_name"),
        "last_name": doc.get("last_name"),
        "bio_summary": doc.get("bio_summary"),
        "scraping_status": doc.get("scraping_status")
    }
    
    # Platform fields
    platform_fields = {}
    if doc.get("uid"):
        platform_fields["uid"] = doc["uid"]
    
    return {
        "_id"        : sha_id("speakerhub|" + str(doc.get("_id", ""))),
        "name"       : doc.get("name"),
        "display_name": doc.get("name"),
        "job_title"  : doc.get("job_title") or doc.get("professional_title"),
        "biography"  : doc.get("full_bio") or doc.get("bio_summary"),
        "location"   : location,
        "contact"    : contact if contact else None,
        "social_media": social_media,
        "speaking_info": speaking_info,
        "topics"     : tops,
        "categories" : tops,
        "topics_unmapped": unmapped,
        "professional_info": professional_info,
        "content"    : content if content else None,
        "media"      : media,
        "publications": publications,
        "testimonials": doc.get("testimonials", []),
        "ratings"    : {
            "average_rating": doc.get("rating"),
            "recommendation_count": doc.get("recommendations_count")
        } if doc.get("rating") or doc.get("recommendations_count") else None,
        "speaking_history": speaking_history,
        "metadata"   : metadata,
        "platform_fields": platform_fields,
        "source_info": {
            "original_source": "speakerhub",
            "source_url"     : doc.get("profile_url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "last_updated"   : safe_date(doc.get("last_updated")),
            "source_id"      : str(doc.get("_id", ""))
        }
    }

def unify_tsh(doc):
    tops, unmapped = norm_topics(doc.get("topics"))
    speaker_id = doc.get("speaker_id") or str(doc.get("_id", ""))
    
    # Extract social links
    social_media = None
    if doc.get("social_links"):
        social_media = doc["social_links"]
        
    # Contact
    contact = None
    if doc.get("contact", {}).get("email"):
        contact = {
            "email": doc["contact"]["email"]
        }
    if doc.get("website"):
        contact = contact or {}
        contact["website"] = doc["website"]
        
    # Professional info
    professional_info = {
        "awards": doc.get("awards", [])
    }
    
    # Speaking info
    speaking_info = {
        "languages": doc.get("languages", []),
        "engagement_types": doc.get("engagement_types", []),
        "event_types": doc.get("event_type", [])
    }
    
    # Fees structure
    if doc.get("fees"):
        speaking_info["fee_structure"] = doc["fees"]
    
    # Media
    media = {
        "profile_image": doc.get("image_url_hd") or doc.get("image_url"),
        "image_gallery": doc.get("image_gallery", []),
        "profile_pdf": doc.get("download_profile_link")
    }
    
    # Video categories
    if doc.get("video_categories"):
        media["video_categories"] = doc["video_categories"]
        
    # Publications
    publications = None
    if doc.get("books"):
        publications = {
            "books": doc["books"]
        }
        
    # Metadata - ALL fields
    metadata = {
        "gender": doc.get("gender"),
        "notability": doc.get("notability", []),
        "biography_highlights": doc.get("biography_highlights", []),
        "membership": doc.get("membership"),
        "nationality": doc.get("nationality"),
        "knows_about": doc.get("knows_about"),
        "page_title": doc.get("page_title"),
        "meta_description": doc.get("meta_description"),
        "home_country": doc.get("home_country"),
        "strapline": doc.get("strapline"),
        "scrape_status": doc.get("scrape_status")
    }
    
    # JSON-LD talks
    if doc.get("json_ld_talks"):
        metadata["json_ld_talks"] = doc["json_ld_talks"]
    
    return {
        "_id"        : sha_id("tsh|" + speaker_id),
        "name"       : doc.get("display_name"),
        "display_name": doc.get("display_name"),
        "job_title"  : doc.get("job_title"),
        "biography"  : doc.get("biography"),
        "tagline"    : doc.get("strapline"),
        "location"   : parse_location(doc.get("travels_from") or doc.get("home_country")),
        "contact"    : contact,
        "social_media": social_media,
        "speaking_info": speaking_info,
        "topics"     : tops,
        "categories" : tops,
        "topics_unmapped": unmapped,
        "professional_info": professional_info if any(professional_info.values()) else None,
        "media"      : media,
        "publications": publications,
        "testimonials": doc.get("testimonials", []),
        "metadata"   : metadata,
        "source_info": {
            "original_source": "thespeakerhandbook",
            "source_url"     : doc.get("profile_url"),
            "scraped_at"     : safe_date(doc.get("scraped_at")),
            "source_id"      : speaker_id
        }
    }

# ──────────────────────────────────────────────────────────────────────────────
# 3. DEDUPLICATION HELPERS
# ──────────────────────────────────────────────────────────────────────────────
def fingerprint_name(name):
    return re.sub(r"[^a-z]", "", name.lower()) if name else ""

def build_dedupe_index(collection):
    index = defaultdict(list)
    for doc in collection.find({}, {"_id":1, "name":1, "location.city":1}):
        key = fingerprint_name(doc.get("name"))
        if key:
            index[key].append((doc["_id"], doc.get("location", {}).get("city")))
    return index

def find_duplicate(unified_doc, index):
    key = fingerprint_name(unified_doc["name"])
    cands = index.get(key, [])
    best = None
    for _id, city in cands:
        score = fuzz.ratio(unified_doc["name"], unified_doc.get("name"))
        if city and unified_doc["location"].get("city") and city.lower()==unified_doc["location"]["city"].lower():
            score += 10  # bonus for same city
        if score > 90:
            best = _id
            break
    return best

# ──────────────────────────────────────────────────────────────────────────────
# 4. MAIN
# ──────────────────────────────────────────────────────────────────────────────
def run():
    client = MongoClient(MONGO_URI)
    target_db = client[TARGET_DB_NAME]
    
    # Use new collection for V3 data
    unified = target_db["unified_speakers_v3"]

    # Build quick dedupe index from existing unified records
    print("Building deduplication index...")
    dedupe_idx = build_dedupe_index(unified)

    bulk_ops = []
    total_in, total_upd, total_new = 0, 0, 0

    # Process each source database
    for db_name, config in SRC_DATABASES.items():
        print(f"\nProcessing {db_name}...")
        
        # Check if database exists
        if db_name not in client.list_database_names():
            print(f"  - Database not found, skipping")
            continue
            
        src_db = client[db_name]
        collection_name = config["collection"]
        
        # Check if collection exists
        if collection_name not in src_db.list_collection_names():
            # Try to find any collection with 'speaker' in the name
            speaker_collections = [c for c in src_db.list_collection_names() if 'speaker' in c.lower()]
            if speaker_collections:
                collection_name = speaker_collections[0]
                print(f"  - Using collection: {collection_name}")
            else:
                print(f"  - No speaker collection found, skipping")
                continue
        
        src_col = src_db[collection_name]
        transformer = globals()[config["transformer"]]
        
        count = 0
        for doc in src_col.find({}):
            total_in += 1
            count += 1
            
            try:
                u_doc = transformer(doc)
                if not u_doc:  # Skip if transformer returns None
                    continue
                dup_id = find_duplicate(u_doc, dedupe_idx)

                if dup_id:               # update existing record
                    u_doc["updated_at"] = datetime.utcnow()
                    # Remove _id from update document as it's immutable
                    update_doc = {k: v for k, v in u_doc.items() if k != "_id"}
                    bulk_ops.append(
                        UpdateOne({"_id": dup_id}, {"$set": update_doc})
                    )
                    total_upd += 1
                else:                    # insert new record
                    u_doc["created_at"] = datetime.utcnow()
                    bulk_ops.append(UpdateOne({"_id": u_doc["_id"]}, {"$setOnInsert": u_doc}, upsert=True))
                    total_new += 1
                    dedupe_idx[fingerprint_name(u_doc["name"])].append((u_doc["_id"], u_doc["location"].get("city")))

                # Execute every 1k ops to avoid huge batches
                if len(bulk_ops) >= 1000:
                    unified.bulk_write(bulk_ops, ordered=False)
                    bulk_ops = []
                    
            except Exception as e:
                print(f"  - Error processing document {doc.get('_id')}: {str(e)}")
                continue
        
        print(f"  - Processed {count} documents")

    if bulk_ops:
        unified.bulk_write(bulk_ops, ordered=False)

    print(f"\n{'='*50}")
    print(f"Standardization V3 Complete!")
    print(f"{'='*50}")
    print(f"Ingested  : {total_in:,}")
    print(f"New       : {total_new:,}")
    print(f"Updated   : {total_upd:,}")
    print(f"Total now : {unified.count_documents({}):,}")
    
    # Show field coverage stats
    print(f"\nField Coverage Analysis:")
    print(f"Documents with social media: {unified.count_documents({'social_media': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with contact info: {unified.count_documents({'contact': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with testimonials: {unified.count_documents({'testimonials': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with professional info: {unified.count_documents({'professional_info': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with company info: {unified.count_documents({'professional_info.company': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with platform fields: {unified.count_documents({'platform_fields': {'$exists': True, '$ne': None}}):,}")
    print(f"Documents with SEO metadata: {unified.count_documents({'metadata.meta_description': {'$exists': True, '$ne': None}}):,}")

if __name__ == "__main__":
    run()