"""
SpeakerHub Details Scraper Package
"""

from .models import DetailedSpeaker, SpeakerFee, PastTalk, Education, Publication, Presentation, Workshop, Testimonial
from .database import SpeakerDetailsDB
from .parser_v5 import SpeakerDetailsParserV5 as SpeakerDetailsParser
from .scraper import SpeakerDetailsScraper
from .utils import DataExporter, ProgressMonitor

__version__ = "1.0.0"
__all__ = [
    'DetailedSpeaker',
    'SpeakerFee',
    'PastTalk',
    'Education',
    'Publication',
    'Presentation',
    'Workshop',
    'Testimonial',
    'SpeakerDetailsDB',
    'SpeakerDetailsParser',
    'SpeakerDetailsScraper',
    'DataExporter',
    'ProgressMonitor'
]