from .rating_scraper import RatingScraper
from .mca_scraper import MCAScraper

SCRAPER_REGISTRY = {
    "rating_agency": RatingScraper,
    "mca_registry": MCAScraper
}
