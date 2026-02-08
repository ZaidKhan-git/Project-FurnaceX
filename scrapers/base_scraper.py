"""
Base Scraper Module.
Defines the interface for all scrapers.
"""
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    """
    
    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url
        self.logger = logging.getLogger(f"scraper.{name}")
        
    def run(self) -> List[Dict[str, Any]]:
        """
        Main execution method.
        Orchestrates scraping, processing, and error handling.
        """
        self.logger.info(f"Starting {self.name} scrape...")
        try:
            results = self.scrape()
            if results:
                self.logger.info(f"Scraped {len(results)} items from {self.name}")
            else:
                self.logger.warning(f"No items scraped from {self.name}")
            return results
        except Exception as e:
            self.logger.error(f"Error running {self.name}: {e}", exc_info=True)
            return []

    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Implement the actual scraping logic here.
        Must return a list of dictionaries representing the scraped data.
        """
        pass
