import os
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime
import re
import hashlib
import json
from time import sleep

from ai_services_api.services.centralized_repository.ai_summarizer import TextSummarizer
from ai_services_api.services.centralized_repository.text_processor import safe_str

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class WebsiteScraper:
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize WebsiteScraper."""
        self.base_url = "https://aphrc.org"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.summarizer = summarizer or TextSummarizer()
        self.seen_urls = set()

    def fetch_content(self, limit: int = 10) -> List[Dict]:
        """Fetch content from website with detailed logging."""
        publications = []
        
        try:
            logger.info(f"\nAccessing base URL: {self.base_url}")
            response = self._make_request(self.base_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Log page structure
            logger.info("\nPage Structure:")
            for tag in soup.find_all(['div', 'section', 'article']):
                if tag.get('class'):
                    logger.info(f"Found {tag.name} with classes: {tag.get('class')}")
            
            # Find all links
            all_links = soup.find_all('a', href=True)
            logger.info(f"\nFound {len(all_links)} total links")
            
            # Process each link
            for link in all_links:
                if len(publications) >= limit:
                    break
                    
                try:
                    href = link.get('href', '').strip()
                    if not href or href in self.seen_urls:
                        continue
                        
                    # Normalize URL
                    url = href if href.startswith('http') else f"{self.base_url}{href}"
                    
                    # Log raw link data
                    logger.info("\n" + "="*80)
                    logger.info("Analyzing content at: " + url)
                    logger.info("Raw Link Data:")
                    logger.info(f"Text: {link.get_text().strip()}")
                    logger.info(f"Classes: {link.get('class', [])}")
                    logger.info("="*80)
                    
                    # Get the page content
                    try:
                        page_response = self._make_request(url)
                        page_soup = BeautifulSoup(page_response.text, 'html.parser')
                        
                        # Log entire page structure
                        logger.info("\nPage Content Structure:")
                        for elem in page_soup.find_all(['main', 'article', 'section']):
                            logger.info(f"Found {elem.name} with classes: {elem.get('class', [])}")
                        logger.info("-"*40)
                        
                        # Extract content with detailed logging
                        title_elem = page_soup.find(['h1', 'h2', 'h3', 'title'])
                        title = title_elem.get_text().strip() if title_elem else link.get_text().strip()
                        logger.info(f"\nTitle found: {title}")
                        
                        # Look for content elements
                        content_elems = page_soup.find_all(['p', 'article', 'section', 'div'], 
                                                         class_=re.compile(r'content|body|text|description', re.I))
                        content_text = '\n'.join(elem.get_text().strip() 
                                               for elem in content_elems 
                                               if elem.get_text().strip())
                        
                        # Log content preview
                        logger.info("\nContent Preview:")
                        logger.info(content_text[:500] + "..." if len(content_text) > 500 else content_text)
                        
                        # Extract date
                        date_elem = page_soup.find(['time', 'span', 'div'], 
                                                 class_=re.compile(r'date|time|published', re.I))
                        date_text = date_elem.get_text().strip() if date_elem else None
                        year = None
                        if date_text:
                            year_match = re.search(r'\b(19|20)\d{2}\b', date_text)
                            if year_match:
                                year = int(year_match.group())
                            logger.info(f"\nDate found: {date_text} (Year: {year})")
                        
                        # Extract authors
                        author_elems = page_soup.find_all(['span', 'div', 'p'], 
                                                        class_=re.compile(r'author|byline|writer', re.I))
                        authors = [author.get_text().strip() 
                                 for author in author_elems 
                                 if author.get_text().strip()]
                        if authors:
                            logger.info(f"\nAuthors found: {authors}")
                        
                        # Generate summary
                        summary = content_text[:1000] if content_text else f"Content from {title}"
                        if self.summarizer:
                            try:
                                summary = self.summarizer.summarize(title, content_text)
                                logger.info("\nSummary generated successfully")
                            except Exception as e:
                                logger.error(f"Summary generation failed: {e}")
                        
                        # Create publication record
                        publication = {
                            'title': safe_str(title),
                            'doi': url,  # Using URL as the DOI
                            'authors': authors,
                            'domains': [],  # No domain info found yet
                            'type': 'publication',  # Default type
                            'publication_year': year,
                            'summary': safe_str(summary),
                            'source': 'website'
                        }
                        
                        # Log final structured data
                        logger.info("\nStructured Data Extracted:")
                        logger.info(json.dumps(publication, indent=2))
                        
                        publications.append(publication)
                        self.seen_urls.add(url)
                        
                    except Exception as e:
                        logger.error(f"Error processing page {url}: {e}")
                        continue
                        
                except Exception as e:
                    logger.error(f"Error processing link: {e}")
                    continue
                    
                sleep(1)  # Rate limiting
                
        except Exception as e:
            logger.error(f"Error in fetch_content: {e}")
            
        logger.info(f"\nTotal publications found: {len(publications)}")
        return publications

    def _make_request(self, url: str) -> requests.Response:
        """Make HTTP request with error handling."""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def close(self):
        """Clean up resources."""
        try:
            if hasattr(self.summarizer, 'close'):
                self.summarizer.close()
            self.seen_urls.clear()
            logger.info("WebsiteScraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """Context manager exit."""
        self.close()