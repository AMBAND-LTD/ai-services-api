import os
import logging
import time
import json
import hashlib
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from collections import Counter
from contextlib import contextmanager

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys

from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.data.openalex.text_processor import safe_str, truncate_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResearchNexusScraper:
    def __init__(self, summarizer: Optional[TextSummarizer] = None, headless: bool = True):
        """Initialize ResearchNexusScraper with Chrome webdriver."""
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless=new')
        
        # Essential Chrome options for Docker
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument('--disable-infobars')
        
        # Set Chrome binary location from environment or default
        chrome_binary = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
        self.chrome_options.binary_location = chrome_binary
        
        # Set up Chrome service
        chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
        self.service = Service(executable_path=chromedriver_path)
        
        # Initialize webdriver with retry mechanism
        self.driver = self._initialize_driver_with_retry()
        
        self.summarizer = summarizer or TextSummarizer()
        self.seen_dois = set()
        self.publication_counter = Counter()
        
        logger.info("ResearchNexusScraper initialized successfully")

    def _initialize_driver_with_retry(self, max_retries=3):
        """Initialize Chrome webdriver with retries."""
        for attempt in range(max_retries):
            try:
                driver = webdriver.Chrome(
                    service=self.service,
                    options=self.chrome_options
                )
                return driver
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to initialize Chrome driver after {max_retries} attempts")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(2)

    def _extract_metadata(self, metadata_text: str) -> Dict:
        """Extract metadata from the citation line."""
        metadata = {
            'citations': 0,
            'num_authors': 0,
            'locations': [],
            'affiliations': 0
        }
        
        try:
            # Extract study locations
            locations_match = re.search(r'Study in: ([^•]+)', metadata_text)
            if locations_match:
                locations = locations_match.group(1).strip()
                metadata['locations'] = [loc.strip() for loc in locations.split(',')]
            
            # Extract citations
            citations_match = re.search(r'Citations (\d+)', metadata_text)
            if citations_match:
                metadata['citations'] = int(citations_match.group(1))
            
            # Extract authors
            authors_match = re.search(r'Authors (\d+)', metadata_text)
            if authors_match:
                metadata['num_authors'] = int(authors_match.group(1))
            
            # Extract affiliations
            affiliations_match = re.search(r'Affiliations (\d+)', metadata_text)
            if affiliations_match:
                metadata['affiliations'] = int(affiliations_match.group(1))
                
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            
        return metadata

    @contextmanager
    def _wait_for_page_load(self, timeout=30):
        """Context manager to wait for page load completion."""
        old_page = self.driver.find_element(By.TAG_NAME, "html")
        yield
        WebDriverWait(self.driver, timeout).until(
            lambda driver: old_page.id != driver.find_element(By.TAG_NAME, "html").id
        )

    def fetch_content(self, limit: int = 10, search_term: str = "aphrc") -> List[Dict]:
        """Fetch publications using Selenium."""
        publications = []
        start_time = datetime.now()
        
        try:
            # Navigate to the search page
            self.driver.get('https://research-nexus.net/search')
            logger.info("Navigated to search page")
            
            # Wait for search input and perform search
            search_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
            )
            search_input.clear()
            search_input.send_keys(search_term)
            search_input.send_keys(Keys.RETURN)
            
            # Wait for initial results
            time.sleep(5)  # Allow time for search results to load
            
            while len(publications) < limit:
                try:
                    # Wait for publications to be visible
                    WebDriverWait(self.driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "ar-search-result"))
                    )
                    
                    # Find all publication elements
                    pub_elements = self.driver.find_elements(By.CLASS_NAME, "ar-search-result")
                    
                    if not pub_elements:
                        logger.info("No more publications found")
                        break
                    
                    for element in pub_elements:
                        if len(publications) >= limit:
                            break
                        
                        try:
                            # Extract publication data
                            title = element.find_element(By.CLASS_NAME, "ar-title").text.strip()
                            abstract = element.find_element(By.CLASS_NAME, "ar-excerpt").text.strip()
                            metadata = element.find_element(By.CLASS_NAME, "ar-meta").text.strip()
                            
                            # Generate DOI and check if seen
                            doi = self._generate_synthetic_doi(title)
                            if doi in self.seen_dois:
                                continue
                            
                            # Extract metadata
                            metadata_dict = self._extract_metadata(metadata)
                            
                            # Create publication object
                            publication = {
                                'title': safe_str(title),
                                'abstract': safe_str(abstract),
                                'doi': doi,
                                **metadata_dict,
                                'source': 'researchnexus',
                                'scrape_date': datetime.now().isoformat()
                            }
                            
                            # Add summary if abstract exists
                            if publication['abstract']:
                                try:
                                    publication['summary'] = self.summarizer.summarize(
                                        publication['title'],
                                        publication['abstract']
                                    )
                                except Exception as e:
                                    logger.error(f"Error generating summary: {e}")
                                    publication['summary'] = f"Publication about {publication['title']}"
                            
                            # Validate and add publication
                            is_valid, validation_message = self._validate_publication(publication)
                            if is_valid:
                                publications.append(publication)
                                self.seen_dois.add(doi)
                                self.publication_counter.update(['total_processed'])
                                logger.info(f"Processed publication: {title[:100]}")
                            
                        except Exception as e:
                            logger.error(f"Error processing publication element: {e}")
                            self.publication_counter.update(['errors'])
                            continue
                    
                    # Try to load more results if needed
                    if len(publications) < limit:
                        try:
                            load_more = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CLASS_NAME, "load-more"))
                            )
                            if load_more.is_displayed():
                                self.driver.execute_script("arguments[0].click();", load_more)
                                time.sleep(3)  # Wait for new results
                            else:
                                break
                        except TimeoutException:
                            logger.info("No more results to load")
                            break
                        
                except Exception as e:
                    logger.error(f"Error processing page: {e}")
                    break
            
            # Log final statistics
            self._log_processing_statistics(start_time, len(publications), limit)
            return publications
            
        except Exception as e:
            logger.error(f"Error in fetch_content: {e}", exc_info=True)
            return publications
        
        finally:
            try:
                self.driver.get("about:blank")  # Clear the page
            except:
                pass

    def _generate_synthetic_doi(self, title: str) -> str:
        """Generate a synthetic DOI for publications."""
        hash_object = hashlib.sha256(title.encode())
        hash_digest = hash_object.hexdigest()[:16]
        return f"10.0000/researchnexus-{hash_digest}"

    def _validate_publication(self, publication: Dict) -> Tuple[bool, str]:
        """Validate publication data structure."""
        required_fields = ['title', 'abstract', 'doi']
        for field in required_fields:
            if not publication.get(field):
                return False, f"Missing required field: {field}"
        return True, "Valid publication"

    def _log_processing_statistics(self, start_time: datetime, processed_count: int, limit: int):
        """Log processing statistics."""
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logger.info("\nProcessing Statistics:")
        logger.info(f"Total time: {processing_time:.2f} seconds")
        logger.info(f"Publications processed: {processed_count}")
        logger.info(f"Success rate: {(processed_count/limit)*100:.1f}%")
        logger.info(f"Publications with citations: {self.publication_counter['has_citations']}")
        logger.info(f"Publications with locations: {self.publication_counter['has_locations']}")
        logger.info(f"Errors encountered: {self.publication_counter['errors']}")

    def get_statistics(self) -> Dict:
        """Get scraping statistics."""
        return dict(self.publication_counter)

    def close(self):
        """Close resources and perform cleanup."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
            
            if hasattr(self.summarizer, 'close'):
                self.summarizer.close()
            
            self.seen_dois.clear()
            self.publication_counter.clear()
            
            logger.info("ResearchNexusScraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error closing ResearchNexusScraper: {e}")

# Example usage
if __name__ == "__main__":
    try:
        scraper = ResearchNexusScraper()
        publications = scraper.fetch_content(limit=5)
        print(f"Retrieved {len(publications)} publications")
        for pub in publications:
            print(f"\nTitle: {pub['title']}")
            print(f"Citations: {pub['citations']}")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        scraper.close()