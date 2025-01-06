import os
import re
import logging
import time
from typing import List, Dict, Optional
from datetime import datetime
from collections import Counter
import hashlib
import tempfile

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.data.openalex.text_processor import safe_str

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResearchNexusScraper:
    """Scraper for Research Nexus with lazy Chrome initialization."""
    
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize scraper without starting Chrome."""
        self.summarizer = summarizer or TextSummarizer()
        self.seen_dois = set()
        self.publication_counter = Counter()
        # Don't initialize Chrome components yet
        self.driver = None
        self.chrome_options = None
        self.service = None
        logger.info("ResearchNexusScraper initialized without Chrome")

    def _initialize_chrome_if_needed(self):
        try:
            # Verbose logging
            logger.info(f"Chrome Binary: {os.environ.get('CHROME_BIN', 'Not Set')}")
            logger.info(f"ChromeDriver Path: {os.environ.get('CHROMEDRIVER_PATH', 'Not Set')}")
            logger.info(f"Chrome Flags: {os.environ.get('CHROME_FLAGS', 'Not Set')}")
            
            # Verify file existence
            chrome_binary = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
            chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
            
            if not os.path.exists(chrome_binary):
                raise FileNotFoundError(f"Chrome binary not found at {chrome_binary}")
            
            if not os.path.exists(chromedriver_path):
                raise FileNotFoundError(f"ChromeDriver not found at {chromedriver_path}")
            
            # Add explicit permission check
            if not os.access(chrome_binary, os.X_OK):
                raise PermissionError(f"Chrome binary is not executable at {chrome_binary}")
            
            if not os.access(chromedriver_path, os.X_OK):
                raise PermissionError(f"ChromeDriver is not executable at {chromedriver_path}")
            
            # Rest of existing initialization code...
        except Exception as e:
            logger.error(f"Detailed Chrome initialization error: {e}")
            raise
    # Rest of the code remains the same
    def fetch_content(self, limit: int = 10, search_term: str = "aphrc") -> List[Dict]:
        """Fetch publications with lazy Chrome initialization."""
        publications = []
        start_time = datetime.now()
        
        try:
            # Only initialize Chrome when we actually need to fetch
            self._initialize_chrome_if_needed()
            
            # Navigate to search page
            self.driver.get('https://research-nexus.net/search')
            logger.info(f"Starting search for term: {search_term}")
            
            # Perform search
            search_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
            )
            search_input.clear()
            search_input.send_keys(search_term)
            search_input.send_keys(Keys.RETURN)
            time.sleep(3)  # Wait for initial results
            
            while len(publications) < limit:
                try:
                    # Wait for publication elements
                    pub_elements = WebDriverWait(self.driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "ar-search-result"))
                    )
                    
                    if not pub_elements:
                        logger.info("No more publications found")
                        break
                    
                    for element in pub_elements:
                        if len(publications) >= limit:
                            break
                            
                        try:
                            publication = self._extract_publication(element)
                            if publication:
                                publications.append(publication)
                                logger.info(f"Processed: {publication['title'][:100]}")
                        except Exception as e:
                            logger.error(f"Error processing publication: {str(e)}")
                            continue
                    
                    # Try to load more results if needed
                    if len(publications) < limit:
                        if not self._load_more_results():
                            break
                    
                except TimeoutException:
                    logger.warning("Timeout waiting for publications")
                    break
                except Exception as e:
                    logger.error(f"Error processing page: {str(e)}")
                    break
            
            return publications
            
        except Exception as e:
            logger.error(f"Error in fetch_content: {str(e)}")
            return publications
        
        finally:
            self._log_processing_statistics(start_time, len(publications), limit)

    def _extract_publication(self, element) -> Optional[Dict]:
        """Extract publication data from a single element."""
        try:
            title = element.find_element(By.CLASS_NAME, "ar-title").text.strip()
            abstract = element.find_element(By.CLASS_NAME, "ar-excerpt").text.strip()
            metadata = element.find_element(By.CLASS_NAME, "ar-meta").text.strip()
            
            # Generate DOI and check if seen
            doi = self._generate_doi(title)
            if doi in self.seen_dois:
                return None
            
            publication = {
                'title': safe_str(title),
                'abstract': safe_str(abstract),
                'doi': doi,
                **self._extract_metadata(metadata),
                'source': 'researchnexus',
                'scrape_date': datetime.now().isoformat()
            }
            
            # Add summary if we have an abstract
            if publication['abstract']:
                try:
                    publication['summary'] = self.summarizer.summarize(
                        publication['title'],
                        publication['abstract']
                    )
                except Exception as e:
                    logger.error(f"Summary generation failed: {str(e)}")
                    publication['summary'] = f"Publication about {publication['title']}"
            
            self.seen_dois.add(doi)
            self.publication_counter.update(['total_processed'])
            return publication
            
        except Exception as e:
            logger.error(f"Error extracting publication data: {str(e)}")
            return None

    def _extract_metadata(self, metadata_text: str) -> Dict:
        """Extract metadata from text."""
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
                metadata['locations'] = [
                    loc.strip() 
                    for loc in locations_match.group(1).split(',')
                ]
            
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
            logger.error(f"Metadata extraction error: {str(e)}")
            
        return metadata

    def _load_more_results(self) -> bool:
        """Attempt to load more results."""
        try:
            load_more = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "load-more"))
            )
            if load_more.is_displayed():
                load_more.click()
                time.sleep(2)
                return True
            return False
        except TimeoutException:
            return False

    def _generate_doi(self, title: str) -> str:
        """Generate a synthetic DOI."""
        hash_object = hashlib.sha256(title.encode())
        hash_digest = hash_object.hexdigest()[:16]
        return f"10.0000/researchnexus-{hash_digest}"

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

    def close(self):
        """Clean up resources."""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
            
            if hasattr(self.summarizer, 'close'):
                self.summarizer.close()
            
            self.seen_dois.clear()
            self.publication_counter.clear()
            
            logger.info("ResearchNexusScraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")