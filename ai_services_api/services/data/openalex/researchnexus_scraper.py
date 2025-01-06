import os
import re
import logging
import time
import socket
import tempfile
import platform
import subprocess
from typing import List, Dict, Optional
from datetime import datetime
from collections import Counter
import hashlib
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.data.openalex.text_processor import safe_str

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG if os.environ.get('DEBUG') == 'True' else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class ResearchNexusScraper:
    """Scraper for Research Nexus with enhanced Chrome handling."""
    
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize scraper without starting Chrome."""
        self.summarizer = summarizer or TextSummarizer()
        self.seen_dois = set()
        self.publication_counter = Counter()
        self.driver = None
        self.chrome_options = None
        self.service = None
        self.temp_dir = None
        self._prepare_directories()
        self._check_system_info()
        logger.info("ResearchNexusScraper initialized without Chrome")

    def _prepare_directories(self):
        """Prepare necessary directories with proper permissions."""
        directories = [
            os.environ.get('CHROME_TMPDIR', '/tmp/chrome-data'),
            os.environ.get('CHROME_PROFILE_DIR', '/tmp/chrome-profile'),
            '/var/run/chrome'
        ]
        
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                os.chmod(directory, 0o1777)  # Set permissions to drwxrwxrwt
                logger.debug(f"Prepared directory: {directory} with permissions: {oct(os.stat(directory).st_mode)[-4:]}")
            except Exception as e:
                logger.error(f"Error preparing directory {directory}: {str(e)}")

    def _check_system_info(self):
        """Log system information for debugging."""
        logger.info(f"Platform: {platform.platform()}")
        logger.info(f"Python version: {platform.python_version()}")
        logger.info(f"Running as user: {os.getuid()}:{os.getgid()}")
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Temp directory: {tempfile.gettempdir()}")

        # Check Chrome-related directories
        chrome_dirs = {
            'CHROME_TMPDIR': os.environ.get('CHROME_TMPDIR', '/tmp/chrome-data'),
            'CHROME_PROFILE_DIR': os.environ.get('CHROME_PROFILE_DIR', '/tmp/chrome-profile'),
            'Chrome binary dir': os.path.dirname(os.environ.get('CHROME_BIN', '/usr/bin/chromium'))
        }

        for name, path in chrome_dirs.items():
            if os.path.exists(path):
                logger.info(f"{name} exists with permissions: {oct(os.stat(path).st_mode)[-4:]}")
            else:
                logger.warning(f"{name} does not exist: {path}")

    def _get_free_port(self) -> int:
        """Get a free port for Chrome remote debugging."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            s.listen(1)
            port = s.getsockname()[1]
            logger.debug(f"Found free port: {port}")
            return port

    def _setup_chrome_options(self, chrome_binary: str) -> Options:
        """Configure Chrome options with debugging and stability flags."""
        options = Options()
        options.binary_location = chrome_binary
        
        # Get Chrome flags from environment or use defaults
        env_flags = os.environ.get('CHROME_FLAGS', '').split()
        
        # Core stability and debugging flags
        stability_flags = [
            '--headless=new',
            '--no-sandbox',
            '--disable-gpu',
            f'--remote-debugging-port={self._get_free_port()}',
            '--disable-dev-shm-usage',
            '--disable-extensions',
            '--disable-software-rasterizer',
            '--disable-default-apps',
            '--disable-setuid-sandbox',
            '--disable-crashpad',
            '--disable-crash-reporter',
            '--no-first-run',
            '--password-store=basic',
            '--disable-infobars',
            '--disable-notifications',
            '--enable-automation',
            '--disable-background-networking',
            '--metrics-recording-only',
            '--mute-audio'
        ]

        # Add Chrome profile directory configuration
        profile_dir = os.environ.get('CHROME_PROFILE_DIR', '/tmp/chrome-profile')
        if os.path.exists(profile_dir):
            stability_flags.extend([
                f'--user-data-dir={profile_dir}',
                '--profile-directory=Scraping'
            ])
            logger.debug(f"Using Chrome profile directory: {profile_dir}")
        else:
            logger.warning(f"Chrome profile directory does not exist: {profile_dir}")

        # Combine environment flags with stability flags
        all_flags = list(set(stability_flags + env_flags))
        
        for flag in all_flags:
            options.add_argument(flag)
        
        # Log all Chrome options and directory permissions
        logger.debug(f"Chrome options configured: {options.arguments}")
        
        return options

    def _verify_chrome_install(self, chrome_binary: str, chromedriver_path: str):
        """Verify Chrome and ChromeDriver installation and permissions."""
        try:
            # Check Chrome version
            chrome_version = subprocess.check_output([chrome_binary, '--version']).decode().strip()
            logger.info(f"Chrome version: {chrome_version}")
            
            # Check ChromeDriver version
            chromedriver_version = subprocess.check_output([chromedriver_path, '--version']).decode().strip()
            logger.info(f"ChromeDriver version: {chromedriver_version}")
            
            # Check binary permissions
            chrome_perms = oct(os.stat(chrome_binary).st_mode)[-4:]
            chromedriver_perms = oct(os.stat(chromedriver_path).st_mode)[-4:]
            
            logger.debug(f"Chrome binary permissions: {chrome_perms}")
            logger.debug(f"ChromeDriver permissions: {chromedriver_perms}")
            
        except Exception as e:
            logger.error(f"Chrome verification failed: {str(e)}")
            raise

    def _initialize_chrome_if_needed(self):
        """Initialize Chrome with extended error handling and verification."""
        if self.driver:
            return

        try:
            # Get binary paths from environment or defaults
            chrome_binary = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
            chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
            
            logger.info(f"Initializing Chrome with binary: {chrome_binary}")
            logger.info(f"ChromeDriver path: {chromedriver_path}")
            
            # Verify Chrome installation
            self._verify_chrome_install(chrome_binary, chromedriver_path)
            
            # Setup Chrome options
            self.chrome_options = self._setup_chrome_options(chrome_binary)
            
            # Configure service with logging and longer timeout
            self.service = Service(
                executable_path=chromedriver_path,
                service_args=['--verbose'],
                log_path='chromedriver.log'
            )
            
            # Initialize WebDriver with retry logic
            max_retries = 3
            retry_count = 0
            last_error = None
            
            while retry_count < max_retries:
                try:
                    self.driver = webdriver.Chrome(
                        service=self.service,
                        options=self.chrome_options
                    )
                    
                    # Configure timeouts
                    self.driver.set_page_load_timeout(30)
                    self.driver.implicitly_wait(10)
                    
                    # Verify initialization
                    if not self.driver.session_id:
                        raise WebDriverException("Failed to create valid session")
                    
                    logger.info("Chrome initialized successfully")
                    return
                    
                except Exception as e:
                    last_error = e
                    retry_count += 1
                    logger.warning(f"Chrome initialization attempt {retry_count} failed: {str(e)}")
                    self.cleanup_chrome()
                    time.sleep(2)  # Wait before retrying
            
            raise Exception(f"Chrome initialization failed after {max_retries} attempts. Last error: {str(last_error)}")
            
        except Exception as e:
            logger.error(f"Chrome initialization failed: {str(e)}")
            self.cleanup_chrome()
            raise

    def fetch_content(self, limit: int = 10, search_term: str = "aphrc") -> List[Dict]:
        """Fetch publications with comprehensive error handling."""
        publications = []
        start_time = datetime.now()
        
        try:
            self._initialize_chrome_if_needed()
            
            # Navigate to search page with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.driver.get('https://research-nexus.net/search')
                    logger.info(f"Searching for term: {search_term}")
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Navigation attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(2)
            
            # Perform search
            search_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='search']"))
            )
            search_input.clear()
            search_input.send_keys(search_term)
            search_input.send_keys(Keys.RETURN)
            time.sleep(5)
            
            # Process results with timeout handling
            while len(publications) < limit:
                try:
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
                    
                    if len(publications) < limit and not self._load_more_results():
                        break
                    
                except TimeoutException:
                    logger.warning("Timeout while processing results")
                    break
                    
                except Exception as e:
                    logger.error(f"Error processing page: {str(e)}")
                    break
            
        except Exception as e:
            logger.error(f"Error in fetch_content: {str(e)}")
            
        finally:
            self._log_processing_statistics(start_time, len(publications), limit)
            
        return publications

    def _extract_publication(self, element) -> Optional[Dict]:
        """Extract publication data with enhanced error handling."""
        try:
            # Extract with explicit waits
            title = WebDriverWait(element, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ar-title"))
            ).text.strip()
            
            abstract = WebDriverWait(element, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ar-excerpt"))
            ).text.strip()
            
            metadata = WebDriverWait(element, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "ar-meta"))
            ).text.strip()
            
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
            self.publication_counter.update(['errors'])
            return None

    def _extract_metadata(self, metadata_text: str) -> Dict:
        """Extract metadata with validation and error handling."""
        metadata = {
            'citations': 0,
            'num_authors': 0,
            'locations': [],
            'affiliations': 0
        }
        
        try:
            locations_match = re.search(r'Study in: ([^•]+)', metadata_text)
            if locations_match:
                metadata['locations'] = [
                    loc.strip() 
                    for loc in locations_match.group(1).split(',')
                ]
                self.publication_counter.update(['has_locations'])
            
            citations_match = re.search(r'Citations (\d+)', metadata_text)
            if citations_match:
                try:
                    metadata['citations'] = int(citations_match.group(1))
                    if metadata['citations'] > 0:
                        self.publication_counter.update(['has_citations'])
                except ValueError:
                    logger.warning(f"Invalid citations value: {citations_match.group(1)}")
            
            authors_match = re.search(r'Authors (\d+)', metadata_text)
            if authors_match:
                try:
                    metadata['num_authors'] = int(authors_match.group(1))
                except ValueError:
                    logger.warning(f"Invalid authors value: {authors_match.group(1)}")
            
            affiliations_match = re.search(r'Affiliations (\d+)', metadata_text)
            if affiliations_match:
                try:
                    metadata['affiliations'] = int(affiliations_match.group(1))
                except ValueError:
                    logger.warning(f"Invalid affiliations value: {affiliations_match.group(1)}")
                
        except Exception as e:
            logger.error(f"Metadata extraction error: {str(e)}")
            self.publication_counter.update(['errors'])
            
        return metadata

    def _load_more_results(self) -> bool:
        """Load more results with enhanced timeout handling."""
        try:
            load_more = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "load-more"))
            )
            
            if load_more.is_displayed() and load_more.is_enabled():
                # Scroll into view and click with JavaScript
                self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more)
                time.sleep(1)  # Allow time for scroll
                
                try:
                    load_more.click()
                except:
                    # Fallback to JavaScript click if regular click fails
                    self.driver.execute_script("arguments[0].click();", load_more)
                
                time.sleep(3)  # Wait for new content
                return True
            return False
            
        except TimeoutException:
            logger.debug("No more results to load")
            return False
        except Exception as e:
            logger.error(f"Error loading more results: {str(e)}")
            return False

    def _generate_doi(self, title: str) -> str:
        """Generate a deterministic DOI."""
        try:
            # Normalize the title
            normalized_title = title.lower().strip()
            # Create hash
            hash_object = hashlib.sha256(normalized_title.encode())
            hash_digest = hash_object.hexdigest()[:16]
            # Generate DOI with prefix
            doi = f"10.0000/researchnexus-{hash_digest}"
            logger.debug(f"Generated DOI: {doi} for title: {title[:50]}...")
            return doi
        except Exception as e:
            logger.error(f"Error generating DOI: {str(e)}")
            return f"10.0000/error-{datetime.now().timestamp()}"

    def _log_processing_statistics(self, start_time: datetime, processed_count: int, limit: int):
        """Log detailed processing statistics."""
        try:
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            success_rate = (processed_count/limit)*100 if limit > 0 else 0

            logger.info("\nProcessing Statistics:")
            logger.info(f"Total time: {processing_time:.2f} seconds")
            logger.info(f"Publications processed: {processed_count}")
            logger.info(f"Success rate: {success_rate:.1f}%")
            logger.info(f"Publications with citations: {self.publication_counter['has_citations']}")
            logger.info(f"Publications with locations: {self.publication_counter['has_locations']}")
            logger.info(f"Errors encountered: {self.publication_counter['errors']}")
            logger.info(f"Average processing time per publication: {processing_time/processed_count if processed_count > 0 else 0:.2f} seconds")
        except Exception as e:
            logger.error(f"Error logging statistics: {str(e)}")

    def cleanup_chrome(self):
        """Clean up Chrome resources with enhanced error handling."""
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    logger.error(f"Error quitting Chrome driver: {str(e)}")
                finally:
                    self.driver = None
            
            # Clean up temp directories while preserving profile
            temp_dirs = [
                self.temp_dir,
                os.environ.get('CHROME_TMPDIR'),
                '/tmp/chrome-data'
            ]
            
            for temp_dir in temp_dirs:
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.debug(f"Cleaned up temp directory: {temp_dir}")
                    except Exception as e:
                        logger.error(f"Error cleaning up temp directory {temp_dir}: {str(e)}")
            
            logger.info("Chrome resources cleaned up")
            
        except Exception as e:
            logger.error(f"Error during Chrome cleanup: {str(e)}")

    def close(self):
        """Clean up all resources."""
        try:
            self.cleanup_chrome()
            
            if hasattr(self.summarizer, 'close'):
                try:
                    self.summarizer.close()
                except Exception as e:
                    logger.error(f"Error closing summarizer: {str(e)}")
            
            self.seen_dois.clear()
            self.publication_counter.clear()
            
            logger.info("All resources cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit with cleanup."""
        self.close()
        if exc_type is not None:
            logger.error(f"Error in context manager: {exc_type.__name__}: {exc_value}")
            return False  # Re-raise the exception