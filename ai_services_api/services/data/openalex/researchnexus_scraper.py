import logging
import time
import os
from datetime import datetime
from functools import wraps
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from contextlib import contextmanager
import subprocess

class ResearchNexusScraper:
    """
    Enhanced scraper for Research Nexus publication data using Selenium with Chromium.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.institution_id = '9000041605'
        self.driver = None
        self.setup_logging()
        
    def setup_logging(self):
        """
        Sets up logging configuration
        """
        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
    def check_chrome_setup(self):
        """
        Verifies Chrome and ChromeDriver installation and versions
        """
        try:
            chrome_path = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
            chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
            
            # Check if binaries exist
            if not os.path.exists(chrome_path):
                raise RuntimeError(f"Chrome binary not found at {chrome_path}")
            if not os.path.exists(chromedriver_path):
                raise RuntimeError(f"ChromeDriver not found at {chromedriver_path}")
            
            # Get versions
            chrome_version = subprocess.check_output([chrome_path, '--version']).decode()
            chromedriver_version = subprocess.check_output([chromedriver_path, '--version']).decode()
            
            self.logger.info(f"Chrome version: {chrome_version.strip()}")
            self.logger.info(f"ChromeDriver version: {chromedriver_version.strip()}")
            
            return True
        except Exception as e:
            self.logger.error(f"Chrome setup verification failed: {str(e)}")
            return False
    
    def setup_driver(self):
        """
        Sets up the Chromium driver with proper options for Docker environment
        """
        if self.driver:
            self.close()
            
        chrome_options = Options()
        
        # Core options needed for Docker
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # Fix for DevToolsActivePort issue
        chrome_options.add_argument('--remote-debugging-port=9222')
        chrome_options.add_argument('--disable-gpu')
        
        # Additional stability options
        chrome_options.add_argument('--disable-setuid-sandbox')
        chrome_options.add_argument('--no-zygote')
        chrome_options.add_argument('--single-process')
        
        # Memory management
        chrome_options.add_argument('--disk-cache-dir=/tmp/chrome-cache')
        chrome_options.add_argument('--disk-cache-size=52428800')
        chrome_options.add_argument('--media-cache-size=52428800')
        
        # Crash reporting and logs
        chrome_options.add_argument('--disable-crash-reporter')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-dev-tools')
        
        # Performance options
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Set environment variable for Chrome binary
        chrome_binary = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
        chrome_options.binary_location = chrome_binary
        
        service = Service(
            executable_path=os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
        )
        
        try:
            self.driver = webdriver.Chrome(
                service=service, 
                options=chrome_options
            )
            self.logger.info("Chrome driver initialized successfully")
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Chrome driver: {str(e)}")
            # Add system information for debugging
            self.logger.error(f"Chrome binary path: {chrome_binary}")
            self.logger.error(f"ChromeDriver path: {service.path}")
            raise
    
    def retry_on_failure(max_retries=3, delay=2):
        """
        Decorator for retrying functions on failure
        """
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                last_exception = None
                for attempt in range(max_retries):
                    try:
                        return func(self, *args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        if attempt < max_retries - 1:
                            self.logger.warning(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                            time.sleep(delay)
                            self.reset_driver()
                raise last_exception
            return wrapper
        return decorator
    
    def reset_driver(self):
        """
        Safely resets the driver
        """
        self.close()
        self.setup_driver()
    
    @contextmanager
    def wait_for_element(self, by, value, timeout=20):
        """
        Context manager for waiting for elements
        """
        try:
            wait = WebDriverWait(self.driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, value)))
            yield element
        except TimeoutException:
            self.logger.error(f"Timeout waiting for element: {value}")
            raise
    
    @retry_on_failure(max_retries=3, delay=2)
    def fetch_content(self, limit=10):
        """
        Fetches publications from Research Nexus using Selenium
        """
        if not self.check_chrome_setup():
            raise RuntimeError("Chrome setup verification failed")
            
        publications = []
        
        try:
            if not self.driver:
                self.setup_driver()
            
            url = (
                f"https://research-nexus.net/research/"
                f"?stp=broad&yrl=1999&yrh=2024"
                f"&ins={self.institution_id}"
                f"&limit={limit}&sort=score_desc"
            )
            
            self.logger.info(f"Fetching page: {url}")
            self.driver.get(url)
            
            # Wait for papers to load with explicit wait
            with self.wait_for_element(By.CSS_SELECTOR, ".research-paper, .paper-item") as _:
                papers = self.driver.find_elements(By.CSS_SELECTOR, ".research-paper, .paper-item")
                self.logger.info(f"Found {len(papers)} papers")
                
                for paper in papers[:limit]:
                    try:
                        pub = self._extract_paper_data(paper)
                        if pub:
                            publications.append(pub)
                    except Exception as e:
                        self.logger.error(f"Error processing paper: {str(e)}")
                        continue
            
            return publications
            
        except Exception as e:
            self.logger.error(f"Error fetching publications: {str(e)}")
            raise
        finally:
            self.close()
    
    def _get_text(self, element, selectors):
        """
        Tries multiple selectors to get text content
        """
        for selector in selectors:
            try:
                found = element.find_element(By.CSS_SELECTOR, selector)
                if found:
                    return found.text.strip()
            except NoSuchElementException:
                continue
        return ''
    
    def _get_attribute(self, element, selectors, attribute):
        """
        Tries multiple selectors to get an attribute
        """
        for selector in selectors:
            try:
                found = element.find_element(By.CSS_SELECTOR, selector)
                if found:
                    value = found.get_attribute(attribute)
                    if value:
                        return value.strip()
            except NoSuchElementException:
                continue
        return None
    
    def _get_authors(self, element):
        """
        Extracts author information trying multiple selectors
        """
        authors = []
        
        # Try different selectors for author elements
        selectors = [
            ".paper-authors a",
            ".authors a",
            ".author-list a",
            ".paper-authors .author",
            ".authors .author",
            "meta[name='citation_author']"
        ]
        
        for selector in selectors:
            try:
                author_elements = element.find_elements(By.CSS_SELECTOR, selector)
                if author_elements:
                    for author_el in author_elements:
                        author_data = {
                            'name': author_el.text.strip(),
                            'affiliations': self._get_affiliations(author_el),
                            'orcid': self._get_attribute(author_el, ['.'], 'data-orcid')
                        }
                        if author_data['name']:  # Only add if name exists
                            authors.append(author_data)
                    break
            except NoSuchElementException:
                continue
        
        # If no authors found, try getting text content
        if not authors:
            author_text = self._get_text(element, [
                ".paper-authors",
                ".authors",
                ".author-list"
            ])
            if author_text:
                author_names = [name.strip() for name in author_text.split(',') if name.strip()]
                authors = [{'name': name, 'affiliations': [], 'orcid': None} 
                          for name in author_names]
        
        return authors

    def _get_affiliations(self, author_element):
        """
        Extracts affiliation information for an author
        """
        affiliations = []
        try:
            # Try to get affiliations from data attribute
            affiliation_data = self._get_attribute(author_element, ['.'], 'data-affiliations')
            if affiliation_data:
                affiliations = [aff.strip() for aff in affiliation_data.split(';') if aff.strip()]
            
            # Try to get from nested elements if data attribute not found
            if not affiliations:
                affiliation_elements = author_element.find_elements(By.CSS_SELECTOR, 
                    ".affiliation, .institution, meta[name='citation_author_institution']")
                affiliations = [aff.text.strip() for aff in affiliation_elements if aff.text.strip()]
                
        except Exception as e:
            self.logger.debug(f"Error extracting affiliations: {str(e)}")
            
        return affiliations

    def _extract_paper_data(self, paper_element):
        """
        Extracts data from a paper element with improved error handling
        """
        try:
            # Extract basic information using various possible selectors
            title = self._get_text(paper_element, [
                ".paper-title", 
                ".title", 
                "h3", 
                ".paper-heading",
                "meta[name='citation_title']"
            ])
            
            if not title:  # Skip if no title found
                self.logger.warning("Skipping paper - no title found")
                return None
            
            abstract = self._get_text(paper_element, [
                ".paper-abstract",
                ".abstract",
                ".description",
                "meta[name='description']"
            ])
            
            # Extract authors
            authors = self._get_authors(paper_element)
            
            # Try to get DOI
            doi = (self._get_attribute(paper_element, [
                ".paper-doi",
                ".doi a",
                "[data-doi]",
                "meta[name='citation_doi']"
            ], "href") or self._get_attribute(paper_element, [
                ".paper-doi",
                ".doi",
                "[data-doi]"
            ], "data-doi"))
            
            # Try to get URL
            url = self._get_attribute(paper_element, [
                ".paper-link",
                ".title a",
                "h3 a",
                "meta[name='citation_fulltext_html_url']"
            ], "href")
            
            # Create publication object
            pub = {
                'title': title,
                'abstract': abstract or '',
                'authors': authors,
                'url': url,
                'doi': doi,
                'source': 'researchnexus',
                'source_id': paper_element.get_attribute("data-id") or '',
                'date': None,
                'year': None,
                'journal': None,
                'citation_count': self._extract_citation_count(paper_element),
                'type': self._determine_publication_type(paper_element),
                'keywords': self._extract_keywords(paper_element)
            }
            
            # Try to extract date/year
            pub.update(self._extract_date_info(paper_element))
            
            # Try to extract journal
            pub['journal'] = self._get_text(paper_element, [
                ".paper-journal",
                ".journal",
                ".publication-venue",
                "meta[name='citation_journal_title']"
            ])
            
            return pub
            
        except Exception as e:
            self.logger.error(f"Error extracting paper data: {str(e)}")
            return None

    def _extract_citation_count(self, element):
        """
        Extracts citation count from the paper element
        """
        try:
            citation_text = self._get_text(element, [
                ".citation-count",
                ".citations",
                "[data-citations]"
            ])
            if citation_text:
                return int(''.join(filter(str.isdigit, citation_text)))
        except Exception:
            pass
        return 0

    def _determine_publication_type(self, element):
        """
        Determines the type of publication
        """
        type_text = self._get_text(element, [
            ".paper-type",
            ".type",
            "meta[name='citation_type']"
        ]).lower()
        
        # Map common type strings to standardized types
        type_mapping = {
            'article': 'journal_article',
            'journal': 'journal_article',
            'conference': 'conference_paper',
            'book': 'book',
            'chapter': 'book_chapter',
            'thesis': 'thesis',
            'dissertation': 'thesis',
            'report': 'report',
            'preprint': 'preprint'
        }
        
        for key, value in type_mapping.items():
            if key in type_text:
                return value
                
        return 'paper'  # default type

    def _extract_keywords(self, element):
        """
        Extracts keywords from the paper element
        """
        keywords = []
        
        # Try different methods to get keywords
        keyword_text = self._get_text(element, [
            ".keywords",
            ".paper-keywords",
            "meta[name='keywords']"
        ])
        
        if keyword_text:
            # Split by common keyword separators
            for separator in [',', ';', '|']:
                if separator in keyword_text:
                    keywords = [k.strip() for k in keyword_text.split(separator) if k.strip()]
                    break
            
            if not keywords:  # If no separator found, treat as single keyword
                keywords = [keyword_text.strip()]
        
        return keywords

    def _extract_date_info(self, element):
        """
        Extracts date and year information
        """
        date_info = {'date': None, 'year': None}
        
        # Try to get date from meta tags first
        date_text = (
            self._get_attribute(element, ["meta[name='citation_publication_date']"], 'content') or
            self._get_attribute(element, ["meta[name='citation_online_date']"], 'content') or
            self._get_text(element, [
                ".paper-date",
                ".date",
                ".published-date"
            ])
        )
        
        if date_text:
            date_info['date'] = date_text
            
            # Try different date formats
            date_formats = [
                '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%B %Y', '%Y',
                '%Y-%m', '%d/%m/%Y', '%m/%d/%Y', '%B %d, %Y'
            ]
            
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_text, fmt)
                    date_info['year'] = date_obj.year
                    break
                except ValueError:
                    continue
            
            # If no format matched but we have a 4-digit number, use it as year
            if not date_info['year']:
                year_match = ''.join(filter(str.isdigit, date_text))
                if len(year_match) == 4:
                    date_info['year'] = int(year_match)
        
        return date_info

    def close(self):
        """
        Safely closes the browser
        """
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.logger.error(f"Error closing driver: {str(e)}")
                pass