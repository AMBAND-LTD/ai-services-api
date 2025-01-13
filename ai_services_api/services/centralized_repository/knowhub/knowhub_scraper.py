import os
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
import json
import hashlib
from datetime import datetime
import re
from time import sleep
from urllib.parse import urljoin

from ai_services_api.services.centralized_repository.ai_summarizer import TextSummarizer
from ai_services_api.services.centralized_repository.text_processor import safe_str, truncate_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KnowhubScraper:
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize KnowhubScraper with authentication capabilities."""
        self.base_url = os.getenv('KNOWHUB_BASE_URL', 'https://knowhub.aphrc.org')
        self.publications_url = f"{self.base_url}/handle/123456789/1"
        
        # Add additional endpoints while preserving original
        self.endpoints = {
            'working_documents': f"{self.base_url}/handle/123456789/2",
            'reports': f"{self.base_url}/handle/123456789/3",
            'multimedia': f"{self.base_url}/handle/123456789/4"
        }
        
        # Request headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        # Initialize summarizer
        self.summarizer = summarizer or TextSummarizer()
        
        # Track seen publications
        self.seen_handles = set()
        
        logger.info("KnowhubScraper initialized")
        logger.info(f"Using publications URL: {self.publications_url}")
        logger.info(f"Additional endpoints: {', '.join(self.endpoints.keys())}")

    def fetch_publications(self, limit: int = 10) -> List[Dict]:
        """Fetch publications from Knowhub."""
        publications = []
        try:
            logger.info(f"Starting to fetch up to {limit} publications from Knowhub")
            
            # Access the main publications page
            try:
                response = self._make_request(self.publications_url)
                logger.info(f"Request status code: {response.status_code}")
                if response.status_code != 200:
                    logger.error(f"Failed to access publications page: {response.status_code}")
                    logger.debug(f"Response content preview: {response.text[:500]}")
                    return publications
            except Exception as e:
                logger.error(f"Failed to make request: {str(e)}", exc_info=True)
                return publications
            
            # Parse HTML content
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                if not soup.find():
                    logger.error("Failed to parse HTML content - empty soup object")
                    logger.debug(f"Raw content preview: {response.text[:500]}")
                    return publications
            except Exception as e:
                logger.error(f"Failed to parse HTML: {str(e)}", exc_info=True)
                return publications
            
            # Find publication listings with detailed logging
            pub_items = soup.find_all(['div', 'article'], class_=['ds-artifact-item', 'item-wrapper', 'row artifact-description'])
            total_items = len(pub_items)
            logger.info(f"Found {total_items} publication items")
            
            if total_items == 0:
                logger.warning("No publication items found. HTML structure may have changed.")
                logger.debug("Classes found in document:")
                for elem in soup.find_all(class_=True):
                    logger.debug(f"Found element with classes: {elem.get('class', [])}")
            
            for i, item in enumerate(pub_items[:limit], 1):
                try:
                    logger.info(f"\nProcessing publication {i}/{min(total_items, limit)}")
                    logger.debug(f"Publication HTML preview: {str(item)[:200]}")
                    
                    # Log item structure before processing
                    logger.debug("Item classes: %s", item.get('class', []))
                    logger.debug("Item name: %s", item.name)
                    
                    publication = self._parse_publication(item)
                    
                    if not publication:
                        logger.warning(f"Failed to parse publication {i}, skipping")
                        continue
                    
                    # Detailed logging of publication data structure
                    logger.debug("Publication data structure:")
                    for key, value in publication.items():
                        if key != 'abstract':  # Avoid logging large text
                            logger.debug(f"{key}: {type(value)}")
                    
                    # Safely parse identifiers with type checking
                    try:
                        if not isinstance(publication.get('identifiers'), (str, dict)):
                            logger.error(f"Invalid identifiers type: {type(publication.get('identifiers'))}")
                            continue
                        
                        identifiers = (json.loads(publication['identifiers']) 
                                    if isinstance(publication['identifiers'], str) 
                                    else publication['identifiers'])
                        
                        # Validate identifiers structure
                        if not isinstance(identifiers, dict):
                            logger.error(f"Parsed identifiers is not a dictionary: {type(identifiers)}")
                            continue
                        
                        handle = identifiers.get('handle')
                        if not handle:
                            logger.warning("No handle found in identifiers")
                            logger.debug(f"Identifiers content: {identifiers}")
                            continue
                        
                        if handle in self.seen_handles:
                            logger.debug(f"Skipping duplicate handle: {handle}")
                            continue
                        
                        # Log successful publication details
                        logger.info("=" * 80)
                        logger.info("Successfully processed publication:")
                        logger.info(f"Title: {publication['title']}")
                        authors_str = ', '.join(publication['authors']) if publication['authors'] else 'No authors listed'
                        logger.info(f"Authors: {authors_str}")
                        logger.info(f"Type: {publication['type']}")
                        logger.info(f"Date: {publication['date_issue'] or 'No date available'}")
                        logger.info(f"DOI: {publication.get('doi', 'Not available')}")
                        logger.info(f"Handle: {handle}")
                        
                        # Log data types for debugging
                        logger.debug("Data types check:")
                        logger.debug(f"Title type: {type(publication['title'])}")
                        logger.debug(f"Authors type: {type(publication['authors'])}")
                        logger.debug(f"Type type: {type(publication['type'])}")
                        logger.debug(f"Date type: {type(publication['date_issue'])}")
                        
                        if identifiers.get('keywords'):
                            logger.info(f"Keywords: {', '.join(identifiers['keywords'])}")
                        
                        if publication['abstract']:
                            preview_length = 200
                            abstract_preview = publication['abstract'][:preview_length]
                            if len(publication['abstract']) > preview_length:
                                abstract_preview += "..."
                            logger.info(f"Abstract preview: {abstract_preview}")
                        
                        logger.info("=" * 80)
                        
                        publications.append(publication)
                        self.seen_handles.add(handle)
                        logger.info(f"Total publications processed: {len(publications)}")
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing error: {str(e)}")
                        logger.debug(f"Failed JSON content: {publication.get('identifiers', '')[:200]}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing publication metadata: {str(e)}", exc_info=True)
                        continue
                    
                except Exception as e:
                    logger.error(f"Error processing publication item: {str(e)}", exc_info=True)
                    continue
                
                if len(publications) >= limit:
                    logger.info(f"Reached desired limit of {limit} publications")
                    break
            
            return publications
            
        except Exception as e:
            logger.error(f"Error in fetch_publications: {str(e)}", exc_info=True)
            return publications
    def fetch_additional_content(self, content_type: str, limit: int = 2) -> List[Dict]:
        """Fetch content from additional endpoints while preserving original functionality."""
        if content_type not in self.endpoints:
            logger.error(f"Invalid content type: {content_type}")
            return []
            
        url = self.endpoints[content_type]
        logger.info(f"Fetching {content_type} from: {url}")

        try:
            # Use existing request and parsing logic
            response = self._make_request(url)
            if response.status_code != 200:
                logger.error(f"Failed to access {content_type} page: {response.status_code}")
                return []
                
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all(['div', 'article'], class_=['ds-artifact-item', 'item-wrapper', 'row artifact-description'])
            
            results = []
            for i, item in enumerate(items[:limit], 1):
                try:
                    content = self._parse_publication(item)
                    if not content:
                        continue
                        
                    # Add content type to identifiers
                    identifiers = json.loads(content['identifiers'])
                    identifiers['content_type'] = content_type
                    content['identifiers'] = json.dumps(identifiers)
                    
                    # Add content type to tags
                    content['tags'].append({
                        'name': content_type,
                        'tag_type': 'content_type',
                        'additional_metadata': json.dumps({
                            'source': 'knowhub',
                            'type': content_type
                        })
                    })
                    
                    results.append(content)
                    
                except Exception as e:
                    logger.error(f"Error processing {content_type} item {i}: {str(e)}")
                    continue
                    
                if len(results) >= limit:
                    break
                    
            return results
            
        except Exception as e:
            logger.error(f"Error fetching {content_type}: {str(e)}")
            return []

    def fetch_all_content(self, limit: int = 2) -> Dict[str, List[Dict]]:
        """Fetch content from all endpoints including original publications."""
        all_content = {}
        
        # Fetch from original publications endpoint
        logger.info("Fetching from original publications endpoint...")
        all_content['publications'] = self.fetch_publications(limit=limit)
        
        # Fetch from additional endpoints
        for content_type in self.endpoints:
            logger.info(f"Fetching from {content_type} endpoint...")
            content = self.fetch_additional_content(content_type, limit=limit)
            all_content[content_type] = content
            
        return all_content

    def _parse_publication(self, element: BeautifulSoup) -> Optional[Dict]:
        """Parse a DSpace publication element with enhanced error handling and logging."""
        try:
            logger.info("\nExtracting publication information...")
            
            # Log element type and structure
            logger.debug(f"Element type: {type(element)}")
            if hasattr(element, 'name'):
                logger.debug(f"Element name: {element.name}")
            if hasattr(element, 'attrs'):
                logger.debug(f"Element attributes: {element.attrs}")
            
            # Extract title with detailed logging
            title_elem = None
            if isinstance(element, BeautifulSoup) or hasattr(element, 'find'):
                logger.debug("Searching for title element...")
                title_elem = (
                    element.find(['h4', 'h3', 'h2'], class_=['artifact-title', 'item-title']) or
                    element.find('a', class_='item-title')
                )
                if title_elem:
                    logger.debug(f"Found title element: {title_elem.name} with classes {title_elem.get('class', [])}")
                else:
                    logger.warning("No title element found with expected classes")
                    # Log available elements for debugging
                    logger.debug("Available elements with similar classes:")
                    for elem in element.find_all(class_=True):
                        if any(c in str(elem.get('class', [])) for c in ['title', 'artifact']):
                            logger.debug(f"Found potential title element: {elem.name} with classes {elem.get('class', [])}")
            
            if not title_elem:
                logger.warning("No title element found")
                return None
            
            # Safely extract title text
            title = ""
            if hasattr(title_elem, 'get_text'):
                title = title_elem.get_text().strip()
            elif hasattr(title_elem, 'text'):
                title = title_elem.text.strip()
            else:
                title = str(title_elem).strip()
                
            title = safe_str(title)
            logger.debug(f"Found title: {title[:100]}...")
            
            # Get URL and handle with defensive programming
            url = None
            handle = None
            
            # Try to find the link in different ways
            link = None
            if hasattr(title_elem, 'find'):
                link = title_elem.find('a')
            if not link and hasattr(title_elem, 'name') and title_elem.name == 'a':
                link = title_elem
                
            # Extract URL and handle safely
            if link and hasattr(link, 'get'):
                href = link.get('href', '')
                if href:
                    url = urljoin(self.base_url, href)
                    handle_match = re.search(r'handle/([0-9/]+)', url)
                    if handle_match:
                        handle = handle_match.group(1)
            
            if not handle:
                logger.warning("No handle found for publication")
                return None
            
            # Extract metadata with defensive programming
            metadata = self._extract_metadata(element)
            
            # Generate summary safely
            abstract = metadata.get('abstract', '')
            try:
                summary = self._generate_summary(title, abstract)
            except Exception as e:
                logger.error(f"Error generating summary: {e}")
                summary = abstract or f"Publication about {title}"
            
            # Create publication record with safe defaults
            publication = {
                'doi': metadata.get('doi'),
                'title': title,
                'abstract': abstract or f"Publication about {title}",
                'summary': summary,
                'authors': metadata.get('authors', []),
                'description': abstract or f"Publication about {title}",
                'expert_id': None,
                'type': metadata.get('type', 'other'),
                'subtitles': json.dumps({}),
                'publishers': json.dumps({
                    'name': 'APHRC',
                    'url': self.base_url,
                    'type': 'repository'
                }),
                'collection': 'knowhub',
                'date_issue': metadata.get('date'),
                'citation': metadata.get('citation'),
                'language': metadata.get('language', 'en'),
                'identifiers': json.dumps({
                    'doi': metadata.get('doi'),
                    'handle': handle,
                    'url': url,
                    'source_id': f"knowhub-{handle.replace('/', '-')}",
                    'keywords': metadata.get('keywords', [])
                }),
                'source': 'knowhub',
                'tags': [
                    {
                        'name': author,
                        'tag_type': 'author',
                        'additional_metadata': json.dumps({
                            'source': 'knowhub',
                            'affiliation': 'APHRC'
                        })
                    }
                    for author in metadata.get('authors', [])
                ] + [
                    {
                        'name': keyword,
                        'tag_type': 'domain',
                        'additional_metadata': json.dumps({
                            'source': 'knowhub',
                            'type': 'keyword'
                        })
                    }
                    for keyword in metadata.get('keywords', [])
                ] + [{
                    'name': metadata.get('type', 'other'),
                    'tag_type': 'publication_type',
                    'additional_metadata': json.dumps({
                        'source': 'knowhub',
                        'original_type': metadata.get('type', 'other')
                    })
                }]
            }
            
            return publication
        
        except Exception as e:
            logger.error(f"Error parsing publication element: {str(e)}", exc_info=True)
            return None

    def _extract_metadata(self, element: BeautifulSoup) -> Dict:
        """Extract metadata from publication element with improved error handling."""
        logger.debug("Extracting metadata fields...")
        metadata = {
            'authors': [],
            'keywords': [],
            'type': 'other',
            'date': None,
            'doi': None,
            'citation': None,
            'language': 'en',
            'abstract': ''
        }
        
        try:
            # Only proceed if element is a proper BeautifulSoup object
            if not isinstance(element, BeautifulSoup) and not hasattr(element, 'find'):
                return metadata
                
            # Find metadata section
            meta_div = element.find('div', class_=['item-metadata', 'artifact-info'])
            if not meta_div:
                return metadata
            
            # Extract authors safely
            author_elems = meta_div.find_all('span', class_=['author', 'creator'])
            metadata['authors'] = [
                author.get_text().strip() if hasattr(author, 'get_text') else str(author).strip()
                for author in author_elems
                if author and (hasattr(author, 'get_text') or str(author).strip())
            ]
            
            # Extract date safely
            date_elem = meta_div.find('span', class_=['date', 'issued'])
            if date_elem and hasattr(date_elem, 'get_text'):
                date_str = date_elem.get_text().strip()
                metadata['date'] = self._parse_date(date_str)
            
            # Extract type safely
            type_elem = meta_div.find('span', class_=['type', 'resourcetype'])
            if type_elem and hasattr(type_elem, 'get_text'):
                metadata['type'] = self._normalize_publication_type(type_elem.get_text().strip())
            
            # Extract DOI safely
            doi_elem = meta_div.find('span', class_='doi')
            if doi_elem and hasattr(doi_elem, 'get_text'):
                doi_text = doi_elem.get_text().strip()
                doi_match = re.search(r'10\.\d{4,}/\S+', doi_text)
                if doi_match:
                    metadata['doi'] = doi_match.group(0)
            
            # Extract keywords safely
            keyword_elems = meta_div.find_all('span', class_=['subject', 'keyword'])
            metadata['keywords'] = [
                kw.get_text().strip() if hasattr(kw, 'get_text') else str(kw).strip()
                for kw in keyword_elems
                if kw and (hasattr(kw, 'get_text') or str(kw).strip())
            ]
            
            # Extract abstract safely
            abstract_elem = meta_div.find('span', class_=['abstract', 'description'])
            if abstract_elem and hasattr(abstract_elem, 'get_text'):
                metadata['abstract'] = safe_str(abstract_elem.get_text().strip())
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return metadata

    def _normalize_publication_type(self, type_str: str) -> str:
        """Normalize publication type strings."""
        type_mapping = {
            'article': 'journal_article',
            'journal article': 'journal_article',
            'research article': 'journal_article',
            'review': 'review_article',
            'book': 'book',
            'book chapter': 'book_chapter',
            'conference': 'conference_paper',
            'proceedings': 'conference_proceedings',
            'report': 'report',
            'technical report': 'technical_report',
            'working paper': 'working_paper',
            'thesis': 'thesis',
            'dissertation': 'dissertation',
            'policy brief': 'policy_brief',
            'data': 'dataset'
        }
        
        type_str = type_str.lower().strip()
        return type_mapping.get(type_str, 'other')

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string into ISO format."""
        if not date_str:
            return None
            
        try:
            # Try common DSpace date formats
            formats = [
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y'
            ]
            
            for fmt in formats:
                try:
                    date = datetime.strptime(date_str.strip(), fmt)
                    return date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Try to extract year if full date parsing fails
            year_match = re.search(r'\d{4}', date_str)
            if year_match:
                return f"{year_match.group(0)}-01-01"
            
            return None
            
        except Exception:
            return None

    def _generate_summary(self, title: str, abstract: str) -> str:
        """Generate a summary using the TextSummarizer."""
        try:
            title = truncate_text(title, max_length=200)
            abstract = truncate_text(abstract, max_length=1000)
            try:
                summary = self.summarizer.summarize(title, abstract)
                return truncate_text(summary, max_length=500)
            except Exception as e:
                logger.error(f"Summary generation error: {e}")
                return abstract if abstract else f"Publication about {title}"
        except Exception as e:
            logger.error(f"Error in summary generation: {e}")
            return title

    def _make_request(self, url: str, method: str = 'get', **kwargs) -> requests.Response:
        """Make an HTTP request with error handling."""
        try:
            logger.debug(f"Making {method.upper()} request to: {url}")
            kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
            kwargs['verify'] = False  # Disable SSL verification
            
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            
            logger.debug(f"Request successful: {response.status_code}")
            sleep(1)  # Basic rate limiting
            
            return response
            
        except requests.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            raise

    def close(self):
        """Close resources and perform cleanup."""
        try:
            if hasattr(self.summarizer, 'close'):
                self.summarizer.close()
            
            self.seen_handles.clear()
            
            logger.info("KnowhubScraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error closing KnowhubScraper: {e}")