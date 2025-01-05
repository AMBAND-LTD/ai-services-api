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

from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.data.openalex.text_processor import safe_str, truncate_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KnowhubScraper:
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize KnowhubScraper with authentication capabilities."""
        self.base_url = os.getenv('KNOWHUB_BASE_URL', 'https://knowhub.aphrc.org')
        self.publications_url = f"{self.base_url}/handle/123456789/1"
        
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

    def fetch_publications(self, limit: int = 10) -> List[Dict]:
        """Fetch publications from Knowhub."""
        publications = []
        try:
            logger.info(f"Starting to fetch up to {limit} publications from Knowhub")
            
            # Access the main publications page
            response = self._make_request(self.publications_url)
            if response.status_code != 200:
                logger.error(f"Failed to access publications page: {response.status_code}")
                return publications
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find publication listings
            pub_items = soup.find_all(['div', 'article'], class_=['ds-artifact-item', 'item-wrapper', 'row artifact-description'])
            total_items = len(pub_items)
            logger.info(f"Found {total_items} publication items")
            
            for i, item in enumerate(pub_items[:limit], 1):
                try:
                    logger.info(f"Processing publication {i}/{min(total_items, limit)}")
                    publication = self._parse_publication(item)
                    
                    if publication and publication['identifiers'].get('handle') not in self.seen_handles:
                        # Log detailed publication info
                        logger.info("=" * 80)
                        logger.info("Publication Details:")
                        logger.info(f"Title: {publication['title']}")
                        logger.info(f"Authors: {', '.join(publication['authors']) if publication['authors'] else 'No authors listed'}")
                        logger.info(f"Type: {publication['type']}")
                        logger.info(f"Date: {publication['date_issue'] or 'No date available'}")
                        
                        # Show DOI if available
                        if publication['doi']:
                            logger.info(f"DOI: {publication['doi']}")
                            
                        # Show handle
                        identifiers = json.loads(publication['identifiers'])
                        logger.info(f"Handle: {identifiers['handle']}")
                        
                        # Log keywords if available
                        if identifiers['keywords']:
                            logger.info(f"Keywords: {', '.join(identifiers['keywords'])}")
                        
                        # Log abstract preview
                        if publication['abstract']:
                            abstract_preview = publication['abstract'][:200] + "..." if len(publication['abstract']) > 200 else publication['abstract']
                            logger.info(f"Abstract preview: {abstract_preview}")
                        
                        logger.info("=" * 80)
                        
                        publications.append(publication)
                        self.seen_handles.add(identifiers['handle'])
                        logger.info(f"Total publications processed so far: {len(publications)}")
                        
                        if len(publications) >= limit:
                            logger.info(f"Reached desired limit of {limit} publications")
                            break
                            
                except Exception as e:
                    logger.error(f"Error processing publication item: {e}")
                    continue
            
            return publications
            
        except Exception as e:
            logger.error(f"Error fetching publications: {e}")
            return publications

    def _parse_publication(self, element: BeautifulSoup) -> Optional[Dict]:
        """Parse a DSpace publication element."""
        try:
            logger.info("\nExtracting publication information...")
            
            # Extract title and URL
            title_elem = element.find(['h4', 'h3', 'h2'], class_=['artifact-title', 'item-title']) or \
                        element.find('a', class_='item-title')
            
            if not title_elem:
                logger.warning("No title element found")
                return None
            
            title = safe_str(title_elem.text.strip())
            logger.debug(f"Found title: {title[:100]}...")
            
            # Get URL and handle
            url = None
            handle = None
            link = title_elem.find('a') if title_elem.name != 'a' else title_elem
            if link:
                url = urljoin(self.base_url, link.get('href', ''))
                handle_match = re.search(r'handle/([0-9/]+)', url)
                if handle_match:
                    handle = handle_match.group(1)
            
            if not handle:
                logger.warning("No handle found for publication")
                return None
            
            # Extract metadata
            metadata = self._extract_metadata(element)
            
            # Generate summary
            abstract = metadata.get('abstract', '')
            try:
                summary = self._generate_summary(title, abstract)
            except Exception as e:
                logger.error(f"Error generating summary: {e}")
                summary = abstract or f"Publication about {title}"
            
            # Create tags
            tags = []
            
            # Add authors as tags
            for author in metadata.get('authors', []):
                tags.append({
                    'name': author,
                    'tag_type': 'author',
                    'additional_metadata': json.dumps({
                        'source': 'knowhub',
                        'affiliation': 'APHRC'
                    })
                })
            
            # Add keywords as tags
            for keyword in metadata.get('keywords', []):
                tags.append({
                    'name': keyword,
                    'tag_type': 'domain',
                    'additional_metadata': json.dumps({
                        'source': 'knowhub',
                        'type': 'keyword'
                    })
                })
            
            # Add publication type tag
            pub_type = metadata.get('type', 'other')
            tags.append({
                'name': pub_type,
                'tag_type': 'publication_type',
                'additional_metadata': json.dumps({
                    'source': 'knowhub',
                    'original_type': pub_type
                })
            })
            
            # Construct publication record
            publication = {
                'doi': metadata.get('doi'),
                'title': title,
                'abstract': abstract or f"Publication about {title}",
                'summary': summary,
                'authors': metadata.get('authors', []),
                'description': abstract or f"Publication about {title}",
                'expert_id': None,
                'type': pub_type,
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
                'tags': tags
            }
            
            return publication
            
        except Exception as e:
            logger.error(f"Error parsing publication element: {e}")
            return None

    def _extract_metadata(self, element: BeautifulSoup) -> Dict:
        """Extract metadata from publication element."""
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
            # Find metadata section
            meta_div = element.find('div', class_=['item-metadata', 'artifact-info'])
            if not meta_div:
                return metadata
            
            # Extract authors
            author_elems = meta_div.find_all('span', class_=['author', 'creator'])
            metadata['authors'] = [
                author.text.strip()
                for author in author_elems
                if author.text.strip()
            ]
            
            # Extract date
            date_elem = meta_div.find('span', class_=['date', 'issued'])
            if date_elem:
                date_str = date_elem.text.strip()
                metadata['date'] = self._parse_date(date_str)
            
            # Extract type
            type_elem = meta_div.find('span', class_=['type', 'resourcetype'])
            if type_elem:
                metadata['type'] = self._normalize_publication_type(type_elem.text.strip())
            
            # Extract DOI
            doi_elem = meta_div.find('span', class_='doi')
            if doi_elem:
                doi_match = re.search(r'10\.\d{4,}/\S+', doi_elem.text)
                if doi_match:
                    metadata['doi'] = doi_match.group(0)
            
            # Extract keywords
            keyword_elems = meta_div.find_all('span', class_=['subject', 'keyword'])
            metadata['keywords'] = [
                kw.text.strip()
                for kw in keyword_elems
                if kw.text.strip()
            ]
            
            # Extract abstract
            abstract_elem = meta_div.find('span', class_=['abstract', 'description'])
            if abstract_elem:
                metadata['abstract'] = safe_str(abstract_elem.text.strip())
            
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