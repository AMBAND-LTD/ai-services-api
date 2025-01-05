import os
import logging
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime
import json
import hashlib
import re
from time import sleep

from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.data.openalex.text_processor import safe_str, truncate_text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResearchNexusScraper:
    def __init__(self, summarizer: Optional[TextSummarizer] = None):
        """Initialize ResearchNexusScraper with summarization capability."""
        self.base_url = "https://research-nexus.net"
        self.aphrc_url = f"{self.base_url}/institution/9000041605/"
        self.search_url = f"{self.base_url}/directory/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        # Initialize summarizer
        self.summarizer = summarizer or TextSummarizer()
        
        # Track seen publications to avoid duplicates
        self.seen_dois = set()

    def fetch_content(self, limit: int = 10) -> List[Dict]:
        """
        Fetch APHRC publications from Research Nexus.
        
        Args:
            limit (int): Maximum number of publications to fetch
            
        Returns:
            List[Dict]: List of publications in standard format
        """
        publications = []
        try:
            # First get the institution page
            response = self._make_request(self.aphrc_url)
            if response.status_code != 200:
                logger.error(f"Failed to access APHRC page: {response.status_code}")
                return publications

            # Parse the institution page
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the publications section and extract publications
            pub_section = soup.find('section', {'id': 'publications'}) or soup.find('div', class_='publications')
            if pub_section:
                pub_items = pub_section.find_all('article') or pub_section.find_all('div', class_='publication-item')
                
                for item in pub_items[:limit]:
                    try:
                        publication = self._parse_publication(item)
                        if publication and publication['doi'] not in self.seen_dois:
                            publications.append(publication)
                            self.seen_dois.add(publication['doi'])
                            
                            if len(publications) >= limit:
                                break
                    except Exception as e:
                        logger.error(f"Error parsing publication: {e}")
                        continue
            
            # If we still need more publications, try the search endpoint
            if len(publications) < limit:
                search_publications = self._fetch_from_search(limit - len(publications))
                for pub in search_publications:
                    if pub['doi'] not in self.seen_dois:
                        publications.append(pub)
                        self.seen_dois.add(pub['doi'])
                        
                        if len(publications) >= limit:
                            break
            
            return publications[:limit]
            
        except Exception as e:
            logger.error(f"Error fetching content: {e}")
            return publications

    def _fetch_from_search(self, limit: int) -> List[Dict]:
        """Fetch publications using the search endpoint."""
        publications = []
        try:
            # Construct search parameters for APHRC
            params = {
                'types': 'institutions',
                'src': 'kwd',
                'search': 'aphrc',
                'limit': 25  # Maximum allowed by the site
            }
            
            response = self._make_request(self.search_url, params=params)
            if response.status_code != 200:
                return publications

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find publication listings
            pub_items = soup.find_all('article') or soup.find_all('div', class_='publication-item')
            
            for item in pub_items[:limit]:
                try:
                    publication = self._parse_publication(item)
                    if publication:
                        publications.append(publication)
                        
                        if len(publications) >= limit:
                            break
                except Exception as e:
                    logger.error(f"Error parsing search result: {e}")
                    continue
            
            return publications
            
        except Exception as e:
            logger.error(f"Error fetching from search: {e}")
            return publications

    def _parse_publication(self, element: BeautifulSoup) -> Optional[Dict]:
        """Parse a publication element into standardized format."""
        try:
            # Extract basic information
            title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'a'], class_='title') or \
                        element.find(['h1', 'h2', 'h3', 'h4', 'a'])
            if not title_elem:
                return None
                
            title = safe_str(title_elem.text.strip())
            
            # Extract URL and generate DOI
            url = title_elem.get('href', '') if title_elem.name == 'a' else \
                  (element.find('a', href=True) or {}).get('href', '')
            
            if url and not url.startswith('http'):
                url = self.base_url + url
            
            # Try to extract DOI from page, otherwise generate synthetic one
            doi_elem = element.find('meta', {'name': 'citation_doi'}) or \
                      element.find('span', class_='doi')
            doi = doi_elem.get('content', '') if doi_elem else None
            
            if not doi:
                doi = self._generate_synthetic_doi(title, url)
            
            # Extract date and publication year
            date = None
            year = None
            
            date_elem = element.find('meta', {'name': 'citation_publication_date'}) or \
                       element.find(['time', 'span'], class_='date')
            if date_elem:
                date_str = date_elem.get('content', '') or date_elem.text.strip()
                date = self._parse_date(date_str)
                if date:
                    year = date.year
            
            # Extract abstract/description
            abstract_elem = element.find('meta', {'name': 'citation_abstract'}) or \
                          element.find(['div', 'p'], class_=['abstract', 'description'])
            abstract = safe_str(abstract_elem.text.strip()) if abstract_elem else ''
            
            # Generate summary
            try:
                summary = self._generate_summary(title, abstract)
            except Exception as e:
                logger.error(f"Error generating summary: {e}")
                summary = abstract or f"Publication about {title}"
            
            # Extract authors
            authors = []
            author_elems = element.find_all('meta', {'name': 'citation_author'}) or \
                          element.find_all(['span', 'div'], class_='author')
            
            for author_elem in author_elems:
                author_name = author_elem.get('content', '') or author_elem.text.strip()
                if author_name:
                    authors.append(author_name)
            
            # Extract publication type
            pub_type = 'journal_article'  # Default type
            type_elem = element.find('meta', {'name': 'citation_type'}) or \
                       element.find(['span', 'div'], class_='type')
            if type_elem:
                type_text = type_elem.get('content', '') or type_elem.text.strip()
                pub_type = self._normalize_publication_type(type_text)
            
            # Extract and process keywords/tags
            tags = []
            keywords = []
            
            # Add publication type tag
            tags.append({
                'name': pub_type,
                'tag_type': 'publication_type',
                'additional_metadata': json.dumps({
                    'source': 'researchnexus',
                    'original_type': pub_type
                })
            })
            
            # Add author tags
            for author in authors:
                tags.append({
                    'name': author,
                    'tag_type': 'author',
                    'additional_metadata': json.dumps({
                        'source': 'researchnexus',
                        'affiliation': 'APHRC'
                    })
                })
            
            # Extract keywords
            keyword_elems = element.find_all('meta', {'name': 'citation_keywords'}) or \
                          element.find_all(['span', 'a'], class_=['keyword', 'tag'])
            
            for kw_elem in keyword_elems:
                keyword = kw_elem.get('content', '') or kw_elem.text.strip()
                if keyword and keyword not in keywords:
                    keywords.append(keyword)
                    tags.append({
                        'name': keyword,
                        'tag_type': 'domain',
                        'additional_metadata': json.dumps({
                            'source': 'researchnexus',
                            'type': 'keyword'
                        })
                    })
            
            # Construct complete publication record
            publication = {
                'doi': doi,
                'title': title,
                'abstract': abstract or f"Publication about {title}",
                'summary': summary,
                'authors': authors,
                'description': abstract or f"Publication about {title}",
                'expert_id': None,
                'type': pub_type,
                'subtitles': json.dumps({}),
                'publishers': json.dumps({
                    'name': 'APHRC',
                    'url': self.aphrc_url,
                    'type': 'institution'
                }),
                'collection': 'research_nexus',
                'date_issue': date.strftime('%Y-%m-%d') if date else None,
                'citation': None,
                'language': 'en',
                'identifiers': json.dumps({
                    'doi': doi,
                    'url': url,
                    'source_id': f"researchnexus-{hashlib.md5(url.encode()).hexdigest()[:8]}",
                    'keywords': keywords
                }),
                'source': 'researchnexus',
                'tags': tags
            }
            
            return publication
            
        except Exception as e:
            logger.error(f"Error parsing publication element: {e}")
            return None

    def _generate_synthetic_doi(self, title: str, url: str) -> str:
        """Generate a synthetic DOI for publications without one."""
        unique_string = f"{title}|{url}"
        hash_object = hashlib.sha256(unique_string.encode())
        hash_digest = hash_object.hexdigest()[:16]
        return f"10.0000/researchnexus-{hash_digest}"

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string into datetime object."""
        if not date_str:
            return None
            
        try:
            # Common date formats in Research Nexus
            formats = [
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%B %d, %Y',
                '%d %B %Y',
                '%Y'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str.strip(), fmt)
                except ValueError:
                    continue
            
            # Try to extract year if full date parsing fails
            year_match = re.search(r'\d{4}', date_str)
            if year_match:
                return datetime(int(year_match.group(0)), 1, 1)
            
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

    def _make_request(self, url: str, params: Dict = None, method: str = 'get', **kwargs) -> requests.Response:
        """Make an HTTP request with error handling and rate limiting."""
        try:
            kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
            if params:
                kwargs['params'] = params
            
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            
            # Basic rate limiting
            sleep(1)
            
            return response
            
        except requests.RequestException as e:
            logger.error(f"Request error for {url}: {e}")
            raise

    def close(self):
        """Close resources and perform cleanup."""
        try:
            if hasattr(self.summarizer, 'close'):
                self.summarizer.close()
            
            self.seen_dois.clear()
            
            logger.info("ResearchNexusScraper resources cleaned up")
        except Exception as e:
            logger.error(f"Error closing ResearchNexusScraper: {e}")