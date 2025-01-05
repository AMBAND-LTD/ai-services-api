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
        self.search_url = f"{self.base_url}/research/"
        self.search_params = {
            'kwd': 'aphrc',
            'stp': 'broad',
            'yrl': '2000',
            'yrh': '2025',
            'limit': '1000',
            'sort': 'score_desc'
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        # Initialize summarizer
        self.summarizer = summarizer or TextSummarizer()
        
        # Track seen publications to avoid duplicates
        self.seen_dois = set()
        
        logger.info("ResearchNexusScraper initialized")

    def fetch_content(self, limit: int = 10) -> List[Dict]:
        """Fetch APHRC publications from Research Nexus."""
        publications = []
        logger.info(f"Starting to fetch up to {limit} publications from Research Nexus")
        logger.info(f"Using search URL: {self.search_url}")
        logger.info(f"Search parameters: {self.search_params}")
        
        try:
            response = self._make_request(self.search_url, params=self.search_params)
            if response.status_code != 200:
                logger.error(f"Failed to access search page: {response.status_code}")
                return publications

            logger.info("Successfully accessed search results page")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all publication items
            pub_items = soup.find_all('div', class_=['search-result', 'research-item', 'result-item'])
            total_items = len(pub_items)
            logger.info(f"Found {total_items} publication items in search results")
            
            for i, item in enumerate(pub_items[:limit], 1):
                try:
                    logger.info(f"\nProcessing publication {i}/{min(total_items, limit)}")
                    publication = self._parse_publication(item)
                    if publication and publication['doi'] not in self.seen_dois:
                        logger.info("=" * 80)
                        logger.info("Publication Details:")
                        logger.info(f"Title: {publication['title']}")
                        logger.info(f"Authors: {', '.join(publication['authors']) if publication['authors'] else 'No authors listed'}")
                        logger.info(f"Type: {publication['type']}")
                        logger.info(f"Date: {publication['date_issue'] or 'No date available'}")
                        logger.info(f"DOI: {publication['doi']}")
                        
                        # Log keywords if available
                        keywords = json.loads(publication['identifiers'])['keywords']
                        if keywords:
                            logger.info(f"Keywords: {', '.join(keywords)}")
                        
                        # Log abstract preview
                        if publication['abstract']:
                            abstract_preview = publication['abstract'][:200] + "..." if len(publication['abstract']) > 200 else publication['abstract']
                            logger.info(f"Abstract preview: {abstract_preview}")
                        
                        logger.info("=" * 80)
                        
                        publications.append(publication)
                        self.seen_dois.add(publication['doi'])
                        logger.info(f"Total publications processed so far: {len(publications)}")
                        
                        if len(publications) >= limit:
                            logger.info(f"Reached desired limit of {limit} publications")
                            break
                except Exception as e:
                    logger.error(f"Error parsing publication: {e}")
                    continue
            
            if not publications:
                logger.warning("No publications found in search results")
                logger.debug("Search results structure:")
                logger.debug(soup.prettify())
            
            return publications
            
        except Exception as e:
            logger.error(f"Error fetching content: {e}")
            return publications

            logger.info("Successfully accessed APHRC institution page")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Debug log the HTML structure
            logger.debug("HTML structure:")
            logger.debug(soup.prettify()[:500])  # First 500 chars of formatted HTML
            
            logger.info("Parsing institution page for publications")
            pub_section = soup.find('section', {'id': 'publications'}) or \
                         soup.find('div', class_='publications') or \
                         soup.find('div', class_=['publication-listing', 'search-results'])
            
            if pub_section:
                logger.info("Found publications section")
                # Try multiple selectors for publication items
                pub_items = pub_section.find_all(['article', 'div'], class_=['publication-item', 'search-result-item', 'result-item']) or \
                           pub_section.find_all(['article', 'div'], class_='row') or \
                           pub_section.find_all('div', recursive=False)
                
                total_items = len(pub_items)
                logger.info(f"Found {total_items} publication items on institution page")
                
                if total_items == 0:
                    logger.debug("Publication section HTML:")
                    logger.debug(pub_section.prettify())
                
                for i, item in enumerate(pub_items[:limit], 1):
                    try:
                        logger.info(f"\nProcessing publication {i}/{min(total_items, limit)}")
                        logger.debug("Publication item HTML:")
                        logger.debug(item.prettify())
                        
                        publication = self._parse_publication(item)
                        if publication and publication['doi'] not in self.seen_dois:
                            logger.info("=" * 80)
                            logger.info("Publication Details:")
                            logger.info(f"Title: {publication['title']}")
                            logger.info(f"Authors: {', '.join(publication['authors']) if publication['authors'] else 'No authors listed'}")
                            logger.info(f"Type: {publication['type']}")
                            logger.info(f"Date: {publication['date_issue'] or 'No date available'}")
                            logger.info(f"DOI: {publication['doi']}")
                            
                            # Log keywords if available
                            keywords = json.loads(publication['identifiers'])['keywords']
                            if keywords:
                                logger.info(f"Keywords: {', '.join(keywords)}")
                            
                            # Log abstract preview
                            if publication['abstract']:
                                abstract_preview = publication['abstract'][:200] + "..." if len(publication['abstract']) > 200 else publication['abstract']
                                logger.info(f"Abstract preview: {abstract_preview}")
                            
                            logger.info("=" * 80)
                            
                            publications.append(publication)
                            self.seen_dois.add(publication['doi'])
                            logger.info(f"Total publications processed so far: {len(publications)}")
                            
                            if len(publications) >= limit:
                                logger.info(f"Reached desired limit of {limit} publications")
                                break
                    except Exception as e:
                        logger.error(f"Error parsing publication: {e}")
                        continue
            else:
                logger.warning("No publications section found on institution page")
                logger.debug("Page structure:")
                logger.debug(soup.prettify())
                
                for i, item in enumerate(pub_items[:limit], 1):
                    try:
                        logger.info(f"Processing publication {i}/{min(total_items, limit)}")
                        publication = self._parse_publication(item)
                        if publication and publication['doi'] not in self.seen_dois:
                            # Log detailed publication info
                            logger.info("=" * 80)
                            logger.info("Publication Details:")
                            logger.info(f"Title: {publication['title']}")
                            logger.info(f"Authors: {', '.join(publication['authors']) if publication['authors'] else 'No authors listed'}")
                            logger.info(f"Type: {publication['type']}")
                            logger.info(f"Date: {publication['date_issue'] or 'No date available'}")
                            logger.info(f"DOI: {publication['doi']}")
                            
                            # Log keywords if available
                            keywords = json.loads(publication['identifiers'])['keywords']
                            if keywords:
                                logger.info(f"Keywords: {', '.join(keywords)}")
                            
                            # Log abstract preview
                            if publication['abstract']:
                                abstract_preview = publication['abstract'][:200] + "..." if len(publication['abstract']) > 200 else publication['abstract']
                                logger.info(f"Abstract preview: {abstract_preview}")
                            
                            logger.info("=" * 80)
                            
                            publications.append(publication)
                            self.seen_dois.add(publication['doi'])
                            logger.info(f"Total publications processed so far: {len(publications)}")
                            
                            if len(publications) >= limit:
                                logger.info(f"Reached desired limit of {limit} publications")
                                break
                    except Exception as e:
                        logger.error(f"Error parsing publication: {e}")
                        continue

            if len(publications) < limit:
                search_publications = self._fetch_from_search(limit - len(publications))
                for pub in search_publications:
                    if pub['doi'] not in self.seen_dois:
                        publications.append(pub)
                        self.seen_dois.add(pub['doi'])
                        
                        if len(publications) >= limit:
                            break
            
            return publications
            
        except Exception as e:
            logger.error(f"Error fetching content: {e}")
            return publications

    def _fetch_from_search(self, limit: int) -> List[Dict]:
        """Fetch publications using the search endpoint."""
        publications = []
        logger.info(f"Attempting to fetch additional {limit} publications from search endpoint")
        try:
            logger.info("Constructing search parameters for APHRC")
            params = {
                'types': 'institutions',
                'src': 'kwd',
                'search': 'aphrc',
                'limit': 25
            }
            
            logger.info(f"Making request to search endpoint with params: {params}")
            response = self._make_request(self.search_url, params=params)
            if response.status_code != 200:
                logger.error(f"Search endpoint returned status code: {response.status_code}")
                return publications

            logger.info("Successfully accessed search endpoint")
            logger.info("\nProcessing publications from search results...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
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
            logger.info("\nExtracting publication information...")
            
            title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'a'], class_='title') or \
                        element.find(['h1', 'h2', 'h3', 'h4', 'a'])
            if not title_elem:
                logger.warning("No title element found in publication")
                return None
            
            title = safe_str(title_elem.text.strip())
            logger.debug(f"Found publication title: {title[:100]}...")
            
            # Extract URL and generate DOI
            url = title_elem.get('href', '') if title_elem.name == 'a' else \
                  (element.find('a', href=True) or {}).get('href', '')
            
            if url and not url.startswith('http'):
                url = self.base_url + url
            
            doi = self._generate_synthetic_doi(title, url)
            
            # Extract date and publication year
            date = None
            date_elem = element.find(['time', 'span'], class_=['date', 'published-date'])
            if date_elem:
                date_str = date_elem.get('datetime', '') or date_elem.text.strip()
                date = self._parse_date(date_str)
            
            # Extract abstract/description
            abstract_elem = element.find(['div', 'p'], class_=['abstract', 'description'])
            abstract = safe_str(abstract_elem.text.strip()) if abstract_elem else ''
            
            # Generate summary
            try:
                summary = self._generate_summary(title, abstract)
            except Exception as e:
                logger.error(f"Error generating summary: {e}")
                summary = abstract or f"Publication about {title}"
            
            # Extract authors
            authors = []
            author_elems = element.find_all(['span', 'div'], class_='author')
            for author_elem in author_elems:
                author_name = author_elem.text.strip()
                if author_name:
                    authors.append(author_name)
            
            # Extract publication type
            pub_type = 'other'
            type_elem = element.find(['span', 'div'], class_='type')
            if type_elem:
                pub_type = self._normalize_publication_type(type_elem.text.strip())
            
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
            keyword_elems = element.find_all(['span', 'a'], class_=['keyword', 'tag'])
            for kw_elem in keyword_elems:
                keyword = kw_elem.text.strip()
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
            
            # Construct publication record
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
            
            year_match = re.search(r'\d{4}', date_str)
            if year_match:
                return datetime(int(year_match.group(0)), 1, 1)
            
            return None
            
        except Exception:
            return None

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

    def _make_request(self, url: str, params: Dict = None, method: str = 'get', **kwargs) -> requests.Response:
        """Make an HTTP request with error handling and rate limiting."""
        try:
            logger.debug(f"Making {method.upper()} request to: {url}")
            kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
            if params:
                kwargs['params'] = params
                logger.debug(f"Request parameters: {params}")
            
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            
            logger.debug("Applying rate limiting delay (1 second)")
            sleep(1)
            
            logger.debug(f"Request successful: {response.status_code}")
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