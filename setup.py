"""
System initialization and database setup module.
"""
import os
import sys
import logging
import argparse
import asyncio
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

from ai_services_api.services.centralized_repository.openalex.openalex_processor import OpenAlexProcessor
from ai_services_api.services.centralized_repository.publication_processor import PublicationProcessor
from ai_services_api.services.centralized_repository.ai_summarizer import TextSummarizer
from ai_services_api.services.recommendation.graph_initializer import GraphDatabaseInitializer
from ai_services_api.services.search.indexing.index_creator import ExpertSearchIndexManager
from ai_services_api.services.chatbot.indexing.redis_index_manager import ExpertRedisIndexManager
from ai_services_api.services.centralized_repository.database_setup import DatabaseInitializer, ExpertManager
from ai_services_api.services.centralized_repository.orcid.orcid_processor import OrcidProcessor
from ai_services_api.services.centralized_repository.knowhub.knowhub_scraper import KnowhubScraper
from ai_services_api.services.centralized_repository.website.website_scraper import WebsiteScraper
from ai_services_api.services.centralized_repository.nexus.researchnexus_scraper import ResearchNexusScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

@dataclass
class SetupConfig:
    """Configuration class for system setup"""
    skip_database: bool = False  
    skip_openalex: bool = False
    skip_publications: bool = False
    skip_graph: bool = False
    skip_search: bool = False
    skip_redis: bool = False
    skip_topics: bool = False
    expertise_csv: str = ''

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'SetupConfig':
        """Create config from command line arguments"""
        return cls(
            skip_database=args.skip_database,
            skip_openalex=args.skip_openalex,
            skip_publications=args.skip_publications,
            skip_graph=args.skip_graph,
            skip_search=args.skip_search,
            skip_redis=args.skip_redis,
            skip_topics=args.skip_topics,
            expertise_csv=args.expertise_csv
        )

class SystemInitializer:
    """Handles system initialization and setup"""
    def __init__(self, config: SetupConfig):
        self.config = config
        self.required_env_vars = [
            'DATABASE_URL',
            'NEO4J_URI',
            'NEO4J_USER',
            'NEO4J_PASSWORD',
            'OPENALEX_API_URL',
            'GEMINI_API_KEY',
            'REDIS_URL',
            'ORCID_CLIENT_ID',
            'ORCID_CLIENT_SECRET',
            'KNOWHUB_BASE_URL',
            'EXPERTISE_CSV'  # Add this to ensure the CSV path is checked
        ]

    def verify_environment(self) -> None:
        """Verify all required environment variables are set"""
        load_dotenv()
        missing_vars = [var for var in self.required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    async def initialize_database(self) -> None:
        """Initialize database and create tables using DatabaseInitializer"""
        try:
            logger.info("Initializing database...")
            initializer = DatabaseInitializer()
            initializer.create_database()
            initializer.initialize_schema()
            logger.info("Database initialization complete!")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def load_initial_experts(self) -> None:
        """Load initial experts from CSV if provided"""
        try:
            csv_path = 'experts.csv'
            
            if os.path.exists(csv_path):
                logger.info(f"Loading experts from {csv_path}...")
                expert_manager = ExpertManager()
                expert_manager.load_experts_from_csv(csv_path)
                logger.info("Initial experts loaded successfully!")
            else:
                logger.warning("No experts.csv found. Skipping expert loading.")
        except Exception as e:
            logger.error(f"Error loading initial experts: {e}")
            raise
    
    def initialize_graph(self) -> bool:
        """Initialize Neo4j graph database"""
        try:
            graph_initializer = GraphDatabaseInitializer()
            logger.info("Initializing graph database...")
            graph_initializer.initialize_graph()
            logger.info("Graph initialization complete!")
            return True
        except Exception as e:
            logger.error(f"Graph initialization failed: {e}")
            return False

    async def process_publications(self, summarizer: Optional[TextSummarizer] = None) -> None:
        """Process publications from all sources"""
        openalex_processor = OpenAlexProcessor()
        publication_processor = PublicationProcessor(openalex_processor.db, TextSummarizer())

        try:
            # Process expert data first
            logger.info("Loading initial expert data...")
            await openalex_processor.load_initial_experts(self.config.expertise_csv)
            
            if not self.config.skip_openalex:
                logger.info("Updating experts with OpenAlex data...")
                await openalex_processor.update_experts_with_openalex()
                logger.info("Expert data enrichment complete!")
            
            if not self.config.skip_publications:
                logger.info("Processing publications data...")
                
                # Process OpenAlex publications
                if not self.config.skip_openalex:
                    try:
                        logger.info("Processing OpenAlex publications...")
                        await openalex_processor.process_publications(publication_processor, source='openalex')
                    except Exception as e:
                        logger.error(f"Error processing OpenAlex publications: {e}")

                # Process ORCID publications
                try:
                    logger.info("Processing ORCID publications...")
                    orcid_processor = OrcidProcessor()
                    await orcid_processor.process_publications(publication_processor, source='orcid')
                    orcid_processor.close()
                except Exception as e:
                    logger.error(f"Error processing ORCID publications: {e}")

                # Process KnowHub content
                try:
                    logger.info("\n" + "="*50)
                    logger.info("Processing KnowHub content...")
                    logger.info("="*50)
                    
                    knowhub_scraper = KnowhubScraper(summarizer=TextSummarizer())
                    all_content = knowhub_scraper.fetch_all_content(limit=2)
                    
                    for content_type, items in all_content.items():
                        if items:
                            logger.info(f"\nProcessing {len(items)} items from {content_type}")
                            for item in items:
                                try:
                                    publication_processor.process_single_work(item, source='knowhub')
                                    logger.info(f"Successfully processed {content_type} item: {item.get('title', 'Unknown Title')}")
                                except Exception as e:
                                    logger.error(f"Error processing {content_type} item: {e}")
                                    continue
                        else:
                            logger.warning(f"No items found for {content_type}")
                    
                    knowhub_scraper.close()
                    logger.info("\nKnowHub content processing complete!")
                    
                except Exception as e:
                    logger.error(f"Error processing KnowHub content: {e}")
                finally:
                    if 'knowhub_scraper' in locals():
                        knowhub_scraper.close()

                # Process ResearchNexus publications
                try:
                    logger.info("Processing Research Nexus publications...")
                    research_nexus_scraper = ResearchNexusScraper(summarizer=TextSummarizer())
                    research_nexus_publications = research_nexus_scraper.fetch_content(limit=2)

                    if research_nexus_publications:
                        for pub in research_nexus_publications:
                            publication_processor.process_single_work(pub, source='researchnexus')
                    else:
                        logger.warning("No Research Nexus publications found")

                except Exception as e:
                    logger.error(f"Error processing Research Nexus publications: {e}")
                finally:
                    if 'research_nexus_scraper' in locals():
                        research_nexus_scraper.close()

                # Process Website publications
                try:
                    logger.info("\n" + "="*50)
                    logger.info("Processing Website publications...")
                    logger.info("="*50)
                    
                    website_scraper = WebsiteScraper(summarizer=TextSummarizer())
                    website_publications = website_scraper.fetch_content(limit=2)
                    
                    if website_publications:
                        logger.info(f"\nProcessing {len(website_publications)} website publications")
                        for pub in website_publications:
                            try:
                                publication_processor.process_single_work(pub, source='website')
                                logger.info(f"Successfully processed website publication: {pub.get('title', 'Unknown Title')}")
                            except Exception as e:
                                logger.error(f"Error processing website publication: {e}")
                                continue
                    else:
                        logger.warning("No website publications found")
                        
                    website_scraper.close()
                    logger.info("\nWebsite publications processing complete!")
                    
                except Exception as e:
                    logger.error(f"Error processing Website publications: {e}")
                finally:
                    if 'website_scraper' in locals():
                        website_scraper.close()

                # Process topics for all publications
                if not self.config.skip_topics:
                    try:
                        logger.info("\n" + "="*50)
                        logger.info("Starting topic classification...")
                        logger.info("="*50)
                        
                        # Get all publications for topic processing
                        publications = openalex_processor.db.get_all_publications()
                        
                        if publications:
                            # Generate topics using Gemini
                            topics = publication_processor.summarizer.generate_topics(publications)
                            logger.info(f"Generated topics: {topics}")
                            
                            # Process publications in batches
                            batch_size = 100
                            total_processed = 0
                            
                            for i in range(0, len(publications), batch_size):
                                batch = publications[i:i + batch_size]
                                for pub in batch:
                                    try:
                                        # Assign topics to publication
                                        assigned_topics = publication_processor.summarizer.assign_topics(pub, topics)
                                        
                                        # Update database
                                        openalex_processor.db.update_publication_topics(pub['id'], assigned_topics)
                                        total_processed += 1
                                        
                                        if total_processed % 10 == 0:
                                            logger.info(f"Processed {total_processed}/{len(publications)} publications")
                                            
                                    except Exception as e:
                                        logger.error(f"Error processing publication {pub.get('id')}: {e}")
                                        continue
                            
                            logger.info(f"\nCompleted topic classification for {total_processed} publications")
                        else:
                            logger.warning("No publications found for topic classification")
                        
                    except Exception as e:
                        logger.error(f"Error during topic classification: {e}")

        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            raise
        finally:
            openalex_processor.close()

    def create_search_indices(self) -> bool:
        """Create search indices in FAISS and Redis"""
        try:
            if not self.config.skip_search:
                logger.info("Creating FAISS search index...")
                if not ExpertSearchIndexManager().create_faiss_index():
                    raise Exception("FAISS index creation failed")

            if not self.config.skip_redis:
                logger.info("Creating Redis search index...")
                redis_manager = ExpertRedisIndexManager()
                if not (redis_manager.clear_redis_indexes() and 
                       redis_manager.create_redis_index()):
                    raise Exception("Redis index creation failed")

            return True
        except Exception as e:
            logger.error(f"Search index creation failed: {e}")
            return False

    async def initialize_system(self) -> None:
        """Main initialization flow"""
        try:
            self.verify_environment()
            
            if not self.config.skip_database:
                await self.initialize_database()
                
                # Load initial experts immediately after database initialization
                self.load_initial_experts()
            
            if not self.config.skip_publications:
                await self.process_publications()
                
            if not self.config.skip_graph:
                if not self.initialize_graph():
                    raise Exception("Graph initialization failed")
                
            if not self.create_search_indices():
                    raise Exception("Search index creation failed")
                
            logger.info("System initialization completed successfully!")
            
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            raise

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Initialize and populate the research database.')
    
    parser.add_argument('--skip-database', action='store_true',
                    help='Skip database initialization')

    parser.add_argument('--skip-openalex', action='store_true',
                    help='Skip OpenAlex data enrichment')
    parser.add_argument('--skip-publications', action='store_true',
                    help='Skip publication processing')
    parser.add_argument('--skip-graph', action='store_true',
                    help='Skip graph database initialization')
    parser.add_argument('--skip-search', action='store_true',
                    help='Skip search index creation')
    parser.add_argument('--skip-redis', action='store_true',
                    help='Skip Redis index creation')
    parser.add_argument('--skip-topics', action='store_true',
                    help='Skip topic classification')
    parser.add_argument('--expertise-csv', type=str, default='',
                        help='Path to the CSV file containing initial expert data')
    return parser.parse_args()

async def main() -> None:
    """Main execution function"""
    args = parse_arguments()
    config = SetupConfig.from_args(args)
    initializer = SystemInitializer(config)
    await initializer.initialize_system()

def run() -> None:
    """Entry point function"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)  
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import sys
    run()