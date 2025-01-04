import os
import logging
import asyncio
from dotenv import load_dotenv

from ai_services_api.services.data.openalex.openalex_processor import OpenAlexProcessor
from ai_services_api.services.data.openalex.publication_processor import PublicationProcessor
from ai_services_api.services.data.openalex.ai_summarizer import TextSummarizer
from ai_services_api.services.recommendation.graph_initializer import GraphDatabaseInitializer
from ai_services_api.services.search.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.redis_index_manager import ExpertRedisIndexManager
from ai_services_api.services.data.openalex.orcid_processor import OrcidProcessor
from ai_services_api.services.data.openalex.knowhub_scraper import KnowhubScraper
from ai_services_api.services.data.openalex.website_scraper import WebsiteScraper
from ai_services_api.services.data.openalex.researchnexus_scraper import ResearchNexusScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def setup_environment():
    """Ensure all required environment variables are set."""
    load_dotenv()
    
    required_vars = [
        'DATABASE_URL',
        'NEO4J_URI',
        'NEO4J_USER',
        'NEO4J_PASSWORD',
        'OPENALEX_API_URL',
        'GEMINI_API_KEY',
        'REDIS_URL',
        'ORCID_CLIENT_ID',
        'ORCID_CLIENT_SECRET',
        'KNOWHUB_BASE_URL'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def initialize_graph():
    """Initialize the Neo4j graph database."""
    try:
        graph_initializer = GraphDatabaseInitializer()
        logger.info("Initializing graph database...")
        graph_initializer.initialize_graph()
        logger.info("Graph initialization complete!")
        return True
    except Exception as e:
        logger.error(f"Graph initialization failed: {e}")
        return False

async def process_data(expertise_csv, skip_openalex, skip_publications):
    """Process experts and publications data from multiple sources."""
    # Initialize processors and scrapers
    processor = OpenAlexProcessor()
    orcid_processor = OrcidProcessor()
    knowhub_scraper = KnowhubScraper()
    website_scraper = WebsiteScraper()
    research_nexus_scraper = ResearchNexusScraper()
    
    try:
        # Process expert data
        logger.info("Loading initial expert data...")
        await processor.load_initial_experts(expertise_csv)
        
        if not skip_openalex:
            logger.info("Updating experts with OpenAlex data...")
            await processor.update_experts_with_openalex()
            logger.info("Expert data enrichment complete!")
        
        if not skip_publications:
            logger.info("Processing publications data...")
            summarizer = TextSummarizer()
            pub_processor = PublicationProcessor(processor.db, summarizer)
            
            # Process publications from different sources
            sources = [
                ('OpenAlex', processor, 'openalex'),
                ('ORCID', orcid_processor, 'orcid'),
                ('Knowhub', knowhub_scraper, 'knowhub'),
                ('Website', website_scraper, 'website'),
                ('Research Nexus', research_nexus_scraper, 'researchnexus')
            ]
            
            for name, source_processor, source_name in sources:
                try:
                    logger.info(f"Processing {name} publications...")
                    
                    if source_name in ['openalex', 'orcid']:
                        await source_processor.process_publications(pub_processor, source=source_name)
                    elif source_name == 'knowhub':
                        # Knowhub specific processing
                        knowhub_publications = source_processor.fetch_publications(limit=10)
                        for i, publication in enumerate(knowhub_publications):
                            if i >= 10:
                                break
                            pub_processor.process_single_work(publication, source=source_name)
                    elif source_name == 'website':
                        try:
                            # Website scraper processing
                            website_publications = source_processor.fetch_content(limit=10)
                            for publication in website_publications:
                                try:
                                    # Process each publication
                                    pub_processor.process_single_work(publication, source=source_name)
                                except Exception as e:
                                    logger.error(f"Error processing website publication: {e}")
                                    continue
                        except Exception as e:
                            logger.error(f"Error fetching website content: {e}")
                    elif source_name == 'researchnexus':
                        # Research Nexus processing
                        research_nexus_publications = source_processor.fetch_content(limit=10)
                        if research_nexus_publications:
                            for i, publication in enumerate(research_nexus_publications):
                                if i >= 10:
                                    break
                                pub_processor.process_single_work(publication, source=source_name)
                    
                    logger.info(f"{name} Publications processing complete!")
                
                except Exception as e:
                    logger.error(f"Error processing {name} publications: {e}")
                    continue
        
    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        raise
    finally:
        # Ensure all resources are closed
        processor.close()
        orcid_processor.close()
        knowhub_scraper.close()
        website_scraper.close()
        research_nexus_scraper.close()

def create_search_index():
    """Create FAISS search index."""
    index_creator = ExpertSearchIndexManager()
    try:
        logger.info("Creating FAISS search index...")
        success = index_creator.create_faiss_index()
        if success:
            logger.info("FAISS index creation complete!")
            return True
        else:
            logger.error("Failed to create FAISS index")
            return False
    except Exception as e:
        logger.error(f"FAISS index creation failed: {e}")
        return False

def create_redis_index():
    """Create Redis search index."""
    redis_creator = ExpertRedisIndexManager()
    try:
        logger.info("Creating Redis search index...")
        if redis_creator.clear_redis_indexes():
            success = redis_creator.create_redis_index()
            if success:
                logger.info("Redis index creation complete!")
                return True
        logger.error("Failed to create Redis index")
        return False
    except Exception as e:
        logger.error(f"Redis index creation failed: {e}")
        return False

async def run_monthly_setup(expertise_csv='expertise.csv', skip_openalex=False, skip_publications=False, 
                             skip_graph=False, skip_search=False, skip_redis=False):
    """Execute monthly data processing tasks."""
    try:
        logger.info("Starting monthly setup process")
        
        # Step 1: Setup environment
        setup_environment()
        
        # Step 2: Process data (this should populate the tables)
        logger.info("Processing data...")
        await process_data(expertise_csv, skip_openalex, skip_publications)
        
        # Step 3: Initialize graph database (if not skipped)
        if not skip_graph:
            if not initialize_graph():
                logger.error("Graph initialization failed")
                raise RuntimeError("Graph initialization failed")
        
        # Step 4: Create search index (if not skipped and data exists)
        if not skip_search:
            if not create_search_index():
                logger.error("FAISS index creation failed")
                raise RuntimeError("FAISS index creation failed")
        
        # Step 5: Create Redis index (if not skipped)
        if not skip_redis:
            if not create_redis_index():
                logger.error("Redis index creation failed")
                raise RuntimeError("Redis index creation failed")
        
        logger.info("Monthly setup completed successfully!")
        return True
    
    except Exception as e:
        logger.error(f"Monthly setup failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(run_monthly_setup())