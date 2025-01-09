
import os
import logging
import argparse
import asyncio
from dotenv import load_dotenv

from ai_services_api.services.data.database_setup import (
    create_database_if_not_exists,
    create_tables,
    fix_experts_table,
    get_db_connection
)

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

def initialize_database(args):
    try:
        logger.info("Ensuring database exists...")
        create_database_if_not_exists()
        
        logger.info("Creating database tables...")
        create_tables()  # Move this before fix_experts_table
        
        # Verify tables exist
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'resources_resource'
                );
            """)
            if cur.fetchone()[0]:
                logger.info("resources_resource table created successfully")
            else:
                raise Exception("Failed to create resources_resource table")
        finally:
            cur.close()
            conn.close()
        
        logger.info("Fixing experts table...")
        fix_experts_table()
        
        logger.info("Database initialization complete!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

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

async def process_data(args):
    """Process experts and publications data from multiple sources."""
    # Initialize OpenAlex since it's always needed and doesn't have heavy dependencies
    processor = OpenAlexProcessor()
    summarizer = TextSummarizer()
    
    try:
        # Process expert data first
        logger.info("Loading initial expert data...")
        await processor.load_initial_experts(args.expertise_csv)
        
        if not args.skip_openalex:
            logger.info("Updating experts with OpenAlex data...")
            await processor.update_experts_with_openalex()
            logger.info("Expert data enrichment complete!")
        
        if not args.skip_publications:
            logger.info("Processing publications data...")
            pub_processor = PublicationProcessor(processor.db, summarizer)
            
            # Process each source separately to handle failures gracefully
            if not args.skip_openalex:
                try:
                    logger.info("Processing OpenAlex publications...")
                    await processor.process_publications(pub_processor, source='openalex')
                except Exception as e:
                    logger.error(f"Error processing OpenAlex publications: {e}")

            try:
                logger.info("Processing ORCID publications...")
                orcid_processor = OrcidProcessor()
                await orcid_processor.process_publications(pub_processor, source='orcid')
                orcid_processor.close()
            except Exception as e:
                logger.error(f"Error processing ORCID publications: {e}")

            try:
                logger.info("Processing KnowHub publications...")
                knowhub_scraper = KnowhubScraper()
                knowhub_publications = knowhub_scraper.fetch_publications(limit=10)
                for pub in knowhub_publications:
                    pub_processor.process_single_work(pub, source='knowhub')
                knowhub_scraper.close()
            except Exception as e:
                logger.error(f"Error processing KnowHub publications: {e}")

            # ResearchNexus is handled separately due to Selenium dependency
            try:
                logger.info("Processing Research Nexus publications...")
                research_nexus_scraper = ResearchNexusScraper(summarizer=summarizer)
                research_nexus_publications = research_nexus_scraper.fetch_content(limit=10)

                if research_nexus_publications:
                    for pub in research_nexus_publications:
                        pub_processor.process_single_work(pub, source='researchnexus')
                else:
                    logger.warning("No Research Nexus publications found")

            except Exception as e:
                logger.error(f"Error processing Research Nexus publications: {e}")
            finally:
                if 'research_nexus_scraper' in locals():
                    research_nexus_scraper.close()

            # Website scraper
            try:
                logger.info("\n" + "="*50)
                logger.info("Processing Website publications...")
                logger.info("="*50)
                
                website_scraper = WebsiteScraper(summarizer=summarizer)
                website_publications = website_scraper.fetch_content(limit=10)
                
                if website_publications:
                    logger.info(f"\nProcessing {len(website_publications)} website publications")
                    for pub in website_publications:
                        try:
                            pub_processor.process_single_work(pub, source='website')
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

    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        raise
    finally:
        processor.close()
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

async def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Initialize and populate the research database.')
    parser.add_argument('--expertise-csv', type=str, default='expertise.csv',
                       help='Path to the expertise CSV file')
    parser.add_argument('--skip-openalex', action='store_true',
                       help='Skip OpenAlex data enrichment')
    parser.add_argument('--skip-publications', action='store_true',
                       help='Skip publications processing')
    parser.add_argument('--skip-graph', action='store_true',
                       help='Skip graph database initialization')
    parser.add_argument('--skip-search', action='store_true',
                       help='Skip search index creation')
    parser.add_argument('--skip-redis', action='store_true',
                       help='Skip Redis index creation')
    args = parser.parse_args()

    try:
        # Step 1: Setup environment
        setup_environment()
        
        # Step 2: Initialize database and ensure tables exist
        logger.info("Running database setup...")
        initialize_database(args)
        
        # Step 3: Process data (this should populate the tables)
        logger.info("Processing data...")
        await process_data(args)

        # Step 4: Verify tables are populated
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM resources_resource LIMIT 1
                );
            """)
            if not cur.fetchone()[0]:
                logger.warning("No data in resources_resource table. Search index may be empty.")
        finally:
            cur.close()
            conn.close()

        # Step 5: Initialize graph database (if not skipped)
        if not args.skip_graph:
            if not initialize_graph():
                logger.error("Graph initialization failed")
                raise RuntimeError("Graph initialization failed")

        # Step 6: Create search index (if not skipped and data exists)
        if not args.skip_search:
            if not create_search_index():
                logger.error("FAISS index creation failed")
                raise RuntimeError("FAISS index creation failed")

        # Step 7: Create Redis index (if not skipped)
        if not args.skip_redis:
            if not create_redis_index():
                logger.error("Redis index creation failed")
                raise RuntimeError("Redis index creation failed")

        logger.info("System initialization completed successfully!")

    except Exception as e:
        logger.error(f"System initialization failed: {e}")
        raise

def run():
    """Entry point function."""
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