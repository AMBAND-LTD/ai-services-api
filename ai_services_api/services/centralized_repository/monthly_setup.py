import os
import logging
import argparse
import asyncio
import sys
from dotenv import load_dotenv

# Import necessary modules
from ai_services_api.services.centralized_repository.openalex.openalex_processor import OpenAlexProcessor 
from ai_services_api.services.centralized_repository.publication_processor import PublicationProcessor
from ai_services_api.services.centralized_repository.ai_summarizer import TextSummarizer
from ai_services_api.services.search.index_creator import ExpertSearchIndexManager
from ai_services_api.services.search.redis_index_manager import ExpertRedisIndexManager
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

async def process_data(args):
    """Process publications data from multiple sources."""
    processor = OpenAlexProcessor()
    summarizer = TextSummarizer()
    
    try:
        if not args.skip_publications:
            logger.info("Processing publications data...")
            pub_processor = PublicationProcessor(processor.db, summarizer)
            
            # Process OpenAlex publications
            if not args.skip_openalex:
                try:
                    logger.info("Processing OpenAlex publications...")
                    await processor.process_publications(pub_processor, source='openalex')
                except Exception as e:
                    logger.error(f"Error processing OpenAlex publications: {e}")

            # Process ORCID publications
            try:
                logger.info("Processing ORCID publications...")
                orcid_processor = OrcidProcessor()
                await orcid_processor.process_publications(pub_processor, source='orcid')
                orcid_processor.close()
            except Exception as e:
                logger.error(f"Error processing ORCID publications: {e}")

            # Process KnowHub content
            try:
                logger.info("\n" + "="*50)
                logger.info("Processing KnowHub content...")
                logger.info("="*50)
                
                knowhub_scraper = KnowhubScraper(summarizer=summarizer)
                all_content = knowhub_scraper.fetch_all_content(limit=2)
                
                for content_type, items in all_content.items():
                    if items:
                        logger.info(f"\nProcessing {len(items)} items from {content_type}")
                        for item in items:
                            try:
                                pub_processor.process_single_work(item, source='knowhub')
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
                research_nexus_scraper = ResearchNexusScraper(summarizer=summarizer)
                research_nexus_publications = research_nexus_scraper.fetch_content(limit=2)

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

            # Process Website publications
            try:
                logger.info("\n" + "="*50)
                logger.info("Processing Website publications...")
                logger.info("="*50)
                
                website_scraper = WebsiteScraper(summarizer=summarizer)
                website_publications = website_scraper.fetch_content(limit=2)
                
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

            # Process topics for all publications
            if not args.skip_topics:
                try:
                    logger.info("\n" + "="*50)
                    logger.info("Starting topic classification...")
                    logger.info("="*50)
                    
                    # Get all publications for topic processing
                    publications = processor.db.get_all_publications()
                    
                    if publications:
                        # Generate topics using Gemini
                        topics = summarizer.generate_topics(publications)
                        logger.info(f"Generated topics: {topics}")
                        
                        # Process publications in batches
                        batch_size = 100
                        total_processed = 0
                        
                        for i in range(0, len(publications), batch_size):
                            batch = publications[i:i + batch_size]
                            for pub in batch:
                                try:
                                    # Assign topics to publication
                                    assigned_topics = summarizer.assign_topics(pub, topics)
                                    
                                    # Update database
                                    processor.db.update_publication_topics(pub['id'], assigned_topics)
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
        processor.close()

def create_search_index():
    """Create FAISS search index."""
    try:
        index_creator = ExpertSearchIndexManager()
        logger.info("Creating FAISS search index...")
        success = index_creator.create_faiss_index()
        
        if success:
            logger.info("FAISS index creation complete!")
            return True
        
        logger.error("Failed to create FAISS index")
        return False
    
    except Exception as e:
        logger.error(f"FAISS index creation failed: {e}")
        return False

def create_redis_index():
    """Create Redis search index."""
    try:
        redis_creator = ExpertRedisIndexManager()
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

async def run_monthly_setup(
    expertise_csv=None,  # Kept for compatibility but won't be used
    skip_openalex=False,
    skip_publications=False, 
    skip_graph=False, 
    skip_search=False, 
    skip_redis=False,
    skip_topics=False
):
    """Monthly setup process."""
    try:
        logger.info("Starting monthly setup process")
        
        # Setup environment
        setup_environment()
        
        # Process data with args
        args = argparse.Namespace(
            expertise_csv=expertise_csv,
            skip_openalex=skip_openalex,
            skip_publications=skip_publications,
            skip_topics=skip_topics
        )
        await process_data(args)
        
        # Optional steps
        if not skip_search and not create_search_index():
            raise RuntimeError("FAISS index creation failed")
        
        if not skip_redis and not create_redis_index():
            raise RuntimeError("Redis index creation failed")
        
        logger.info("Monthly setup completed successfully!")
        return True
    
    except Exception as e:
        logger.error(f"Monthly setup failed: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Monthly Research Data Setup')
    parser.add_argument('--expertise-csv', default=None)  # Kept for compatibility
    parser.add_argument('--skip-openalex', action='store_true')
    parser.add_argument('--skip-publications', action='store_true')
    parser.add_argument('--skip-graph', action='store_true')
    parser.add_argument('--skip-search', action='store_true')
    parser.add_argument('--skip-redis', action='store_true')
    parser.add_argument('--skip-topics', action='store_true')
    
    args = parser.parse_args()
    
    asyncio.run(run_monthly_setup(
        expertise_csv=args.expertise_csv,
        skip_openalex=args.skip_openalex,
        skip_publications=args.skip_publications,
        skip_graph=args.skip_graph,
        skip_search=args.skip_search,
        skip_redis=args.skip_redis,
        skip_topics=args.skip_topics
    ))

if __name__ == "__main__":
    main()