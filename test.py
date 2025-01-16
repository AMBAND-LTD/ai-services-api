"""
System initialization and database setup module.
"""
import os
import logging
import argparse
import asyncio
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

from services.centralized_repository.openalex.openalex_processor import OpenAlexProcessor
from services.centralized_repository.publication_processor import PublicationProcessor
from services.centralized_repository.ai_summarizer import TextSummarizer
from services.recommendation.graph_initializer import GraphDatabaseInitializer
from services.search.indexing.index_creator import ExpertSearchIndexManager
from services.chatbot.indexing.redis_index_manager import ExpertRedisIndexManager
from services.centralized_repository.database_setup import DatabaseInitializer, ExpertManager

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
    expertise_csv: str = 'expertise.csv'
    skip_database: bool = False  
    skip_experts: bool = False
    skip_openalex: bool = False
    skip_publications: bool = False
    skip_graph: bool = False
    skip_search: bool = False
    skip_redis: bool = False
    skip_topics: bool = False

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'SetupConfig':
        """Create config from command line arguments"""
        return cls(
            expertise_csv=args.expertise_csv,
            skip_database=args.skip_database,
            skip_experts=args.skip_experts,
            skip_openalex=args.skip_openalex,
            skip_publications=args.skip_publications,
            skip_graph=args.skip_graph,
            skip_search=args.skip_search,
            skip_redis=args.skip_redis,
            skip_topics=args.skip_topics
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
            'OPENALEX_API_KEY',
            'GEMINI_API_KEY',
            'REDIS_URL',
            'ORCID_CLIENT_ID',
            'ORCID_CLIENT_SECRET',
            'KNOWHUB_BASE_URL'  
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

    async def process_expert_data(self) -> None:
        """Process expert data using ExpertManager"""
        try:
            logger.info("Loading initial expert data...")
            expert_manager = ExpertManager()
            if self.config.expertise_csv and os.path.exists(self.config.expertise_csv):
                expert_manager.load_experts_from_csv(self.config.expertise_csv)
            logger.info("Expert data processing complete!")
        except Exception as e:
            logger.error(f"Expert data processing failed: {e}")
            raise

    async def process_publications(self, processor: OpenAlexProcessor, summarizer: TextSummarizer) -> None:
        """Process publications from all sources"""
        pub_processor = PublicationProcessor(processor.db, summarizer)
        
        sources = {
            'openalex': (processor.process_publications, {'source': 'openalex'}),
            'orcid': (OrcidProcessor().process_publications, {'source': 'orcid'}),
            'knowhub': (KnowhubScraper(summarizer=summarizer).fetch_all_content, {'limit': 2}),
            'website': (WebsiteScraper(summarizer=summarizer).fetch_content, {'limit': 2}),
            'researchnexus': (ResearchNexusScraper(summarizer=summarizer).fetch_content, {'limit': 2})
        }
        
        for source_name, (processor_func, kwargs) in sources.items():
            try:
                logger.info(f"Processing {source_name} publications...")
                publications = await processor_func(**kwargs)
                
                if isinstance(publications, dict):
                    for content_type, items in publications.items():
                        for item in items:
                            pub_processor.process_single_work(item, source=source_name)
                else:
                    for pub in publications:
                        pub_processor.process_single_work(pub, source=source_name)
                        
            except Exception as e:
                logger.error(f"Error processing {source_name} publications: {e}")

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
                
            if not self.config.skip_experts:
                await self.process_expert_data()

            processor = OpenAlexProcessor()
            summarizer = TextSummarizer()
                
            if not self.config.skip_publications:
                await self.process_publications(processor, summarizer)
                
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
    parser.add_argument('--expertise-csv', type=str, default='expertise.csv',
                       help='Path to the expertise CSV file')
    parser.add_argument('--skip-database', action='store_true',
                       help='Skip database initialization')
    parser.add_argument('--skip-experts', action='store_true',
                       help='Skip expert data processing') 
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