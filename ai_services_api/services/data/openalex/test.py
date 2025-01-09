import asyncio
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any
import logging

from ai_summarizer import TextSummarizer
from publication_processor import PublicationProcessor
from expert_processor import ExpertProcessor

# Import all source classes
from openalex_processor import OpenAlexProcessor
from orcid_processor import OrcidProcessor
from knowhub_scraper import KnowhubScraper
from research_nexus_scraper import ResearchNexusScraper
from website_scraper import WebsiteScraper

class DataPipelineCoordinator:
    def __init__(self, output_dir: str = "pipeline_output"):
        """Initialize the pipeline coordinator."""
        self.output_dir = output_dir
        self.db = DatabaseManager()
        self.summarizer = TextSummarizer()
        self.pub_processor = PublicationProcessor(self.db, self.summarizer)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _save_to_csv(self, data: List[Dict], filename: str) -> str:
        """Save data to CSV file."""
        if not data:
            self.logger.warning(f"No data to save for {filename}")
            return ""
            
        try:
            filepath = os.path.join(self.output_dir, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            self.logger.info(f"Saved {len(data)} records to {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Error saving CSV {filename}: {e}")
            return ""

    async def process_openalex(self, limit: int = 10) -> str:
        """Process OpenAlex data."""
        try:
            processor = OpenAlexProcessor()
            await processor.update_experts_with_openalex()
            data = await processor.process_publications(self.pub_processor)
            return self._save_to_csv(data, "openalex_publications")
        except Exception as e:
            self.logger.error(f"Error processing OpenAlex: {e}")
            return ""

    async def process_orcid(self, limit: int = 10) -> str:
        """Process ORCID data."""
        try:
            processor = OrcidProcessor(db=self.db, summarizer=self.summarizer)
            await processor.process_publications(self.pub_processor)
            return self._save_to_csv(processor.processed_publications, "orcid_publications")
        except Exception as e:
            self.logger.error(f"Error processing ORCID: {e}")
            return ""

    def process_knowhub(self, limit: int = 10) -> str:
        """Process Knowhub data."""
        try:
            scraper = KnowhubScraper(summarizer=self.summarizer)
            publications = scraper.fetch_publications(limit=limit)
            return self._save_to_csv(publications, "knowhub_publications")
        except Exception as e:
            self.logger.error(f"Error processing Knowhub: {e}")
            return ""

    def process_research_nexus(self, limit: int = 10) -> str:
        """Process Research Nexus data."""
        try:
            scraper = ResearchNexusScraper(summarizer=self.summarizer)
            publications = scraper.fetch_content(limit=limit)
            return self._save_to_csv(publications, "research_nexus_publications")
        except Exception as e:
            self.logger.error(f"Error processing Research Nexus: {e}")
            return ""

    def process_website(self, limit: int = 10) -> str:
        """Process website data."""
        try:
            scraper = WebsiteScraper(summarizer=self.summarizer)
            publications = scraper.fetch_content(limit=limit)
            return self._save_to_csv(publications, "website_publications")
        except Exception as e:
            self.logger.error(f"Error processing website: {e}")
            return ""

    async def run_pipeline(self, limit: int = 10) -> Dict[str, str]:
        """Run the complete pipeline."""
        results = {}
        
        # Process each source
        self.logger.info("Starting pipeline processing...")
        
        # Process async sources
        openalex_file = await self.process_openalex(limit)
        results['openalex'] = openalex_file
        
        orcid_file = await self.process_orcid(limit)
        results['orcid'] = orcid_file
        
        # Process sync sources
        knowhub_file = self.process_knowhub(limit)
        results['knowhub'] = knowhub_file
        
        nexus_file = self.process_research_nexus(limit)
        results['research_nexus'] = nexus_file
        
        website_file = self.process_website(limit)
        results['website'] = website_file
        
        # Create integrated view
        self.create_integrated_view(results)
        
        return results

    def create_integrated_view(self, source_files: Dict[str, str]) -> str:
        """Create an integrated view of all sources."""
        try:
            all_data = []
            
            # Common columns for standardization
            common_columns = [
                'doi', 'title', 'abstract', 'authors', 'publication_year',
                'source', 'url', 'type', 'language'
            ]
            
            # Read and standardize each source
            for source, filepath in source_files.items():
                if not filepath or not os.path.exists(filepath):
                    continue
                    
                df = pd.read_csv(filepath)
                
                # Standardize columns
                standardized = pd.DataFrame()
                for col in common_columns:
                    if col in df.columns:
                        standardized[col] = df[col]
                    else:
                        standardized[col] = None
                        
                standardized['data_source'] = source
                all_data.append(standardized)
            
            # Combine all sources
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Save integrated view
                integrated_file = os.path.join(
                    self.output_dir, 
                    f"integrated_view_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )
                combined_df.to_csv(integrated_file, index=False)
                self.logger.info(f"Created integrated view: {integrated_file}")
                return integrated_file
            
            return ""
            
        except Exception as e:
            self.logger.error(f"Error creating integrated view: {e}")
            return ""

    def close(self):
        """Cleanup resources."""
        try:
            if hasattr(self, 'db'):
                self.db.close()
            if hasattr(self, 'summarizer'):
                self.summarizer.close()
            self.logger.info("Pipeline resources cleaned up")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

# Example usage
async def main():
    pipeline = DataPipelineCoordinator()
    try:
        results = await pipeline.run_pipeline(limit=10)
        print("Pipeline results:", results)
    finally:
        pipeline.close()

if __name__ == "__main__":
    asyncio.run(main())