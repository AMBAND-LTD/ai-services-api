import os
import logging
import google.generativeai as genai
from typing import Optional, List, Dict
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class TextSummarizer:
    def __init__(self):
        """Initialize the TextSummarizer with Gemini model."""
        self.model = self._setup_gemini()
        logger.info("TextSummarizer initialized successfully")

    def _setup_gemini(self):
        """Set up and configure the Gemini model."""
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                raise ValueError("GEMINI_API_KEY environment variable is not set")
            
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-pro')
            logger.info("Gemini model setup completed")
            return model
            
        except Exception as e:
            logger.error(f"Error setting up Gemini model: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def summarize(self, title: str, abstract: str) -> Optional[str]:
        """
        Generate a summary of the title and abstract using Gemini.
        If abstract is missing, generates a brief description based on the title.
        
        Args:
            title: Title of the publication
            abstract: Abstract of the publication
            
        Returns:
            str: Generated summary or brief description
        """
        try:
            if not title:
                logger.error("Title is required for summarization")
                return "Cannot generate summary: title is missing"

            if not abstract or abstract.strip() == "N/A":
                logger.info("No abstract available, generating description from title")
                prompt = self._create_title_only_prompt(title)
            else:
                prompt = self._create_prompt(title, abstract)
            
            # Generate summary
            response = self.model.generate_content(prompt)
            summary = response.text.strip()
            
            if not summary:
                logger.warning("Generated content is empty")
                return "Failed to generate meaningful content"
            
            # Clean and format summary
            cleaned_summary = self._clean_summary(summary)
            logger.info(f"Successfully generated content for: {title[:100]}...")
            return cleaned_summary

        except Exception as e:
            logger.error(f"Error in content generation: {e}")
            return "Failed to generate content due to technical issues"

    def _create_prompt(self, title: str, abstract: str) -> str:
        """Create a prompt for the summarization model."""
        return f"""
        Please create a concise summary combining the following title and abstract.
        
        Title: {title}
        
        Abstract: {abstract}
        
        Instructions:
        1. Provide a clear and concise summary in 2-3 sentences
        2. Focus on the main research findings and implications
        3. Use academic but accessible language
        4. Keep the summary under 200 words
        5. Retain technical terms and key concepts
        6. Begin directly with the summary, do not include phrases like "This paper" or "This research"
        """

    def _create_title_only_prompt(self, title: str) -> str:
        """Create a prompt for generating a brief description from title only."""
        return f"""
        Please create a brief description based on the following academic publication title.
        
        Title: {title}
        
        Instructions:
        1. Provide a single sentence describing what this publication likely discusses
        2. Use phrases like "This publication appears to discuss..." or "This work likely explores..."
        3. Make educated guesses about the main focus based on key terms in the title
        4. Keep the description under 50 words
        5. Use cautious language to acknowledge this is based only on the title
        6. Retain any technical terms present in the title
        """

    def _clean_summary(self, summary: str) -> str:
        """Clean and format the generated summary."""
        try:
            # Basic cleaning
            cleaned = summary.strip()
            cleaned = ' '.join(cleaned.split())  # Normalize whitespace
            
            # Remove common prefixes if present
            prefixes = [
                'Summary:', 
                'Here is a summary:', 
                'The summary is:', 
                'Here is a concise summary:',
                'This paper',
                'This research',
                'This study'
            ]
            
            lower_cleaned = cleaned.lower()
            for prefix in prefixes:
                if lower_cleaned.startswith(prefix.lower()):
                    cleaned = cleaned[len(prefix):].strip()
                    break
            
            # Ensure the summary starts with a capital letter
            if cleaned:
                cleaned = cleaned[0].upper() + cleaned[1:]
            
            # Add a period at the end if missing
            if cleaned and cleaned[-1] not in ['.', '!', '?']:
                cleaned += '.'
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning summary: {e}")
            return summary

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def generate_topics(self, publications: List[Dict], num_topics: int = 10) -> List[str]:
        """Generate broader research topics based on publication data."""
        try:
            # Prepare text for topic generation
            corpus = []
            for pub in publications:
                text = f"{pub['title']} {pub.get('summary', '')}"
                corpus.append(text)
                
            combined_text = "\n\n".join(corpus[:100])
            
            prompt = f"""
            Based on the following publications, generate {num_topics} broad research topics.
            
            Requirements:
            - Each topic should be 2-4 words
            - Topics should be broad enough to group similar research
            - Avoid location-specific topics
            - Focus on research areas, not specific implementations
            
            Examples of good topics:
            Reproductive Healthcare
            Educational Access
            Public Health Systems
            Disease Prevention
            Healthcare Equity
            
            Return only the topic names, one per line, without any additional text.
            
            Publications:
            {combined_text}
            """
            
            response = self.model.generate_content(prompt)
            if not response.text:
                logger.warning("Empty response from model")
                return []
                
            # Clean and validate topics
            topics = []
            for topic in response.text.strip().split('\n'):
                topic = topic.strip('- ').strip()
                if topic and 2 <= len(topic.split()) <= 4:
                    topics.append(topic)
                    
            topics = topics[:num_topics]
            logger.info(f"Generated {len(topics)} topics")
            return topics
            
        except Exception as e:
            logger.error(f"Error generating topics: {e}")
            raise

    def assign_topics(self, publication: Dict, available_topics: List[str]) -> List[str]:
        """Assign broader topics to a single publication."""
        try:
            if not available_topics:
                logger.warning("No available topics to assign")
                return []
                
            text = f"{publication['title']} {publication.get('summary', '')}"
            
            prompt = f"""
            Given the following publication, assign 2-3 most relevant topics from the list.
            Only choose topics that strongly match the publication's content.
            Return exactly one topic per line.
            
            Publication:
            {text}
            
            Available topics:
            {', '.join(available_topics)}
            """
            
            response = self.model.generate_content(prompt)
            if not response.text:
                logger.warning("Empty response from model")
                return []
                
            # Clean and validate assigned topics
            assigned_topics = []
            for topic in response.text.strip().split('\n'):
                topic = topic.strip('- ').strip()
                if topic in available_topics:
                    assigned_topics.append(topic)
                    
            if assigned_topics:
                logger.info(f"Assigned {len(assigned_topics)} topics to publication")
            else:
                logger.warning("No topics assigned to publication")
                
            return assigned_topics
            
        except Exception as e:
            logger.error(f"Error assigning topics: {e}")
            return []

    def __del__(self):
        """Cleanup any resources."""
        try:
            # Add any cleanup code if needed
            pass
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")