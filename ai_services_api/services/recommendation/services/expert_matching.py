import os
import logging
import json
import asyncio
import psycopg2
from typing import List, Dict, Any
from neo4j import GraphDatabase
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

# Configure logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DatabaseConnectionManager:
    """Centralized database connection management"""
    
    @staticmethod
    def get_postgres_connection():
        """Create a connection to PostgreSQL database."""
        database_url = os.getenv('DATABASE_URL')
        
        if database_url:
            parsed_url = urlparse(database_url)
            conn_params = {
                'host': parsed_url.hostname,
                'port': parsed_url.port,
                'dbname': parsed_url.path[1:],
                'user': parsed_url.username,
                'password': parsed_url.password
            }
        else:
            conn_params = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': os.getenv('POSTGRES_PORT', '5432'),
                'dbname': os.getenv('POSTGRES_DB', 'aphrc'),
                'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD', 'p0stgres')
            }

        try:
            conn = psycopg2.connect(**conn_params)
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    @staticmethod
    def get_neo4j_driver():
        """Create a connection to Neo4j database."""
        try:
            driver = GraphDatabase.driver(
                os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                auth=(
                    os.getenv('NEO4J_USER', 'neo4j'),
                    os.getenv('NEO4J_PASSWORD')
                )
            )
            return driver
        except Exception as e:
            logging.error(f"Error connecting to Neo4j: {e}")
            raise

class ExpertMatchingService:
    def __init__(self):
        """Initialize ExpertMatchingService with semantic matching capabilities"""
        self.logger = logging.getLogger(__name__)
        
        # Initialize Neo4j connection
        self._neo4j_driver = DatabaseConnectionManager.get_neo4j_driver()
        
        # Updated weights for semantic matching
        self.weights = {
            'concept': 0.40,      # Core concept matches
            'research': 0.25,     # Research area overlap
            'method': 0.20,       # Method/approach similarity
            'theme': 0.10,        # Organizational proximity
            'interaction': 0.05   # Historical interaction success
        }
        logger.info("ExpertMatchingService initialized with semantic weights")

    async def find_similar_experts(self, expert_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Find similar experts using more lenient semantic matching"""
        query = """
        MATCH (e1:Expert {id: $expert_id})
        MATCH (e2:Expert)
        WHERE e1 <> e2
        
        // Any concept overlap
        OPTIONAL MATCH p1=(e1)-[:HAS_CONCEPT]->(c:Concept)<-[:HAS_CONCEPT]-(e2)
        WITH e1, e2, COUNT(p1) as concept_count,
        COLLECT(DISTINCT c.name) as shared_concepts
        
        // Any research overlap
        OPTIONAL MATCH p2=(e1)-[:RESEARCHES_IN]->(a:ResearchArea)<-[:RESEARCHES_IN]-(e2)
        WITH e1, e2, concept_count, shared_concepts,
        COUNT(p2) as area_count,
        COLLECT(DISTINCT a.name) as shared_areas
        
        // Any method overlap
        OPTIONAL MATCH p3=(e1)-[:USES_METHOD]->(m:Method)<-[:USES_METHOD]-(e2)
        WITH e1, e2, concept_count, shared_concepts,
        area_count, shared_areas,
        COUNT(p3) as method_count,
        COLLECT(DISTINCT m.name) as shared_methods
        
        // Same theme or unit is a bonus
        OPTIONAL MATCH (e1)-[:BELONGS_TO_THEME]->(t:Theme)<-[:BELONGS_TO_THEME]-(e2)
        OPTIONAL MATCH (e1)-[:BELONGS_TO_UNIT]->(u:Unit)<-[:BELONGS_TO_UNIT]-(e2)
        
        WITH e1, e2, 
            concept_count, shared_concepts,
            area_count, shared_areas,
            method_count, shared_methods,
            COUNT(DISTINCT t) as theme_match,
            COUNT(DISTINCT u) as unit_match
        
        // More lenient scoring - any match counts
        WITH e2,
            concept_count, shared_concepts,
            area_count, shared_areas,
            method_count, shared_methods,
            theme_match, unit_match,
            CASE 
                WHEN concept_count + area_count + method_count + theme_match + unit_match > 0 
                THEN (
                    concept_count * 0.3 + 
                    area_count * 0.3 + 
                    method_count * 0.2 + 
                    (theme_match + unit_match) * 0.2
                )
                ELSE 0
            END as similarity_score
        
        // Return even low similarity matches
        WHERE similarity_score > 0
        
        RETURN {
            id: e2.id,
            name: e2.name,
            designation: e2.designation,
            theme: e2.theme,
            unit: e2.unit,
            similarity_score: similarity_score,
            match_details: {
                shared_concepts: shared_concepts,
                shared_areas: shared_areas,
                shared_methods: shared_methods,
                same_theme: theme_match > 0,
                same_unit: unit_match > 0
            },
            total_matches: concept_count + area_count + method_count + theme_match + unit_match
        } as result
        ORDER BY result.similarity_score DESC
        LIMIT $limit
        """
        
        try:
            with self._neo4j_driver.session() as session:
                logger.info(f"Finding similar experts for expert_id: {expert_id}")
                result = session.run(query, {
                    "expert_id": expert_id,
                    "limit": limit
                })
                
                similar_experts = [record["result"] for record in result]
                logger.info(f"Found {len(similar_experts)} similar experts")
                
                return similar_experts
                    
        except Exception as e:
            logger.error(f"Error finding similar experts: {str(e)}", exc_info=True)
            return []
            
    async def _record_recommendations(self, expert_id: str, recommendations: List[Dict[str, Any]]):
        """Record recommendations with semantic match details"""
        try:
            query = """
                MATCH (e1:Expert {id: $expert_id})
                MATCH (e2:Expert {id: $matched_id})
                MERGE (e1)-[r:RECOMMENDED]->(e2)
                SET r.timestamp = datetime(),
                    r.similarity_score = $similarity_score,
                    r.match_details = $match_details
            """
            
            with self._neo4j_driver.session() as session:
                for rec in recommendations:
                    session.run(query, {
                        "expert_id": expert_id,
                        "matched_id": rec['id'],
                        "similarity_score": rec['similarity_score'],
                        "match_details": json.dumps({
                            'match_scores': rec['match_scores'],
                            'match_details': rec['match_details']
                        })
                    })
            
        except Exception as e:
            self.logger.error(f"Error recording recommendations: {e}")

    def close(self):
        """Close database connections"""
        if self._neo4j_driver:
            self._neo4j_driver.close()
            self.logger.info("Neo4j connection closed")