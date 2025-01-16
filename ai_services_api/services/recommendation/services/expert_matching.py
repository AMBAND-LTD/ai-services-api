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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

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
        """Initialize ExpertMatchingService with enhanced matching capabilities"""
        self.logger = logging.getLogger(__name__)
        
        # Establish database connections
        self._neo4j = DatabaseConnectionManager.get_neo4j_driver()
        
        # Initialize weights for different match types
        self.weights = {
            'expertise': 0.35,    # Direct expertise match
            'domain': 0.25,       # Domain match
            'field': 0.20,        # Field match
            'theme': 0.15,        # Same theme/unit
            'interaction': 0.05   # Previous interaction success
        }
        self.logger.info("ExpertMatchingService initialized")

    async def find_similar_experts(self, expert_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Enhanced similar experts search using multiple matching criteria"""
        query = """
        MATCH (e1:Expert {id: $expert_id})
        MATCH (e2:Expert)
        WHERE e1 <> e2 AND e2.is_active = true
        
        // Expertise overlap
        OPTIONAL MATCH (e1)-[:HAS_EXPERTISE]->(ex:Expertise)<-[:HAS_EXPERTISE]-(e2)
        WITH e1, e2, COUNT(DISTINCT ex) as expertise_count,
             COLLECT(DISTINCT ex.name) as shared_expertise
        
        // Domain overlap with weights
        OPTIONAL MATCH (e1)-[rd1:HAS_DOMAIN]->(d:Domain)<-[rd2:HAS_DOMAIN]-(e2)
        WITH e1, e2, expertise_count, shared_expertise,
             COUNT(DISTINCT d) as domain_count,
             COLLECT(DISTINCT d.name) as shared_domains
        
        // Field overlap with weights
        OPTIONAL MATCH (e1)-[rf1:HAS_FIELD]->(f:Field)<-[rf2:HAS_FIELD]-(e2)
        WITH e1, e2, expertise_count, shared_expertise,
             domain_count, shared_domains,
             COUNT(DISTINCT f) as field_count,
             COLLECT(DISTINCT f.name) as shared_fields
        
        // Theme/Unit proximity
        OPTIONAL MATCH (e1)-[:BELONGS_TO_THEME]->(t:Theme)<-[:BELONGS_TO_THEME]-(e2)
        OPTIONAL MATCH (e1)-[:BELONGS_TO_UNIT]->(u:Unit)<-[:BELONGS_TO_UNIT]-(e2)
        WITH e1, e2, expertise_count, shared_expertise,
             domain_count, shared_domains,
             field_count, shared_fields,
             COUNT(DISTINCT t) as theme_match,
             COUNT(DISTINCT u) as unit_match
        
        // Calculate weighted similarity score
        WITH e2, 
             expertise_count, shared_expertise,
             domain_count, shared_domains,
             field_count, shared_fields,
             theme_match, unit_match,
             (expertise_count * $expertise_weight +
              domain_count * $domain_weight +
              field_count * $field_weight +
              (theme_match + unit_match) * $theme_weight) /
             (CASE 
                WHEN expertise_count + domain_count + field_count + theme_match + unit_match = 0 
                THEN 1 
                ELSE expertise_count + domain_count + field_count + theme_match + unit_match
             END) as similarity_score
        WHERE similarity_score > 0
        
        RETURN {
            id: e2.id,
            name: e2.name,
            designation: e2.designation,
            theme: e2.theme,
            unit: e2.unit,
            similarity_score: similarity_score,
            shared_expertise: shared_expertise,
            shared_domains: shared_domains,
            shared_fields: shared_fields,
            organizational_proximity: theme_match + unit_match,
            match_details: {
                expertise_matches: expertise_count,
                domain_matches: domain_count,
                field_matches: field_count,
                theme_match: theme_match > 0,
                unit_match: unit_match > 0
            }
        } as result
        ORDER BY result.similarity_score DESC
        LIMIT $limit
        """
        
        try:
            # Get base recommendations
            with self._neo4j.session() as session:
                result = session.run(query, {
                    "expert_id": expert_id,
                    "limit": limit,
                    "expertise_weight": self.weights['expertise'],
                    "domain_weight": self.weights['domain'],
                    "field_weight": self.weights['field'],
                    "theme_weight": self.weights['theme']
                })
                
                similar_experts = []
                for record in result:
                    expert_data = record["result"]
                    
                    # Enhance with interaction data
                    success_rate = await self._calculate_success_rate(
                        expert_id=expert_id,
                        matched_expert_id=expert_data['id']
                    )
                    
                    # Adjust final score with interaction history
                    final_score = (
                        expert_data['similarity_score'] * (1 - self.weights['interaction']) +
                        success_rate * self.weights['interaction']
                    )
                    
                    expert_data['final_score'] = final_score
                    expert_data['interaction_success_rate'] = success_rate
                    
                    similar_experts.append(expert_data)
                
                # Final sort by adjusted scores
                similar_experts.sort(key=lambda x: x['final_score'], reverse=True)
                
                # Record recommendations
                await self._record_recommendations(expert_id, similar_experts)
                
                return similar_experts
                
        except Exception as e:
            self.logger.error(f"Error finding similar experts: {str(e)}", exc_info=True)
            return []

    async def _calculate_success_rate(self, expert_id: str, matched_expert_id: str, days: int = 30) -> float:
        """Calculate success rate based on past interactions"""
        conn = None
        try:
            conn = DatabaseConnectionManager.get_postgres_connection()
            cur = conn.cursor()
            
            # Get interaction history
            cur.execute("""
                WITH expert_interactions AS (
                    SELECT 
                        success,
                        interaction_type,
                        metadata
                    FROM expert_interactions
                    WHERE (sender_id = %s AND receiver_id = %s)
                       OR (sender_id = %s AND receiver_id = %s)
                    AND created_at >= NOW() - interval '%s days'
                ),
                search_interactions AS (
                    SELECT 
                        clicked,
                        rank_position
                    FROM expert_searches es
                    JOIN search_logs sl ON es.search_id = sl.id
                    WHERE sl.user_id = %s 
                    AND es.expert_id = %s
                    AND sl.timestamp >= NOW() - interval '%s days'
                )
                SELECT 
                    COALESCE(AVG(CASE WHEN success = true THEN 1 ELSE 0 END), 0) as interaction_success,
                    COALESCE(AVG(CASE WHEN clicked = true THEN 1 ELSE 0 END), 0) as click_rate
                FROM expert_interactions, search_interactions
            """, (
                expert_id, matched_expert_id,
                matched_expert_id, expert_id,
                days, expert_id, matched_expert_id, days
            ))
            
            result = cur.fetchone()
            
            # Calculate combined success rate
            interaction_success = result[0] or 0
            click_rate = result[1] or 0
            
            success_rate = (interaction_success * 0.7 + click_rate * 0.3)
            
            return success_rate
            
        except Exception as e:
            self.logger.error(f"Error calculating success rate: {e}")
            return 0.0
        finally:
            if conn:
                conn.close()

    async def _record_recommendations(self, expert_id: str, recommendations: List[Dict[str, Any]]):
        """Record recommendations with enhanced metadata"""
        conn = None
        try:
            conn = DatabaseConnectionManager.get_postgres_connection()
            cur = conn.cursor()
            
            for rec in recommendations:
                cur.execute("""
                    INSERT INTO expert_interactions (
                        sender_id,
                        receiver_id,
                        interaction_type,
                        success,
                        metadata
                    ) VALUES (%s, %s, %s, %s, %s::jsonb)
                """, (
                    expert_id,
                    rec['id'],
                    'recommendation_shown',
                    True,
                    json.dumps({
                        'similarity_score': rec['similarity_score'],
                        'final_score': rec['final_score'],
                        'shared_expertise': rec['shared_expertise'],
                        'shared_domains': rec['shared_domains'],
                        'shared_fields': rec['shared_fields'],
                        'organizational_proximity': rec['organizational_proximity'],
                        'match_details': rec['match_details']
                    })
                ))
            
            conn.commit()
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Error recording recommendations: {e}")
        finally:
            if conn:
                conn.close()

    def close(self):
        """Close database connections"""
        if self._neo4j:
            self._neo4j.close()
            self.logger.info("Neo4j connection closed")

def main():
    """Example usage of ExpertMatchingService"""
    matching_service = ExpertMatchingService()
    try:
        # Example of how to use the service (would typically be in an async context)
        async def run_example():
            expert_id = "some_expert_id"  # Replace with actual expert ID
            similar_experts = await matching_service.find_similar_experts(expert_id)
            
            print("Similar Experts:")
            for expert in similar_experts:
                print(f"Name: {expert['name']}")
                print(f"Similarity Score: {expert['final_score']}")
                print(f"Shared Expertise: {expert['shared_expertise']}")
                print("---")
        
        # Run the async example
        asyncio.run(run_example())
    
    except Exception as e:
        logging.error(f"Error in expert matching: {e}")
    
    finally:
        matching_service.close()

if __name__ == "__main__":
    main()