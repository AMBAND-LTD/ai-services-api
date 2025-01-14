import logging
from typing import List, Dict, Any, Optional
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv
import google.generativeai as genai
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from ai_services_api.services.message.core.database import get_db_connection


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

class ExpertMatchingService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._neo4j = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD'))
        )
        self.logger.info("ExpertMatchingService initialized")

    async def find_similar_experts(self, expert_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Find similar experts using combined Neo4j similarity and interaction patterns"""
        query = """
        MATCH (e1:Expert {id: $expert_id})
        MATCH (e2:Expert)
        WHERE e1 <> e2
        
        // Calculate domain overlap
        OPTIONAL MATCH (e1)-[:HAS_DOMAIN]->(d:Domain)<-[:HAS_DOMAIN]-(e2)
        WITH e1, e2, COLLECT(DISTINCT d.name) as shared_domains, COUNT(DISTINCT d) as domain_count
        
        // Calculate field overlap
        OPTIONAL MATCH (e1)-[:HAS_FIELD]->(f:Field)<-[:HAS_FIELD]-(e2)
        WITH e1, e2, shared_domains, domain_count, 
             COLLECT(DISTINCT f.name) as shared_fields, COUNT(DISTINCT f) as field_count
        
        // Calculate skill overlap
        OPTIONAL MATCH (e1)-[:HAS_SKILL]->(s:Skill)<-[:HAS_SKILL]-(e2)
        WITH e1, e2, shared_domains, domain_count, 
             shared_fields, field_count,
             COLLECT(DISTINCT s.name) as shared_skills, COUNT(DISTINCT s) as skill_count,
             // Calculate weighted similarity score
             (domain_count * 3.0 + field_count * 2.0 + COUNT(DISTINCT s) * 1.0) / 
             (CASE 
                 WHEN domain_count + field_count + COUNT(DISTINCT s) = 0 
                 THEN 1 
                 ELSE domain_count + field_count + COUNT(DISTINCT s)
             END) as similarity_score
        
        WHERE similarity_score > 0
        
        RETURN {
            id: e2.id,
            name: e2.name,
            shared_domains: shared_domains,
            shared_fields: shared_fields,
            shared_skills: shared_skills,
            similarity_score: similarity_score,
            match_details: {
                domains: domain_count,
                fields: field_count,
                skills: skill_count
            }
        } as result
        ORDER BY similarity_score DESC
        LIMIT $limit
        """
        
        try:
            self.logger.info(f"Finding similar experts for expert_id: {expert_id}")
            with self._neo4j.session() as session:
                result = session.run(query, {
                    "expert_id": expert_id,
                    "limit": limit
                })
                
                similar_experts = []
                for record in result:
                    expert_data = record["result"]
                    similar_experts.append({
                        "id": expert_data["id"],
                        "name": expert_data["name"],
                        "similarity_score": expert_data["similarity_score"],
                        "shared_domains": expert_data["shared_domains"],
                        "shared_fields": expert_data["shared_fields"],
                        "shared_skills": expert_data["shared_skills"],
                        "match_details": expert_data["match_details"]
                    })
                
                self.logger.info(f"Found {len(similar_experts)} similar experts for {expert_id}")
                return similar_experts
                
        except Exception as e:
            self.logger.error(f"Error finding similar experts: {str(e)}", exc_info=True)
            return []

    async def _get_interaction_patterns(self, expert_id: str, days: int = 30) -> Dict[str, float]:
        """Get interaction patterns from expert_interactions table"""
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT 
                    receiver_id,
                    COUNT(*) as interaction_count,
                    AVG(CASE WHEN success = true THEN 1 ELSE 0 END) as success_rate
                FROM expert_interactions
                WHERE sender_id = %s 
                AND created_at >= NOW() - interval '%s days'
                GROUP BY receiver_id
                ORDER BY interaction_count DESC
            """, (expert_id, days))
            
            interactions = cur.fetchall()
            
            # Calculate interaction scores
            interaction_scores = {}
            for interaction in interactions:
                score = (interaction['interaction_count'] * 0.7 + 
                        interaction['success_rate'] * 0.3)
                interaction_scores[interaction['receiver_id']] = score
                
            return interaction_scores
            
        finally:
            if conn:
                conn.close()

    async def _get_search_patterns(self, expert_id: str, days: int = 30) -> Dict[str, float]:
        """Get search patterns from expert_searches table"""
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("""
                SELECT 
                    es.expert_id,
                    COUNT(*) as appearance_count,
                    AVG(CASE WHEN es.clicked = true THEN 1 ELSE 0 END) as click_rate,
                    AVG(es.rank_position) as avg_rank
                FROM expert_searches es
                JOIN search_logs sl ON es.search_id = sl.id
                WHERE sl.user_id = %s
                AND sl.timestamp >= NOW() - interval '%s days'
                GROUP BY es.expert_id
                ORDER BY appearance_count DESC
            """, (expert_id, days))
            
            searches = cur.fetchall()
            
            # Calculate search relevance scores
            search_scores = {}
            for search in searches:
                score = (
                    search['appearance_count'] * 0.4 +
                    search['click_rate'] * 0.4 +
                    (1.0 / (search['avg_rank'] + 1)) * 0.2
                )
                search_scores[search['expert_id']] = score
                
            return search_scores
            
        finally:
            if conn:
                conn.close()

    async def _get_neo4j_similarity(self, expert_id: str) -> List[Dict[str, Any]]:
        """Get similarity scores from Neo4j"""
        query = """
        MATCH (e1:Expert {id: $expert_id})
        MATCH (e2:Expert)
        WHERE e1 <> e2
        
        // Calculate domain overlap
        OPTIONAL MATCH (e1)-[:HAS_DOMAIN]->(d:Domain)<-[:HAS_DOMAIN]-(e2)
        WITH e1, e2, COLLECT(d.name) as shared_domains, COUNT(d) as domain_count
        
        // Calculate field overlap
        OPTIONAL MATCH (e1)-[:HAS_FIELD]->(f:Field)<-[:HAS_FIELD]-(e2)
        WITH e1, e2, shared_domains, domain_count, 
                COLLECT(f.name) as shared_fields, COUNT(f) as field_count
        
        // Calculate skill overlap
        OPTIONAL MATCH (e1)-[:HAS_SKILL]->(s:Skill)<-[:HAS_SKILL]-(e2)
        WITH e2, shared_domains, domain_count, 
                shared_fields, field_count,
                COLLECT(s.name) as shared_skills, COUNT(s) as skill_count,
                (domain_count * 3 + field_count * 2 + skill_count) / 
                (CASE WHEN domain_count + field_count + skill_count = 0 
                    THEN 1 
                    ELSE domain_count + field_count + skill_count 
                END) as similarity_score
        
        WHERE similarity_score > 0
        
        RETURN {
            expert_id: e2.id,
            shared_domains: shared_domains,
            shared_fields: shared_fields,
            shared_skills: shared_skills,
            similarity_score: similarity_score
        } as result
        """
        
        with self._neo4j.session() as session:
            result = session.run(query, {"expert_id": expert_id})
            return [record["result"] for record in result]

    async def _record_recommendations(self, expert_id: str, recommendations: List[Dict[str, Any]]):
        """Record recommendations for feedback"""
        conn = None
        try:
            conn = get_db_connection()
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
                    '{"source": "recommendation", "score": ' + str(rec['similarity_score']) + '}'
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
        """Close Neo4j connection"""
        if self._neo4j:
            try:
                self._neo4j.close()
                self.logger.info("Neo4j connection closed")
            except Exception as e:
                self.logger.error(f"Error closing Neo4j connection: {str(e)}")