import logging
import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Any, Optional
import redis
from dotenv import load_dotenv
from ai_services_api.services.centralized_repository.database_setup import get_db_connection
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RedisIndexManager:
    def __init__(self):
        """Initialize Redis index manager."""
        try:
            load_dotenv()
            self.embedding_model = SentenceTransformer(
                os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
            )
            self.setup_redis_connections()
            logger.info("RedisIndexManager initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing RedisIndexManager: {e}")
            raise

    def setup_redis_connections(self):
        """Setup Redis connections with retry logic."""
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
                
                # Initialize Redis connections
                self.redis_text = redis.StrictRedis.from_url(
                    self.redis_url, 
                    decode_responses=True,
                    db=0
                )
                self.redis_binary = redis.StrictRedis.from_url(
                    self.redis_url, 
                    decode_responses=False,
                    db=0
                )
                
                # Test connections
                self.redis_text.ping()
                self.redis_binary.ping()
                
                logger.info("Redis connections established successfully")
                return
                
            except redis.ConnectionError as e:
                if attempt == max_retries - 1:
                    logger.error("Failed to connect to Redis after maximum retries")
                    raise
                logger.warning(f"Redis connection attempt {attempt + 1} failed, retrying...")
                time.sleep(retry_delay)

    def fetch_resources_and_experts(self) -> List[Dict[str, Any]]:
        """Fetch resources and associated expert data."""
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            conn = None
            cur = None
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                
                # First check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'resources_resource'
                    );
                """)
                if not cur.fetchone()[0]:
                    logger.warning("resources_resource table does not exist yet")
                    return []
                
                cur.execute("""
                    SELECT 
                        r.doi,
                        r.title,
                        r.abstract,
                        r.summary,
                        r.authors,
                        r.description,
                        e.first_name,
                        e.last_name,
                        e.knowledge_expertise,
                        e.fields,
                        e.subfields,
                        e.domains
                    FROM resources_resource r
                    LEFT JOIN experts_expert e ON e.id = r.expert_id
                    WHERE r.doi IS NOT NULL 
                    AND r.title IS NOT NULL
                """)
                
                resources = [{
                    'doi': row[0],
                    'title': row[1],
                    'abstract': row[2] or '',
                    'summary': row[3] or '',
                    'authors': row[4] if row[4] else [],
                    'description': row[5] or '',
                    'expert_name': f"{row[6]} {row[7]}" if row[6] and row[7] else None,
                    'knowledge_expertise': row[8] if row[8] else [],
                    'fields': row[9] if row[9] else [],
                    'subfields': row[10] if row[10] else [],
                    'domains': row[11] if row[11] else []
                } for row in cur.fetchall()]
                
                logger.info(f"Fetched {len(resources)} resources from database")
                return resources
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("All retry attempts failed")
                    raise
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

    def create_redis_index(self) -> bool:
        """Create Redis indexes for resources."""
        try:
            logger.info("Creating Redis indexes...")
            resources = self.fetch_resources_and_experts()
            
            if not resources:
                logger.warning("No resources found to index")
                return False
            
            for resource in resources:
                try:
                    # Create combined text for embedding
                    text_content = self._create_text_content(resource)
                    
                    # Generate embedding
                    embedding = self.embedding_model.encode(text_content)
                    
                    # Store in Redis
                    self._store_resource_data(resource, text_content, embedding)
                    
                    logger.info(f"Indexed resource: {resource['title'][:100]}...")
                    
                except Exception as e:
                    logger.error(f"Error indexing resource {resource.get('doi', 'Unknown DOI')}: {e}")
                    continue
            
            logger.info(f"Successfully created Redis indexes for {len(resources)} resources")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Redis indexes: {e}")
            return False

    def _create_text_content(self, resource: Dict[str, Any]) -> str:
        """Create combined text content for embedding."""
        text_parts = [f"Title: {resource['title']}"]
        
        if resource['abstract']:
            text_parts.append(f"Abstract: {resource['abstract']}")
        if resource['summary']:
            text_parts.append(f"Summary: {resource['summary']}")
        if resource['description']:
            text_parts.append(f"Description: {resource['description']}")
        if resource['authors']:
            text_parts.append(f"Authors: {', '.join(resource['authors'])}")
        if resource['expert_name']:
            text_parts.append(f"Expert: {resource['expert_name']}")
        if resource['knowledge_expertise']:
            text_parts.append(f"Expertise: {' | '.join(resource['knowledge_expertise'])}")
        if resource['fields']:
            text_parts.append(f"Fields: {' | '.join(resource['fields'])}")
        if resource['subfields']:
            text_parts.append(f"Subfields: {' | '.join(resource['subfields'])}")
        if resource['domains']:
            text_parts.append(f"Domains: {' | '.join(resource['domains'])}")
            
        return '\n'.join(text_parts)

    def _store_resource_data(self, resource: Dict[str, Any], text_content: str, 
                           embedding: np.ndarray) -> None:
        """Store resource data in Redis."""
        base_key = f"res:{resource['doi']}"
        
        pipeline = self.redis_text.pipeline()
        try:
            # Store text content
            pipeline.set(f"text:{base_key}", text_content)
            
            # Store embedding as binary
            self.redis_binary.set(
                f"emb:{base_key}", 
                embedding.astype(np.float32).tobytes()
            )
            
            # Store metadata
            metadata = {
                'title': resource['title'],
                'doi': resource['doi'],
                'authors': '|'.join(resource['authors']),
                'expert_name': resource['expert_name'] or '',
                'fields': '|'.join(resource['fields']),
                'domains': '|'.join(resource['domains'])
            }
            pipeline.hset(f"meta:{base_key}", mapping=metadata)
            
            pipeline.execute()
            
        except Exception as e:
            pipeline.reset()
            raise e

    def clear_redis_indexes(self) -> bool:
        """Clear all Redis indexes."""
        try:
            patterns = ['text:res:*', 'emb:res:*', 'meta:res:*']
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = self.redis_text.scan(cursor, match=pattern, count=100)
                    if keys:
                        self.redis_text.delete(*keys)
                    if cursor == 0:
                        break
            
            logger.info("Cleared all Redis indexes")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing Redis indexes: {e}")
            return False

    def close(self):
        """Close Redis connections."""
        try:
            if hasattr(self, 'redis_text'):
                self.redis_text.close()
            if hasattr(self, 'redis_binary'):
                self.redis_binary.close()
            logger.info("Redis connections closed")
        except Exception as e:
            logger.error(f"Error closing Redis connections: {e}")

    def __del__(self):
        """Ensure connections are closed on deletion."""
        self.close()