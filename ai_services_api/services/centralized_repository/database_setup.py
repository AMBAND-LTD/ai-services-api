"""
Database initialization and management module.
Handles creation and maintenance of database schema, tables, and indexes.
"""
import os
import logging
import json
import secrets
import string
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration management."""
    
    @staticmethod
    def get_connection_params() -> Dict[str, str]:
        """Get database connection parameters from environment variables."""
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            parsed_url = urlparse(database_url)
            return {
                'host': parsed_url.hostname,
                'port': parsed_url.port,
                'dbname': parsed_url.path[1:],
                'user': parsed_url.username,
                'password': parsed_url.password
            }
        
        in_docker = os.getenv('DOCKER_ENV', 'false').lower() == 'true'
        return {
            'host': '167.86.85.127' if in_docker else 'localhost',
            'port': '5432',
            'dbname': os.getenv('POSTGRES_DB', 'aphrc'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'p0stgres')
        }

class DatabaseConnection:
    """Database connection management."""
    
    @staticmethod
    @contextmanager
    def get_connection(dbname: Optional[str] = None):
        """Get database connection with optional database name override."""
        params = DatabaseConfig.get_connection_params()
        if dbname:
            params['dbname'] = dbname
            
        try:
            conn = psycopg2.connect(**params)
            logger.info(f"Connected to database: {params['dbname']} at {params['host']}")
            yield conn
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    @staticmethod
    @contextmanager
    def get_cursor(autocommit: bool = False):
        """Get database cursor with transaction management."""
        with DatabaseConnection.get_connection() as conn:
            conn.autocommit = autocommit
            cur = conn.cursor()
            try:
                yield cur, conn
            except Exception as e:
                if not autocommit:
                    conn.rollback()
                raise
            finally:
                cur.close()

class SchemaManager:
    """Database schema management."""
    
    def __init__(self):
        self.table_definitions = self._load_table_definitions()
        self.index_definitions = self._load_index_definitions()
        self.view_definitions = self._load_view_definitions()
        
    @staticmethod
    def _load_table_definitions() -> Dict[str, Dict[str, str]]:
        """Load SQL definitions for all tables in the system."""
        return {
            'core_tables': {
                'resources_resource': """
                    CREATE TABLE IF NOT EXISTS resources_resource (
                        id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL,
                        doi VARCHAR(255) UNIQUE,
                        authors TEXT[],
                        domains TEXT[],
                        type VARCHAR(50) DEFAULT 'publication',
                        publication_year INTEGER,
                        summary TEXT,
                        source VARCHAR(50) DEFAULT 'openalex',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'experts_expert': """
                    CREATE TABLE IF NOT EXISTS experts_expert (
                        id SERIAL PRIMARY KEY,
                        first_name VARCHAR(255) NOT NULL,
                        last_name VARCHAR(255) NOT NULL,
                        designation VARCHAR(255),
                        theme VARCHAR(255),
                        unit VARCHAR(255),
                        contact_details VARCHAR(255),
                        knowledge_expertise JSONB,
                        orcid VARCHAR(255),
                        domains TEXT[],
                        fields TEXT[],
                        subfields TEXT[],
                        password VARCHAR(255),
                        is_superuser BOOLEAN DEFAULT FALSE,
                        is_staff BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        last_login TIMESTAMP WITH TIME ZONE,
                        date_joined TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        bio TEXT,
                        email VARCHAR(200),
                        middle_name VARCHAR(200)
                    )
                """
            },
            'analytics_tables': {
                'search_sessions': """
                    CREATE TABLE IF NOT EXISTS search_sessions (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(255) UNIQUE NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        end_time TIMESTAMP WITH TIME ZONE,
                        successful BOOLEAN DEFAULT FALSE,
                        sentiment_score FLOAT
                    )
                """,
                'search_analytics': """
                    CREATE TABLE IF NOT EXISTS search_analytics (
                        id SERIAL PRIMARY KEY,
                        search_id INTEGER UNIQUE NOT NULL,
                        query TEXT NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        response_time FLOAT,
                        result_count INTEGER,
                        search_type VARCHAR(50),
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'expert_search_matches': """
                    CREATE TABLE IF NOT EXISTS expert_search_matches (
                        id SERIAL PRIMARY KEY,
                        search_id INTEGER NOT NULL,
                        expert_id VARCHAR(255) NOT NULL,
                        rank_position INTEGER,
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """
            },
            'interaction_tables': {
                'expert_interactions': """
                    CREATE TABLE IF NOT EXISTS expert_interactions (
                        id SERIAL PRIMARY KEY,
                        sender_id INTEGER NOT NULL,
                        receiver_id INTEGER NOT NULL,
                        interaction_type VARCHAR(100) NOT NULL,
                        success BOOLEAN DEFAULT TRUE,
                        metadata JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (sender_id) REFERENCES experts_expert(id),
                        FOREIGN KEY (receiver_id) REFERENCES experts_expert(id)
                    )
                """,
                'expert_messages': """
                    CREATE TABLE IF NOT EXISTS expert_messages (
                        id SERIAL PRIMARY KEY,
                        sender_id INTEGER NOT NULL,
                        receiver_id INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        draft BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        FOREIGN KEY (sender_id) REFERENCES experts_expert(id),
                        FOREIGN KEY (receiver_id) REFERENCES experts_expert(id)
                    )
                """
            },
            'ml_tables': {
                'query_predictions': """
                    CREATE TABLE IF NOT EXISTS query_predictions (
                        id SERIAL PRIMARY KEY,
                        partial_query TEXT NOT NULL,
                        predicted_query TEXT NOT NULL,
                        confidence_score FLOAT,
                        user_id VARCHAR(255) NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'expert_matching_logs': """
                    CREATE TABLE IF NOT EXISTS expert_matching_logs (
                        id SERIAL PRIMARY KEY,
                        expert_id VARCHAR(255) NOT NULL,
                        matched_expert_id VARCHAR(255) NOT NULL,
                        similarity_score FLOAT,
                        shared_domains TEXT[],
                        shared_fields INTEGER,
                        shared_skills INTEGER,
                        successful BOOLEAN DEFAULT TRUE,
                        user_id VARCHAR(255),
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """
            },
            'analytics_metadata_tables': {
                'domain_expertise_analytics': """
                    CREATE TABLE IF NOT EXISTS domain_expertise_analytics (
                        domain_name VARCHAR(255) PRIMARY KEY,
                        match_count INTEGER DEFAULT 0,
                        last_matched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """,
                'interactions': """
                    CREATE TABLE IF NOT EXISTS interactions (
                        id SERIAL PRIMARY KEY,
                        session_id VARCHAR(255) NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        query TEXT NOT NULL,
                        response TEXT NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        metrics JSONB,
                        response_time FLOAT,
                        sentiment_score FLOAT,
                        error_occurred BOOLEAN DEFAULT FALSE
                    )
                """
            }
        }

    @staticmethod
    def _load_index_definitions() -> List[str]:
        """Load SQL definitions for all indexes."""
        return [
            # Core table indexes
            "CREATE INDEX IF NOT EXISTS idx_experts_name ON experts_expert (first_name, last_name)",
            "CREATE INDEX IF NOT EXISTS idx_resources_source ON resources_resource(source)",
            
            # Search and analytics indexes
            "CREATE INDEX IF NOT EXISTS idx_search_sessions_user ON search_sessions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_search_analytics_user ON search_analytics(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_search_analytics_timestamp ON search_analytics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_expert_search_matches_search_id ON expert_search_matches(search_id)",
            
            # Interaction indexes
            "CREATE INDEX IF NOT EXISTS idx_expert_interactions_sender ON expert_interactions(sender_id)",
            "CREATE INDEX IF NOT EXISTS idx_expert_interactions_receiver ON expert_interactions(receiver_id)",
            "CREATE INDEX IF NOT EXISTS idx_expert_interactions_type ON expert_interactions(interaction_type)",
            "CREATE INDEX IF NOT EXISTS idx_expert_messages_sender ON expert_messages(sender_id)",
            "CREATE INDEX IF NOT EXISTS idx_expert_messages_receiver ON expert_messages(receiver_id)",
            
            # ML and matching indexes
            "CREATE INDEX IF NOT EXISTS idx_expert_matching_logs_expert ON expert_matching_logs(expert_id)",
            "CREATE INDEX IF NOT EXISTS idx_query_predictions_user ON query_predictions(user_id)",
            
            # General interaction indexes
            "CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_interactions_timestamp ON interactions(timestamp)"
        ]

    @staticmethod
    def _load_view_definitions() -> Dict[str, str]:
        """Load SQL definitions for all views."""
        return {
            'daily_search_metrics': """
                CREATE OR REPLACE VIEW daily_search_metrics AS
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as total_searches,
                    COUNT(DISTINCT user_id) as unique_users,
                    AVG(response_time) as avg_response_time,
                    SUM(CASE WHEN error_occurred THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as error_rate
                FROM search_analytics
                GROUP BY DATE(timestamp)
            """,
            'expert_interaction_metrics': """
                CREATE OR REPLACE VIEW expert_interaction_metrics AS
                SELECT 
                    sender_id,
                    receiver_id,
                    interaction_type,
                    COUNT(*) as total_interactions,
                    AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END) as success_rate,
                    MIN(created_at) as first_interaction,
                    MAX(created_at) as last_interaction
                FROM expert_interactions
                GROUP BY sender_id, receiver_id, interaction_type
            """,
            'domain_expertise_ranking': """
                CREATE OR REPLACE VIEW domain_expertise_ranking AS
                SELECT 
                    domain_name,
                    match_count,
                    last_matched_at,
                    RANK() OVER (ORDER BY match_count DESC) as domain_rank
                FROM domain_expertise_analytics
            """,
            'expert_matching_performance': """
                CREATE OR REPLACE VIEW expert_matching_performance AS
                SELECT 
                    expert_id,
                    COUNT(*) as total_matches,
                    AVG(similarity_score) as avg_similarity_score,
                    SUM(CASE WHEN successful THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
                FROM expert_matching_logs
                GROUP BY expert_id
            """
        }

class DatabaseInitializer:
    """Handle database initialization and setup."""
    
    def __init__(self):
        self.schema_manager = SchemaManager()
    
    def create_database(self):
        """Create the database if it doesn't exist."""
        params = DatabaseConfig.get_connection_params()
        target_dbname = params['dbname']
        
        try:
            # Try connecting to target database first
            with DatabaseConnection.get_connection():
                logger.info(f"Database {target_dbname} already exists")
                return
        except psycopg2.OperationalError:
            # Create database if connection failed
            with DatabaseConnection.get_connection('postgres') as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_dbname,))
                    if not cur.fetchone():
                        logger.info(f"Creating database {target_dbname}...")
                        cur.execute(sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(target_dbname)))
                        logger.info(f"Database {target_dbname} created successfully")
    
    def initialize_schema(self):
        """Initialize the complete database schema."""
        with DatabaseConnection.get_cursor(autocommit=True) as (cur, _):
            # Create extensions
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            
            # Create tables in a specific order
            table_groups = [
                self.schema_manager.table_definitions['core_tables'],
                self.schema_manager.table_definitions['analytics_tables'],
                self.schema_manager.table_definitions['interaction_tables'],
                self.schema_manager.table_definitions['ml_tables'],
                self.schema_manager.table_definitions['analytics_metadata_tables']
            ]
            
            for table_group in table_groups:
                for table_name, table_sql in table_group.items():
                    try:
                        cur.execute(table_sql)
                        logger.info(f"Created table: {table_name}")
                    except Exception as e:
                        logger.error(f"Error creating {table_name}: {e}")
                        raise
            
            # Create indexes
            for index_sql in self.schema_manager.index_definitions:
                try:
                    cur.execute(index_sql)
                    logger.info("Created index successfully")
                except Exception as e:
                    logger.warning(f"Index creation warning: {e}")
            
            # Create views
            for view_name, view_sql in self.schema_manager.view_definitions.items():
                try:
                    cur.execute(view_sql)
                    logger.info(f"Created view: {view_name}")
                except Exception as e:
                    logger.warning(f"View creation warning: {e}")
            
            logger.info("Schema initialization completed successfully")

class ExpertManager:
    """Handle expert-related database operations."""
    def load_experts_from_csv(self, csv_path: str):
        """Load expert data from CSV file."""
        with DatabaseConnection.get_cursor() as (cur, conn):
            try:
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    self._process_expert_row(cur, row)
                conn.commit()
                logger.info(f"Successfully loaded experts from {csv_path}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error loading experts from CSV: {e}")
                raise

    def _process_expert_row(self, cur, row):
        """Process a single expert row from CSV."""
        # Extract expert data with robust fallbacks
        first_name = row.get('First_name', row.get('first_name', 'Unknown'))
        last_name = row.get('Last_name', row.get('last_name', 'Unknown'))
        
        # Extract optional fields with default empty values
        designation = row.get('Designation', '')
        theme = row.get('Theme', '')
        unit = row.get('Unit', '')
        contact_details = row.get('Contact Details', '')
        email = row.get('Email', '')
        orcid = row.get('ORCID', '')
        
        # Process expertise
        expertise_str = row.get('Knowledge and Expertise', '')
        expertise_list = [exp.strip() for exp in expertise_str.split(',') if exp.strip()]
        
        # Process domains and fields
        domains = row.get('Domains', [])
        fields = row.get('Fields', [])
        subfields = row.get('Subfields', [])
        
        # Convert to lists if they're not already
        domains = domains if isinstance(domains, list) else [domains] if domains else []
        fields = fields if isinstance(fields, list) else [fields] if fields else []
        subfields = subfields if isinstance(subfields, list) else [subfields] if subfields else []
        
        try:
            # Insert expert with comprehensive data
            cur.execute("""
                INSERT INTO experts_expert (
                    first_name, last_name, designation, theme, unit, 
                    contact_details, knowledge_expertise, email, orcid,
                    domains, fields, subfields, password
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    designation = EXCLUDED.designation,
                    theme = EXCLUDED.theme,
                    unit = EXCLUDED.unit,
                    contact_details = EXCLUDED.contact_details,
                    knowledge_expertise = EXCLUDED.knowledge_expertise,
                    domains = EXCLUDED.domains,
                    fields = EXCLUDED.fields,
                    subfields = EXCLUDED.subfields
                RETURNING id
            """, (
                first_name, last_name, designation, theme, unit,
                contact_details, 
                json.dumps(expertise_list) if expertise_list else None,
                email, orcid,
                domains, fields, subfields,
                self.generate_password()  # Generate a random password
            ))
            
            expert_id = cur.fetchone()[0]
            logger.info(f"Processed expert: {first_name} {last_name} (ID: {expert_id})")
            return expert_id
        
        except Exception as e:
            logger.error(f"Error processing expert {first_name} {last_name}: {e}")
            raise

def main():
    """Main entry point for database initialization."""
    try:
        # Initialize database
        initializer = DatabaseInitializer()
        initializer.create_database()
        initializer.initialize_schema()
        
        # Load initial expert data if CSV exists
        expert_manager = ExpertManager()
        expertise_csv = os.getenv('EXPERTISE_CSV')
        if expertise_csv and os.path.exists(expertise_csv):
            expert_manager.load_experts_from_csv(expertise_csv)
            
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

if __name__ == "__main__":
    main()