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
    def _load_table_definitions() -> Dict[str, str]:
        """Load SQL definitions for all tables."""
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
           
        }

    @staticmethod
    def _load_index_definitions() -> List[str]:
        """Load SQL definitions for all indexes."""
        return [
            "CREATE INDEX IF NOT EXISTS idx_experts_name ON experts_expert (first_name, last_name)",
            "CREATE INDEX IF NOT EXISTS idx_resources_source ON resources_resource(source)",
            # Add other indexes here...
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
                    AVG(EXTRACT(EPOCH FROM response_time)) as avg_response_time_seconds,
                    SUM(CASE WHEN clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_through_rate
                FROM search_logs
                GROUP BY DATE(timestamp)
            """
            # Add other views here...
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
            
            # Create tables
            self._create_tables(cur)
            
            # Create indexes
            self._create_indexes(cur)
            
            # Create views
            self._create_views(cur)
            
            logger.info("Schema initialization completed successfully")

    def _create_tables(self, cur):
        """Create all database tables."""
        # Create core tables first
        for table_name, table_sql in self.schema_manager.table_definitions['core_tables'].items():
            try:
                cur.execute(table_sql)
                logger.info(f"Created core table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating {table_name}: {e}")
                raise

        # Create dependent tables
        for table_name, table_sql in self.schema_manager.table_definitions['dependent_tables'].items():
            try:
                cur.execute(table_sql)
                logger.info(f"Created dependent table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating {table_name}: {e}")
                raise

    def _create_indexes(self, cur):
        """Create all database indexes."""
        for index_sql in self.schema_manager.index_definitions:
            try:
                cur.execute(index_sql)
                logger.info("Created index successfully")
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")

    def _create_views(self, cur):
        """Create all database views."""
        for view_name, view_sql in self.schema_manager.view_definitions.items():
            try:
                cur.execute(view_sql)
                logger.info(f"Created view: {view_name}")
            except Exception as e:
                logger.warning(f"View creation warning: {e}")

class ExpertManager:
    """Handle expert-related database operations."""
    
    @staticmethod
    def generate_password() -> str:
        """Generate a random password."""
        return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))
    
    def load_experts_from_csv(self, csv_path: str):
        """Load expert data from CSV file."""
        with DatabaseConnection.get_cursor() as (cur, conn):
            df = pd.read_csv(csv_path)
            for _, row in df.iterrows():
                try:
                    self._process_expert_row(cur, row)
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Error processing expert row: {e}")
                    continue

    def _process_expert_row(self, cur, row):
        """Process a single expert row from CSV."""
        # Extract expert data with fallbacks
        first_name = row.get('First_name', row.get('first_name', 'Unknown'))
        last_name = row.get('Last_name', row.get('last_name', 'Unknown'))
        designation = row.get('Designation', '')
        theme = row.get('Theme', '')
        unit = row.get('Unit', '')
        contact_details = row.get('Contact Details', '')
        expertise_str = row.get('Knowledge and Expertise', '')
        
        # Process expertise
        expertise_list = [exp.strip() for exp in expertise_str.split(',') if exp.strip()]
        
        # Insert expert
        cur.execute("""
            INSERT INTO experts_expert (
                first_name, last_name, designation, theme, unit, 
                contact_details, knowledge_expertise
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            first_name, last_name, designation, theme, unit,
            contact_details, json.dumps(expertise_list) if expertise_list else None
        ))
        
        logger.info(f"Added expert: {first_name} {last_name}")

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