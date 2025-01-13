import os
import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse
import logging
import json
import secrets
import string
import pandas as pd
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def get_connection_params():
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        parsed_url = urlparse(database_url)
        return {'host': parsed_url.hostname, 'port': parsed_url.port, 'dbname': parsed_url.path[1:],
                'user': parsed_url.username, 'password': parsed_url.password}
    else:
        in_docker = os.getenv('DOCKER_ENV', 'false').lower() == 'true'
        return {'host': '167.86.85.127' if in_docker else 'localhost', 'port': '5432',
                'dbname': os.getenv('POSTGRES_DB', 'aphrc'), 'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD', 'p0stgres')}

def get_db_connection(dbname=None):
    params = get_connection_params()
    if dbname:
        params['dbname'] = dbname
    try:
        conn = psycopg2.connect(**params)
        logger.info(f"Successfully connected to database: {params['dbname']} at {params['host']}")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise
@contextmanager
def get_db_cursor(autocommit: bool = False):
    """Context manager for database connections."""
    conn = get_db_connection()
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
        conn.close()

def verify_table_exists(cur, table_name: str) -> bool:
    """Verify if a specific table exists."""
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        );
    """, (table_name,))
    return cur.fetchone()[0]

def create_database_if_not_exists():
    params = get_connection_params()
    target_dbname = params['dbname']
    try:
        try:
            conn = get_db_connection()
            logger.info(f"Database {target_dbname} already exists")
            conn.close()
            return
        except psycopg2.OperationalError:
            pass
        conn = get_db_connection('postgres')
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_dbname,))
        if not cur.fetchone():
            logger.info(f"Creating database {target_dbname}...")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_dbname)))
            logger.info(f"Database {target_dbname} created successfully")
        else:
            logger.info(f"Database {target_dbname} already exists")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def generate_fake_password():
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(12))

def fix_experts_table():
    conn = get_db_connection()
    conn.autocommit = True  # Switch to autocommit mode
    cur = conn.cursor()
    
    try:
        # Check if table exists
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'experts_expert');")
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            # Define all column alterations
            alterations = [
                "ADD COLUMN IF NOT EXISTS normalized_domains text[] DEFAULT '{}'",
                "ADD COLUMN IF NOT EXISTS normalized_fields text[] DEFAULT '{}'",
                "ADD COLUMN IF NOT EXISTS normalized_skills text[] DEFAULT '{}'",
                "ADD COLUMN IF NOT EXISTS keywords text[] DEFAULT '{}'",
                "ADD COLUMN IF NOT EXISTS search_text text",
                "ADD COLUMN IF NOT EXISTS last_updated timestamp with time zone DEFAULT NOW()"
            ]
            
            # Execute each alteration in separate transaction
            for alter_sql in alterations:
                try:
                    cur.execute(f"ALTER TABLE experts_expert {alter_sql}")
                    logger.info(f"Column alteration completed: {alter_sql}")
                except Exception as e:
                    logger.warning(f"Column alteration warning: {alter_sql}: {e}")
                    continue

            # Create search text update function
            try:
                cur.execute("""
                    CREATE OR REPLACE FUNCTION update_expert_search_text()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        NEW.search_text = 
                            COALESCE(NEW.knowledge_expertise::text, '') || ' ' ||
                            COALESCE(array_to_string(NEW.normalized_domains, ' '), '') || ' ' ||
                            COALESCE(array_to_string(NEW.normalized_fields, ' '), '') || ' ' ||
                            COALESCE(array_to_string(NEW.normalized_skills, ' '), '');
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                logger.info("Created search text update function")
            except Exception as e:
                logger.warning(f"Search text function creation warning: {e}")

            # Create trigger
            try:
                cur.execute("""
                    DROP TRIGGER IF EXISTS expert_search_text_trigger ON experts_expert;
                    CREATE TRIGGER expert_search_text_trigger
                    BEFORE INSERT OR UPDATE ON experts_expert
                    FOR EACH ROW
                    EXECUTE FUNCTION update_expert_search_text();
                """)
                logger.info("Created search text update trigger")
            except Exception as e:
                logger.warning(f"Trigger creation warning: {e}")

            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_expert_domains ON experts_expert USING gin(normalized_domains)",
                "CREATE INDEX IF NOT EXISTS idx_expert_fields ON experts_expert USING gin(normalized_fields)",
                "CREATE INDEX IF NOT EXISTS idx_expert_skills ON experts_expert USING gin(normalized_skills)",
                "CREATE INDEX IF NOT EXISTS idx_expert_keywords ON experts_expert USING gin(keywords)",
                "CREATE INDEX IF NOT EXISTS idx_expert_search ON experts_expert USING gin(to_tsvector('english', COALESCE(search_text, '')))"
            ]
            
            for index_sql in indexes:
                try:
                    cur.execute(index_sql)
                    logger.info("Created index successfully")
                except Exception as e:
                    logger.warning(f"Index creation warning: {e}")
                    continue

            # Update existing rows
            try:
                cur.execute("UPDATE experts_expert SET last_updated = NOW();")
                logger.info("Updated existing rows to trigger search text generation")
            except Exception as e:
                logger.warning(f"Row update warning: {e}")

            logger.info("Fixed experts_expert table structure and handled all columns and indexes")
            return True
        else:
            logger.warning("experts_expert table does not exist. It will be created when needed.")
            return True
            
    except Exception as e:
        logger.error(f"Error fixing experts_expert table: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def create_tables():
    """Create all database tables with proper dependency handling."""
    conn = get_db_connection()
    conn.autocommit = True  # Keep autocommit mode
    cur = conn.cursor()
    try:
        # Create extension
        try:
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            logger.info("UUID extension creation completed")
        except Exception as e:
            logger.warning(f"UUID extension warning: {e}")

        # Core tables (no foreign key dependencies)
        core_tables = [
            # Core table: resources_resource
            """
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

            # Core table: experts_expert
            """
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
            """,

            # Core table: auth_group
            """
            CREATE TABLE IF NOT EXISTS auth_group (
                id SERIAL PRIMARY KEY,
                name VARCHAR(150)
            )
            """,

            # Core table: auth_permission
            """
            CREATE TABLE IF NOT EXISTS auth_permission (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                content_type_id INTEGER,
                codename VARCHAR(100)
            )
            """,

            # Core table: authors_ai
            """
            CREATE TABLE IF NOT EXISTS authors_ai (
                author_id SERIAL PRIMARY KEY,
                name TEXT,
                orcid VARCHAR(255),
                author_identifier VARCHAR(255)
            )
            """,

            # Core table: publications_ai
            """
            CREATE TABLE IF NOT EXISTS publications_ai (
                doi VARCHAR(255) PRIMARY KEY,
                title TEXT,
                abstract TEXT,
                summary TEXT
            )
            """,

            # Core table: tags
            """
            CREATE TABLE IF NOT EXISTS tags (
                tag_id SERIAL PRIMARY KEY,
                tag_name VARCHAR(255) NOT NULL,
                tag_type VARCHAR(50) DEFAULT 'concept',
                additional_metadata JSONB,
                UNIQUE (tag_name, tag_type)
            )
            """,

            # Core table: search_logs
            """
            CREATE TABLE IF NOT EXISTS search_logs (
                id SERIAL PRIMARY KEY,
                query TEXT NOT NULL,
                user_id VARCHAR(255),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                response_time INTERVAL,
                result_count INTEGER,
                clicked BOOLEAN DEFAULT FALSE,
                search_type VARCHAR(50),
                success_rate FLOAT,
                page_number INTEGER DEFAULT 1,
                search_count INTEGER DEFAULT 1,
                filters JSONB
            )
            """,

            # Core table: chat_sessions
            """
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(255) UNIQUE NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP WITH TIME ZONE,
                total_messages INTEGER DEFAULT 0,
                successful BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Core table: domain_expertise_analytics
            """
            CREATE TABLE IF NOT EXISTS domain_expertise_analytics (
                id SERIAL PRIMARY KEY,
                domain_name VARCHAR(255) NOT NULL UNIQUE,
                field_name VARCHAR(255),
                subfield_name VARCHAR(255),
                expert_count INTEGER DEFAULT 0,
                match_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]

        # Create core tables first and verify
        for table_sql in core_tables:
            try:
                cur.execute(table_sql)
                logger.info("Core table creation completed successfully")
            except Exception as e:
                logger.error(f"Core table creation failed: {e}")
                raise

        # Verify core tables exist
        core_table_names = [
            'resources_resource', 'experts_expert', 'auth_group', 'auth_permission',
            'authors_ai', 'publications_ai', 'tags', 'search_logs', 'chat_sessions',
            'domain_expertise_analytics'
        ]
        
        for table in core_table_names:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table,))
            if not cur.fetchone()[0]:
                raise Exception(f"Failed to create core table: {table}")
            logger.info(f"Verified core table {table} exists")

        # Dependent tables (with foreign key references)
        dependent_tables = [
            # Dependent table: publication_tags
            """
            CREATE TABLE IF NOT EXISTS publication_tags (
                doi VARCHAR(255),
                tag_id INTEGER,
                PRIMARY KEY (doi, tag_id),
                FOREIGN KEY (doi) REFERENCES resources_resource(doi),
                FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
            )
            """,

            # Dependent table: expert_messages
            """
            CREATE TABLE IF NOT EXISTS expert_messages (
                id SERIAL PRIMARY KEY,
                sender_id INTEGER REFERENCES experts_expert(id),
                receiver_id INTEGER REFERENCES experts_expert(id),
                content TEXT,
                draft BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Dependent table: chat_interactions
            """
            CREATE TABLE IF NOT EXISTS chat_interactions (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(255) REFERENCES chat_sessions(session_id),
                user_id VARCHAR(255) NOT NULL,
                query TEXT NOT NULL,
                response TEXT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                response_time FLOAT,
                intent_type VARCHAR(255),
                intent_confidence FLOAT,
                navigation_matches INTEGER DEFAULT 0,
                publication_matches INTEGER DEFAULT 0,
                error_occurred BOOLEAN DEFAULT FALSE,
                context JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Dependent table: sentiment_metrics
            """
            CREATE TABLE IF NOT EXISTS sentiment_metrics (
                id SERIAL PRIMARY KEY,
                interaction_id INTEGER REFERENCES chat_interactions(id),
                sentiment_score FLOAT,
                emotion_labels TEXT[],
                satisfaction_score FLOAT,
                urgency_score FLOAT,
                clarity_score FLOAT,
                chat_count INTEGER DEFAULT 1,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Dependent table: sentiment_trends
            """
            CREATE TABLE IF NOT EXISTS sentiment_trends (
                id SERIAL PRIMARY KEY,
                session_id TEXT REFERENCES chat_sessions(session_id),
                avg_sentiment FLOAT,
                avg_satisfaction FLOAT,
                period_start TIMESTAMP,
                period_end TIMESTAMP
            )
            """,

            # Dependent table: expert_searches
            """
            CREATE TABLE IF NOT EXISTS expert_searches (
                id SERIAL PRIMARY KEY,
                search_id INTEGER REFERENCES search_logs(id),
                expert_id VARCHAR(255),
                rank_position INTEGER,
                clicked BOOLEAN DEFAULT FALSE,
                click_timestamp TIMESTAMP,
                session_duration INTERVAL
            )
            """
        ]

        # Create dependent tables
        for table_sql in dependent_tables:
            try:
                cur.execute(table_sql)
                logger.info("Dependent table creation completed successfully")
            except Exception as e:
                logger.warning(f"Dependent table creation warning: {e}")
                continue

        # Independent tables (no foreign key relationships)
        independent_tables = [
            # Independent table: author_publication_ai
            """
            CREATE TABLE IF NOT EXISTS author_publication_ai (
                author_id INTEGER,
                doi VARCHAR(255)
            )
            """,

            # Independent table: chat_chatbox
            """
            CREATE TABLE IF NOT EXISTS chat_chatbox (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expert_from_id INTEGER,
                expert_to_id INTEGER,
                name VARCHAR(200)
            )
            """,

            # Independent table: chat_chatboxmessage
            """
            CREATE TABLE IF NOT EXISTS chat_chatboxmessage (
                id SERIAL PRIMARY KEY,
                message TEXT,
                read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expert_from_id INTEGER,
                expert_to_id INTEGER,
                chatbox_id INTEGER
            )
            """,

            # Independent table: expertise_categories
            """
            CREATE TABLE IF NOT EXISTS expertise_categories (
                id SERIAL PRIMARY KEY,
                expert_orcid TEXT,
                original_term TEXT,
                domain TEXT,
                field TEXT,
                subfield TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Independent table: roles_role
            """
            CREATE TABLE IF NOT EXISTS roles_role (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                description VARCHAR(255),
                active BOOLEAN DEFAULT TRUE,
                permissions JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                default_expert_role BOOLEAN DEFAULT FALSE
            )
            """,

            # Independent table: query_history_ai
            """
            CREATE TABLE IF NOT EXISTS query_history_ai (
                query_id SERIAL PRIMARY KEY,
                query TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                result_count INTEGER,
                search_type VARCHAR(50),
                user_id TEXT
            )
            """,

            # Independent table: term_frequencies
            """
            CREATE TABLE IF NOT EXISTS term_frequencies (
                term VARCHAR(255) PRIMARY KEY,
                frequency INTEGER DEFAULT 1,
                expert_id INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Independent table: query_predictions
            """
            CREATE TABLE IF NOT EXISTS query_predictions (
                id SERIAL PRIMARY KEY,
                partial_query TEXT NOT NULL,
                predicted_query TEXT NOT NULL,
                selected BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                confidence_score FLOAT,
                user_id VARCHAR(255)
            )
            """,

            # Independent table: search_sessions
            """
            CREATE TABLE IF NOT EXISTS search_sessions (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255),
                start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_timestamp TIMESTAMP,
                query_count INTEGER DEFAULT 1,
                successful_searches INTEGER DEFAULT 0
            )
            """,

            # Independent table: search_performance
            """
            CREATE TABLE IF NOT EXISTS search_performance (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                avg_response_time INTERVAL,
                cache_hit_rate FLOAT,
                error_rate FLOAT,
                total_queries INTEGER,
                unique_users INTEGER
            )
            """,

            # Independent table: chat_analytics
            """
            CREATE TABLE IF NOT EXISTS chat_analytics (
                id SERIAL PRIMARY KEY,
                interaction_id INTEGER,
                content_id VARCHAR(255),
                content_type VARCHAR(50),
                similarity_score FLOAT,
                rank_position INTEGER,
                clicked BOOLEAN DEFAULT FALSE,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT valid_content_type CHECK (content_type IN ('navigation', 'publication'))
            )
            """,

            # Independent table: expert_matching_logs
            """
            CREATE TABLE IF NOT EXISTS expert_matching_logs (
                id SERIAL PRIMARY KEY,
                expert_id VARCHAR(255) NOT NULL,
                matched_expert_id VARCHAR(255) NOT NULL,
                similarity_score FLOAT,
                shared_domains JSONB,
                shared_fields INTEGER,
                shared_skills INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                successful BOOLEAN DEFAULT TRUE
            )
            """,

            # Independent table: expert_processing_logs
            """
            CREATE TABLE IF NOT EXISTS expert_processing_logs (
                id SERIAL PRIMARY KEY,
                expert_id VARCHAR(255),
                processing_time FLOAT,
                domains_count INTEGER,
                fields_count INTEGER,
                success BOOLEAN,
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Independent table: expert_summary_logs
            """
            CREATE TABLE IF NOT EXISTS expert_summary_logs (
                id SERIAL PRIMARY KEY,
                expert_id VARCHAR(255),
                total_domains INTEGER,
                total_fields INTEGER,
                total_subfields INTEGER,
                expertise_depth INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,

            # Independent table: collaboration_history
            """
            CREATE TABLE IF NOT EXISTS collaboration_history (
                id SERIAL PRIMARY KEY,
                expert_id VARCHAR(255) NOT NULL,
                collaborator_id VARCHAR(255) NOT NULL,
                collaboration_score FLOAT NOT NULL,
                shared_domains JSONB,
                result_type VARCHAR(50),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]

        # Create independent tables
        for table_sql in independent_tables:
            try:
                cur.execute(table_sql)
                logger.info("Independent table creation completed successfully")
            except Exception as e:
                logger.warning(f"Independent table creation warning: {e}")
                continue

        # Create all indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_experts_name ON experts_expert (first_name, last_name)",
            "CREATE INDEX IF NOT EXISTS idx_query_history_timestamp ON query_history_ai (timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_query_history_user ON query_history_ai (user_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_updated ON chat_chatbox (updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_chat_message_created ON chat_chatboxmessage (created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_expertise_orcid ON expertise_categories (expert_orcid)",
            "CREATE INDEX IF NOT EXISTS idx_publications_title ON publications_ai USING gin(to_tsvector('english', title))",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_timestamp ON search_logs(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_user_id ON search_logs(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_search_logs_query ON search_logs(query)",
            "CREATE INDEX IF NOT EXISTS idx_expert_searches_expert_id ON expert_searches(expert_id)",
            "CREATE INDEX IF NOT EXISTS idx_query_predictions_partial ON query_predictions(partial_query)",
            "CREATE INDEX IF NOT EXISTS idx_search_sessions_user_id ON search_sessions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_sessions_user ON chat_sessions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_sessions_timestamp ON chat_sessions(start_time)",
            "CREATE INDEX IF NOT EXISTS idx_chat_interactions_session ON chat_interactions(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_interactions_timestamp ON chat_interactions(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_chat_analytics_interaction ON chat_analytics(interaction_id)",
            "CREATE INDEX IF NOT EXISTS idx_matching_expert_id ON expert_matching_logs(expert_id)",
            "CREATE INDEX IF NOT EXISTS idx_matching_matched_expert ON expert_matching_logs(matched_expert_id)",
            "CREATE INDEX IF NOT EXISTS idx_domain_expertise_name ON domain_expertise_analytics(domain_name)",
            "CREATE INDEX IF NOT EXISTS idx_collab_experts ON collaboration_history(expert_id, collaborator_id)",
            "CREATE INDEX IF NOT EXISTS idx_domain_expertise_name_match ON domain_expertise_analytics(domain_name, match_count)",
            "CREATE INDEX IF NOT EXISTS idx_messages_sender ON expert_messages(sender_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_receiver ON expert_messages(receiver_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_created_at ON expert_messages(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_metrics_interaction ON sentiment_metrics(interaction_id)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_metrics_timestamp ON sentiment_metrics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_metrics_scores ON sentiment_metrics(sentiment_score, satisfaction_score)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_metrics_emotion ON sentiment_metrics USING gin(emotion_labels)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_trends_session ON sentiment_trends(session_id)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_trends_period ON sentiment_trends(period_start, period_end)",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_trends_sentiment ON sentiment_trends(avg_sentiment)",
            "CREATE INDEX IF NOT EXISTS idx_resources_source ON resources_resource(source)",
            "CREATE INDEX IF NOT EXISTS idx_tags_name_type ON tags (tag_name, tag_type)",
            "CREATE INDEX IF NOT EXISTS idx_tags_metadata ON tags USING gin(additional_metadata)",
            "CREATE INDEX IF NOT EXISTS idx_publication_tags_doi ON publication_tags (doi)",
            "CREATE INDEX IF NOT EXISTS idx_publication_tags_tag ON publication_tags (tag_id)"
        ]

        # Create indexes
        for index_sql in indexes:
            try:
                cur.execute(index_sql)
                logger.info("Index creation completed successfully")
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
                continue

        # Create views
        views = [
            """
            CREATE OR REPLACE VIEW daily_search_metrics AS
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as total_searches,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(EXTRACT(EPOCH FROM response_time)) as avg_response_time_seconds,
                SUM(CASE WHEN clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_through_rate
            FROM search_logs
            GROUP BY DATE(timestamp)
            """,
            """
            CREATE OR REPLACE VIEW sentiment_analysis_metrics AS
            SELECT 
                DATE_TRUNC('hour', sm.timestamp) as time_period,
                AVG(sm.sentiment_score) as avg_sentiment,
                AVG(sm.satisfaction_score) as avg_satisfaction,
                AVG(sm.urgency_score) as avg_urgency,
                AVG(sm.clarity_score) as avg_clarity, 
                COUNT(*) as total_interactions,
                array_agg(DISTINCT e) as emotions
            FROM sentiment_metrics sm
            CROSS JOIN LATERAL unnest(sm.emotion_labels) as e
            GROUP BY DATE_TRUNC('hour', sm.timestamp);
            """,
            """
            CREATE OR REPLACE VIEW expert_search_metrics AS
            SELECT 
                es.expert_id,
                COUNT(*) as total_appearances,
                AVG(es.rank_position) as avg_rank,
                SUM(CASE WHEN es.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_through_rate
            FROM expert_searches es
            GROUP BY es.expert_id
            """,
            """
            CREATE OR REPLACE VIEW chat_daily_metrics AS
            SELECT 
                DATE(timestamp) as date,
                COUNT(DISTINCT session_id) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_interactions,
                AVG(response_time) as avg_response_time,
                SUM(CASE WHEN error_occurred THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as error_rate
            FROM chat_interactions
            GROUP BY DATE(timestamp)
            """,
            """
            CREATE OR REPLACE VIEW chat_expert_matching_metrics AS
            SELECT
                ca.expert_id,
                COUNT(*) as total_matches,
                AVG(ca.similarity_score) as avg_similarity,
                AVG(ca.rank_position) as avg_rank,
                SUM(CASE WHEN ca.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_through_rate
            FROM chat_analytics ca 
            GROUP BY ca.expert_id
            """,
            """
            CREATE OR REPLACE VIEW expert_matching_metrics AS
            SELECT
                expert_id,
                COUNT(*) as total_matches,
                AVG(similarity_score) as avg_similarity,
                AVG(CAST(jsonb_array_length(shared_domains) AS FLOAT)) as avg_shared_domains,
                COUNT(DISTINCT matched_expert_id) as unique_matches,
                SUM(CASE WHEN successful THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
            FROM expert_matching_logs
            GROUP BY expert_id
            """,
            """
            CREATE OR REPLACE VIEW domain_matching_metrics AS
            SELECT
                d.domain_name,
                d.expert_count,
                d.match_count,
                d.match_count::FLOAT / NULLIF(d.expert_count, 0) as match_rate,
                COUNT(DISTINCT em.expert_id) as active_experts
            FROM domain_expertise_analytics d
            LEFT JOIN expert_matching_logs em
                ON em.timestamp >= NOW() - interval '30 days'
            GROUP BY d.domain_name, d.expert_count, d.match_count
            """
        ]

        # Create views
        for view_sql in views:
            try:
                cur.execute(view_sql)
                logger.info("View creation completed successfully")
            except Exception as e:
                logger.warning(f"View creation warning: {e}")
                continue

        # Create trigger function
        trigger_function = """
            CREATE OR REPLACE FUNCTION update_domain_match_count()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.shared_domains IS NOT NULL THEN
                    IF jsonb_typeof(NEW.shared_domains) = 'array' THEN
                        UPDATE domain_expertise_analytics
                        SET match_count = match_count + 1,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE domain_name IN (
                            SELECT value::text
                            FROM jsonb_array_elements_text(NEW.shared_domains)
                        );
                    ELSE
                        UPDATE domain_expertise_analytics
                        SET match_count = match_count + 1,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE domain_name = NEW.shared_domains::text;
                    END IF;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """

        try:
            cur.execute(trigger_function)
            logger.info("Trigger function creation completed successfully")
        except Exception as e:
            logger.warning(f"Trigger function creation warning: {e}")

        # Create trigger
        try:
            cur.execute("DROP TRIGGER IF EXISTS trigger_update_domain_matches ON expert_matching_logs;")
            cur.execute("""
                CREATE TRIGGER trigger_update_domain_matches
                    AFTER INSERT ON expert_matching_logs
                    FOR EACH ROW
                    EXECUTE FUNCTION update_domain_match_count();
            """)
            logger.info("Trigger creation completed successfully")
        except Exception as e:
            logger.warning(f"Trigger creation warning: {e}")

        logger.info("All database objects created/verified successfully")
        return True

    except Exception as e:
        logger.error(f"Error in database initialization: {e}")
        return False
    finally:
        cur.close()
        conn.close()
def verify_database_setup():
    """Verify that all required database objects exist."""
    with get_db_cursor() as (cur, conn):
        # Check core tables
        required_tables = [
            'resources_resource',
            'tags',
            'experts_expert',
            'publication_tags'
        ]
        
        for table in required_tables:
            if not verify_table_exists(cur, table):
                raise Exception(f"Required table {table} does not exist")
            
        # Check indexes
        cur.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'resources_resource'
        """)
        indexes = [row[0] for row in cur.fetchall()]
        if 'idx_resources_source' not in indexes:
            raise Exception("Required index idx_resources_source does not exist")
            
        logger.info("Database verification completed successfully")
        return True
def create_airflow_tables():
    """Create Airflow-specific tables."""
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        # Create Airflow schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS airflow;")
        
        # Create base tables for Airflow
        airflow_tables = [
            """
            CREATE TABLE IF NOT EXISTS airflow.dag (
                dag_id VARCHAR(250) NOT NULL PRIMARY KEY,
                is_paused BOOLEAN,
                is_active BOOLEAN,
                last_scheduler_run TIMESTAMP WITH TIME ZONE,
                last_pickled TIMESTAMP WITH TIME ZONE,
                last_expired TIMESTAMP WITH TIME ZONE,
                scheduler_lock BOOLEAN,
                pickle_id INTEGER,
                fileloc VARCHAR(2000),
                owners VARCHAR(2000),
                description TEXT,
                default_view VARCHAR(25),
                schedule_interval TEXT,
                root_dag_id VARCHAR(250)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS airflow.dag_run (
                id SERIAL PRIMARY KEY,
                dag_id VARCHAR(250),
                execution_date TIMESTAMP WITH TIME ZONE,
                state VARCHAR(50),
                run_id VARCHAR(250),
                external_trigger BOOLEAN,
                conf JSON,
                end_date TIMESTAMP WITH TIME ZONE,
                start_date TIMESTAMP WITH TIME ZONE,
                UNIQUE (dag_id, execution_date)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS airflow.task_instance (
                task_id VARCHAR(250),
                dag_id VARCHAR(250),
                execution_date TIMESTAMP WITH TIME ZONE,
                start_date TIMESTAMP WITH TIME ZONE,
                end_date TIMESTAMP WITH TIME ZONE,
                duration INTERVAL,
                state VARCHAR(20),
                try_number INTEGER,
                hostname VARCHAR(1000),
                unixname VARCHAR(1000),
                job_id INTEGER,
                pool VARCHAR(256),
                queue VARCHAR(256),
                priority_weight INTEGER,
                operator VARCHAR(1000),
                queued_dttm TIMESTAMP WITH TIME ZONE,
                pid INTEGER,
                max_tries INTEGER,
                PRIMARY KEY (task_id, dag_id, execution_date)
            )
            """
        ]

        for table_sql in airflow_tables:
            try:
                cur.execute(table_sql)
                logger.info("Airflow table creation completed successfully")
            except Exception as e:
                logger.warning(f"Airflow table creation warning: {e}")
                continue

        logger.info("All Airflow database objects created/verified successfully")
        return True

    except Exception as e:
        logger.error(f"Error in Airflow database initialization: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def migrate_chat_tables():
    """Migrate the chat tables to include navigation and publication matches."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Drop the constraint first
        cur.execute("""
            DO $$ 
            BEGIN
                -- Drop the foreign key constraint if it exists
                IF EXISTS (
                    SELECT 1 
                    FROM information_schema.table_constraints 
                    WHERE constraint_name = 'chat_analytics_interaction_id_fkey'
                    AND table_name = 'chat_analytics'
                ) THEN
                    ALTER TABLE chat_analytics 
                    DROP CONSTRAINT chat_analytics_interaction_id_fkey;
                END IF;
            END $$;
        """)

        # Modify chat_interactions table
        cur.execute("""
            BEGIN;
            -- Drop expert_matches column if it exists
            ALTER TABLE chat_interactions 
            DROP COLUMN IF EXISTS expert_matches;

            -- Add new columns
            ALTER TABLE chat_interactions 
            ADD COLUMN IF NOT EXISTS navigation_matches INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS publication_matches INTEGER DEFAULT 0;

            COMMIT;
        """)

        # Update chat_analytics table
        cur.execute("""
            BEGIN;
            -- Drop expert_id column and add new content columns
            ALTER TABLE chat_analytics 
            DROP COLUMN IF EXISTS expert_id,
            ADD COLUMN IF NOT EXISTS content_id VARCHAR(255),
            ADD COLUMN IF NOT EXISTS content_type VARCHAR(50);

            -- Add constraint for content_type
            ALTER TABLE chat_analytics
            ADD CONSTRAINT valid_content_type 
            CHECK (content_type IN ('navigation', 'publication'));

            -- Recreate the foreign key constraint
            ALTER TABLE chat_analytics
            ADD CONSTRAINT chat_analytics_interaction_id_fkey
            FOREIGN KEY (interaction_id) 
            REFERENCES chat_interactions(id);

            COMMIT;
        """)

        logger.info("Successfully migrated chat tables")
        return True

    except Exception as e:
        conn.rollback()
        logger.error(f"Error migrating chat tables: {e}")
        return False

    finally:
        cur.close()
        conn.close()

def load_initial_experts(expertise_csv: str):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        df = pd.read_csv(expertise_csv)
        for _, row in df.iterrows():
            try:
                # Try different possible column names
                first_name = row.get('First_name', row.get('first_name', row.get('Firstname', row.get('firstname', 'Unknown'))))
                last_name = row.get('Last_name', row.get('last_name', row.get('Lastname', row.get('lastname', 'Unknown'))))
                designation = row.get('Designation', row.get('designation', ''))
                theme = row.get('Theme', row.get('theme', ''))
                unit = row.get('Unit', row.get('unit', ''))
                contact_details = row.get('Contact Details', row.get('contact_details', row.get('ContactDetails', '')))
                expertise_str = row.get('Knowledge and Expertise', row.get('knowledge_expertise', row.get('Expertise', '')))
                
                # Handle empty expertise string
                expertise_list = [exp.strip() for exp in expertise_str.split(',') if exp.strip()] if expertise_str else []
                fake_password = generate_fake_password()

                cur.execute("""
                    INSERT INTO experts_expert (
                        first_name, last_name, designation, theme, unit, contact_details, knowledge_expertise
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """, (
                    first_name, last_name, designation, theme, unit, contact_details,
                    json.dumps(expertise_list) if expertise_list else None
                ))
                conn.commit()
                logger.info(f"Added/updated expert data for {first_name} {last_name}")
            except Exception as row_error:
                logger.warning(f"Skipping row due to error: {row_error}")
                continue
                
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading initial expert data: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        create_database_if_not_exists()
        if not create_tables():
            raise Exception("Failed to create tables")
        if not fix_experts_table():
            raise Exception("Failed to fix experts table")
        if not create_airflow_tables():
            raise Exception("Failed to create Airflow tables")
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
