import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import time
import logging
from typing import Dict, Optional, List, Any, Union
from datetime import datetime
import json
from contextlib import contextmanager

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('analytics_dashboard')

@contextmanager
def database_cursor(conn):
    """Context manager for database cursor handling."""
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

def setup_database_connection(conn) -> None:
    """Set up database parameters for better query performance."""
    with database_cursor(conn) as cursor:
        try:
            cursor.execute("SET statement_timeout = '30s';")
            cursor.execute("SET work_mem = '50MB';")
        except Exception as e:
            logger.warning(f"Failed to set database parameters: {str(e)}")

def safe_json_loads(value: str) -> dict:
    """Safely parse JSON string."""
    try:
        return json.loads(value) if value else {}
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse JSON: {str(e)}")
        return {}

def safe_aggregate(df: pd.DataFrame, column: str, operation: str = 'sum') -> float:
    """Safely perform aggregation operations on DataFrame columns."""
    try:
        if df.empty or column not in df.columns:
            return 0
        if operation == 'sum':
            return df[column].sum()
        elif operation == 'count':
            return df[column].count()
        return 0
    except Exception as e:
        logger.warning(f"Failed to aggregate column {column}: {str(e)}")
        return 0

def get_content_metrics(
    conn,
    start_date: datetime,
    end_date: datetime,
    page_size: int = 10,
    offset: int = 0
) -> Dict[str, pd.DataFrame]:
    """
    Retrieve content metrics with enhanced error handling and logging.
    """
    start_time = time.time()
    metrics_template = {
        'resource_metrics': pd.DataFrame(),
        'popular_resources': pd.DataFrame(),
        'domain_metrics': pd.DataFrame(),
        'author_metrics': pd.DataFrame()
    }
    
    try:
        setup_database_connection(conn)
        
        with database_cursor(conn) as cursor:
            # Execute the main query
            cursor.execute("""
                WITH ResourceMetrics AS (
                    SELECT 
                        r.type as resource_type,
                        r.source,
                        COUNT(DISTINCT r.id) as total_resources,
                        COUNT(DISTINCT s.user_id) as unique_users,
                        COUNT(s.id) as total_views,
                        COUNT(DISTINCT jsonb_array_elements_text(r.domains)) as unique_domains
                    FROM resources_resource r
                    LEFT JOIN search_logs s ON s.query LIKE '%' || COALESCE(r.doi, r.title) || '%'
                    WHERE s.timestamp BETWEEN %s AND %s
                    GROUP BY r.type, r.source
                ),
                PopularResources AS (
                    SELECT 
                        r.id,
                        r.title,
                        r.type,
                        r.source,
                        r.publication_year,
                        COUNT(s.id) as view_count,
                        COUNT(DISTINCT s.user_id) as unique_viewers,
                        r.authors as authors,
                        r.domains as domains,
                        r.topics as topics
                    FROM resources_resource r
                    LEFT JOIN search_logs s ON s.query LIKE '%' || COALESCE(r.doi, r.title) || '%'
                    WHERE s.timestamp BETWEEN %s AND %s
                    GROUP BY r.id, r.title, r.type, r.source, r.publication_year, r.authors, r.domains, r.topics
                    ORDER BY COUNT(s.id) DESC
                    OFFSET %s
                    LIMIT %s
                ),
                DomainMetrics AS (
                    SELECT 
                        domain_name,
                        COUNT(*) as resource_count,
                        COUNT(DISTINCT s.user_id) as user_count,
                        array_agg(DISTINCT r.type) as resource_types
                    FROM resources_resource r
                    CROSS JOIN LATERAL jsonb_array_elements_text(r.domains) as domain_name
                    LEFT JOIN search_logs s ON s.query LIKE '%' || COALESCE(r.doi, r.title) || '%'
                    WHERE s.timestamp BETWEEN %s AND %s
                    GROUP BY domain_name
                    ORDER BY COUNT(*) DESC
                    LIMIT 15
                ),
                AuthorMetrics AS (
                    SELECT 
                        author_name,
                        COUNT(*) as publication_count,
                        array_agg(DISTINCT r.type) as publication_types,
                        COUNT(DISTINCT s.user_id) as reader_count
                    FROM resources_resource r
                    CROSS JOIN LATERAL jsonb_array_elements_text(r.authors) as author_name
                    LEFT JOIN search_logs s ON s.query LIKE '%' || COALESCE(r.doi, r.title) || '%'
                    WHERE s.timestamp BETWEEN %s AND %s
                    GROUP BY author_name
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                )
                SELECT 
                    json_build_object(
                        'resource_metrics', (SELECT json_agg(row_to_json(ResourceMetrics)) FROM ResourceMetrics),
                        'popular_resources', (SELECT json_agg(row_to_json(PopularResources)) FROM PopularResources),
                        'domain_metrics', (SELECT json_agg(row_to_json(DomainMetrics)) FROM DomainMetrics),
                        'author_metrics', (SELECT json_agg(row_to_json(AuthorMetrics)) FROM AuthorMetrics)
                    ) as metrics
            """, (start_date, end_date, start_date, end_date, offset, page_size, 
                  start_date, end_date, start_date, end_date))
            
            query_time = time.time() - start_time
            if query_time > 5:
                logger.warning(f"Slow query detected: {query_time:.2f} seconds")
            
            raw_result = cursor.fetchone()
            
            if not raw_result:
                logger.warning("No results returned from query")
                return metrics_template
            
            try:
                result = raw_result[0]
            except (IndexError, TypeError) as e:
                logger.error(f"Error accessing query result: {str(e)}")
                return metrics_template
            
            if not result:
                logger.warning("Query returned None")
                return metrics_template
            
            metrics = {}
            for key in metrics_template.keys():
                try:
                    df = pd.DataFrame(result.get(key, []))
                    if not df.empty:
                        # Process JSON columns
                        for col in df.columns:
                            try:
                                if (df[col].size > 0 and 
                                    isinstance(df[col].iloc[0], str) and 
                                    df[col].iloc[0].startswith('{')):
                                    df[col] = df[col].apply(safe_json_loads)
                            except IndexError:
                                logger.warning(f"Empty DataFrame encountered while processing column {col}")
                                continue
                    metrics[key] = df
                except Exception as e:
                    logger.error(f"Error processing {key} metrics: {str(e)}")
                    metrics[key] = pd.DataFrame()
            
            return metrics
            
    except Exception as e:
        logger.error(f"Error retrieving content metrics: {str(e)}")
        return metrics_template

def create_visualization(
    data: pd.DataFrame,
    viz_type: str,
    x: str,
    y: Union[str, List[str]],
    title: str,
    **kwargs
) -> Optional[go.Figure]:
    """Create visualization with error handling."""
    try:
        if data.empty:
            logger.warning(f"Empty DataFrame provided for visualization: {title}")
            return None
            
        if viz_type == 'bar':
            fig = px.bar(data, x=x, y=y, title=title, **kwargs)
        elif viz_type == 'pie':
            fig = px.pie(data, values=y, names=x, title=title, **kwargs)
        elif viz_type == 'scatter':
            fig = px.scatter(data, x=x, y=y, title=title, **kwargs)
        elif viz_type == 'line':
            fig = px.line(data, x=x, y=y, title=title, **kwargs)
        else:
            logger.error(f"Unsupported visualization type: {viz_type}")
            return None
            
        return fig
    except Exception as e:
        logger.error(f"Error creating visualization {title}: {str(e)}")
        return None

def display_content_analytics(metrics: Dict[str, pd.DataFrame], conn=None) -> None:
    """Display content analytics dashboard with enhanced error handling."""
    try:
        st.title("Content Analytics Dashboard")
        
        if all(df.empty for df in metrics.values()):
            st.warning("No data available for the selected date range. Please adjust your filters and try again.")
            return

        # 1. Overview Section
        st.header("Overview")
        resource_metrics = metrics['resource_metrics']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_resources = safe_aggregate(resource_metrics, 'total_resources')
            st.metric("Total Resources", f"{total_resources:,}")
        with col2:
            total_views = safe_aggregate(resource_metrics, 'total_views')
            st.metric("Total Views", f"{total_views:,}")
        with col3:
            unique_users = safe_aggregate(resource_metrics, 'unique_users')
            st.metric("Unique Users", f"{unique_users:,}")
        with col4:
            total_domains = safe_aggregate(resource_metrics, 'unique_domains')
            st.metric("Unique Domains", f"{total_domains:,}")

        # 2. Resource Distribution Section
        st.header("Resource Distribution")
        if not resource_metrics.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_source = create_visualization(
                    resource_metrics,
                    'pie',
                    'source',
                    'total_resources',
                    'Content Sources',
                    hole=0.4
                )
                if fig_source:
                    fig_source.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_source, use_container_width=True)
            
            with col2:
                fig_type = create_visualization(
                    resource_metrics,
                    'pie',
                    'resource_type',
                    'total_resources',
                    'Content Types',
                    hole=0.4
                )
                if fig_type:
                    fig_type.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_type, use_container_width=True)

        # 3. Popular Content Section
        st.header("Popular Content")
        popular_resources = metrics['popular_resources']
        if not popular_resources.empty:
            fig_popularity = create_visualization(
                popular_resources.head(10),
                'bar',
                'title',
                ['view_count', 'unique_viewers'],
                'Top 10 Most Viewed Resources',
                barmode='group'
            )
            if fig_popularity:
                fig_popularity.update_layout(
                    xaxis_tickangle=-45,
                    xaxis_title="",
                    yaxis_title="Count",
                    height=500
                )
                st.plotly_chart(fig_popularity, use_container_width=True)

            # Popular resources table
            st.subheader("Detailed Resource Metrics")
            try:
                df_display = popular_resources.copy()
                df_display['domains'] = df_display['domains'].apply(
                    lambda x: ', '.join(x) if isinstance(x, list) else str(x)
                )
                df_display['authors'] = df_display['authors'].apply(
                    lambda x: ', '.join(x) if isinstance(x, list) else str(x)
                )
                display_columns = [
                    'title', 'type', 'source', 'publication_year', 
                    'view_count', 'unique_viewers', 'authors', 'domains'
                ]
                if all(col in df_display.columns for col in display_columns):
                    st.dataframe(
                        df_display[display_columns]
                        .sort_values('view_count', ascending=False)
                        .style.background_gradient(
                            subset=['view_count', 'unique_viewers'], 
                            cmap='Blues'
                        )
                    )
            except Exception as e:
                logger.error(f"Error displaying resource table: {str(e)}")

        # 4. Author Analytics Section
        st.header("Author Analytics")
        author_metrics = metrics['author_metrics']
        if not author_metrics.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_authors = create_visualization(
                    author_metrics.head(10),
                    'bar',
                    'author_name',
                    'publication_count',
                    'Top 10 Authors by Publication Count'
                )
                if fig_authors:
                    fig_authors.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_authors, use_container_width=True)
            
            with col2:
                fig_readers = create_visualization(
                    author_metrics.head(10),
                    'bar',
                    'author_name',
                    'reader_count',
                    'Top 10 Authors by Reader Count'
                )
                if fig_readers:
                    fig_readers.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_readers, use_container_width=True)

        # 5. Domain Analytics Section
        st.header("Domain Analytics")
        domain_metrics = metrics['domain_metrics']
        if not domain_metrics.empty:
            fig_domains = create_visualization(
                domain_metrics,
                'scatter',
                'resource_count',
                'user_count',
                'Domain Popularity Analysis',
                hover_data=['domain_name'],
                size='resource_count'
            )
            if fig_domains:
                st.plotly_chart(fig_domains, use_container_width=True)

            # Domain distribution table
            st.subheader("Top Domains")
            try:
                display_columns = ['domain_name', 'resource_count', 'user_count']
                if all(col in domain_metrics.columns for col in display_columns):
                    st.dataframe(
                        domain_metrics[display_columns]
                        .sort_values('resource_count', ascending=False)
                        .style.background_gradient(
                            subset=['resource_count', 'user_count'],
                            cmap='Blues'
                        )
                    )
            except Exception as e:
                logger.error(f"Error displaying domain table: {str(e)}")
                st.error("Unable to display domain metrics table.")

        # 6. Publication Year Analysis
        st.header("Publication Year Analysis")
        popular_resources = metrics['popular_resources']
        try:
            if not popular_resources.empty and 'publication_year' in popular_resources.columns:
                required_columns = ['publication_year', 'view_count', 'unique_viewers']
                if all(col in popular_resources.columns for col in required_columns):
                    # Prepare aggregation dictionary
                    agg_dict = {
                        'view_count': 'sum',
                        'unique_viewers': 'sum'
                    }
                    
                    # Add 'id' count if available
                    if 'id' in popular_resources.columns:
                        agg_dict['id'] = 'count'
                    
                    year_dist = popular_resources.groupby('publication_year').agg(agg_dict).reset_index()
                    
                    if not year_dist.empty:
                        fig_years = create_visualization(
                            year_dist,
                            'line',
                            'publication_year',
                            list(agg_dict.keys()),
                            'Content and Engagement by Publication Year'
                        )
                        if fig_years:
                            st.plotly_chart(fig_years, use_container_width=True)
        except Exception as e:
            logger.error(f"Error creating publication year analysis: {str(e)}")
            st.error("Unable to display publication year analysis.")

    except Exception as e:
        logger.error(f"Error displaying analytics: {str(e)}")
        st.error("An error occurred while displaying analytics. Please try again later.")

if __name__ == "__main__":
    # Add type hints for imports
    from typing import Union, Dict, List, Optional, Any
    
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger('analytics_dashboard')