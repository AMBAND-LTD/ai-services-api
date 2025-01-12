import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import sys
import traceback
import time
import logging
from typing import Dict, Optional, List, Any, Union
from datetime import datetime, date
import json

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('analytics_dashboard')

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
    start_date: Union[datetime, date],
    end_date: Union[datetime, date],
    page_size: int = 10,
    offset: int = 0
) -> Dict[str, pd.DataFrame]:
    """
    Retrieve content metrics with enhanced error handling and logging.
    """
    # Define metrics template upfront
    metrics_template = {
        'resource_metrics': pd.DataFrame(),
        'popular_resources': pd.DataFrame(),
        'domain_metrics': pd.DataFrame(),
        'expert_metrics': pd.DataFrame()
    }

    # Logging of input parameters
    logger.info("=" * 50)
    logger.info("Starting get_content_metrics")
    logger.info("=" * 50)
    logger.info(f"Input Parameters:")
    logger.info(f"  Start Date: {start_date} (type: {type(start_date)})")
    logger.info(f"  End Date: {end_date} (type: {type(end_date)})")
    logger.info(f"  Page Size: {page_size}")
    logger.info(f"  Offset: {offset}")
    
    # Convert dates to datetime
    try:
        start_date = datetime.combine(start_date, datetime.min.time()) if isinstance(start_date, date) else start_date
        end_date = datetime.combine(end_date, datetime.max.time()) if isinstance(end_date, date) else end_date
    except Exception as date_conv_error:
        logger.error(f"Date conversion error: {str(date_conv_error)}")
        return metrics_template

    start_time = time.time()
    
    try:
        with conn.cursor() as cursor:
            query = """
            WITH ResourceMetrics AS (
                SELECT 
                    r.type as resource_type,
                    r.source,
                    COUNT(DISTINCT r.id) as total_resources,
                    COALESCE(array_length(r.domains, 1), 0) as unique_domains
                FROM resources_resource r
                GROUP BY r.type, r.source, r.domains
            ),
            PopularResources AS (
                SELECT 
                    r.id,
                    r.title,
                    r.type,
                    r.source,
                    r.publication_year,
                    r.authors,
                    r.domains,
                    r.topics,
                    COALESCE(e.expert_views, 0) as expert_views
                FROM resources_resource r
                LEFT JOIN (
                    SELECT 
                        e.id as expert_id,
                        COUNT(s.id) as expert_views
                    FROM experts_expert e
                    LEFT JOIN search_logs s ON 
                        s.search_type = 'expert_search' AND
                        s.clicked = true AND
                        s.query = e.first_name || ' ' || e.last_name
                    WHERE s.timestamp BETWEEN %s AND %s
                    GROUP BY e.id
                ) e ON r.id = e.expert_id
                ORDER BY expert_views DESC
                OFFSET %s
                LIMIT %s
            ),
            DomainMetrics AS (
                SELECT 
                    domain_name,
                    COUNT(*) as resource_count,
                    COUNT(DISTINCT e.id) as expert_count,
                    array_agg(DISTINCT r.type) as resource_types
                FROM (
                    SELECT r.id, r.type, unnest(r.domains) as domain_name
                    FROM resources_resource r
                    WHERE r.domains IS NOT NULL
                ) r
                LEFT JOIN experts_expert e ON 
                    e.domains && ARRAY[r.domain_name]
                GROUP BY domain_name
                ORDER BY COUNT(*) DESC
                LIMIT 15
            ),
            ExpertMetrics AS (
                SELECT 
                    e.first_name || ' ' || e.last_name as expert_name,
                    e.designation,
                    e.theme,
                    COUNT(s.id) as search_count,
                    COUNT(DISTINCT s.user_id) as unique_searchers,
                    e.domains as expertise_domains
                FROM experts_expert e
                LEFT JOIN search_logs s ON 
                    s.search_type = 'expert_search' AND
                    s.query = e.first_name || ' ' || e.last_name
                WHERE s.timestamp BETWEEN %s AND %s
                GROUP BY e.id, e.first_name, e.last_name, e.designation, e.theme, e.domains
                ORDER BY COUNT(s.id) DESC
                LIMIT 10
            )
            SELECT 
                json_build_object(
                    'resource_metrics', COALESCE((SELECT json_agg(row_to_json(ResourceMetrics)) FROM ResourceMetrics), '[]'::json),
                    'popular_resources', COALESCE((SELECT json_agg(row_to_json(PopularResources)) FROM PopularResources), '[]'::json),
                    'domain_metrics', COALESCE((SELECT json_agg(row_to_json(DomainMetrics)) FROM DomainMetrics), '[]'::json),
                    'expert_metrics', COALESCE((SELECT json_agg(row_to_json(ExpertMetrics)) FROM ExpertMetrics), '[]'::json)
                ) as metrics;
            """

            try:
                cursor.execute(query, (
                    start_date, end_date,     # PopularResources date range (2)
                    offset, page_size,        # PopularResources pagination (2)
                    start_date, end_date      # ExpertMetrics date range (2)
                ))
            except Exception as query_error:
                logger.error(f"Query execution error: {str(query_error)}")
                logger.error(f"Error occurred at line {sys.exc_info()[-1].tb_lineno}")
                logger.error(traceback.format_exc())
                return metrics_template
            
            query_time = time.time() - start_time
            logger.info(f"Query execution time: {query_time:.2f} seconds")
            
            if query_time > 5:
                logger.warning(f"Slow query detected: {query_time:.2f} seconds")
            
            raw_result = cursor.fetchone()
            
            if raw_result is None:
                logger.warning("No results returned from query")
                return metrics_template
            
            try:
                result = raw_result[0]
            except (IndexError, TypeError) as e:
                logger.error(f"Error accessing query result: {str(e)}")
                logger.error(f"Raw result: {raw_result}")
                return metrics_template
            
            if not result:
                logger.warning("Query returned None or empty result")
                return metrics_template
            
            # Process metrics
            metrics = {}
            for key in metrics_template.keys():
                try:
                    raw_data = result.get(key, [])
                    df = pd.DataFrame(raw_data)
                    
                    if not df.empty:
                        # Convert array/json columns
                        for col in df.columns:
                            try:
                                if df[col].dtype == 'object':
                                    # Try array conversion first
                                    try:
                                        if isinstance(df[col].iloc[0], str) and df[col].iloc[0].startswith('{'):
                                            # PostgreSQL array format
                                            df[col] = df[col].apply(lambda x: x.strip('{}').split(',') if x else [])
                                        elif isinstance(df[col].iloc[0], str) and df[col].iloc[0].startswith('['):
                                            # JSON array format
                                            df[col] = df[col].apply(json.loads)
                                    except:
                                        # If array conversion fails, try JSON object
                                        if isinstance(df[col].iloc[0], str) and (
                                            df[col].iloc[0].startswith('{') or 
                                            df[col].iloc[0].startswith('[')):
                                            df[col] = df[col].apply(safe_json_loads)
                            except (IndexError, AttributeError):
                                continue
                    
                    metrics[key] = df
                except Exception as key_error:
                    logger.error(f"Error processing {key} metrics: {str(key_error)}")
                    metrics[key] = pd.DataFrame()
            
            logger.info("Content metrics retrieval completed successfully")
            return metrics
            
    except Exception as global_error:
        logger.error("Unexpected error in get_content_metrics")
        logger.error(f"Error details: {str(global_error)}")
        logger.error(traceback.format_exc())
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

def display_content_analytics(
    metrics: Dict[str, pd.DataFrame], 
    filters: Optional[Dict[str, Any]] = None
) -> None:
    """Display content analytics dashboard with robust error handling."""
    try:
        st.title("Content Analytics Dashboard")
        
        # Ensure metrics is a dictionary
        if not isinstance(metrics, dict):
            st.error("Invalid metrics data. Unable to display analytics.")
            return

        # Check if all metrics are empty
        if all(df.empty for df in metrics.values()):
            st.warning("No data available for the selected date range. Please adjust your filters and try again.")
            return

        # 1. Overview Section
        st.header("Overview")
        resource_metrics = metrics['resource_metrics']
        
        col1, col2, col3 = st.columns(3)
        with col1:
            total_resources = safe_aggregate(resource_metrics, 'total_resources')
            st.metric("Total Resources", f"{total_resources:,}")
        with col2:
            expert_metrics = metrics['expert_metrics']
            total_searches = safe_aggregate(expert_metrics, 'search_count')
            st.metric("Total Expert Searches", f"{total_searches:,}")
        with col3:
            unique_domains = safe_aggregate(resource_metrics, 'unique_domains')
            st.metric("Unique Domains", f"{unique_domains:,}")

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

        # 3. Expert Analytics Section
        st.header("Expert Analytics")
        expert_metrics = metrics['expert_metrics']
        if not expert_metrics.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_experts = create_visualization(
                    expert_metrics.head(10),
                    'bar',
                    'expert_name',
                    'search_count',
                    'Top 10 Most Searched Experts'
                )
                if fig_experts:
                    fig_experts.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_experts, use_container_width=True)
            
            with col2:
                fig_searchers = create_visualization(
                    expert_metrics.head(10),
                    'bar',
                    'expert_name',
                    'unique_searchers',
                    'Top 10 Experts by Unique Searchers'
                )
                if fig_searchers:
                    fig_searchers.update_layout(xaxis_tickangle=-45)
                    st.plotly_chart(fig_searchers, use_container_width=True)

            # Expert details table
            st.subheader("Expert Details")
            try:
                display_columns = [
                    'expert_name', 'designation', 'theme', 
                    'search_count', 'unique_searchers'
                ]
                if all(col in expert_metrics.columns for col in display_columns):
                    st.dataframe(
                        expert_metrics[display_columns]
                        .sort_values('search_count', ascending=False)
                        .style.background_gradient(
                            subset=['search_count', 'unique_searchers'],
                            cmap='Blues'
                        )
                    )
            except Exception as e:
                logger.error(f"Error displaying expert table: {str(e)}")

        # 4. Domain Analytics Section
        st.header("Domain Analytics")
        domain_metrics = metrics['domain_metrics']
        if not domain_metrics.empty:
            fig_domains = create_visualization(
                domain_metrics,
                'scatter',
                'resource_count',
                'expert_count',
                'Domain Coverage Analysis',
                hover_data=['domain_name'],
                size='resource_count'
            )
            if fig_domains:
                st.plotly_chart(fig_domains, use_container_width=True)

            # Domain distribution table
            # Domain distribution table
            st.subheader("Top Domains")
            try:
                display_columns = ['domain_name', 'resource_count', 'expert_count']
                if all(col in domain_metrics.columns for col in display_columns):
                    st.dataframe(
                        domain_metrics[display_columns]
                        .sort_values('resource_count', ascending=False)
                        .style.background_gradient(
                            subset=['resource_count', 'expert_count'],
                            cmap='Blues'
                        )
                    )
            except Exception as e:
                logger.error(f"Error displaying domain table: {str(e)}")
                st.error("Unable to display domain metrics table.")

        # 5. Popular Resources Section
        st.header("Popular Resources")
        popular_resources = metrics['popular_resources']
        if not popular_resources.empty:
            fig_popularity = create_visualization(
                popular_resources.head(10),
                'bar',
                'title',
                'expert_views',
                'Top 10 Most Viewed Resources',
                barmode='group'
            )
            if fig_popularity:
                fig_popularity.update_layout(
                    xaxis_tickangle=-45,
                    xaxis_title="",
                    yaxis_title="Views",
                    height=500
                )
                st.plotly_chart(fig_popularity, use_container_width=True)

            # Popular resources table
            st.subheader("Detailed Resource Metrics")
            try:
                df_display = popular_resources.copy()
                # Convert array columns to strings
                if 'domains' in df_display.columns:
                    df_display['domains'] = df_display['domains'].apply(
                        lambda x: ', '.join(x) if isinstance(x, list) else str(x)
                    )
                if 'authors' in df_display.columns:
                    df_display['authors'] = df_display['authors'].apply(
                        lambda x: ', '.join(x) if isinstance(x, list) else str(x)
                    )
                
                display_columns = [
                    'title', 'type', 'source', 'publication_year', 
                    'expert_views', 'domains'
                ]
                if all(col in df_display.columns for col in display_columns):
                    st.dataframe(
                        df_display[display_columns]
                        .sort_values('expert_views', ascending=False)
                        .style.background_gradient(
                            subset=['expert_views'],
                            cmap='Blues'
                        )
                    )
            except Exception as e:
                logger.error(f"Error displaying resource table: {str(e)}")

    except Exception as e:
        logger.error(f"Error displaying content analytics: {str(e)}")
        st.error("An unexpected error occurred while displaying content analytics.")