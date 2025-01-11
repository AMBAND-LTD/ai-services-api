import pandas as pd
import plotly.express as px
import streamlit as st
import time
import logging
from typing import Dict, Optional, Any
from datetime import datetime
import plotly.graph_objects as go

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('analytics_dashboard')

def setup_database_connection(conn) -> None:
    """Set up database parameters with error handling."""
    with conn.cursor() as cursor:
        try:
            cursor.execute("SET statement_timeout = '30s';")
            cursor.execute("SET work_mem = '50MB';")
        except Exception as e:
            logger.warning(f"Failed to set database parameters: {str(e)}")

def get_usage_metrics(
    conn,
    start_date: datetime,
    end_date: datetime
) -> Dict[str, pd.DataFrame]:
    """
    Retrieve usage metrics with enhanced error handling and NULL value handling.
    """
    start_time = time.time()
    metrics_template = {
        'activity_metrics': pd.DataFrame(),
        'sessions': pd.DataFrame(),
        'performance': pd.DataFrame()
    }
    
    try:
        setup_database_connection(conn)
        
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH UserActivityMetrics AS (
                    SELECT 
                        DATE(COALESCE(c.date, s.date)) as date,
                        COALESCE(COUNT(DISTINCT COALESCE(c.user_id, s.user_id)), 0) as total_users,
                        COALESCE(COUNT(DISTINCT c.user_id), 0) as chat_users,
                        COALESCE(COUNT(DISTINCT s.user_id), 0) as search_users,
                        COALESCE(SUM(COALESCE(c.chat_count, 0) + COALESCE(s.search_count, 0)), 0) as total_interactions
                    FROM (
                        SELECT 
                            DATE(timestamp) as date,
                            user_id,
                            COUNT(*) as chat_count
                        FROM chat_interactions
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY DATE(timestamp), user_id
                    ) c
                    FULL OUTER JOIN (
                        SELECT 
                            DATE(timestamp) as date,
                            user_id,
                            COUNT(*) as search_count
                        FROM search_logs
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY DATE(timestamp), user_id
                    ) s ON c.date = s.date AND c.user_id = s.user_id
                    GROUP BY COALESCE(c.date, s.date)
                ),
                SessionMetrics AS (
                    SELECT 
                        DATE(start_time) as date,
                        COUNT(*) as total_sessions,
                        COALESCE(AVG(EXTRACT(epoch FROM (end_time - start_time))), 0) as avg_session_duration,
                        COALESCE(AVG(total_messages), 0) as avg_messages_per_session,
                        COALESCE(SUM(CASE WHEN successful THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) as success_rate
                    FROM chat_sessions
                    WHERE start_time BETWEEN %s AND %s
                    GROUP BY DATE(start_time)
                ),
                PerformanceMetrics AS (
                    SELECT 
                        DATE(timestamp) as date,
                        COALESCE(AVG(EXTRACT(epoch FROM avg_response_time)), 0) as avg_response_time,
                        COALESCE(cache_hit_rate, 0) as cache_hit_rate,
                        COALESCE(error_rate, 0) as error_rate
                    FROM search_performance
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY DATE(timestamp), cache_hit_rate, error_rate
                )
                SELECT json_build_object(
                    'activity_metrics', COALESCE((SELECT json_agg(row_to_json(UserActivityMetrics)) FROM UserActivityMetrics), '[]'::json),
                    'sessions', COALESCE((SELECT json_agg(row_to_json(SessionMetrics)) FROM SessionMetrics), '[]'::json),
                    'performance', COALESCE((SELECT json_agg(row_to_json(PerformanceMetrics)) FROM PerformanceMetrics), '[]'::json)
                ) as metrics;
            """, (start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date))
            
            query_time = time.time() - start_time
            if query_time > 5:
                logger.warning(f"Slow query detected: {query_time:.2f} seconds")
            
            raw_result = cursor.fetchone()
            logger.info(f"Query result: {raw_result}")
            
            if not raw_result or raw_result[0] is None:
                logger.info("No data found for the specified date range")
                return metrics_template
                
            result = raw_result[0]
            logger.info(f"Parsed result structure: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
            
            metrics = {}
            for key in ['activity_metrics', 'sessions', 'performance']:
                try:
                    data = result.get(key, []) or []
                    df = pd.DataFrame(data)
                    if not df.empty and 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date']).dt.date
                    metrics[key] = df
                except Exception as e:
                    logger.error(f"Error processing {key} metrics: {str(e)}")
                    metrics[key] = pd.DataFrame()
            
            return metrics
            
    except Exception as e:
        logger.error(f"Error retrieving usage metrics: {str(e)}")
        return metrics_template

def create_metric_chart(
    data: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
    title: str,
    height: int = 400
) -> Optional[go.Figure]:
    """Create a metric chart with error handling."""
    try:
        if data.empty or not all(col in data.columns for col in [x_col] + y_cols):
            return None
            
        fig = px.line(
            data,
            x=x_col,
            y=y_cols,
            title=title,
            labels={'value': 'Value', 'variable': 'Metric'}
        )
        fig.update_layout(height=height)
        return fig
    except Exception as e:
        logger.error(f"Error creating chart {title}: {str(e)}")
        return None

def display_usage_analytics(filters: Dict[str, Any], metrics: Dict[str, pd.DataFrame]) -> None:
    """Display usage analytics with enhanced error handling."""
    try:
        st.subheader("Usage Analytics")
        
        activity_data = metrics.get('activity_metrics', pd.DataFrame())
        session_data = metrics.get('sessions', pd.DataFrame())
        perf_data = metrics.get('performance', pd.DataFrame())
        
        if all(df.empty for df in [activity_data, session_data, perf_data]):
            st.warning("No data available for the selected date range. Please adjust your filters and try again.")
            return
            
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            total_users = activity_data['total_users'].sum() if not activity_data.empty else 0
            st.metric("Total Users", f"{total_users:,}")
        with col2:
            total_sessions = session_data['total_sessions'].sum() if not session_data.empty else 0
            st.metric("Total Sessions", f"{total_sessions:,}")
        with col3:
            avg_success = session_data['success_rate'].mean() if not session_data.empty else 0
            st.metric("Average Success Rate", f"{avg_success:.1%}")
        
        # User Activity Trends
        if not activity_data.empty:
            fig = create_metric_chart(
                activity_data,
                'date',
                ['chat_users', 'search_users'],
                'Daily Active Users by Type'
            )
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        # Session Analysis
        if not session_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = create_metric_chart(
                    session_data,
                    'date',
                    ['avg_session_duration'],
                    'Average Session Duration (seconds)',
                    300
                )
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = create_metric_chart(
                    session_data,
                    'date',
                    ['avg_messages_per_session'],
                    'Average Messages per Session',
                    300
                )
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        # Performance Metrics
        if not perf_data.empty:
            st.subheader("System Performance")
            fig = create_metric_chart(
                perf_data,
                'date',
                ['avg_response_time', 'error_rate'],
                'System Performance Over Time'
            )
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        logger.error(f"Error displaying analytics: {str(e)}")
        st.error("An error occurred while displaying the analytics. Please try again later.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    st.set_page_config(page_title="Usage Analytics Dashboard", layout="wide")