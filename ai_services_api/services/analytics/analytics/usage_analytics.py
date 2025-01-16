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
    start_time = time.time()
    metrics_template = {
        'activity_metrics': pd.DataFrame(),
        'sessions': pd.DataFrame(),
        'performance': pd.DataFrame()
    }
    
    try:
        setup_database_connection(conn)
        
        with conn.cursor() as cursor:
            query = """
            WITH UserActivityMetrics AS (
                SELECT 
                    DATE(COALESCE(c.created_at, s.timestamp)) as date,
                    COALESCE(COUNT(DISTINCT COALESCE(c.user_id, s.user_id)), 0) as total_users,
                    COALESCE(COUNT(DISTINCT c.user_id), 0) as chat_users,
                    COALESCE(COUNT(DISTINCT s.user_id), 0) as search_users,
                    COALESCE(COUNT(c.id), 0) + COALESCE(COUNT(s.id), 0) as total_interactions
                FROM (
                    SELECT 
                        DATE(created_at) as created_at,
                        user_id,
                        id
                    FROM chat_interactions
                    WHERE created_at BETWEEN %s AND %s
                    GROUP BY DATE(created_at), user_id, id
                ) c
                FULL OUTER JOIN (
                    SELECT 
                        DATE(timestamp) as timestamp,
                        user_id,
                        id
                    FROM search_logs
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY DATE(timestamp), user_id, id
                ) s ON DATE(c.created_at) = DATE(s.timestamp) AND c.user_id = s.user_id
                GROUP BY DATE(COALESCE(c.created_at, s.timestamp))
                ORDER BY DATE(COALESCE(c.created_at, s.timestamp))
            ),
            SessionMetrics AS (
                SELECT 
                    DATE(start_time) as date,
                    COUNT(*) as total_sessions,
                    COALESCE(
                        AVG(
                            CASE 
                                WHEN end_time IS NOT NULL 
                                THEN EXTRACT(epoch FROM (end_time - start_time))
                                ELSE EXTRACT(epoch FROM (CURRENT_TIMESTAMP - start_time))
                            END
                        ), 
                        0
                    ) as avg_session_duration,
                    COALESCE(AVG(total_messages), 0) as avg_messages_per_session,
                    COALESCE(SUM(CASE WHEN successful THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0), 0) as success_rate
                FROM chat_sessions
                WHERE start_time BETWEEN %s AND %s
                GROUP BY DATE(start_time)
                ORDER BY DATE(start_time)
            ),
            PerformanceMetrics AS (
                SELECT 
                    DATE(s.timestamp) as date,
                    COALESCE(AVG(EXTRACT(epoch FROM s.response_time::interval)), 0) as avg_response_time,
                    COUNT(CASE WHEN s.clicked = true OR s.success_rate = 1 THEN 1 END)::float / 
                        NULLIF(COUNT(*), 0) * 100 as success_rate,
                    COUNT(CASE WHEN s.success_rate < 1 THEN 1 END)::float / 
                        NULLIF(COUNT(*), 0) * 100 as error_rate,
                    COUNT(*) as total_queries,
                    COUNT(DISTINCT s.user_id) as unique_users
                FROM search_logs s
                WHERE s.timestamp BETWEEN %s AND %s
                GROUP BY DATE(s.timestamp)
                ORDER BY DATE(s.timestamp)
            )
            SELECT json_build_object(
                'activity_metrics', COALESCE((SELECT json_agg(row_to_json(UserActivityMetrics)) FROM UserActivityMetrics), '[]'::json),
                'sessions', COALESCE((SELECT json_agg(row_to_json(SessionMetrics)) FROM SessionMetrics), '[]'::json),
                'performance', COALESCE((SELECT json_agg(row_to_json(PerformanceMetrics)) FROM PerformanceMetrics), '[]'::json)
            ) as metrics;
            """

            cursor.execute(query, (
                start_date, end_date,  # Chat interactions date range
                start_date, end_date,  # Search logs date range
                start_date, end_date,  # Chat sessions date range
                start_date, end_date   # Search logs performance range
            ))
            
            raw_result = cursor.fetchone()
            
            if not raw_result or raw_result[0] is None:
                logger.info("No data found for the specified date range")
                return metrics_template
                
            result = raw_result[0]
            
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
        fig.update_layout(
            height=height,
            xaxis_title="Date",
            yaxis_title="Count",
            legend_title="Metrics",
            hovermode='x unified'
        )
        return fig
    except Exception as e:
        logger.error(f"Error creating chart {title}: {str(e)}")
        return None

def display_usage_analytics(filters: Dict[str, Any], metrics: Dict[str, pd.DataFrame]) -> None:
    try:
        # Check if all expected DataFrames exist and have data
        activity_data = metrics.get('activity_metrics', pd.DataFrame())
        session_data = metrics.get('sessions', pd.DataFrame())
        perf_data = metrics.get('performance', pd.DataFrame())
        
        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_interactions = activity_data['total_interactions'].sum() if not activity_data.empty else 0
            st.metric("Total Interactions", f"{total_interactions:,}")
        with col2:
            chat_users = activity_data['chat_users'].sum() if not activity_data.empty else 0
            st.metric("Chat Users", f"{chat_users:,}")
        with col3:
            search_users = activity_data['search_users'].sum() if not activity_data.empty else 0
            st.metric("Search Users", f"{search_users:,}")
        with col4:
            total_users = activity_data['total_users'].sum() if not activity_data.empty else 0
            st.metric("Total Users", f"{total_users:,}")

        # Daily Active Users
        if not activity_data.empty:
            st.plotly_chart(
                px.line(
                    activity_data,
                    x="date",
                    y=["total_users", "chat_users", "search_users"],
                    title="Daily Active Users",
                    labels={"value": "Count", "variable": "Metric"}
                )
            )
        
        # Total Interactions
        if not activity_data.empty:
            st.plotly_chart(
                px.line(
                    activity_data,
                    x="date",
                    y="total_interactions",
                    title="Total Interactions"
                )
            )

        # Session Duration
        if not session_data.empty:
            st.plotly_chart(
                px.line(
                    session_data,
                    x="date",
                    y="avg_session_duration",
                    title="Average Session Duration"
                )
            )

        # Performance Metrics
        if not perf_data.empty:
            # Average Response Time
            st.plotly_chart(
                px.line(
                    perf_data,
                    x="date",
                    y="avg_response_time",
                    title="Average Response Time"
                )
            )
            
            # Success and Error Rates
            st.plotly_chart(
                px.line(
                    perf_data,
                    x="date",
                    y=["success_rate", "error_rate"],
                    title="Success and Error Rates"
                )
            )

    except Exception as e:
        logger.error(f"Error displaying analytics: {str(e)}")
        st.error("An error occurred while displaying the analytics. Please try again later.")