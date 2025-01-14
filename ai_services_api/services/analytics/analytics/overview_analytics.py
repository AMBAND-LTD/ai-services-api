import pandas as pd
import plotly.express as px
import streamlit as st

def get_overview_metrics(conn, start_date, end_date):
    """
    Retrieve overall metrics from the database for the specified date range.
    
    Parameters:
    - conn: Database connection object
    - start_date: Start date for the analysis
    - end_date: End date for the analysis
    
    Returns:
    - DataFrame containing overview metrics
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            WITH DailyMetrics AS (
                -- Chat metrics
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as chat_interactions,
                    COUNT(DISTINCT user_id) as chat_users
                FROM chat_interactions
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY DATE(timestamp)
            ),
            SearchMetrics AS (
                -- Search metrics
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as search_count,
                    COUNT(DISTINCT user_id) as search_users,
                    AVG(EXTRACT(epoch FROM response_time)) as avg_response_time
                FROM search_logs
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY DATE(timestamp)
            ),
            ExpertMetrics AS (
                -- Expert matching metrics
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as expert_matches,
                    AVG(similarity_score) as avg_similarity
                FROM expert_matching_logs
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY DATE(timestamp)
            ),
            SentimentMetrics AS (
                -- Sentiment metrics
                SELECT 
                    DATE(timestamp) as date,
                    AVG(sentiment_score) as avg_sentiment,
                    AVG(satisfaction_score) as avg_satisfaction
                FROM sentiment_metrics
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY DATE(timestamp)
            )
            SELECT 
                COALESCE(d.date, s.date, e.date, sm.date) as date,
                COALESCE(d.chat_interactions, 0) as chat_interactions,
                COALESCE(d.chat_users, 0) as chat_users,
                COALESCE(s.search_count, 0) as searches,
                COALESCE(s.search_users, 0) as search_users,
                COALESCE(s.avg_response_time, 0) as avg_response_time,
                COALESCE(e.expert_matches, 0) as expert_matches,
                COALESCE(e.avg_similarity, 0) as avg_similarity,
                COALESCE(sm.avg_sentiment, 0) as avg_sentiment,
                COALESCE(sm.avg_satisfaction, 0) as avg_satisfaction
            FROM DailyMetrics d
            FULL OUTER JOIN SearchMetrics s ON d.date = s.date
            FULL OUTER JOIN ExpertMetrics e ON COALESCE(d.date, s.date) = e.date
            FULL OUTER JOIN SentimentMetrics sm ON COALESCE(d.date, s.date, e.date) = sm.date
            ORDER BY date
        """, (start_date, end_date) * 4)
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

def display_overview_analytics(metrics_df,filters):
    """
    Display overview analytics visualizations.
    
    Parameters:
    - metrics_df: DataFrame containing overview metrics
    """
    st.subheader("Overview Analytics")

    # Key metrics summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Interactions", 
                 f"{metrics_df['chat_interactions'].sum() + metrics_df['searches'].sum():,}")
    with col2:
        st.metric("Total Users", 
                 f"{metrics_df['chat_users'].sum() + metrics_df['search_users'].sum():,}")
    with col3:
        st.metric("Expert Matches", 
                 f"{metrics_df['expert_matches'].sum():,}")
    with col4:
        st.metric("Avg Satisfaction", 
                 f"{metrics_df['avg_satisfaction'].mean():.2f}")

    # Activity Overview
    st.plotly_chart(
        px.line(
            metrics_df,
            x="date",
            y=["chat_interactions", "searches", "expert_matches"],
            title="Daily Activity Overview",
            labels={"value": "Count", "variable": "Activity Type"}
        )
    )

    # User Engagement
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            px.line(
                metrics_df,
                x="date",
                y=["chat_users", "search_users"],
                title="Daily Active Users",
                labels={"value": "Users", "variable": "Activity Type"}
            )
        )
    
    with col2:
        st.plotly_chart(
            px.line(
                metrics_df,
                x="date",
                y=["avg_sentiment", "avg_satisfaction"],
                title="User Satisfaction Metrics",
                labels={"value": "Score", "variable": "Metric"}
            )
        )