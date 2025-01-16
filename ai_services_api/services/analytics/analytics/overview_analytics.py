import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import logging
from typing import Dict, Optional, Any

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

def display_overview_analytics(metrics_df, filters):
    """
    Display overview analytics visualizations.
    
    Parameters:
    - metrics_df: DataFrame containing overview metrics
    - filters: Additional filters (not used in this implementation)
    """
    st.subheader("Overview Analytics")

    # Safe calculation of metrics with default values
    total_interactions = metrics_df['chat_interactions'].sum() + metrics_df['searches'].sum()
    total_users = metrics_df['chat_users'].sum() + metrics_df['search_users'].sum()
    expert_matches = metrics_df['expert_matches'].sum()
    
    # Robust satisfaction calculation
    avg_satisfaction = metrics_df['avg_satisfaction']
    # Remove any NaN or infinite values
    avg_satisfaction = avg_satisfaction[avg_satisfaction.notna() & (avg_satisfaction != float('inf'))]
    # Use 0 if no valid satisfaction scores
    satisfaction_value = avg_satisfaction.mean() if not avg_satisfaction.empty else 0
    
    # Key metrics summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Interactions", f"{total_interactions:,}")
    with col2:
        st.metric("Total Users", f"{total_users:,}")
    with col3:
        st.metric("Expert Matches", f"{expert_matches:,}")
    with col4:
        st.metric("Avg Satisfaction", f"{satisfaction_value:.2f}")
    
    # Create multi-panel visualization with more space
    fig = make_subplots(
        rows=2, 
        cols=2, 
        subplot_titles=(
            "Interaction Trends", 
            "User Activity", 
            "Expert Engagement", 
            "Sentiment Analysis"
        ),
        vertical_spacing=0.15,  # Increased vertical spacing
        horizontal_spacing=0.1  # Increased horizontal spacing
    )

    # 1. Interaction Trends (Top Left)
    fig.add_trace(
        go.Scatter(
            x=metrics_df['date'], 
            y=metrics_df['chat_interactions'], 
            name='Chat Interactions',
            mode='lines+markers'
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=metrics_df['date'], 
            y=metrics_df['searches'], 
            name='Searches',
            mode='lines+markers'
        ),
        row=1, col=1
    )

    # 2. User Activity (Top Right)
    fig.add_trace(
        go.Scatter(
            x=metrics_df['date'], 
            y=metrics_df['chat_users'], 
            name='Chat Users',
            mode='lines+markers'
        ),
        row=1, col=2
    )
    fig.add_trace(
        go.Scatter(
            x=metrics_df['date'], 
            y=metrics_df['search_users'], 
            name='Search Users',
            mode='lines+markers'
        ),
        row=1, col=2
    )

    # 3. Expert Engagement (Bottom Left)
    fig.add_trace(
        go.Bar(
            x=metrics_df['date'], 
            y=metrics_df['expert_matches'], 
            name='Expert Matches'
        ),
        row=2, col=1
    )

    # 4. Sentiment Analysis (Bottom Right)
    # Check and plot available sentiment metrics
    sentiment_cols = ['avg_sentiment', 'avg_satisfaction']
    valid_sentiment_cols = [col for col in sentiment_cols if metrics_df[col].notna().any() and metrics_df[col].sum() != 0]
    
    if valid_sentiment_cols:
        for col in valid_sentiment_cols:
            fig.add_trace(
                go.Scatter(
                    x=metrics_df['date'], 
                    y=metrics_df[col], 
                    name=col.replace('_', ' ').title(),
                    mode='lines+markers'
                ),
                row=2, col=2
            )
    else:
        fig.add_annotation(
            text="No Sentiment Data Available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            row=2, col=2
        )

    # Update layout for better readability
    fig.update_layout(
        height=900,  # Increased height
        width=1200,  # Added width to prevent squeezing
        title_text="Comprehensive Platform Overview",
        showlegend=True,
        legend_orientation="h",
        legend=dict(y=1.1, x=0.5, xanchor='center'),
        margin=dict(l=50, r=50, t=100, b=50)  # Adjusted margins
    )

    # Update axis labels
    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=2)
    
    fig.update_yaxes(title_text="Interactions", row=1, col=1)
    fig.update_yaxes(title_text="Users", row=1, col=2)
    fig.update_yaxes(title_text="Expert Matches", row=2, col=1)
    fig.update_yaxes(title_text="Sentiment Score", row=2, col=2)

    # Display the comprehensive visualization
    st.plotly_chart(fig, use_container_width=True)

    # Insights Section
    with st.expander("Overview Insights"):
        # Calculate and display key insights
        insights = []
        
        # Interaction Growth
        interaction_growth = (metrics_df['chat_interactions'] + metrics_df['searches']).pct_change().mean() * 100
        insights.append(f"Average Interaction Growth: {interaction_growth:.2f}%")
        
        # User Engagement
        if 'total_users' in metrics_df.columns:
            user_engagement = (metrics_df['chat_users'] / metrics_df['total_users']).mean() * 100
            insights.append(f"Average User Engagement Rate: {user_engagement:.2f}%")
        
        # Expert Match Rate
        expert_match_rate = (metrics_df['expert_matches'] / (metrics_df['chat_interactions'] + metrics_df['searches'])).mean() * 100
        insights.append(f"Expert Match Rate: {expert_match_rate:.2f}%")
        
        # Display insights
        for insight in insights:
            st.write(f"• {insight}")

    # Warning for limited data
    if len(metrics_df) < 7:
        st.warning("Limited historical data. More insights will become available with more data points.")