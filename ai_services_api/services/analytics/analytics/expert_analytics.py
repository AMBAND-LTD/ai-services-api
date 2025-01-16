import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from plotly.subplots import make_subplots
import logging
from typing import Dict, Optional, Any

def get_expert_metrics(conn, start_date, end_date, expert_count):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            WITH ChatExperts AS (
                SELECT 
                    a.id::text,  -- Cast to text since we're comparing with text
                    COUNT(*) as chat_matches,
                    AVG(a.similarity_score) as chat_similarity,
                    SUM(CASE WHEN a.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as chat_click_rate
                FROM chat_analytics a
                JOIN chat_interactions i ON a.interaction_id = i.id
                WHERE i.timestamp BETWEEN %s AND %s
                GROUP BY a.id
            ),
            SearchExperts AS (
                SELECT 
                    expert_id,
                    COUNT(*) as search_matches,
                    AVG(rank_position) as avg_rank,
                    SUM(CASE WHEN expert_searches.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as search_click_rate
                FROM expert_searches
                JOIN search_logs sl ON expert_searches.search_id = sl.id
                WHERE sl.timestamp BETWEEN %s AND %s
                GROUP BY expert_id
            )
            SELECT 
                e.first_name || ' ' || e.last_name as expert_name,
                e.unit,
                COALESCE(ce.chat_matches, 0) as chat_matches,
                COALESCE(ce.chat_similarity, 0) as chat_similarity,
                COALESCE(ce.chat_click_rate, 0) as chat_click_rate,
                COALESCE(se.search_matches, 0) as search_matches,
                COALESCE(se.avg_rank, 0) as search_avg_rank,
                COALESCE(se.search_click_rate, 0) as search_click_rate
            FROM experts_expert e
            LEFT JOIN ChatExperts ce ON e.id::text = ce.id
            LEFT JOIN SearchExperts se ON e.id::text = se.expert_id
            WHERE e.is_active = true
            ORDER BY (COALESCE(ce.chat_matches, 0) + COALESCE(se.search_matches, 0)) DESC
            LIMIT %s
        """, (start_date, end_date, start_date, end_date, expert_count))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

def display_expert_analytics(expert_metrics, filters):
    """
    Display enhanced expert analytics visualizations.
    
    Parameters:
    - expert_metrics (pandas.DataFrame): A DataFrame containing the expert metrics data.
    - filters (dict): Filters applied to the analytics
    """
    st.subheader("Expert Analytics")

    # Key metrics summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Experts", f"{len(expert_metrics):,}")
    with col2:
        st.metric("Total Chat Matches", f"{expert_metrics['chat_matches'].sum():,}")
    with col3:
        st.metric("Total Search Matches", f"{expert_metrics['search_matches'].sum():,}")
    with col4:
        st.metric("Avg Chat Similarity", f"{expert_metrics['chat_similarity'].mean():.2f}")
    
    # Create multi-panel visualization with more space
    fig = make_subplots(
        rows=2, 
        cols=2, 
        subplot_titles=(
            "Expert Performance Matrix", 
            "Top Experts by Chat Matches", 
            "Expert Similarity Distribution", 
            "Search vs Chat Engagement"
        ),
        vertical_spacing=0.15,  # Increased vertical spacing
        horizontal_spacing=0.1  # Increased horizontal spacing
    )

    # 1. Expert Performance Heatmap (Top Left)
    performance_matrix = go.Heatmap(
        z=[
            expert_metrics.chat_similarity,
            expert_metrics.chat_click_rate,
            expert_metrics.search_click_rate
        ],
        x=expert_metrics.expert_name,
        y=['Similarity Score', 'Chat CTR', 'Search CTR'],
        colorscale='Viridis',
        name='Performance Matrix'
    )
    fig.add_trace(performance_matrix, row=1, col=1)

    # 2. Top Experts by Chat Matches (Top Right)
    top_experts = expert_metrics.nlargest(10, 'chat_matches')
    top_experts_bar = go.Bar(
        x=top_experts['expert_name'], 
        y=top_experts['chat_matches'], 
        name='Chat Matches',
        text=top_experts['chat_matches'].round(2),
        textposition='auto',
        orientation='v'  # Vertical orientation
    )
    fig.add_trace(top_experts_bar, row=1, col=2)

    # 3. Expert Similarity Distribution (Bottom Left)
    similarity_hist = go.Histogram(
        x=expert_metrics['chat_similarity'], 
        name='Chat Similarity Distribution',
        nbinsx=20
    )
    fig.add_trace(similarity_hist, row=2, col=1)

    # 4. Search vs Chat Engagement (Bottom Right)
    scatter = go.Scatter(
        x=expert_metrics['chat_matches'], 
        y=expert_metrics['search_matches'], 
        mode='markers',
        name='Expert Engagement',
        text=expert_metrics['expert_name'],
        hoverinfo='text+x+y',
        marker=dict(
            size=10,
            color=expert_metrics['chat_similarity'],  # Color by similarity
            colorscale='Viridis',
            showscale=True
        )
    )
    fig.add_trace(scatter, row=2, col=2)

    # Update layout for better readability
    fig.update_layout(
        height=900,  # Increased height
        width=1200,  # Added width to prevent squeezing
        title_text="Comprehensive Expert Analytics",
        showlegend=True,
        legend_orientation="h",
        legend=dict(y=1.1, x=0.5, xanchor='center'),
        margin=dict(l=50, r=50, t=100, b=50)  # Adjusted margins
    )

    # Update axis labels
    fig.update_xaxes(title_text="Experts", row=1, col=1, tickangle=-45)
    fig.update_xaxes(title_text="Experts", row=1, col=2, tickangle=-45)
    fig.update_xaxes(title_text="Chat Similarity", row=2, col=1)
    fig.update_xaxes(title_text="Chat Matches", row=2, col=2)
    
    fig.update_yaxes(title_text="Metrics", row=1, col=1)
    fig.update_yaxes(title_text="Frequency", row=2, col=1)
    fig.update_yaxes(title_text="Search Matches", row=2, col=2)

    # Display the comprehensive visualization
    st.plotly_chart(fig, use_container_width=True)

    # Expert Details Table
    st.subheader("Expert Performance Details")
    display_columns = [
        'expert_name', 'unit', 'chat_matches', 'search_matches',
        'chat_click_rate', 'search_click_rate', 'chat_similarity'
    ]
    
    # Create a styled DataFrame
    expert_display = expert_metrics[display_columns].sort_values('chat_matches', ascending=False)
    st.dataframe(
        expert_display.style.background_gradient(
            subset=['chat_matches', 'search_matches', 'chat_click_rate', 'search_click_rate'], 
            cmap='YlGnBu'
        ),
        use_container_width=True
    )

    # Insights and Analysis
    with st.expander("Expert Analytics Insights"):
        # Calculate and display key insights
        insights = []
        
        # Top Performing Experts
        top_chat_expert = expert_metrics.loc[expert_metrics['chat_matches'].idxmax(), 'expert_name']
        insights.append(f"Top Chat Expert: {top_chat_expert}")
        
        # Similarity Score Analysis
        avg_similarity = expert_metrics['chat_similarity'].mean()
        similarity_std = expert_metrics['chat_similarity'].std()
        insights.append(f"Average Chat Similarity: {avg_similarity:.2f} ± {similarity_std:.2f}")
        
        # Engagement Analysis
        engagement_correlation = expert_metrics['chat_matches'].corr(expert_metrics['search_matches'])
        insights.append(f"Chat vs Search Matches Correlation: {engagement_correlation:.2f}")
        
        # Display insights
        for insight in insights:
            st.write(f"• {insight}")

    # Warning for limited data
    if len(expert_metrics) < 5:
        st.warning("Limited expert data. More comprehensive insights will become available with more experts.")