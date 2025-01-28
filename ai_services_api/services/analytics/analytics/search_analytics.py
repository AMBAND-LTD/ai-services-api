import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import logging
from typing import Dict

def get_search_metrics(conn, start_date, end_date):
    """Get comprehensive search metrics using our views"""
    cursor = conn.cursor()
    try:
        # Get daily metrics
        cursor.execute("""
            SELECT * FROM daily_search_metrics 
            WHERE date BETWEEN %s AND %s
        """, (start_date, end_date))
        
        daily_metrics = pd.DataFrame(cursor.fetchall(), columns=[
            'date', 'total_searches', 'unique_users', 'avg_response_time',
            'success_rate', 'avg_results', 'total_sessions'
        ])
        
        # Get session analytics
        cursor.execute("""
            SELECT * FROM session_analytics
            WHERE date BETWEEN %s AND %s
        """, (start_date, end_date))
        
        session_metrics = pd.DataFrame(cursor.fetchall(), columns=[
            'date', 'total_sessions', 'avg_queries_per_session',
            'session_success_rate', 'avg_session_duration'
        ])
        
        # Get top expert matches
        cursor.execute("""
            SELECT 
                e.expert_id,
                COUNT(*) as match_count,
                AVG(e.similarity_score) as avg_similarity,
                SUM(CASE WHEN e.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_rate
            FROM expert_search_matches e
            JOIN search_analytics sa ON e.search_id = sa.search_id
            WHERE sa.timestamp BETWEEN %s AND %s
            GROUP BY e.expert_id
            ORDER BY match_count DESC
            LIMIT 10
        """, (start_date, end_date))
        
        expert_metrics = pd.DataFrame(cursor.fetchall(), columns=[
            'expert_id', 'match_count', 'avg_similarity', 'click_rate'
        ])
        
        # Get domain performance
        cursor.execute("""
            SELECT * FROM domain_performance_metrics
            ORDER BY match_count DESC
            LIMIT 10
        """)
        
        domain_metrics = pd.DataFrame(cursor.fetchall(), columns=[
            'domain_name', 'match_count', 'total_clicks', 'avg_similarity_score',
            'last_matched_at', 'popularity_rank', 'click_rate'
        ])
        
        return {
            'daily_metrics': daily_metrics,
            'session_metrics': session_metrics,
            'expert_metrics': expert_metrics,
            'domain_metrics': domain_metrics
        }
    finally:
        cursor.close()

def display_search_analytics(metrics: Dict[str, pd.DataFrame], filters: Dict = None):
    """Display search analytics using updated metrics"""
    st.subheader("Search Analytics Dashboard")

    daily_data = metrics['daily_metrics']
    if daily_data.empty:
        st.warning("No search data available")
        return

    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Total Searches", 
            f"{daily_data['total_searches'].sum():,}"
        )
    with col2:
        st.metric(
            "Unique Users", 
            f"{daily_data['unique_users'].sum():,}"
        )
    with col3:
        st.metric(
            "Success Rate", 
            f"{daily_data['success_rate'].mean():.1%}"
        )
    with col4:
        st.metric(
            "Avg Response", 
            f"{daily_data['avg_response_time'].mean():.2f}s"
        )

    # Main dashboard
    st.subheader("Search Trends")
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Search Volume",
            "Response & Success Metrics",
            "Session Analytics",
            "Domain Performance"
        ),
        specs=[[{"secondary_y": True}, {"secondary_y": True}],
               [{"secondary_y": True}, {"secondary_y": False}]]
    )

    # 1. Search Volume Plot
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['total_searches'],
            name="Total Searches",
            line=dict(color='blue')
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['unique_users'],
            name="Unique Users",
            line=dict(color='green')
        ),
        row=1, col=1,
        secondary_y=True
    )

    # 2. Response Time and Success Rate
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['avg_response_time'],
            name="Response Time",
            line=dict(color='orange')
        ),
        row=1, col=2
    )
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['success_rate'],
            name="Success Rate",
            line=dict(color='purple')
        ),
        row=1, col=2,
        secondary_y=True
    )

    # 3. Session Analytics
    session_data = metrics['session_metrics']
    fig.add_trace(
        go.Scatter(
            x=session_data['date'],
            y=session_data['avg_queries_per_session'],
            name="Queries/Session",
            line=dict(color='red')
        ),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=session_data['date'],
            y=session_data['session_success_rate'],
            name="Session Success",
            line=dict(color='cyan')
        ),
        row=2, col=1,
        secondary_y=True
    )

    # 4. Domain Performance
    domain_data = metrics['domain_metrics']
    fig.add_trace(
        go.Bar(
            x=domain_data['domain_name'][:5],
            y=domain_data['match_count'][:5],
            name="Top Domains",
            marker_color='lightblue'
        ),
        row=2, col=2
    )

    # Update layout
    fig.update_layout(
        height=800,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    st.plotly_chart(fig, use_container_width=True)

    # Expert Performance Table
    st.subheader("Top Performing Experts")
    expert_data = metrics['expert_metrics']
    if not expert_data.empty:
        styled_expert_data = expert_data.style.format({
            'avg_similarity': '{:.2%}',
            'click_rate': '{:.2%}'
        }).background_gradient(
            subset=['match_count', 'click_rate'],
            cmap='Blues'
        )
        st.dataframe(styled_expert_data)

    # Domain Analytics Table
    st.subheader("Domain Performance")
    domain_data = metrics['domain_metrics']
    if not domain_data.empty:
        styled_domain_data = domain_data.style.format({
            'avg_similarity_score': '{:.2%}',
            'click_rate': '{:.2%}'
        }).background_gradient(
            subset=['match_count', 'total_clicks'],
            cmap='Greens'
        )
        st.dataframe(styled_domain_data)

    # Add date/time of last update
    st.markdown(f"*Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}*")