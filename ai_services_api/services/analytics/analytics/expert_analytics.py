import pandas as pd
import plotly.graph_objects as go
import streamlit as st

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

def display_expert_analytics(expert_metrics,filters):
    """
    Display expert analytics visualizations using the provided expert metrics DataFrame.

    This function creates various visualizations to analyze and compare expert performance, including:
    1. Expert performance matrix heatmap showing similarity score, chat click-through rate (CTR), and search CTR.
    2. Expert metrics table displaying expert name, unit, chat matches, search matches, chat CTR, and search CTR.

    The visualizations are created using Plotly Graph Objects and displayed using Streamlit's `st.plotly_chart` 
    and `st.dataframe` functions.

    Parameters:
    - expert_metrics (pandas.DataFrame): A DataFrame containing the expert metrics data.
    """
    st.subheader("Expert Analytics")
    
    # Expert performance heatmap
    fig = go.Figure(data=go.Heatmap(
        z=[
            expert_metrics.chat_similarity,
            expert_metrics.chat_click_rate,
            expert_metrics.search_click_rate
        ],
        x=expert_metrics.expert_name,
        y=['Similarity Score', 'Chat CTR', 'Search CTR'],
        colorscale='Viridis'
    ))
    fig.update_layout(title='Expert Performance Matrix')
    st.plotly_chart(fig)
    
    # Expert metrics table
    st.dataframe(
        expert_metrics[[
            'expert_name', 'unit', 'chat_matches', 'search_matches',
            'chat_click_rate', 'search_click_rate'
        ]].sort_values('chat_matches', ascending=False)
    )
