import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_chat_metrics(conn, start_date, end_date):
    """
    Retrieve chat metrics including sentiment analysis from the database.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            WITH ChatMetrics AS (
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as total_interactions,
                    COUNT(DISTINCT session_id) as unique_sessions,
                    COUNT(DISTINCT user_id) as unique_users,
                    AVG(response_time) as avg_response_time,
                    SUM(CASE WHEN error_occurred THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as error_rate,
                    -- Fix sentiment metrics extraction
                    AVG(CASE 
                        WHEN metrics->>'sentiment_score' IS NOT NULL 
                        THEN CAST(metrics->>'sentiment_score' AS FLOAT) 
                    END) as avg_sentiment_score,
                    AVG(CASE 
                        WHEN metrics->'aspects'->>'satisfaction' IS NOT NULL 
                        THEN CAST(metrics->'aspects'->>'satisfaction' AS FLOAT) 
                    END) as avg_satisfaction,
                    AVG(CASE 
                        WHEN metrics->'aspects'->>'urgency' IS NOT NULL 
                        THEN CAST(metrics->'aspects'->>'urgency' AS FLOAT) 
                    END) as avg_urgency,
                    AVG(CASE 
                        WHEN metrics->'aspects'->>'clarity' IS NOT NULL 
                        THEN CAST(metrics->'aspects'->>'clarity' AS FLOAT) 
                    END) as avg_clarity,
                    -- Add emotion aggregation
                    array_agg(DISTINCT 
                        CASE 
                            WHEN metrics->'emotion_labels' IS NOT NULL 
                            THEN metrics->'emotion_labels'->0 
                        END
                    ) FILTER (WHERE metrics->'emotion_labels' IS NOT NULL) as emotions
                FROM chat_interactions
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY DATE(timestamp)
            ),
            ExpertMatchMetrics AS (
                SELECT 
                    DATE(i.timestamp) as date,
                    COUNT(*) as total_matches,
                    AVG(a.similarity_score) as avg_similarity,
                    SUM(CASE WHEN a.clicked THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as click_rate
                FROM chat_analytics a
                JOIN chat_interactions i ON a.interaction_id = i.id
                WHERE i.timestamp BETWEEN %s AND %s
                GROUP BY DATE(i.timestamp)
            )
            SELECT 
                cm.*,
                em.total_matches,
                em.avg_similarity,
                em.click_rate
            FROM ChatMetrics cm
            LEFT JOIN ExpertMatchMetrics em ON cm.date = em.date
            ORDER BY cm.date
        """, (start_date, end_date, start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Convert NaN to 0 for better display
        sentiment_cols = ['avg_sentiment_score', 'avg_satisfaction', 'avg_urgency', 'avg_clarity']
        df[sentiment_cols] = df[sentiment_cols].fillna(0)
        
        return df
    finally:
        cursor.close()

def display_chat_analytics(chat_metrics, filters):
    """
    Display chat analytics with sentiment analysis visualizations.
    """
    st.subheader("Chat Analytics")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Total Interactions", 
            f"{int(chat_metrics['total_interactions'].sum()):,}"
        )
    with col2:
        sentiment = chat_metrics['avg_sentiment_score'].mean()
        st.metric(
            "Avg Sentiment Score", 
            f"{sentiment:.2f}",
            delta=None if pd.isna(sentiment) else f"{sentiment:+.2f}"
        )
    with col3:
        satisfaction = chat_metrics['avg_satisfaction'].mean()
        st.metric(
            "Avg Satisfaction", 
            "N/A" if pd.isna(satisfaction) else f"{satisfaction:.1%}"
        )
    with col4:
        error_rate = chat_metrics['error_rate'].mean()
        st.metric(
            "Error Rate", 
            f"{error_rate:.1%}",
            delta=f"{-error_rate:.1%}" if error_rate > 0 else None,
            delta_color="inverse"
        )

    # Chat Volume and Sentiment Trends
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add chat volume
    fig.add_trace(
        go.Scatter(
            x=chat_metrics['date'],
            y=chat_metrics['total_interactions'],
            name="Chat Volume",
            line=dict(color='blue'),
            hovertemplate="<b>Date:</b> %{x}<br>" +
                         "<b>Interactions:</b> %{y}<br><extra></extra>"
        ),
        secondary_y=False
    )
    
    # Add sentiment score
    fig.add_trace(
        go.Scatter(
            x=chat_metrics['date'],
            y=chat_metrics['avg_sentiment_score'],
            name="Sentiment Score",
            line=dict(color='red'),
            hovertemplate="<b>Date:</b> %{x}<br>" +
                         "<b>Sentiment:</b> %{y:.2f}<br><extra></extra>"
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title="Chat Volume and Sentiment Trends",
        xaxis_title="Date",
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig.update_yaxes(title_text="Number of Interactions", secondary_y=False)
    fig.update_yaxes(title_text="Average Sentiment Score", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)

    # Sentiment Aspects Analysis
    st.subheader("Sentiment Aspects Analysis")
    
    fig_aspects = go.Figure()
    aspects = ['avg_satisfaction', 'avg_urgency', 'avg_clarity']
    aspect_names = ['Satisfaction', 'Urgency', 'Clarity']
    colors = ['#2ecc71', '#e74c3c', '#3498db']
    
    for aspect, name, color in zip(aspects, aspect_names, colors):
        fig_aspects.add_trace(
            go.Scatter(
                x=chat_metrics['date'],
                y=chat_metrics[aspect],
                name=name,
                mode='lines+markers',
                line=dict(color=color),
                hovertemplate="<b>Date:</b> %{x}<br>" +
                             f"<b>{name}:</b> " + "%{y:.2f}<br><extra></extra>"
            )
        )
    
    fig_aspects.update_layout(
        title="Sentiment Aspects Over Time",
        xaxis_title="Date",
        yaxis_title="Score",
        hovermode='x unified',
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig_aspects, use_container_width=True)

def display_chat_analytics(chat_metrics, filters):
    """
    Display chat analytics with sentiment analysis visualizations.
    """
    st.subheader("Chat Analytics")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Total Interactions", 
            f"{int(chat_metrics['total_interactions'].sum()):,}"
        )
    with col2:
        st.metric(
            "Avg Sentiment Score", 
            f"{chat_metrics['avg_sentiment_score'].mean():.2f}"
        )
    with col3:
        st.metric(
            "Avg Satisfaction", 
            f"{chat_metrics['avg_satisfaction'].mean():.2%}"
        )
    with col4:
        st.metric(
            "Error Rate", 
            f"{chat_metrics['error_rate'].mean():.2%}"
        )

    # Chat Volume and Sentiment Trends
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add chat volume
    fig.add_trace(
        go.Scatter(
            x=chat_metrics['date'],
            y=chat_metrics['total_interactions'],
            name="Chat Volume",
            line=dict(color='blue')
        ),
        secondary_y=False
    )
    
    # Add sentiment score
    fig.add_trace(
        go.Scatter(
            x=chat_metrics['date'],
            y=chat_metrics['avg_sentiment_score'],
            name="Sentiment Score",
            line=dict(color='red')
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title="Chat Volume and Sentiment Trends",
        xaxis_title="Date",
        hovermode='x unified'
    )
    fig.update_yaxes(title_text="Number of Interactions", secondary_y=False)
    fig.update_yaxes(title_text="Average Sentiment Score", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)

    # Sentiment Aspects Analysis
    st.subheader("Sentiment Aspects Analysis")
    
    fig_aspects = go.Figure()
    aspects = ['avg_satisfaction', 'avg_urgency', 'avg_clarity']
    aspect_names = ['Satisfaction', 'Urgency', 'Clarity']
    
    for aspect, name in zip(aspects, aspect_names):
        fig_aspects.add_trace(
            go.Scatter(
                x=chat_metrics['date'],
                y=chat_metrics[aspect],
                name=name,
                mode='lines+markers'
            )
        )
    
    fig_aspects.update_layout(
        title="Sentiment Aspects Over Time",
        xaxis_title="Date",
        yaxis_title="Score",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_aspects, use_container_width=True)

    # Response Time and Error Analysis
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            px.line(
                chat_metrics,
                x="date",
                y="avg_response_time",
                title="Average Response Time",
                labels={"avg_response_time": "Response Time (s)"}
            ),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            px.line(
                chat_metrics,
                x="date",
                y="error_rate",
                title="Error Rate Trend",
                labels={"error_rate": "Error Rate"}
            ),
            use_container_width=True
        )

    # Add correlation analysis
    if st.checkbox("Show Correlation Analysis"):
        correlation_cols = [
            'avg_sentiment_score', 'avg_satisfaction', 
            'avg_urgency', 'avg_clarity', 'error_rate', 
            'avg_response_time'
        ]
        
        corr_matrix = chat_metrics[correlation_cols].corr()
        
        fig_corr = px.imshow(
            corr_matrix,
            labels=dict(color="Correlation"),
            color_continuous_scale="RdBu"
        )
        
        fig_corr.update_layout(title="Metric Correlations")
        st.plotly_chart(fig_corr, use_container_width=True)