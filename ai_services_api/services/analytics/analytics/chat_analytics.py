import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# View-based query functions

async def get_daily_metrics(conn, start_date, end_date):
    """Get metrics from daily_chat_metrics view."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM daily_chat_metrics 
            WHERE date BETWEEN %s AND %s
            ORDER BY date
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_intent_performance(conn, start_date, end_date):
    """Get metrics from intent_performance_metrics view."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM intent_performance_metrics 
            WHERE DATE(timestamp) BETWEEN %s AND %s
            ORDER BY total_queries DESC
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_sentiment_trends(conn, start_date, end_date):
    """Get metrics from sentiment_analysis_metrics view."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM sentiment_analysis_metrics 
            WHERE date BETWEEN %s AND %s
            ORDER BY date
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

async def get_content_matching_stats(conn, start_date, end_date):
    """Get metrics from content_matching_metrics view."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM content_matching_metrics 
            WHERE DATE(timestamp) BETWEEN %s AND %s
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        cursor.close()

# Updated get_chat_metrics to use views
async def get_chat_metrics(conn, start_date, end_date):
    """
    Get comprehensive chat metrics using materialized views.
    """
    try:
        # Get metrics from each view
        daily_metrics = await get_daily_metrics(conn, start_date, end_date)
        intent_metrics = await get_intent_performance(conn, start_date, end_date)
        sentiment_metrics = await get_sentiment_trends(conn, start_date, end_date)
        content_metrics = await get_content_matching_stats(conn, start_date, end_date)
        
        # Combine metrics as needed
        metrics = daily_metrics.merge(
            sentiment_metrics,
            on='date',
            how='left'
        )
        
        # Add aggregated intent and content metrics
        metrics['intent_distribution'] = intent_metrics.to_dict('records')
        metrics['content_performance'] = content_metrics.to_dict('records')
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting chat metrics: {e}")
        raise

# Example of how to use in Streamlit app
async def display_chat_analytics_page():
    st.title("Chat Analytics Dashboard")
    
    # Date range selector
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", value=datetime.now())
    
    try:
        async with get_db_connection() as conn:
            # Get all metrics using views
            metrics = await get_chat_metrics(conn, start_date, end_date)
            
            # Display metrics using our display function
            display_chat_analytics(metrics, None)
            
            # Additional view-specific displays
            if st.checkbox("Show Intent Analysis"):
                st.subheader("Intent Performance")
                st.dataframe(metrics['intent_distribution'])
            
            if st.checkbox("Show Content Matching Analysis"):
                st.subheader("Content Matching Performance")
                st.dataframe(metrics['content_performance'])
                
    except Exception as e:
        st.error(f"Error loading analytics: {e}")

if __name__ == "__main__":
    asyncio.run(display_chat_analytics_page())
