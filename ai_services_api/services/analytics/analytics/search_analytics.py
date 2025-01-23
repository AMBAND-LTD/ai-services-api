import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import logging
from typing import Dict

def get_search_metrics(conn, start_date, end_date):
   cursor = conn.cursor()
   try:
       cursor.execute("""
           WITH SearchStats AS (
               SELECT 
                   DATE(i.timestamp) as date,
                   COUNT(*) as total_searches,
                   COUNT(DISTINCT i.user_id) as unique_users,
                   AVG(CAST(i.metrics->>'response_time' AS FLOAT)) as avg_response_time,
                   COUNT(CASE WHEN em.id IS NOT NULL THEN 1 END)::FLOAT / 
                       NULLIF(COUNT(*), 0) as click_through_rate
               FROM interactions i
               LEFT JOIN expert_messages em ON em.id = i.id
               WHERE i.timestamp BETWEEN %s AND %s
               GROUP BY DATE(i.timestamp)
           ),
           TopSearches AS (
               SELECT 
                   i.user_id,
                   COUNT(*) as search_count,
                   AVG(CAST(i.metrics->>'response_time' AS FLOAT)) as avg_response,
                   COUNT(CASE WHEN em.id IS NOT NULL THEN 1 END)::FLOAT / 
                       NULLIF(COUNT(*), 0) * 100 as conversion_rate
               FROM interactions i
               LEFT JOIN expert_messages em ON em.id = i.id
               WHERE i.timestamp BETWEEN %s AND %s
               GROUP BY i.user_id
               ORDER BY COUNT(*) DESC
               LIMIT 10
           ),
           ErrorMetrics AS (
               SELECT
                   DATE(timestamp) as date, 
                   COUNT(CASE WHEN CAST(metrics->>'error_occurred' AS BOOLEAN) THEN 1 END)::FLOAT / 
                       NULLIF(COUNT(*), 0) * 100 as error_rate
               FROM interactions
               WHERE timestamp BETWEEN %s AND %s
               GROUP BY DATE(timestamp)
           )
           SELECT json_build_object(
               'search_metrics', COALESCE((SELECT json_agg(row_to_json(SearchStats)) FROM SearchStats), '[]'::json),
               'top_searches', COALESCE((SELECT json_agg(row_to_json(TopSearches)) FROM TopSearches), '[]'::json),
               'error_metrics', COALESCE((SELECT json_agg(row_to_json(ErrorMetrics)) FROM ErrorMetrics), '[]'::json)
           ) as metrics;
       """, (start_date, end_date) * 3)
       
       result = cursor.fetchone()[0]
       metrics = {}
       for key in ['search_metrics', 'top_searches', 'error_metrics']:
           try:
               df = pd.DataFrame(result.get(key, []))
               metrics[key] = df
           except Exception as e:
               logging.error(f"Error processing {key}: {str(e)}")
               metrics[key] = pd.DataFrame()
       
       return metrics
   finally:
       cursor.close()

def display_search_analytics(metrics: Dict[str, pd.DataFrame], filters: Dict = None):
   st.subheader("Search Analytics")

   search_data = metrics['search_metrics']
   if search_data.empty:
       st.warning("No search data available")
       return

   # Overview metrics
   col1, col2, col3, col4 = st.columns(4)
   with col1:
       st.metric("Total Searches", f"{search_data['total_searches'].sum():,}")
   with col2:
       st.metric("Unique Users", f"{search_data['unique_users'].sum():,}")
   with col3:
       st.metric("Avg Response", f"{search_data['avg_response_time'].mean():.2f}s")
   with col4:
       st.metric("CTR", f"{search_data['click_through_rate'].mean():.1%}")

   # Search trends
   fig = make_subplots(
       rows=2, cols=2,
       subplot_titles=(
           "Search Volume", 
           "Response Times",
           "Click-through Rate",
           "Error Rate"
       )
   )

   # Search volume
   fig.add_trace(
       go.Scatter(
           x=search_data['date'],
           y=search_data['total_searches'],
           name="Total Searches"
       ),
       row=1, col=1
   )
   fig.add_trace(
       go.Scatter(
           x=search_data['date'],
           y=search_data['unique_users'],
           name="Unique Users"
       ),
       row=1, col=1
   )

   # Response times
   fig.add_trace(
       go.Scatter(
           x=search_data['date'],
           y=search_data['avg_response_time'],
           name="Response Time"
       ),
       row=1, col=2
   )

   # Click-through rate
   fig.add_trace(
       go.Scatter(
           x=search_data['date'],
           y=search_data['click_through_rate'],
           name="CTR"
       ),
       row=2, col=1
   )

   # Error rate
   if 'error_metrics' in metrics and not metrics['error_metrics'].empty:
       fig.add_trace(
           go.Scatter(
               x=metrics['error_metrics']['date'],
               y=metrics['error_metrics']['error_rate'],
               name="Error Rate"
           ),
           row=2, col=2
       )

   fig.update_layout(height=800, showlegend=True)
   st.plotly_chart(fig, use_container_width=True)

   # Top users table
   if 'top_searches' in metrics and not metrics['top_searches'].empty:
       st.subheader("Top Users")
       st.dataframe(
           metrics['top_searches'].style.background_gradient(
               subset=['search_count', 'conversion_rate'],
               cmap='Blues'
           )
       )