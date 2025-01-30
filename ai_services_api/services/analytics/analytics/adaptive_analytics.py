import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import numpy as np
import logging
from typing import Dict, Optional, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('adaptive_analytics')

TEST_USER_ID = "test_user_123"

def safe_format_number(value) -> str:
    """Safely format numbers handling None and NaN values"""
    try:
        if pd.isna(value) or value is None:
            return "0"
        return f"{int(float(value)):,}"
    except (ValueError, TypeError):
        return "0"

def safe_format_percentage(value) -> str:
    """Safely format percentage handling None and NaN values"""
    try:
        if pd.isna(value) or value is None:
            return "0.00%"
        return f"{float(value):.2%}"
    except (ValueError, TypeError):
        return "0.00%"

def get_adaptive_metrics(conn, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
    """Get comprehensive adaptive recommendation metrics with improved error handling"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            WITH DailyMetrics AS (
                SELECT 
                    DATE(created_at) as date,
                    COUNT(*) as total_recommendations,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT expert_id) as unique_requesters,
                    COUNT(DISTINCT matched_expert_id) as unique_experts_recommended,
                    COALESCE(
                        SUM(CASE WHEN successful THEN 1 ELSE 0 END)::float / 
                        NULLIF(COUNT(*), 0),
                        0
                    ) as success_rate,
                    COALESCE(AVG(NULLIF(similarity_score, 0)), 0) as avg_similarity_score,
                    COALESCE(AVG(NULLIF(array_length(shared_domains, 1), 0)), 0) as avg_shared_domains,
                    COALESCE(AVG(NULLIF(shared_fields, 0)), 0) as avg_shared_fields,
                    COALESCE(AVG(NULLIF(shared_skills, 0)), 0) as avg_shared_skills
                FROM expert_matching_logs
                WHERE created_at BETWEEN %s AND %s
                    AND user_id != %s
                GROUP BY DATE(created_at)
                ORDER BY date
            ),
            DomainPerformance AS (
                SELECT 
                    d.domain,
                    COUNT(*) as match_count,
                    COALESCE(
                        SUM(CASE WHEN l.successful THEN 1 ELSE 0 END)::float / 
                        NULLIF(COUNT(*), 0),
                        0
                    ) as domain_success_rate,
                    COALESCE(AVG(NULLIF(l.similarity_score, 0)), 0) as domain_avg_similarity
                FROM expert_matching_logs l
                CROSS JOIN LATERAL (
                    SELECT CASE 
                        WHEN shared_domains IS NULL OR array_length(shared_domains, 1) = 0 
                        THEN ARRAY['Unknown']
                        ELSE shared_domains
                    END AS domains
                ) d1
                CROSS JOIN LATERAL unnest(d1.domains) AS d(domain)
                WHERE l.created_at BETWEEN %s AND %s
                    AND l.user_id != %s
                GROUP BY d.domain
                HAVING COUNT(*) > 0
                ORDER BY match_count DESC
                LIMIT 10
            ),
            ExpertPerformance AS (
                SELECT 
                    COALESCE(e.unit, 'Unknown') as unit,
                    COUNT(*) as match_count,
                    COUNT(DISTINCT eml.user_id) as unique_users,
                    COALESCE(
                        SUM(CASE WHEN eml.successful THEN 1 ELSE 0 END)::float / 
                        NULLIF(COUNT(*), 0),
                        0
                    ) as success_rate,
                    COALESCE(AVG(NULLIF(eml.similarity_score, 0)), 0) as avg_similarity
                FROM expert_matching_logs eml
                LEFT JOIN experts_expert e ON eml.matched_expert_id = e.id::text
                WHERE eml.created_at BETWEEN %s AND %s
                    AND eml.user_id != %s
                GROUP BY e.unit
                HAVING COUNT(*) > 0
                ORDER BY match_count DESC
            ),
            HourlyPattern AS (
                SELECT 
                    EXTRACT(HOUR FROM created_at) as hour,
                    COUNT(*) as match_count,
                    COALESCE(
                        SUM(CASE WHEN successful THEN 1 ELSE 0 END)::float / 
                        NULLIF(COUNT(*), 0),
                        0
                    ) as hourly_success_rate
                FROM expert_matching_logs
                WHERE created_at BETWEEN %s AND %s
                    AND user_id != %s
                GROUP BY EXTRACT(HOUR FROM created_at)
                ORDER BY hour
            )
            SELECT json_build_object(
                'daily_metrics', COALESCE((SELECT json_agg(row_to_json(DailyMetrics)) FROM DailyMetrics), '[]'::json),
                'domain_performance', COALESCE((SELECT json_agg(row_to_json(DomainPerformance)) FROM DomainPerformance), '[]'::json),
                'expert_performance', COALESCE((SELECT json_agg(row_to_json(ExpertPerformance)) FROM ExpertPerformance), '[]'::json),
                'hourly_pattern', COALESCE((SELECT json_agg(row_to_json(HourlyPattern)) FROM HourlyPattern), '[]'::json)
            ) as metrics;
        """, (start_date, end_date, TEST_USER_ID) * 4)
        
        result = cursor.fetchone()[0]
        
        metrics = {}
        for key in ['daily_metrics', 'domain_performance', 'expert_performance', 'hourly_pattern']:
            try:
                df = pd.DataFrame(result.get(key, []))
                if not df.empty:
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    # Replace NaN with 0 for numeric columns
                    numeric_cols = df.select_dtypes(include=[np.number]).columns
                    df[numeric_cols] = df[numeric_cols].fillna(0)
                metrics[key] = df
            except Exception as e:
                logger.error(f"Error processing {key}: {str(e)}")
                metrics[key] = pd.DataFrame()
        
        return metrics
        
    finally:
        cursor.close()

def display_adaptive_analytics(metrics: Dict[str, pd.DataFrame], filters: Optional[Dict[str, Any]] = None):
    """Display comprehensive adaptive analytics dashboard with improved error handling"""
    st.title("Adaptive Recommendation Analytics")
    
    try:
        daily_data = metrics.get('daily_metrics', pd.DataFrame())
        domain_data = metrics.get('domain_performance', pd.DataFrame())
        expert_data = metrics.get('expert_performance', pd.DataFrame())
        hourly_data = metrics.get('hourly_pattern', pd.DataFrame())
        
        if daily_data.empty:
            st.info("No adaptive recommendation data available for the selected period")
            return
            
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_recs = daily_data['total_recommendations'].sum()
            st.metric("Total Recommendations", safe_format_number(total_recs))
            
        with col2:
            avg_success = daily_data['success_rate'].mean()
            st.metric("Success Rate", safe_format_percentage(avg_success))
            
        with col3:
            unique_experts = daily_data['unique_experts_recommended'].sum()
            st.metric("Unique Experts Recommended", safe_format_number(unique_experts))
            
        col4, col5 = st.columns(2)
        
        with col4:
            avg_fields = daily_data['avg_shared_fields'].mean()
            st.metric("Avg Shared Fields", f"{avg_fields:.1f}")
            
        with col5:
            avg_similarity = daily_data['avg_similarity_score'].mean()
            st.metric("Avg Similarity Score", f"{avg_similarity:.2f}")

        # Activity Analysis
        st.subheader("Activity Analysis")
        fig = px.line(
            daily_data,
            x='date',
            y=['total_recommendations', 'unique_users'],
            title="Daily Activity Trends",
            labels={'value': 'Count', 'variable': 'Metric'}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Success Rate Analysis
        st.subheader("Success Metrics")
        success_fig = px.line(
            daily_data,
            x='date',
            y=['success_rate', 'avg_similarity_score'],
            title="Success Rate and Similarity Score Trends",
            labels={'date': 'Date', 'value': 'Score', 'variable': 'Metric'}
        )
        success_fig.update_layout(yaxis_tickformat='.1%')
        st.plotly_chart(success_fig, use_container_width=True)

        # Domain Analysis
        if not domain_data.empty:
            st.subheader("Domain Performance")
            domain_fig = go.Figure()
            domain_fig.add_trace(go.Bar(
                name='Match Count',
                x=domain_data['domain'],
                y=domain_data['match_count'],
                yaxis='y'
            ))
            domain_fig.add_trace(go.Scatter(
                name='Success Rate',
                x=domain_data['domain'],
                y=domain_data['domain_success_rate'],
                yaxis='y2',
                mode='lines+markers'
            ))
            domain_fig.update_layout(
                title='Domain Performance Analysis',
                yaxis=dict(title='Match Count'),
                yaxis2=dict(title='Success Rate', overlaying='y', side='right', tickformat='.1%'),
                barmode='group'
            )
            st.plotly_chart(domain_fig, use_container_width=True)

        # Expert Unit Analysis
        if not expert_data.empty:
            st.subheader("Expert Unit Performance")
            unit_fig = go.Figure()
            unit_fig.add_trace(go.Bar(
                name='Match Count',
                x=expert_data['unit'],
                y=expert_data['match_count'],
                yaxis='y'
            ))
            unit_fig.add_trace(go.Scatter(
                name='Success Rate',
                x=expert_data['unit'],
                y=expert_data['success_rate'],
                yaxis='y2',
                mode='lines+markers'
            ))
            unit_fig.update_layout(
                title='Expert Unit Performance Analysis',
                yaxis=dict(title='Match Count'),
                yaxis2=dict(title='Success Rate', overlaying='y', side='right', tickformat='.1%'),
                barmode='group'
            )
            st.plotly_chart(unit_fig, use_container_width=True)

        # Hourly Pattern
        if not hourly_data.empty:
            st.subheader("Hourly Activity Pattern")
            hourly_fig = go.Figure()
            hourly_fig.add_trace(go.Bar(
                name='Match Count',
                x=hourly_data['hour'],
                y=hourly_data['match_count']
            ))
            hourly_fig.add_trace(go.Scatter(
                name='Success Rate',
                x=hourly_data['hour'],
                y=hourly_data['hourly_success_rate'],
                yaxis='y2',
                mode='lines+markers'
            ))
            hourly_fig.update_layout(
                title='Hourly Distribution',
                xaxis=dict(title='Hour of Day'),
                yaxis=dict(title='Match Count'),
                yaxis2=dict(title='Success Rate', overlaying='y', side='right', tickformat='.1%')
            )
            st.plotly_chart(hourly_fig, use_container_width=True)
        
        # Detailed Metrics Tables
        with st.expander("View Detailed Metrics"):
            tabs = st.tabs(["Daily Metrics", "Domain Performance", "Expert Performance", "Hourly Patterns"])
            
            with tabs[0]:
                if not daily_data.empty:
                    styled_daily = daily_data.style.format({
                        'success_rate': '{:.2%}',
                        'avg_similarity_score': '{:.2f}',
                        'avg_shared_fields': '{:.1f}',
                        'avg_shared_domains': '{:.1f}',
                        'avg_shared_skills': '{:.1f}'
                    })
                    st.dataframe(styled_daily)
            
            with tabs[1]:
                if not domain_data.empty:
                    styled_domain = domain_data.style.format({
                        'domain_success_rate': '{:.2%}',
                        'domain_avg_similarity': '{:.2f}'
                    })
                    st.dataframe(styled_domain)
            
            with tabs[2]:
                if not expert_data.empty:
                    styled_expert = expert_data.style.format({
                        'success_rate': '{:.2%}',
                        'avg_similarity': '{:.2f}'
                    })
                    st.dataframe(styled_expert)
                    
            with tabs[3]:
                if not hourly_data.empty:
                    styled_hourly = hourly_data.style.format({
                        'hourly_success_rate': '{:.2%}'
                    })
                    st.dataframe(styled_hourly)
                    
    except Exception as e:
        logger.error(f"Error displaying adaptive analytics: {str(e)}")
        st.error("An error occurred while displaying the analytics.")
        
    # Add timestamp
    st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")