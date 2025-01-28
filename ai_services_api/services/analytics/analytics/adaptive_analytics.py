import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np

TEST_USER_ID = "test_user_123"  # Constant for test user

def safe_format_number(value) -> str:
    """Safely format numbers handling None and NaN values"""
    try:
        if pd.isna(value) or value is None:  # More comprehensive check
            return "0"
        return f"{int(float(value)):,}"  # Added float conversion for flexibility
    except (ValueError, TypeError):
        return "0"

def safe_format_percentage(value) -> str:
    """Safely format percentage handling None and NaN values"""
    try:
        if pd.isna(value) or value is None:  # More comprehensive check
            return "0.00%"
        return f"{float(value):.2%}"
    except (ValueError, TypeError):
        return "0.00%"

def get_adaptive_metrics(conn, start_date, end_date):
    """
    Retrieve adaptive recommendation metrics from the database for the specified date range.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                DATE(created_at) as date,
                COALESCE(COUNT(*), 0) as total_recommendations,
                COALESCE(COUNT(DISTINCT user_id), 0) as unique_users,
                COALESCE(COUNT(DISTINCT expert_id), 0) as unique_requesters,
                COALESCE(COUNT(DISTINCT matched_expert_id), 0) as unique_experts_recommended,
                COALESCE(AVG(CASE WHEN successful = true THEN 1.0 ELSE 0.0 END), 0.0) as success_rate,
                COALESCE(AVG(similarity_score), 0.0) as avg_similarity_score,
                COALESCE(AVG(shared_fields), 0.0) as avg_shared_fields,
                COALESCE(AVG(shared_skills), 0.0) as avg_shared_skills
            FROM expert_matching_logs
            WHERE created_at BETWEEN %s AND %s
            AND user_id != %s  -- Exclude test user
            GROUP BY DATE(created_at)
            ORDER BY date
        """, (start_date, end_date, TEST_USER_ID))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        if not data:
            return pd.DataFrame(columns=columns)
        
        df = pd.DataFrame(data, columns=columns)
        
        # Ensure numeric types
        numeric_columns = ['total_recommendations', 'unique_users', 'unique_requesters',
                         'unique_experts_recommended', 'success_rate', 'avg_similarity_score',
                         'avg_shared_fields', 'avg_shared_skills']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching adaptive metrics: {str(e)}")
        return pd.DataFrame()
    finally:
        cursor.close()

def display_adaptive_analytics(metrics, filters=None):
    """
    Display adaptive recommendation analytics visualizations.
    """
    try:
        st.subheader("Adaptive Recommendation Analytics")
        
        if metrics is None or metrics.empty:
            st.info("No recommendation data available for the selected period")
            return

        # Metrics calculations
        total_recs = metrics['total_recommendations'].sum()
        avg_success = metrics['success_rate'].mean()
        unique_experts = metrics['unique_experts_recommended'].sum()
        avg_shared_fields = metrics['avg_shared_fields'].mean()
        avg_shared_skills = metrics['avg_shared_skills'].mean()

        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Recommendations", safe_format_number(total_recs))
        with col2:
            st.metric("Average Success Rate", safe_format_percentage(avg_success))
        with col3:
            st.metric("Unique Experts Recommended", safe_format_number(unique_experts))

        col4, col5 = st.columns(2)
        with col4:
            st.metric("Avg Shared Fields", f"{avg_shared_fields:.1f}")
        with col5:
            st.metric("Avg Shared Skills", f"{avg_shared_skills:.1f}")

        # Create visualizations
        def create_safe_line_plot(df, x_col, y_cols, title, labels=None):
            try:
                existing_cols = [col for col in y_cols if col in df.columns and df[col].notna().any()]
                
                if not existing_cols:
                    st.warning(f"No data available for {title}")
                    return None
                
                fig = px.line(
                    df,
                    x=x_col,
                    y=existing_cols,
                    title=title,
                    labels=labels or {}
                )
                return fig
            except Exception as plot_error:
                st.error(f"Error creating {title} plot: {plot_error}")
                return None

        # Daily volume chart
        volume_fig = create_safe_line_plot(
            metrics, 
            x_col="date", 
            y_cols=["total_recommendations", "unique_users", "unique_requesters"],
            title="Daily Activity Volume",
            labels={"value": "Count", "variable": "Metric"}
        )
        if volume_fig:
            st.plotly_chart(volume_fig)

        # Success and similarity metrics
        metrics_fig = create_safe_line_plot(
            metrics,
            x_col="date", 
            y_cols=["success_rate", "avg_similarity_score"],
            title="Success Rate and Similarity Score Trends",
            labels={"value": "Score", "variable": "Metric"}
        )
        if metrics_fig:
            metrics_fig.update_layout(yaxis_tickformat='.2%')
            st.plotly_chart(metrics_fig)

        # Shared attributes trends
        shared_fig = create_safe_line_plot(
            metrics,
            x_col="date", 
            y_cols=["avg_shared_fields", "avg_shared_skills"],
            title="Average Shared Attributes Trend",
            labels={"value": "Count", "variable": "Attribute"}
        )
        if shared_fig:
            st.plotly_chart(shared_fig)

    except Exception as e:
        st.error(f"Error displaying adaptive analytics: {str(e)}")
        import traceback
        st.error(traceback.format_exc())