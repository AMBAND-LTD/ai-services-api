import pandas as pd
import plotly.express as px
import streamlit as st
import numpy as np

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
                COALESCE(COUNT(DISTINCT sender_id), 0) as unique_users,
                COALESCE(AVG(CASE WHEN success = true THEN 1.0 ELSE 0.0 END), 0.0) as success_rate,
                COALESCE(AVG(CAST(NULLIF(metadata->>'similarity_score', '') AS FLOAT)), 0.0) as avg_similarity_score,
                COALESCE(COUNT(DISTINCT receiver_id), 0) as unique_experts_recommended
            FROM expert_interactions
            WHERE created_at BETWEEN %s AND %s
            AND interaction_type = 'recommendation_shown'
            GROUP BY DATE(created_at)
            ORDER BY date
        """, (start_date, end_date))
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        # If no data, return an empty DataFrame with correct columns
        if not data:
            return pd.DataFrame(columns=columns)
        
        df = pd.DataFrame(data, columns=columns)
        
        # Ensure numeric types
        numeric_columns = ['total_recommendations', 'unique_users', 'success_rate', 'avg_similarity_score', 'unique_experts_recommended']
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

        # Defensive programming for metric calculations
        total_recs = metrics['total_recommendations'].sum() if 'total_recommendations' in metrics.columns else 0
        avg_success = metrics['success_rate'].mean() if 'success_rate' in metrics.columns else 0
        unique_experts = metrics['unique_experts_recommended'].sum() if 'unique_experts_recommended' in metrics.columns else 0

        # Show overall metrics with safe formatting
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Total Recommendations", 
                safe_format_number(total_recs)
            )
        
        with col2:
            st.metric(
                "Average Success Rate", 
                safe_format_percentage(avg_success)
            )
        
        with col3:
            st.metric(
                "Unique Experts Recommended", 
                safe_format_number(unique_experts)
            )
        
        # Defensive visualization creation
        def create_safe_line_plot(df, x_col, y_cols, title, labels=None):
            """Create a safe line plot with error handling"""
            try:
                # Ensure columns exist and have data
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
            y_cols=["total_recommendations", "unique_users"],
            title="Daily Recommendation Volume",
            labels={"value": "Count", "variable": "Metric"}
        )
        if volume_fig:
            st.plotly_chart(volume_fig)
        
        # Success rate trend
        success_fig = create_safe_line_plot(
            metrics,
            x_col="date", 
            y_cols=["success_rate"],
            title="Recommendation Success Rate",
            labels={"success_rate": "Success Rate"}
        )
        if success_fig:
            success_fig.update_layout(yaxis_tickformat='.2%')
            st.plotly_chart(success_fig)
        
        # Similarity score trend
        similarity_fig = create_safe_line_plot(
            metrics,
            x_col="date", 
            y_cols=["avg_similarity_score"],
            title="Average Similarity Score Trend",
            labels={"avg_similarity_score": "Similarity Score"}
        )
        if similarity_fig:
            st.plotly_chart(similarity_fig)

    except Exception as e:
        st.error(f"Error displaying adaptive analytics: {str(e)}")
        import traceback
        st.error(traceback.format_exc())