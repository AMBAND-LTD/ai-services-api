from analytics.chat_analytics import get_chat_metrics, display_chat_analytics
from analytics.search_analytics import get_search_metrics, display_search_analytics
from analytics.expert_analytics import get_expert_metrics, display_expert_analytics
from analytics.overview_analytics import get_overview_metrics, display_overview_analytics
from analytics.content_analytics import get_content_metrics, display_content_analytics
from analytics.usage_analytics import get_usage_metrics, display_usage_analytics
from analytics.adaptive_analytics import get_adaptive_metrics, display_adaptive_analytics
from components.sidebar import create_sidebar_filters
from utils.db_utils import DatabaseConnector
from utils.logger import setup_logger
from utils.theme import toggle_theme, apply_theme, update_plot_theme
from datetime import datetime
import logging
import streamlit as st
class UnifiedAnalyticsDashboard:
    """
    Main dashboard class that integrates all analytics components and manages the application state.
    Features a dynamic sidebar with interactive navigation and contextual filters.
    """
    
    def __init__(self):
        """Initialize the dashboard with database connection and basic configuration."""
        try:
            self.logger = setup_logger(name="analytics_dashboard")
        except Exception as e:
            print(f"Warning: Logger initialization failed: {str(e)}")
            self.logger = logging.getLogger("analytics_dashboard")
            self.logger.setLevel(logging.INFO)
        
        try:
            self.db = DatabaseConnector()
            self.conn = self.db.get_connection()
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            st.error("Failed to connect to the database. Please check your connection settings.")
            return
        
        if 'theme' not in st.session_state:
            st.session_state.theme = 'light'

    def main(self):
        """Main application loop with enhanced sidebar integration."""
        try:
            st.set_page_config(
                page_title="APHRC Analytics Dashboard",
                layout="wide",
                initial_sidebar_state="expanded"
            )
            
            apply_theme()
            
            # Get filters and selected analytics type from enhanced sidebar
            start_date, end_date, analytics_type, filters = create_sidebar_filters()
            
            # Display header with selected analytics type
            self.display_header(analytics_type)
            
            # Display analytics content
            try:
                self.display_analytics(analytics_type, start_date, end_date, filters)
            except Exception as e:
                self.logger.error(f"Error displaying analytics: {str(e)}")
                st.error("An error occurred while displaying analytics. Please try again.")
            
            self.display_footer()
            
        except Exception as e:
            self.logger.error(f"Application error: {str(e)}")
            st.error("An unexpected error occurred. Please contact support if the issue persists.")

    def display_header(self, analytics_type):
        """Display the dashboard header with current analytics type."""
        title_color = "#FFFFFF" if st.session_state.theme == 'dark' else "#000000"
        st.markdown(
            f"""
            <h1 style="color: {title_color};">APHRC Analytics Dashboard</h1>
            <h3 style="color: {title_color};">{analytics_type} Analytics</h3>
            """,
            unsafe_allow_html=True
        )

    def display_analytics(self, analytics_type, start_date, end_date, filters):
        """Display analytics based on selected type and filters."""
        # Display overall metrics for context
        self.display_overall_metrics(start_date, end_date)
        
        # Display specific analytics based on selection
        analytics_map = {
            "Overview": (get_overview_metrics, display_overview_analytics),
            "Chat": (get_chat_metrics, display_chat_analytics),
            "Search": (get_search_metrics, display_search_analytics),
            "Expert": (get_expert_metrics, display_expert_analytics),
            "Content": (get_content_metrics, display_content_analytics),
            "Usage": (get_usage_metrics, display_usage_analytics),
            "Adaptive": (get_adaptive_metrics, display_adaptive_analytics)  # Add this line

        }
        
        if analytics_type in analytics_map:
            get_metrics, display_analytics = analytics_map[analytics_type]
            
            # Get metrics with appropriate filters
            if analytics_type == "Expert":
                metrics = get_metrics(
                    self.conn, 
                    start_date, 
                    end_date, 
                    filters.get('expert_count', 20)
                )
            else:
                metrics = get_metrics(self.conn, start_date, end_date)
            
            # Display analytics with filters applied
            display_analytics(metrics, filters)
            
            # Handle export if enabled
            if 'export_format' in filters:
                self.export_analytics(metrics, analytics_type, filters['export_format'])

    def display_overall_metrics(self, start_date, end_date):
        col1, col2, col3, col4, col5 = st.columns(5)  # 5 columns
    
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT 
                    COALESCE((SELECT COUNT(*) FROM chat_interactions 
                    WHERE timestamp BETWEEN %s AND %s), 0) as total_chat_interactions,
                    COALESCE((SELECT COUNT(*) FROM search_logs 
                    WHERE timestamp BETWEEN %s AND %s), 0) as total_searches,
                    COALESCE((SELECT COUNT(DISTINCT user_id) FROM (
                        SELECT user_id FROM chat_interactions 
                        WHERE timestamp BETWEEN %s AND %s
                        UNION
                        SELECT user_id FROM search_logs 
                        WHERE timestamp BETWEEN %s AND %s
                    ) u), 0) as unique_users,
                    COALESCE((SELECT COUNT(*) FROM expert_searches 
                    WHERE clicked = true), 0) as total_expert_clicks,
                    COALESCE((SELECT AVG(CASE WHEN success = true THEN 1.0 ELSE 0.0 END) 
                    FROM expert_interactions
                    WHERE created_at BETWEEN %s AND %s), 0.0) as adaptive_success_rate
                """, (start_date, end_date) * 5)
                
            metrics = cursor.fetchone()
            
            # Defensive formatting with default values
            def safe_format_metric(value, is_percentage=False):
                try:
                    if value is None:
                        return "0"
                    return f"{value:,.2%}" if is_percentage else f"{value:,}"
                except Exception as e:
                    self.logger.error(f"Metric formatting error: {e}")
                    return "N/A"
            
            with col1:
                st.metric("Total Interactions", safe_format_metric(metrics[0] + metrics[1]))
            with col2:
                st.metric("Chat Interactions", safe_format_metric(metrics[0]))
            with col3:
                st.metric("Unique Users", safe_format_metric(metrics[2]))
            with col4:
                st.metric("Expert Clicks", safe_format_metric(metrics[3]))
            with col5:
                st.metric("Adaptive Success Rate", safe_format_metric(metrics[4], is_percentage=True))
                    
        except Exception as e:
            self.logger.error(f"Error in overall metrics: {str(e)}")
            st.error("Could not retrieve overall metrics")
        finally:
            cursor.close()
    def export_analytics(self, metrics, analytics_type, export_format):
        """Export analytics data in the specified format."""
        try:
            if export_format == "CSV":
                st.download_button(
                    f"Download {analytics_type} Analytics (CSV)",
                    metrics.to_csv(index=False),
                    f"{analytics_type.lower()}_analytics.csv",
                    "text/csv"
                )
            elif export_format == "Excel":
                # Implement Excel export
                pass
            elif export_format == "PDF":
                # Implement PDF export
                pass
        except Exception as e:
            self.logger.error(f"Export error: {str(e)}")
            st.error("Failed to export analytics data. Please try again.")

    def display_footer(self):
        """Display the dashboard footer."""
        st.markdown(
            f"""
            <div style="
                position: fixed;
                bottom: 0;
                width: 100%;
                text-align: center;
                padding: 10px;
                background-color: {'#262730' if st.session_state.theme == 'dark' else '#FFFFFF'};
                color: {'#FFFFFF' if st.session_state.theme == 'dark' else '#000000'};
            ">
                APHRC Analytics Dashboard • {datetime.now().year}
            </div>
            """,
            unsafe_allow_html=True
        )

if __name__ == "__main__":
    dashboard = UnifiedAnalyticsDashboard()
    dashboard.main()