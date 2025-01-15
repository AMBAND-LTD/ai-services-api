from datetime import datetime, timedelta
from utils.theme import toggle_theme
import streamlit as st
def create_sidebar_filters():
    """
    Create an enhanced sidebar with interactive navigation buttons and dynamic filters.
    The sidebar now features distinct sections for navigation and contextual filters
    that update based on the selected analytics type.
    
    Returns:
        tuple: Contains the following elements:
            - start_date (datetime): The selected start date
            - end_date (datetime): The selected end date
            - analytics_type (str): The selected analytics type
            - filters (dict): All applicable filters for the selected analytics type
    """
    st.sidebar.title("Settings")
    
    # Theme toggle with appropriate icon
    theme_label = "🌙 Dark Mode" if st.session_state.theme == 'light' else "☀️ Light Mode"
    st.sidebar.button(theme_label, on_click=toggle_theme)
    
    # Initialize session state for selected analytics if not exists
    if 'selected_analytics' not in st.session_state:
        st.session_state.selected_analytics = "Overview"
    
    # Navigation Section
    st.sidebar.markdown("### Navigation")
    
    # Create a row of buttons for analytics types
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("📊 Overview", use_container_width=True):
            st.session_state.selected_analytics = "Overview"
        if st.button("💬 Chat", use_container_width=True):
            st.session_state.selected_analytics = "Chat"
        if st.button("🔍 Search", use_container_width=True):
            st.session_state.selected_analytics = "Search"
            
    with col2:
        if st.button("👥 Expert", use_container_width=True):
            st.session_state.selected_analytics = "Expert"
        if st.button("📚 Content", use_container_width=True):
            st.session_state.selected_analytics = "Content"
        if st.button("📈 Usage", use_container_width=True):
            st.session_state.selected_analytics = "Usage"
        if st.button("📈 Adaptive", use_container_width=True):
            st.session_state.selected_analytics = "Adaptive"
    
    # Common Filters Section
    st.sidebar.markdown("### Time Range")
    
    # Date range selector with validation
    start_date = st.sidebar.date_input(
        "Start Date",
        datetime.now() - timedelta(days=30)
    )
    end_date = st.sidebar.date_input(
        "End Date",
        datetime.now()
    )
    
    if end_date < start_date:
        st.sidebar.error("End date must be after start date")
        end_date = start_date + timedelta(days=1)
    
    # Initialize filters dictionary
    filters = {}
    
    # Dynamic Filters Section based on selected analytics type
    st.sidebar.markdown(f"### {st.session_state.selected_analytics} Filters")
    
    if st.session_state.selected_analytics == "Overview":
        filters['metric_type'] = st.sidebar.multiselect(
            "Metrics to Display",
            ["User Activity", "Performance", "Engagement", "Success Rate"],
            default=["User Activity", "Performance"]
        )
        filters['comparison'] = st.sidebar.checkbox("Show Period Comparison")
        
    elif st.session_state.selected_analytics == "Chat":
        filters['interaction_type'] = st.sidebar.multiselect(
            "Interaction Types",
            ["Questions", "Responses", "Expert Matches", "Feedback"],
            default=["Questions", "Responses"]
        )
        filters['sentiment_analysis'] = st.sidebar.checkbox("Include Sentiment Analysis")
        filters['response_time_threshold'] = st.sidebar.slider(
            "Response Time Threshold (seconds)",
            0, 60, 30
        )
        
    elif st.session_state.selected_analytics == "Search":
        filters['search_type'] = st.sidebar.multiselect(
            "Search Types",
            ["Expert Search", "Content Search", "Domain Search"],
            default=["Expert Search"]
        )
        filters['min_results'] = st.sidebar.number_input(
            "Minimum Results",
            min_value=0,
            value=1
        )
        filters['include_failed'] = st.sidebar.checkbox("Include Failed Searches")
        
    elif st.session_state.selected_analytics == "Expert":
        filters['min_similarity'] = st.sidebar.slider(
            "Minimum Similarity Score",
            0.0, 1.0, 0.5
        )
        filters['expert_count'] = st.sidebar.slider(
            "Number of Experts",
            5, 50, 20
        )
        filters['domains'] = st.sidebar.multiselect(
            "Expert Domains",
            ["Health", "Population", "Policy", "Research Methods"],
            default=["Health"]
        )
        filters['show_network'] = st.sidebar.checkbox("Show Expert Network")
        
    elif st.session_state.selected_analytics == "Content":
        filters['content_type'] = st.sidebar.multiselect(
            "Content Types",
            ["Research Papers", "Reports", "Presentations", "Datasets"],
            default=["Research Papers"]
        )
        filters['collections'] = st.sidebar.multiselect(
            "Collections",
            ["Public Health", "Population Studies", "Policy Research"],
            default=["Public Health"]
        )
        filters['min_views'] = st.sidebar.number_input(
            "Minimum Views",
            min_value=0,
            value=10
        )
        
    elif st.session_state.selected_analytics == "Usage":
        filters['user_type'] = st.sidebar.multiselect(
            "User Types",
            ["Researchers", "Students", "Staff", "External"],
            default=["Researchers"]
        )
        filters['activity_type'] = st.sidebar.multiselect(
            "Activity Types",
            ["Searches", "Downloads", "Expert Consultations", "Chat Interactions"],
            default=["Searches", "Downloads"]
        )
        filters['show_conversion'] = st.sidebar.checkbox("Show Conversion Metrics")
    
    # Export Options (common across all types)
    if st.sidebar.checkbox("Enable Export"):
        filters['export_format'] = st.sidebar.selectbox(
            "Export Format",
            ["CSV", "Excel", "PDF"]
        )
    
    return start_date, end_date, st.session_state.selected_analytics, filters