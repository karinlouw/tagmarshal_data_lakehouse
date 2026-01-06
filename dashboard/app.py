"""Tagmarshal data quality dashboard.

A Streamlit dashboard for presenting data quality insights to Tagmarshal,
supporting Phase 1 deliverables: completeness, consistency, and anomaly analysis.
"""

import streamlit as st

# Page imports
from modules import data_quality
from modules import course_configuration
from modules import course_topology
from modules import seasonality
from modules import device_and_pace

# Page configuration
st.set_page_config(
    page_title="Tagmarshal data quality",
    page_icon="⛳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for clean styling
st.markdown(
    """
<style>
    /* Clean, minimal styling */
    .stMetric {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e9ecef;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* Header styling */
    h1 {
        color: #2c3e50;
        font-weight: 600;
    }
    
    h2, h3 {
        color: #34495e;
        font-weight: 500;
    }
    
    /* Expander styling for SQL queries */
    .streamlit-expanderHeader {
        font-size: 0.85rem;
        color: #6c757d;
    }
    
    /* Status indicators */
    .status-good { color: #28a745; }
    .status-warning { color: #ffc107; }
    .status-critical { color: #dc3545; }
</style>
""",
    unsafe_allow_html=True,
)

# Page definitions
PAGES = {
    "Data quality": data_quality,
    "Course configuration": course_configuration,
    "Course topology": course_topology,
    "Seasonality": seasonality,
    "Device and pace analysis": device_and_pace,
}


def main():
    """Main application entry point."""

    # Sidebar
    with st.sidebar:
        st.markdown("# ⛳")
        st.title("Tagmarshal")
        st.caption("Data quality dashboard")

        st.markdown("---")

        # Page navigation
        st.subheader("Navigation")
        selected_page = st.selectbox(
            "Select page",
            options=list(PAGES.keys()),
            label_visibility="collapsed",
        )

        st.markdown("---")

        # About section
        st.markdown(
            """
        **Phase 1 deliverable**
        
        This dashboard presents data quality 
        metrics for the 5 pilot courses:
        - Bradshaw Farm GC
        - Indian Creek
        - American Falls
        - Erin Hills
        - Pinehurst 4
        """
        )

        st.markdown("---")
        st.caption("Built with Streamlit + Trino")

    # Main content area
    page_module = PAGES[selected_page]
    page_module.render()


if __name__ == "__main__":
    main()
