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
from modules import data_explorer
from utils.database import execute_query
from utils import queries

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
    "Data explorer": data_explorer,
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

        # Course information section (only for Course configuration page)
        if selected_page == "Course configuration":
            try:
                config_df = execute_query(queries.COURSE_CONFIGURATION)
                
                # Course display name mapping
                course_display_names = {
                    "americanfalls": "American Falls",
                    "bradshawfarmgc": "Bradshaw Farm GC",
                    "erinhills": "Erin Hills",
                    "indiancreek": "Indian Creek",
                    "pinehurst4": "Pinehurst 4",
                }
                
                # Create course summary
                course_summary = []
                for _, row in config_df.iterrows():
                    course_id = row["course_id"]
                    course_type = row["likely_course_type"]
                    display_name = course_display_names.get(course_id, course_id)
                    
                    # Extract hole count from course type
                    if course_type == "27-hole":
                        hole_count = "27"
                    elif course_type == "18-hole":
                        hole_count = "18"
                    else:
                        hole_count = "9"
                    
                    course_summary.append({
                        "name": display_name,
                        "hole_count": hole_count,
                        "course_id": course_id,
                    })
                
                # Sort alphabetically by display name
                course_summary.sort(key=lambda x: x["name"])
                
                # Display course information
                st.markdown("**Course summary**")
                for course in course_summary:
                    st.markdown(f"- **{course['name']}** - {course['hole_count']}-hole")
                    
            except Exception:
                # If query fails, show fallback
                st.markdown("**Course summary**")
                st.caption("Unable to load course data")
        else:
            # Default about section for other pages
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
