"""Centralised color scheme for the dashboard.

All visualisations should use colors from this module to ensure consistency.
"""

# Primary status colors
COLOR_GOOD = "#28a745"  # Green - good/success
COLOR_WARNING = "#ffc107"  # Amber/yellow - warning/moderate
COLOR_CRITICAL = "#dc3545"  # Red - critical/bad

# Secondary colors
COLOR_INFO = "#17a2b8"  # Teal - informational
COLOR_PRIMARY = "#007bff"  # Blue - primary actions
COLOR_SECONDARY = "#6c757d"  # Grey - secondary

# Neutral colors
COLOR_LIGHT = "#f8f9fa"  # Light grey background
COLOR_BORDER = "#e9ecef"  # Border grey
COLOR_TEXT = "#2c3e50"  # Dark text
COLOR_TEXT_SECONDARY = "#6c757d"  # Secondary text

# Color scales for continuous data
COLOR_SCALE_QUALITY = [
    [0, COLOR_CRITICAL],  # Red at 0
    [0.5, COLOR_WARNING],  # Amber at 50%
    [1, COLOR_GOOD],  # Green at 100%
]

COLOR_SCALE_REVERSE = [
    [0, COLOR_GOOD],  # Green at 0
    [0.5, COLOR_WARNING],  # Amber at 50%
    [1, COLOR_CRITICAL],  # Red at 100%
]

# Discrete color mappings
COLOR_MAP_STATUS = {
    "good": COLOR_GOOD,
    "warning": COLOR_WARNING,
    "critical": COLOR_CRITICAL,
}

COLOR_MAP_COMPLETE = {
    "Complete": COLOR_GOOD,
    "Incomplete": COLOR_CRITICAL,
}

COLOR_MAP_DAY_TYPE = {
    "Weekend": COLOR_GOOD,
    "Weekday": COLOR_INFO,
}

COLOR_MAP_ROUND_TYPE = {
    "9-hole": COLOR_PRIMARY,  # Blue for 9-hole
    "Full round": COLOR_GOOD,  # Green for full round
}

# Qualitative color sequence (for categorical data)
COLOR_SEQUENCE = [
    COLOR_PRIMARY,
    COLOR_INFO,
    COLOR_GOOD,
    COLOR_WARNING,
    COLOR_SECONDARY,
    "#6f42c1",  # Purple
    "#fd7e14",  # Orange
]
